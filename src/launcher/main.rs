use std::collections::BTreeMap;
use std::ffi::{CString, OsString};
use std::fs;
use std::fs::OpenOptions;
use std::io::{self, BufWriter, Read, Write};
use std::os::unix::ffi::OsStringExt;
use std::os::unix::process::CommandExt;
use std::path::{Component, Path, PathBuf};
use std::process::{Command, ExitCode, Stdio};
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::sync::mpsc;
use std::time::{SystemTime, UNIX_EPOCH};

use clap::{Parser, Subcommand};
use chrono::Local;
use serde::Serialize;
use signal_hook::iterator::Signals;
use users::{get_group_by_name, get_user_by_name};

/// `launcher` provides cgroup v2 based process launching utilities.
/// Primary entrypoint is `launcher start`.
#[derive(Debug, Parser)]
#[command(
    name = "launcher",
    about = "Launch a command inside a Linux cgroup v2 (group path relative to cgroot).",
    disable_help_subcommand = true
)]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Debug, Subcommand)]
enum Cmd {
    /// Launch a program, wait for it to exit, and exit with the same exit code.
    Start(StartCmd),

    /// Print PIDs currently in the cgroup and exit.
    Pids(CgroupCmd),

    /// Print ps-like process info for PIDs currently in the cgroup and exit.
    Ps(CgroupCmd),

    /// Send a signal to all PIDs currently in the cgroup and exit.
    Kill(KillCmd),

    /// Kill everything in the cgroup using cgroup v2 `cgroup.kill` and exit.
    KillAll(CgroupCmd),

    /// Wait until the cgroup becomes empty and exit.
    Wait(CgroupCmd),
}

#[derive(Debug, Parser, Clone)]
struct CgroupCmd {
    /// Cgroup v2 mount root (default: /sys/fs/cgroup).
    #[arg(long, default_value = "/sys/fs/cgroup")]
    cgroot: PathBuf,

    /// Cgroup relative path from cgroot. Example: app/process1
    #[arg(long = "cgroup")]
    cgroup: String,
}

#[derive(Debug, Parser, Clone)]
struct KillCmd {
    #[command(flatten)]
    cg: CgroupCmd,

    /// Signal number to send (default: 15 / SIGTERM)
    #[arg(long, default_value_t = 15)]
    sig: i32,
}

#[derive(Debug, Parser, Clone)]
#[command(trailing_var_arg = true)]
struct StartCmd {
    /// Resource specification string. Example: "cpu=100m;memory=1G;io-weight=400;read_iops=8:0:100;memory_swap=0G;"
    #[arg(long)]
    resource: Option<String>,

    /// Extra environment variables (KEY=VALUE). Can be repeated.
    /// VALUE can be bare, or `base64://...`, or `hex://...`.
    #[arg(long = "env")]
    env: Vec<String>,

    /// Redirect stdout to this file. Enables rotation for stdout logs.
    #[arg(long)]
    stdout: Option<PathBuf>,

    /// Redirect stderr to this file. Enables rotation for stderr logs.
    #[arg(long)]
    stderr: Option<PathBuf>,

    /// Max size per log file before rotation (default: 100m).
    #[arg(long = "max-log-size", default_value = "100m")]
    max_log_size: String,

    /// Number of rotated files to keep (default: 10).
    #[arg(long = "max-log-files", default_value_t = 10)]
    max_log_files: u32,

    /// Keep the most recent N rotated files uncompressed.
    ///
    /// Default is 0 => compress all rotated files.
    #[arg(long = "no-compress-logs", default_value_t = 0)]
    no_compress_logs: u32,

    /// Disable argv[0] decoration (default is to set argv[0] to "<binary>[<group>]").
    #[arg(long = "no-decoration", action = clap::ArgAction::SetTrue)]
    no_decoration: bool,

    /// Cgroup v2 mount root (default: /sys/fs/cgroup).
    #[arg(long, default_value = "/sys/fs/cgroup")]
    cgroot: PathBuf,

    /// Cgroup relative path from cgroot. Example: app/process1
    #[arg(long = "cgroup")]
    cgroup: String,

    /// Launch the process in the cgroup specified by --cgroup (do not append /run).
    ///
    /// By default, launcher applies resources to the target cgroup but runs the process in
    /// a leaf child cgroup `<target>/run` to avoid cgroup v2 "no internal processes" constraints.
    #[arg(long = "cgroup-exact", action = clap::ArgAction::SetTrue)]
    cgroup_exact: bool,

    /// Run the command as this user (requires privilege).
    #[arg(long)]
    user: Option<String>,

    /// Run the command with this primary UNIX group (requires privilege).
    #[arg(long = "group")]
    group: Option<String>,

    /// Command to run (everything after `--`).
    #[arg(required = true)]
    command: Vec<OsString>,
}

#[derive(Debug, Default, Clone)]
struct ResourceSpec {
    cpu: Option<String>,
    memory: Option<String>,
    memory_swap: Option<String>,
    cpuset: Option<String>,
    io_weight: Option<u32>,
    // device -> io.max fields
    io_max: BTreeMap<(u32, u32), IoMaxSpec>,
}

#[derive(Debug, Default, Clone)]
struct IoMaxSpec {
    rbps: Option<u64>,
    wbps: Option<u64>,
    riops: Option<u64>,
    wiops: Option<u64>,
}

fn main() -> ExitCode {
    match real_main() {
        Ok(code) => ExitCode::from((code.clamp(0, 255)) as u8),
        Err(e) => {
            eprintln!("launcher: {:#}", e);
            ExitCode::from(1)
        }
    }
}

fn real_main() -> anyhow::Result<i32> {
    let cli = Cli::parse();
    match cli.cmd {
        Cmd::Start(s) => start(s),
        Cmd::Pids(cg) => {
            let cg_path = resolve_cgroup_path(&cg.cgroot, &cg.cgroup)?;
            let pids = read_cgroup_pids(&cg_path)?;
            for pid in pids {
                println!("{pid}");
            }
            Ok(0)
        }
        Cmd::Ps(cg) => {
            let cg_path = resolve_cgroup_path(&cg.cgroot, &cg.cgroup)?;
            let pids = read_cgroup_pids(&cg_path)?;
            let printed = try_print_ps_for_pids(&pids)?;
            if !printed {
                print_pidex_fallback(&pids)?;
            }
            Ok(0)
        }
        Cmd::Kill(k) => {
            let cg_path = resolve_cgroup_path(&k.cg.cgroot, &k.cg.cgroup)?;
            let sig = nix::sys::signal::Signal::try_from(k.sig)
                .map_err(|_| anyhow::anyhow!("Invalid --sig {} (must be a valid signal number)", k.sig))?;
            let pids = read_cgroup_pids(&cg_path)?;
            kill_pids(&pids, sig)?;
            Ok(0)
        }
        Cmd::KillAll(cg) => {
            let cg_path = resolve_cgroup_path(&cg.cgroot, &cg.cgroup)?;
            kill_all_cgroup(&cg_path)?;
            Ok(0)
        }
        Cmd::Wait(cg) => {
            let cg_path = resolve_cgroup_path(&cg.cgroot, &cg.cgroup)?;
            wait_for_cgroup_empty(&cg_path)?;
            Ok(0)
        }
    }
}

fn start(s: StartCmd) -> anyhow::Result<i32> {
    // Determine the actual run cgroup to avoid cgroup v2 "no internal processes":
    // - default: run in `<target>/run`
    // - exception 1: if last path token is already "run", use target as-is
    // - exception 2: if --cgroup-exact is set, use target as-is
    // Parse and apply resource spec.
    let res = parse_resource_spec(s.resource.as_deref().unwrap_or(""))?;

    // Compute paths.
    let target_cg_path = resolve_cgroup_path(&s.cgroot, &s.cgroup)?;
    let run_cg_path = pick_run_cgroup_path(&target_cg_path, &s.cgroup, s.cgroup_exact)?;

    // Required controllers: always pids, plus controllers used by requested features.
    let required = required_controllers(&res);

    // Setup hierarchy and subtree_control:
    // - ensure each ancestor of the run cgroup has subtree_control = required controllers
    // - ensure leaf (run cgroup OR exact group) has EMPTY subtree_control (leaf must be able to host PIDs)
    //
    // If any setup fails, refuse to start.
    ensure_cgroup_hierarchy(&s.cgroot, &run_cg_path, &required)?;

    // Apply resources to the target cgroup (even if we run in <target>/run).
    apply_resource_spec(&target_cg_path, &res)?;

    // Resolve uid/gid information (if requested).
    let (uid_opt, gid_opt, c_user_opt) = resolve_user_group(s.user.as_deref(), s.group.as_deref())?;

    // Prepare command
    let mut cmd = Command::new(&s.command[0]);
    if s.command.len() > 1 {
        cmd.args(&s.command[1..]);
    }

    if !s.no_decoration {
        let bin_name = Path::new(&s.command[0])
            .file_name()
            .and_then(|x| x.to_str())
            .unwrap_or("cmd");
        let decorated = format!("{bin_name}[{}]", s.cgroup);
        cmd.arg0(decorated);
    }

    // Apply env
    for e in &s.env {
        let (k, v) = parse_env_kv(e)?;
        let v = resolve_env_value_spec(v)?;
        cmd.env(k, v);
    }

    // Set up stdout/stderr piping + log rotation if paths provided.
    let log_max_bytes = parse_memory_to_bytes(&s.max_log_size)?
        .ok_or_else(|| anyhow::anyhow!("Invalid --max-log-size '{}': 'max' is not allowed", s.max_log_size))?;
    let no_compress_recent: u32 = s.no_compress_logs as u32;

    let mut stdout_handle: Option<JoinHandle<()>> = None;
    let mut stderr_handle: Option<JoinHandle<()>> = None;

    if s.stdout.is_some() {
        cmd.stdout(Stdio::piped());
    }
    if s.stderr.is_some() {
        cmd.stderr(Stdio::piped());
    }

    // Join the run cgroup in the parent; the child inherits it.
    join_cgroup(&run_cg_path)?;

    unsafe {
        cmd.pre_exec(move || {
            // Run as requested uid/gid in the child only.
            if let Some(gid) = gid_opt {
                if libc::setgid(gid) != 0 {
                    return Err(io::Error::last_os_error());
                }
            }

            if let (Some(uid), Some(gid), Some(c_user)) = (uid_opt, gid_opt, &c_user_opt) {
                // initgroups only if we have a user name.
                if libc::initgroups(c_user.as_ptr(), gid) != 0 {
                    return Err(io::Error::last_os_error());
                }
                if libc::setuid(uid) != 0 {
                    return Err(io::Error::last_os_error());
                }
            } else if let Some(uid) = uid_opt {
                // No user name (shouldn't happen), but if uid is set, still attempt setuid.
                if libc::setuid(uid) != 0 {
                    return Err(io::Error::last_os_error());
                }
            }

            Ok(())
        });
    }

    #[derive(Debug, Serialize)]
    struct LauncherJsonOut {
        launch_ok: bool,
        launch_time: String,
        /// User-requested key name (typo preserved for compatibility):
        /// "launch_tims_ms: the unix time"
        launch_tims_ms: i64,
        launch_error_code: i32,
        launch_error_os_code: i32,
        pid: String,
        exit_time: Option<String>,
        exit_time_ms: Option<i64>,
        duration_ms: Option<i64>,
        exit_code: Option<i32>,
    }

    // Trap termination signals so we can forward them to the child and still print JSON on exit.
    // Note: SIGKILL cannot be trapped.
    let child_pid_ref = Arc::new(AtomicI32::new(0));
    let mut signals = Signals::new([
        libc::SIGTERM,
        libc::SIGINT,
        libc::SIGHUP,
        libc::SIGQUIT,
    ])
    .map_err(|e| anyhow::anyhow!("Failed to install signal handlers: {}", e))?;
    let signals_handle = signals.handle();
    let forward_pid_ref = Arc::clone(&child_pid_ref);
    let forwarder = std::thread::spawn(move || {
        for sig in signals.forever() {
            let pid = forward_pid_ref.load(Ordering::SeqCst);
            if pid <= 0 {
                continue;
            }
            if let Ok(nix_sig) = nix::sys::signal::Signal::try_from(sig) {
                let _ = nix::sys::signal::kill(nix::unistd::Pid::from_raw(pid), nix_sig);
            }
        }
    });

    let start_time = Local::now();
    let mut child = match cmd.spawn() {
        Ok(c) => c,
        Err(e) => {
            let out = LauncherJsonOut {
                launch_ok: false,
                launch_time: start_time.format("%Y-%m-%d %H:%M:%S%.3f").to_string(),
                launch_tims_ms: start_time.timestamp_millis(),
                launch_error_code: 1,
                launch_error_os_code: e.raw_os_error().unwrap_or(-1),
                pid: String::new(),
                exit_time: None,
                exit_time_ms: None,
                duration_ms: None,
                exit_code: None,
            };
            println!("{}", serde_json::to_string(&out).unwrap_or_else(|_| "{}".to_string()));
            signals_handle.close();
            let _ = forwarder.join();
            return Ok(1);
        }
    };

    let pid_s = child.id().to_string();
    child_pid_ref.store(child.id() as i32, Ordering::SeqCst);

    if let Some(p) = s.stdout.clone() {
        let out = child.stdout.take().expect("stdout piped but missing");
        stdout_handle = Some(spawn_log_thread(
            out,
            p,
            log_max_bytes,
            s.max_log_files,
            no_compress_recent,
        ));
    }
    if let Some(p) = s.stderr.clone() {
        let out = child.stderr.take().expect("stderr piped but missing");
        stderr_handle = Some(spawn_log_thread(
            out,
            p,
            log_max_bytes,
            s.max_log_files,
            no_compress_recent,
        ));
    }

    let status = match child.wait() {
        Ok(s) => s,
        Err(e) => {
            // Waiting failed (rare). Emit a JSON with exit_code=1, and log details to stderr.
            eprintln!(
                "launcher: failed to wait for child: {} (errno={:?})",
                e,
                e.raw_os_error()
            );
            let end_time = Local::now();
            let out = LauncherJsonOut {
                launch_ok: true,
                launch_time: start_time.format("%Y-%m-%d %H:%M:%S%.3f").to_string(),
                launch_tims_ms: start_time.timestamp_millis(),
                launch_error_code: 0,
                launch_error_os_code: 0,
                pid: pid_s,
                exit_time: Some(end_time.format("%Y-%m-%d %H:%M:%S%.3f").to_string()),
                exit_time_ms: Some(end_time.timestamp_millis()),
                duration_ms: Some(end_time.timestamp_millis() - start_time.timestamp_millis()),
                exit_code: Some(1),
            };
            println!("{}", serde_json::to_string(&out).unwrap_or_else(|_| "{}".to_string()));
            signals_handle.close();
            let _ = forwarder.join();
            return Ok(1);
        }
    };

    // Wait for log threads to drain/exit.
    if let Some(h) = stdout_handle {
        let _ = h.join();
    }
    if let Some(h) = stderr_handle {
        let _ = h.join();
    }

    // Stop signal thread; we're done and about to print final JSON + exit.
    signals_handle.close();
    let _ = forwarder.join();

    let end_time = Local::now();
    let end_ms = end_time.timestamp_millis();
    let start_ms = start_time.timestamp_millis();

    let exit_code: i32 = if let Some(code) = status.code() {
        code
    } else {
        #[cfg(unix)]
        {
            use std::os::unix::process::ExitStatusExt;
            if let Some(sig) = status.signal() {
                128 + sig
            } else {
                1
            }
        }
        #[cfg(not(unix))]
        {
            1
        }
    };

    let out = LauncherJsonOut {
        launch_ok: true,
        launch_time: start_time.format("%Y-%m-%d %H:%M:%S%.3f").to_string(),
        launch_tims_ms: start_ms,
        launch_error_code: 0,
        launch_error_os_code: 0,
        pid: pid_s,
        exit_time: Some(end_time.format("%Y-%m-%d %H:%M:%S%.3f").to_string()),
        exit_time_ms: Some(end_ms),
        duration_ms: Some(end_ms - start_ms),
        exit_code: Some(exit_code),
    };
    println!("{}", serde_json::to_string(&out).unwrap_or_else(|_| "{}".to_string()));

    Ok(exit_code)
}

fn resolve_cgroup_path(cgroot: &Path, group: &str) -> anyhow::Result<PathBuf> {
    if group.is_empty() {
        anyhow::bail!("--group cannot be empty");
    }
    let p = Path::new(group);
    for c in p.components() {
        match c {
            Component::Normal(_) => {}
            Component::CurDir => {}
            Component::ParentDir => anyhow::bail!("Invalid --group '{}': must not contain '..'", group),
            Component::RootDir | Component::Prefix(_) => {
                anyhow::bail!("Invalid --group '{}': must be a relative path", group)
            }
        }
    }
    Ok(cgroot.join(p))
}

fn pick_run_cgroup_path(target_cg_path: &Path, cgroup_str: &str, exact: bool) -> anyhow::Result<PathBuf> {
    if exact {
        return Ok(target_cg_path.to_path_buf());
    }
    // If the user's cgroup already ends with "run", treat it as exact.
    let last = Path::new(cgroup_str).components().last();
    if let Some(std::path::Component::Normal(os)) = last {
        if os == "run" {
            return Ok(target_cg_path.to_path_buf());
        }
    }
    Ok(target_cg_path.join("run"))
}

fn write_cgroup_file(cg_path: &Path, file: &str, content: &str) -> anyhow::Result<()> {
    let p = cg_path.join(file);
    fs::write(&p, content).map_err(|e| {
        anyhow::anyhow!(
            "Failed to write {:?}: {} (are controllers enabled in parent? do you have permission?)",
            p,
            e
        )
    })?;
    Ok(())
}

fn apply_resource_spec(cg_path: &Path, spec: &ResourceSpec) -> anyhow::Result<()> {
    // CPU
    if let Some(cpu_s) = spec.cpu.as_deref() {
        let period_us: u64 = 100_000;
        match parse_cpu_to_quota_us(cpu_s, period_us)? {
            None => write_cgroup_file(cg_path, "cpu.max", "max\n")?,
            Some(quota) => write_cgroup_file(cg_path, "cpu.max", &format!("{quota} {period_us}\n"))?,
        }
    }

    // Memory
    if let Some(mem_s) = spec.memory.as_deref() {
        match parse_memory_to_bytes(mem_s)? {
            None => write_cgroup_file(cg_path, "memory.max", "max\n")?,
            Some(bytes) => write_cgroup_file(cg_path, "memory.max", &format!("{bytes}\n"))?,
        }
    }
    if let Some(swap_s) = spec.memory_swap.as_deref() {
        match parse_memory_to_bytes(swap_s)? {
            None => write_cgroup_file(cg_path, "memory.swap.max", "max\n")?,
            Some(bytes) => write_cgroup_file(cg_path, "memory.swap.max", &format!("{bytes}\n"))?,
        }
    }

    // Cpuset: bind to specific CPUs (requires cpuset controller).
    if let Some(cpus) = spec.cpuset.as_deref() {
        ensure_cpuset_mems_configured_from_parent(cg_path)?;
        write_cgroup_file(cg_path, "cpuset.cpus", &format!("{cpus}\n"))?;
    }

    // IO weight
    if let Some(w) = spec.io_weight {
        if !(1..=10_000).contains(&w) {
            anyhow::bail!("io-weight must be in range 1..=10000");
        }
        write_cgroup_file(cg_path, "io.weight", &format!("{w}\n"))?;
    }

    // IO max (throttling)
    if !spec.io_max.is_empty() {
        let mut out = String::new();
        for ((maj, min), io) in &spec.io_max {
            let mut parts = Vec::new();
            if let Some(v) = io.rbps {
                parts.push(format!("rbps={v}"));
            }
            if let Some(v) = io.wbps {
                parts.push(format!("wbps={v}"));
            }
            if let Some(v) = io.riops {
                parts.push(format!("riops={v}"));
            }
            if let Some(v) = io.wiops {
                parts.push(format!("wiops={v}"));
            }
            if parts.is_empty() {
                continue;
            }
            out.push_str(&format!("{maj}:{min} {}\n", parts.join(" ")));
        }
        if !out.is_empty() {
            write_cgroup_file(cg_path, "io.max", &out)?;
        }
    }

    Ok(())
}

fn ensure_cpuset_mems_configured_from_parent(cg_path: &Path) -> anyhow::Result<()> {
    // cpuset requires cpuset.mems to be configured. We mirror the parent's effective mems.
    let parent = cg_path
        .parent()
        .ok_or_else(|| anyhow::anyhow!("No parent cgroup for {:?}", cg_path))?;

    let parent_mems = fs::read_to_string(parent.join("cpuset.mems.effective"))
        .or_else(|_| fs::read_to_string(parent.join("cpuset.mems")))
        .map_err(|e| anyhow::anyhow!("Failed to read parent cpuset mems for {:?}: {}", parent, e))?;
    let parent_mems = parent_mems.trim();
    if parent_mems.is_empty() {
        anyhow::bail!("Parent cpuset.mems is empty for {:?}; cannot configure cpuset", parent);
    }

    // Some kernels require cpuset.mems to be written before cpuset.cpus.
    write_cgroup_file(cg_path, "cpuset.mems", &format!("{parent_mems}\n"))?;
    Ok(())
}

fn required_controllers(spec: &ResourceSpec) -> Vec<String> {
    let mut out = Vec::new();
    // Always required.
    out.push("pids".to_string());
    if spec.cpu.is_some() {
        out.push("cpu".to_string());
    }
    if spec.memory.is_some() || spec.memory_swap.is_some() {
        out.push("memory".to_string());
    }
    if spec.io_weight.is_some() || !spec.io_max.is_empty() {
        out.push("io".to_string());
    }
    if spec.cpuset.is_some() {
        out.push("cpuset".to_string());
    }
    out.sort();
    out.dedup();
    out
}

fn ensure_cgroup_hierarchy(
    cgroot: &Path,
    leaf_cg: &Path,
    required: &[String],
) -> anyhow::Result<()> {
    // Build the chain from cgroot -> ... -> leaf.
    let rel = leaf_cg
        .strip_prefix(cgroot)
        .map_err(|_| anyhow::anyhow!("cgroup path {:?} is not under cgroot {:?}", leaf_cg, cgroot))?;

    let mut cur = cgroot.to_path_buf();
    // Do NOT attempt to modify cgroot (system root). It must already exist and be configured by the system/admin.
    // We only validate that it is configured to allow the requested controllers for its children.
    validate_root_subtree_control(cgroot, required)?;

    for comp in rel.components() {
        match comp {
            Component::Normal(name) => {
                // Ensure the current parent (including cgroot) can provide the required controllers to its child.
                // For cgroot we only validate (done above). For non-root parents we enforce below when we visit them.
                if cur != cgroot {
                    // Parent must expose required controllers for children; if this fails due to "no internal processes"
                    // or permissions, we must refuse to start.
                    set_subtree_control_at_least(&cur, required)?;
                }

                let next = cur.join(name);
                let created = if !next.exists() {
                    fs::create_dir(&next).map_err(|e| {
                        anyhow::anyhow!(
                            "Failed to create cgroup directory {:?}: {} (are you root? is cgroup v2 mounted?)",
                            next,
                            e
                        )
                    })?;
                    true
                } else {
                    false
                };

                cur = next;
                // For every cgroup along the path:
                // - non-leaf: must have subtree_control set so its child can get controllers
                // - leaf: must have EMPTY subtree_control so it can host PIDs (no-internal-processes rule)
                if cur == leaf_cg {
                    if created {
                        // We created the leaf, so it's safe to configure it to what we need (empty subtree_control).
                        set_subtree_control_empty(&cur)?;
                    } else {
                        // Do not reduce scope of existing cgroups (intrusive). If leaf already exists and has
                        // subtree_control enabled, refuse to start.
                        ensure_subtree_control_empty(&cur)?;
                    }
                } else {
                    if created {
                        set_subtree_control_exact(&cur, required)?;
                    } else {
                        set_subtree_control_at_least(&cur, required)?;
                    }
                }
            }
            Component::CurDir => {}
            Component::ParentDir => anyhow::bail!("Invalid cgroup path {:?}: contains '..'", rel),
            Component::RootDir | Component::Prefix(_) => anyhow::bail!("Invalid cgroup path {:?}: not relative", rel),
        }
    }

    Ok(())
}

fn validate_root_subtree_control(cgroot: &Path, required: &[String]) -> anyhow::Result<()> {
    let st = fs::read_to_string(cgroot.join("cgroup.subtree_control")).map_err(|e| {
        anyhow::anyhow!(
            "Failed to read cgroot subtree control {:?}: {}",
            cgroot.join("cgroup.subtree_control"),
            e
        )
    })?;
    let enabled: std::collections::HashSet<&str> = st.split_whitespace().collect();
    let mut missing = Vec::new();
    for r in required {
        if !enabled.contains(r.as_str()) {
            missing.push(r.clone());
        }
    }
    if !missing.is_empty() {
        anyhow::bail!(
            "cgroot {:?} is missing required controllers in cgroup.subtree_control: {}. Configure cgroot to enable these controllers before starting.",
            cgroot,
            missing.join(",")
        );
    }
    Ok(())
}

fn set_subtree_control_exact(cg_path: &Path, desired: &[String]) -> anyhow::Result<()> {
    // Validate availability.
    let available = fs::read_to_string(cg_path.join("cgroup.controllers"))
        .map_err(|e| anyhow::anyhow!("Failed to read {:?}: {}", cg_path.join("cgroup.controllers"), e))?;
    let avail_set: std::collections::HashSet<&str> = available.split_whitespace().collect();
    for d in desired {
        if !avail_set.contains(d.as_str()) {
            anyhow::bail!(
                "Controller '{}' not available in {:?} (available: {})",
                d,
                cg_path,
                available.trim()
            );
        }
    }

    let current = fs::read_to_string(cg_path.join("cgroup.subtree_control")).unwrap_or_default();
    let current_set: std::collections::HashSet<&str> = current.split_whitespace().collect();

    let mut disable = Vec::new();
    for c in current_set {
        if !desired.iter().any(|d| d == c) {
            disable.push(format!("-{c}"));
        }
    }
    let mut enable = Vec::new();
    for d in desired {
        if !current.split_whitespace().any(|c| c == d) {
            enable.push(format!("+{d}"));
        }
    }

    let mut tokens = Vec::new();
    tokens.extend(disable);
    tokens.extend(enable);
    if tokens.is_empty() {
        return Ok(());
    }
    fs::write(cg_path.join("cgroup.subtree_control"), format!("{}\n", tokens.join(" "))).map_err(|e| {
        anyhow::anyhow!(
            "Failed to set subtree_control for {:?}: {} (need permission / delegation; cgroup must have no processes)",
            cg_path,
            e
        )
    })?;
    Ok(())
}

fn set_subtree_control_at_least(cg_path: &Path, desired: &[String]) -> anyhow::Result<()> {
    // Never disable existing controllers here; only ensure required ones are enabled.
    // This avoids intrusive scope reduction for cgroups created/managed by others.
    let available = fs::read_to_string(cg_path.join("cgroup.controllers"))
        .map_err(|e| anyhow::anyhow!("Failed to read {:?}: {}", cg_path.join("cgroup.controllers"), e))?;
    let avail_set: std::collections::HashSet<&str> = available.split_whitespace().collect();
    for d in desired {
        if !avail_set.contains(d.as_str()) {
            anyhow::bail!(
                "Controller '{}' not available in {:?} (available: {})",
                d,
                cg_path,
                available.trim()
            );
        }
    }

    let current = fs::read_to_string(cg_path.join("cgroup.subtree_control")).unwrap_or_default();
    let current_set: std::collections::HashSet<&str> = current.split_whitespace().collect();

    let mut enable = Vec::new();
    for d in desired {
        if !current_set.contains(d.as_str()) {
            enable.push(format!("+{d}"));
        }
    }

    if enable.is_empty() {
        return Ok(());
    }

    fs::write(cg_path.join("cgroup.subtree_control"), format!("{}\n", enable.join(" "))).map_err(|e| {
        anyhow::anyhow!(
            "Failed to extend subtree_control for {:?}: {} (need permission / delegation; cgroup must have no processes)",
            cg_path,
            e
        )
    })?;
    Ok(())
}

fn set_subtree_control_empty(cg_path: &Path) -> anyhow::Result<()> {
    let current = fs::read_to_string(cg_path.join("cgroup.subtree_control")).unwrap_or_default();
    let mut disable = Vec::new();
    for c in current.split_whitespace() {
        if c.trim().is_empty() {
            continue;
        }
        disable.push(format!("-{c}"));
    }
    if disable.is_empty() {
        return Ok(());
    }
    fs::write(cg_path.join("cgroup.subtree_control"), format!("{}\n", disable.join(" "))).map_err(|e| {
        anyhow::anyhow!(
            "Failed to clear subtree_control for {:?}: {} (need permission / delegation; cgroup must have no processes)",
            cg_path,
            e
        )
    })?;
    Ok(())
}

fn ensure_subtree_control_empty(cg_path: &Path) -> anyhow::Result<()> {
    let current = fs::read_to_string(cg_path.join("cgroup.subtree_control")).unwrap_or_default();
    if current.split_whitespace().any(|s| !s.trim().is_empty()) {
        anyhow::bail!(
            "Refusing to start: leaf cgroup {:?} already exists with non-empty cgroup.subtree_control ('{}'). Leaf must have empty subtree_control to host processes; choose a different cgroup or remove subtree_control out-of-band.",
            cg_path,
            current.trim()
        );
    }
    Ok(())
}

fn parse_resource_spec(spec: &str) -> anyhow::Result<ResourceSpec> {
    let mut out = ResourceSpec::default();
    for token in spec.split(';') {
        let t = token.trim();
        if t.is_empty() {
            continue;
        }
        let (k, v) = t
            .split_once('=')
            .ok_or_else(|| anyhow::anyhow!("Invalid resource token '{}': expected key=value", t))?;
        let key = k.trim();
        let val = v.trim();
        if key.is_empty() {
            anyhow::bail!("Invalid resource token '{}': empty key", t);
        }
        match key {
            "cpu" => {
                if out.cpu.is_some() {
                    anyhow::bail!("Duplicate entry for resource key 'cpu'");
                }
                out.cpu = Some(val.to_string());
            }
            "memory" => {
                if out.memory.is_some() {
                    anyhow::bail!("Duplicate entry for resource key 'memory'");
                }
                out.memory = Some(val.to_string());
            }
            "memory_swap" | "memory-swap" => {
                if out.memory_swap.is_some() {
                    anyhow::bail!("Duplicate entry for resource key 'memory_swap'");
                }
                out.memory_swap = Some(val.to_string());
            }
            "cpuset" => {
                if out.cpuset.is_some() {
                    anyhow::bail!("Duplicate entry for resource key 'cpuset'");
                }
                out.cpuset = Some(val.to_string());
            }
            "io-weight" => {
                if out.io_weight.is_some() {
                    anyhow::bail!("Duplicate entry for resource key 'io-weight'");
                }
                let w: u32 = val.parse().map_err(|_| anyhow::anyhow!("Invalid io-weight '{}'", val))?;
                out.io_weight = Some(w);
            }
            "read_iops" => apply_io_device_limit(&mut out, val, IoLimitKind::ReadIops)?,
            "write_iops" => apply_io_device_limit(&mut out, val, IoLimitKind::WriteIops)?,
            "read_mbps" => apply_io_device_limit(&mut out, val, IoLimitKind::ReadMbps)?,
            "write_mbps" => apply_io_device_limit(&mut out, val, IoLimitKind::WriteMbps)?,
            other => anyhow::bail!("Unknown resource key '{}'", other),
        }
    }
    Ok(out)
}

#[derive(Clone, Copy)]
enum IoLimitKind {
    ReadIops,
    WriteIops,
    ReadMbps,
    WriteMbps,
}

fn apply_io_device_limit(out: &mut ResourceSpec, spec: &str, kind: IoLimitKind) -> anyhow::Result<()> {
    // Format: MAJ:MIN:LIMIT
    let mut parts = spec.split(':').map(|s| s.trim());
    let maj_s = parts.next().ok_or_else(|| anyhow::anyhow!("Invalid io spec '{}'", spec))?;
    let min_s = parts.next().ok_or_else(|| anyhow::anyhow!("Invalid io spec '{}'", spec))?;
    let lim_s = parts.next().ok_or_else(|| anyhow::anyhow!("Invalid io spec '{}'", spec))?;
    if parts.next().is_some() {
        anyhow::bail!("Invalid io spec '{}': expected MAJ:MIN:LIMIT", spec);
    }
    let maj: u32 = maj_s.parse().map_err(|_| anyhow::anyhow!("Invalid io spec '{}': bad MAJ", spec))?;
    let min: u32 = min_s.parse().map_err(|_| anyhow::anyhow!("Invalid io spec '{}': bad MIN", spec))?;
    let lim: u64 = lim_s.parse().map_err(|_| anyhow::anyhow!("Invalid io spec '{}': bad LIMIT", spec))?;

    let entry = out.io_max.entry((maj, min)).or_default();
    match kind {
        IoLimitKind::ReadIops => {
            if entry.riops.is_some() {
                anyhow::bail!("Duplicate entry for resource key 'read_iops' for device {maj}:{min}");
            }
            entry.riops = Some(lim);
        }
        IoLimitKind::WriteIops => {
            if entry.wiops.is_some() {
                anyhow::bail!("Duplicate entry for resource key 'write_iops' for device {maj}:{min}");
            }
            entry.wiops = Some(lim);
        }
        IoLimitKind::ReadMbps => {
            if entry.rbps.is_some() {
                anyhow::bail!("Duplicate entry for resource key 'read_mbps' for device {maj}:{min}");
            }
            entry.rbps = Some(lim.saturating_mul(1024 * 1024));
        }
        IoLimitKind::WriteMbps => {
            if entry.wbps.is_some() {
                anyhow::bail!("Duplicate entry for resource key 'write_mbps' for device {maj}:{min}");
            }
            entry.wbps = Some(lim.saturating_mul(1024 * 1024));
        }
    }
    Ok(())
}

fn parse_env_kv(s: &str) -> anyhow::Result<(&str, &str)> {
    let (k, v) = s
        .split_once('=')
        .ok_or_else(|| anyhow::anyhow!("Invalid env '{}': expected KEY=VALUE", s))?;
    if k.is_empty() {
        anyhow::bail!("Invalid env '{}': key is empty", s);
    }
    Ok((k, v))
}

fn resolve_env_value_spec(raw_value: &str) -> anyhow::Result<OsString> {
    const B64_PREFIX: &str = "base64://";
    const HEX_PREFIX: &str = "hex://";

    if let Some(rest) = raw_value.strip_prefix(HEX_PREFIX) {
        let bytes = hex::decode(rest).map_err(|e| anyhow::anyhow!("Invalid hex env value: {}", e))?;
        return Ok(OsString::from_vec(bytes));
    }
    if let Some(rest) = raw_value.strip_prefix(B64_PREFIX) {
        use base64::Engine;
        let engine = base64::engine::general_purpose::STANDARD;
        let bytes = engine
            .decode(rest.as_bytes())
            .map_err(|e| anyhow::anyhow!("Invalid base64 env value: {}", e))?;
        return Ok(OsString::from_vec(bytes));
    }

    Ok(OsString::from(raw_value))
}

fn resolve_user_group(user: Option<&str>, run_group: Option<&str>) -> anyhow::Result<(Option<u32>, Option<u32>, Option<CString>)> {
    let mut uid: Option<u32> = None;
    let mut gid: Option<u32> = None;
    let mut c_user: Option<CString> = None;

    if let Some(u) = user {
        let urec = get_user_by_name(u).ok_or_else(|| anyhow::anyhow!("Unknown user '{}'", u))?;
        uid = Some(urec.uid());
        gid = Some(urec.primary_group_id());
        c_user = Some(CString::new(u).map_err(|_| anyhow::anyhow!("Invalid username (contains NUL)"))?);
    }

    if let Some(gname) = run_group {
        let grec = get_group_by_name(gname).ok_or_else(|| anyhow::anyhow!("Unknown group '{}'", gname))?;
        gid = Some(grec.gid());
    }

    // If neither is set, return (None, None, None) (run as current user).
    Ok((uid, gid, c_user))
}

fn cgroup_procs_cstring(cg_path: &Path) -> anyhow::Result<CString> {
    let procs = cg_path.join("cgroup.procs");
    let procs_s = procs
        .to_str()
        .ok_or_else(|| anyhow::anyhow!("cgroup.procs path is not valid UTF-8: {:?}", procs))?;
    CString::new(procs_s.as_bytes())
        .map_err(|_| anyhow::anyhow!("cgroup.procs path contains NUL byte: {:?}", procs))
}

fn join_cgroup(cg_path: &Path) -> anyhow::Result<()> {
    let procs = cg_path.join("cgroup.procs");
    let procs_cs = cgroup_procs_cstring(cg_path)?;

    unsafe {
        let fd = libc::open(procs_cs.as_ptr(), libc::O_WRONLY | libc::O_CLOEXEC);
        if fd < 0 {
            anyhow::bail!(
                "Failed to open {:?} for writing: {} (need root or delegated cgroup)",
                procs,
                io::Error::last_os_error()
            );
        }

        let pid = libc::getpid();
        let mut buf = [0u8; 32];
        let n = libc::snprintf(
            buf.as_mut_ptr() as *mut libc::c_char,
            buf.len(),
            b"%d\n\0".as_ptr() as *const libc::c_char,
            pid,
        );
        if n < 0 {
            let _ = libc::close(fd);
            anyhow::bail!("Failed to format PID string for cgroup.procs");
        }

        let to_write = n as usize;
        let w = libc::write(fd, buf.as_ptr() as *const libc::c_void, to_write);
        let errno = *libc::__errno_location();
        let _ = libc::close(fd);

        if w < 0 {
            anyhow::bail!(
                "Failed to write PID to {:?}: {} (errno={}). Common causes: cgroup v2 type mismatch (threaded/domain), or invalid migration per kernel rules.",
                procs,
                io::Error::last_os_error(),
                errno
            );
        }
        if w as usize != to_write {
            anyhow::bail!("Short write to {:?} (wrote {} of {})", procs, w, to_write);
        }
    }
    Ok(())
}

fn kill_all_cgroup(cg_path: &Path) -> anyhow::Result<()> {
    write_cgroup_file(cg_path, "cgroup.kill", "1\n")
}

fn read_cgroup_pids(path: &Path) -> anyhow::Result<Vec<i32>> {
    let procs = path.join("cgroup.procs");
    let content = fs::read_to_string(&procs).map_err(|e| {
        anyhow::anyhow!(
            "Failed to read {:?}: {} (does this cgroup exist?)",
            procs,
            e
        )
    })?;
    let mut out = Vec::new();
    for line in content.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let pid: i32 = line
            .parse()
            .map_err(|e| anyhow::anyhow!("Invalid PID in {:?}: {:?} ({})", procs, line, e))?;
        out.push(pid);
    }
    Ok(out)
}

fn try_print_ps_for_pids(pids: &[i32]) -> anyhow::Result<bool> {
    if pids.is_empty() {
        return Ok(true);
    }
    let pid_csv = pids
        .iter()
        .map(|p| p.to_string())
        .collect::<Vec<_>>()
        .join(",");

    let variants: [&[&str]; 2] = [&["ewwf", "-p"], &["-ewwf", "-p"]];
    for v in variants {
        let out = Command::new("ps")
            .args(v)
            .arg(&pid_csv)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output();

        match out {
            Ok(o) if o.status.success() => {
                print!("{}", String::from_utf8_lossy(&o.stdout));
                return Ok(true);
            }
            Ok(_) => continue,
            Err(_) => continue,
        }
    }
    Ok(false)
}

fn read_proc_file(pid: i32, name: &str) -> anyhow::Result<Vec<u8>> {
    let p = PathBuf::from(format!("/proc/{pid}/{name}"));
    let b = fs::read(&p).map_err(|e| anyhow::anyhow!("Failed to read {:?}: {}", p, e))?;
    Ok(b)
}

fn read_proc_cmdline(pid: i32) -> anyhow::Result<String> {
    let b = read_proc_file(pid, "cmdline")?;
    if b.is_empty() {
        return Ok(String::new());
    }
    let parts = b
        .split(|c| *c == 0u8)
        .filter(|s| !s.is_empty())
        .map(|s| String::from_utf8_lossy(s).to_string())
        .collect::<Vec<_>>();
    Ok(parts.join(" "))
}

fn read_proc_environ(pid: i32, max_bytes: usize) -> anyhow::Result<String> {
    let mut b = read_proc_file(pid, "environ")?;
    if b.len() > max_bytes {
        b.truncate(max_bytes);
    }
    let parts = b
        .split(|c| *c == 0u8)
        .filter(|s| !s.is_empty())
        .map(|s| String::from_utf8_lossy(s).to_string())
        .collect::<Vec<_>>();
    Ok(parts.join(" "))
}

fn read_proc_status_fields(pid: i32) -> anyhow::Result<(String, String, String, String)> {
    let status = read_proc_file(pid, "status")?;
    let s = String::from_utf8_lossy(&status);
    let mut name = String::new();
    let mut state = String::new();
    let mut uid = String::new();
    let mut gid = String::new();
    for line in s.lines() {
        if let Some(v) = line.strip_prefix("Name:") {
            name = v.trim().to_string();
        } else if let Some(v) = line.strip_prefix("State:") {
            state = v.trim().to_string();
        } else if let Some(v) = line.strip_prefix("Uid:") {
            uid = v.split_whitespace().next().unwrap_or("").to_string();
        } else if let Some(v) = line.strip_prefix("Gid:") {
            gid = v.split_whitespace().next().unwrap_or("").to_string();
        }
    }
    Ok((name, state, uid, gid))
}

fn print_pidex_fallback(pids: &[i32]) -> anyhow::Result<()> {
    if pids.is_empty() {
        return Ok(());
    }
    println!("PID\tUID\tGID\tSTATE\tNAME\tCMDLINE\tENV");
    for pid in pids {
        let (name, state, uid, gid) = match read_proc_status_fields(*pid) {
            Ok(v) => v,
            Err(_) => continue,
        };
        let cmdline = read_proc_cmdline(*pid).unwrap_or_default();
        let env = read_proc_environ(*pid, 32 * 1024).unwrap_or_default();
        println!("{pid}\t{uid}\t{gid}\t{state}\t{name}\t{cmdline}\t{env}");
    }
    Ok(())
}

fn kill_pids(pids: &[i32], sig: nix::sys::signal::Signal) -> anyhow::Result<()> {
    for pid in pids {
        let r = nix::sys::signal::kill(nix::unistd::Pid::from_raw(*pid), sig);
        if let Err(e) = r {
            if matches!(e, nix::errno::Errno::ESRCH) {
                continue;
            }
            return Err(anyhow::anyhow!("Failed to send {} to {}: {}", sig, pid, e));
        }
    }
    Ok(())
}

fn wait_for_cgroup_empty(cg_path: &Path) -> anyhow::Result<()> {
    // Simple polling wait as this is primarily an operator tool.
    let mut consecutive_empty = 0u32;
    loop {
        let mut pids = read_cgroup_pids(cg_path).unwrap_or_default();
        pids.sort_unstable();
        pids.dedup();

        if pids.is_empty() {
            consecutive_empty += 1;
            if consecutive_empty >= 3 {
                return Ok(());
            }
            std::thread::sleep(std::time::Duration::from_millis(100));
            continue;
        }

        consecutive_empty = 0;
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
}

// --------------------------
// Log rotation + compression
// --------------------------

fn spawn_log_thread<R: Read + Send + 'static>(
    mut reader: R,
    path: PathBuf,
    max_bytes: u64,
    max_files: u32,
    no_compress_recent: u32,
) -> JoinHandle<()> {
    std::thread::spawn(move || {
        if let Err(e) = log_copy_with_rotation(
            &mut reader,
            &path,
            max_bytes,
            max_files,
            no_compress_recent,
        ) {
            eprintln!("launcher: log thread error for {}: {e}", path.display());
        }
    })
}

fn log_copy_with_rotation<R: Read>(
    reader: &mut R,
    path: &Path,
    max_bytes: u64,
    max_files: u32,
    no_compress_recent: u32,
) -> anyhow::Result<()> {
    if max_files == 0 {
        anyhow::bail!("--max-log-files must be >= 1");
    }

    // Compression is done by a separate background worker so the log receiver thread keeps draining
    // stdout/stderr without being blocked by gzip CPU / IO.
    let (compress_tx, compress_rx) = mpsc::channel::<CompressTask>();
    let compressor = std::thread::spawn(move || {
        while let Ok(task) = compress_rx.recv() {
            match task {
                CompressTask::Compress { src_tmp, dst_gz } => {
                    if let Err(e) = compress_gzip(&src_tmp, &dst_gz) {
                        eprintln!(
                            "launcher: failed to compress {:?} -> {:?}: {e}",
                            src_tmp, dst_gz
                        );
                    }
                    let _ = fs::remove_file(&src_tmp);
                }
                CompressTask::Sync(done) => {
                    let _ = done.send(());
                }
            }
        }
    });

    // Requirement: on start, stat any existing log file and treat its size as bytes_already_written.
    // (We open in append mode, so we continue writing after the existing content.)
    let mut current_size = file_len(path).unwrap_or(0);
    let mut writer = open_log_writer(path)?;

    const MAX_UNSPLIT_LINE_BYTES: usize = 16 * 1024;

    let mut buf = [0u8; 32 * 1024];
    let mut pending: Vec<u8> = Vec::new();
    loop {
        let n = reader.read(&mut buf)?;
        if n == 0 {
            // Flush any trailing data as a final "line". Only split if it exceeds 16KiB.
            while !pending.is_empty() {
                let chunk_len = pending.len().min(MAX_UNSPLIT_LINE_BYTES);
                let chunk: Vec<u8> = pending.drain(..chunk_len).collect();

                // Rotate between chunks/lines only.
                if current_size > 0
                    && (current_size >= max_bytes
                        || current_size.saturating_add(chunk.len() as u64) > max_bytes)
                {
                    writer.flush()?;
                    drop(writer);
                    rotate_logs(path, max_files, no_compress_recent, &compress_tx)?;
                    writer = open_log_writer(path)?;
                    current_size = 0;
                }

                writer.write_all(&chunk)?;
                current_size = current_size.saturating_add(chunk.len() as u64);
            }
            writer.flush()?;
            // Finish any pending compression work before returning.
            drop(compress_tx);
            let _ = compressor.join();
            return Ok(());
        }

        pending.extend_from_slice(&buf[..n]);

        // Drain full lines (newline-terminated) first so we never split "normal" lines.
        loop {
            let nl_pos = pending.iter().position(|b| *b == b'\n');
            let Some(pos) = nl_pos else { break };
            let line: Vec<u8> = pending.drain(..=pos).collect();

            if current_size > 0
                && (current_size >= max_bytes
                    || current_size.saturating_add(line.len() as u64) > max_bytes)
            {
                writer.flush()?;
                drop(writer);
                rotate_logs(path, max_files, no_compress_recent, &compress_tx)?;
                writer = open_log_writer(path)?;
                current_size = 0;
            }

            writer.write_all(&line)?;
            current_size = current_size.saturating_add(line.len() as u64);
        }

        // If the trailing (non-newline-terminated) "line" becomes too large, start flushing it in
        // 16KiB chunks. This is the only case where a log line may be split.
        while pending.len() > MAX_UNSPLIT_LINE_BYTES {
            let chunk: Vec<u8> = pending.drain(..MAX_UNSPLIT_LINE_BYTES).collect();

            if current_size > 0
                && (current_size >= max_bytes
                    || current_size.saturating_add(chunk.len() as u64) > max_bytes)
            {
                writer.flush()?;
                drop(writer);
                rotate_logs(path, max_files, no_compress_recent, &compress_tx)?;
                writer = open_log_writer(path)?;
                current_size = 0;
            }

            writer.write_all(&chunk)?;
            current_size = current_size.saturating_add(chunk.len() as u64);
        }
    }
}

fn open_log_writer(path: &Path) -> anyhow::Result<BufWriter<fs::File>> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)?;
        }
    }
    // Append so subsequent launcher runs continue the same log stream unless rotated.
    let f = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)?;
    Ok(BufWriter::new(f))
}

fn file_len(path: &Path) -> io::Result<u64> {
    Ok(fs::metadata(path)?.len())
}

#[derive(Debug)]
enum CompressTask {
    Compress { src_tmp: PathBuf, dst_gz: PathBuf },
    Sync(mpsc::Sender<()>),
}

fn unique_tmp_suffix() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0)
}

fn rotate_logs(
    path: &Path,
    max_files: u32,
    no_compress_recent: u32,
    compress_tx: &mpsc::Sender<CompressTask>,
) -> anyhow::Result<()> {
    // If rotation is happening faster than compression, we must not shift/delete indices while the
    // compressor thread is still writing `.gz`. Sync here to ensure all prior compress tasks finish.
    let (done_tx, done_rx) = mpsc::channel::<()>();
    compress_tx
        .send(CompressTask::Sync(done_tx))
        .map_err(|_| anyhow::anyhow!("compressor thread is not available"))?;
    let _ = done_rx.recv();

    // Keep rotated logs as:
    // - most recent N rotated files: <path>.1 .. <path>.<N> (uncompressed)
    // - older rotated files:         <path>.<i>.gz (compressed) for i > N
    // Current file is always <path>.
    let base = path.to_string_lossy().to_string();

    // Remove the oldest (delete both variants).
    let _ = fs::remove_file(format!("{base}.{max_files}"));
    let _ = fs::remove_file(format!("{base}.{max_files}.gz"));

    // Shift down (max_files-1 .. 1) without changing compression state.
    // We never decompress gz->plain.
    if max_files >= 2 {
        for i in (1..max_files).rev() {
            let src_plain = format!("{base}.{i}");
            let src_gz = format!("{base}.{i}.gz");
            let dst_index = i + 1;
            let dst_plain = format!("{base}.{dst_index}");
            let dst_gz = format!("{base}.{dst_index}.gz");

            // Clean any existing destination variants to avoid rename failures.
            let _ = fs::remove_file(&dst_plain);
            let _ = fs::remove_file(&dst_gz);

            if Path::new(&src_gz).exists() {
                // gz -> gz
                let _ = fs::rename(&src_gz, &dst_gz);
            } else if Path::new(&src_plain).exists() {
                // plain -> plain
                let _ = fs::rename(&src_plain, &dst_plain);
            }
        }
    }

    // Rotate current to .1
    if Path::new(&base).exists() {
        let rotated_plain = format!("{base}.1");
        let _ = fs::rename(&base, &rotated_plain);
        if no_compress_recent == 0 {
            // Compress in background.
            let tmp = PathBuf::from(format!("{rotated_plain}.tmp.{}", unique_tmp_suffix()));
            let _ = fs::rename(&rotated_plain, &tmp);
            let dst = PathBuf::from(format!("{rotated_plain}.gz"));
            let _ = compress_tx.send(CompressTask::Compress { src_tmp: tmp, dst_gz: dst });
        }
    }

    // Enforce compression for "older than N" rotated files by compressing plain files for i > N.
    // We never decompress gz->plain; if a recent index already has .gz from a previous policy, we leave it.
    let n = no_compress_recent.min(max_files);
    for i in (n + 1)..=max_files {
        let plain = format!("{base}.{i}");
        let gz = format!("{base}.{i}.gz");
        if Path::new(&plain).exists() && !Path::new(&gz).exists() {
            let tmp = PathBuf::from(format!("{plain}.tmp.{}", unique_tmp_suffix()));
            let _ = fs::rename(&plain, &tmp);
            let _ = compress_tx.send(CompressTask::Compress {
                src_tmp: tmp,
                dst_gz: PathBuf::from(gz),
            });
        }
    }

    Ok(())
}

fn compress_gzip(src: &Path, dst: &Path) -> anyhow::Result<()> {
    use flate2::write::GzEncoder;
    use flate2::Compression;

    let input = fs::read(src)?;
    let f = OpenOptions::new().create(true).truncate(true).write(true).open(dst)?;
    let mut enc = GzEncoder::new(f, Compression::default());
    enc.write_all(&input)?;
    enc.finish()?;
    Ok(())
}

// --------------------------
// CPU + memory parsing (as LAUNCHER.md describes)
// --------------------------

fn parse_cpu_to_quota_us(cpu: &str, period_us: u64) -> anyhow::Result<Option<u64>> {
    let s = cpu.trim();
    if s.eq_ignore_ascii_case("max") {
        return Ok(None);
    }
    let cores: f64 = if let Some(m) = s.strip_suffix('m').or_else(|| s.strip_suffix('M')) {
        let milli: f64 = m.parse().map_err(|_| anyhow::anyhow!("Invalid cpu '{}'", cpu))?;
        milli / 1000.0
    } else {
        s.parse().map_err(|_| anyhow::anyhow!("Invalid cpu '{}'", cpu))?
    };
    if !(cores > 0.0) {
        anyhow::bail!("Invalid cpu '{}': must be > 0 (or 'max')", cpu);
    }
    let quota = (cores * (period_us as f64)).round();
    if quota < 1.0 {
        anyhow::bail!("Invalid cpu '{}': too small for period {}us", cpu, period_us);
    }
    Ok(Some(quota as u64))
}

fn parse_memory_to_bytes(mem: &str) -> anyhow::Result<Option<u64>> {
    let raw = mem.trim();
    if raw.eq_ignore_ascii_case("max") {
        return Ok(None);
    }

    let mut i = 0usize;
    for (idx, ch) in raw.char_indices() {
        if ch.is_ascii_digit() || ch == '.' {
            i = idx + ch.len_utf8();
            continue;
        }
        break;
    }
    let (num_s, unit_s) = raw.split_at(i);
    if num_s.is_empty() {
        anyhow::bail!("Invalid size '{}': missing number", mem);
    }
    let num: f64 = num_s
        .parse()
        .map_err(|_| anyhow::anyhow!("Invalid size '{}': bad number", mem))?;
    if num < 0.0 {
        anyhow::bail!("Invalid size '{}': must be >= 0 (or 'max')", mem);
    }

    let mut unit = unit_s.trim().to_ascii_lowercase();
    if unit.is_empty() {
        return Ok(Some(num.round() as u64));
    }
    if unit.ends_with('b') {
        unit.pop();
    }

    let mult: f64 = match unit.as_str() {
        "" => 1.0,
        "k" => 1_000.0,
        "m" => 1_000_000.0,
        "g" => 1_000_000_000.0,
        "t" => 1_000_000_000_000.0,
        "p" => 1_000_000_000_000_000.0,
        "e" => 1_000_000_000_000_000_000.0,
        "ki" => 1024.0,
        "mi" => 1024.0 * 1024.0,
        "gi" => 1024.0 * 1024.0 * 1024.0,
        "ti" => 1024.0 * 1024.0 * 1024.0 * 1024.0,
        "pi" => 1024.0 * 1024.0 * 1024.0 * 1024.0 * 1024.0,
        "ei" => 1024.0 * 1024.0 * 1024.0 * 1024.0 * 1024.0 * 1024.0,
        _ => anyhow::bail!("Invalid size '{}': unknown unit '{}'", mem, unit_s.trim()),
    };
    let bytes = (num * mult).round();
    Ok(Some(bytes as u64))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn cpu_parsing_formats() {
        let period = 100_000u64;
        assert_eq!(parse_cpu_to_quota_us("200m", period).unwrap(), Some(20_000));
        assert_eq!(parse_cpu_to_quota_us("7", period).unwrap(), Some(700_000));
        assert_eq!(parse_cpu_to_quota_us("4.5", period).unwrap(), Some(450_000));
        assert_eq!(parse_cpu_to_quota_us("max", period).unwrap(), None);
        assert_eq!(parse_cpu_to_quota_us("200M", period).unwrap(), Some(20_000));
    }

    #[test]
    fn memory_parsing_formats() {
        assert_eq!(parse_memory_to_bytes("1024").unwrap(), Some(1024));
        assert_eq!(parse_memory_to_bytes("0").unwrap(), Some(0));
        assert_eq!(parse_memory_to_bytes("4M").unwrap(), Some(4_000_000));
        assert_eq!(parse_memory_to_bytes("4m").unwrap(), Some(4_000_000));
        assert_eq!(parse_memory_to_bytes("4MB").unwrap(), Some(4_000_000));
        assert_eq!(parse_memory_to_bytes("1KiB").unwrap(), Some(1024));
        assert_eq!(parse_memory_to_bytes("4Mi").unwrap(), Some(4 * 1024 * 1024));
        assert_eq!(parse_memory_to_bytes("4mI").unwrap(), Some(4 * 1024 * 1024));
        assert_eq!(parse_memory_to_bytes("max").unwrap(), None);
    }

    #[test]
    fn resource_parsing_ignores_empty_tokens_and_optional_trailing_semicolon() {
        let r1 = parse_resource_spec("cpu=100m;;;memory=1G;").unwrap();
        let r2 = parse_resource_spec("cpu=100m;memory=1G").unwrap();
        assert_eq!(r1.cpu.as_deref(), Some("100m"));
        assert_eq!(r1.memory.as_deref(), Some("1G"));
        assert_eq!(r2.cpu.as_deref(), Some("100m"));
        assert_eq!(r2.memory.as_deref(), Some("1G"));
    }

    #[test]
    fn resource_parsing_rejects_duplicate_singletons() {
        let e = parse_resource_spec("cpu=100m;cpu=200m").unwrap_err();
        assert!(format!("{e:#}").contains("Duplicate entry for resource key 'cpu'"));
        let e = parse_resource_spec("io-weight=100;io-weight=200").unwrap_err();
        assert!(format!("{e:#}").contains("Duplicate entry for resource key 'io-weight'"));
        let e = parse_resource_spec("memory=1G;memory=2G").unwrap_err();
        assert!(format!("{e:#}").contains("Duplicate entry for resource key 'memory'"));
        let e = parse_resource_spec("memory_swap=0G;memory_swap=1G").unwrap_err();
        assert!(format!("{e:#}").contains("Duplicate entry for resource key 'memory_swap'"));
    }

    #[test]
    fn resource_parsing_rejects_duplicate_cpuset() {
        let e = parse_resource_spec("cpuset=0-1;cpuset=2").unwrap_err();
        assert!(format!("{e:#}").contains("Duplicate entry for resource key 'cpuset'"));
    }

    #[test]
    fn required_controllers_minimum_is_pids() {
        let r = parse_resource_spec("").unwrap();
        assert_eq!(required_controllers(&r), vec!["pids".to_string()]);
    }

    #[test]
    fn required_controllers_include_used_features() {
        let r = parse_resource_spec("cpu=100m;memory=1G;io-weight=10;cpuset=0-1").unwrap();
        let req = required_controllers(&r);
        assert!(req.contains(&"pids".to_string()));
        assert!(req.contains(&"cpu".to_string()));
        assert!(req.contains(&"memory".to_string()));
        assert!(req.contains(&"io".to_string()));
        assert!(req.contains(&"cpuset".to_string()));
    }

    #[test]
    fn resource_parsing_rejects_duplicate_io_rules_for_same_device() {
        let e = parse_resource_spec("read_iops=8:0:100;read_iops=8:0:200").unwrap_err();
        assert!(format!("{e:#}").contains("Duplicate entry for resource key 'read_iops' for device 8:0"));

        let e = parse_resource_spec("write_iops=8:0:100;write_iops=8:0:200").unwrap_err();
        assert!(format!("{e:#}").contains("Duplicate entry for resource key 'write_iops' for device 8:0"));

        let e = parse_resource_spec("read_mbps=8:0:10;read_mbps=8:0:20").unwrap_err();
        assert!(format!("{e:#}").contains("Duplicate entry for resource key 'read_mbps' for device 8:0"));

        let e = parse_resource_spec("write_mbps=8:0:10;write_mbps=8:0:20").unwrap_err();
        assert!(format!("{e:#}").contains("Duplicate entry for resource key 'write_mbps' for device 8:0"));

        // Different devices are allowed.
        let r = parse_resource_spec("read_iops=8:0:100;read_iops=8:1:200").unwrap();
        assert_eq!(r.io_max.get(&(8, 0)).and_then(|x| x.riops), Some(100));
        assert_eq!(r.io_max.get(&(8, 1)).and_then(|x| x.riops), Some(200));
    }

    #[test]
    fn run_cgroup_default_is_target_run() {
        let target = PathBuf::from("/sys/fs/cgroup/app/process1");
        let run = pick_run_cgroup_path(&target, "app/process1", false).unwrap();
        assert_eq!(run, PathBuf::from("/sys/fs/cgroup/app/process1/run"));
    }

    #[test]
    fn run_cgroup_exact_if_last_token_is_run() {
        let target = PathBuf::from("/sys/fs/cgroup/app/process1/run");
        let run = pick_run_cgroup_path(&target, "app/process1/run", false).unwrap();
        assert_eq!(run, target);
    }

    #[test]
    fn run_cgroup_exact_if_flag_set() {
        let target = PathBuf::from("/sys/fs/cgroup/app/process1");
        let run = pick_run_cgroup_path(&target, "app/process1", true).unwrap();
        assert_eq!(run, target);
    }

    #[test]
    fn log_rotation_does_not_split_lines_under_16k() {
        let dir = tempdir().unwrap();
        let p = dir.path().join("stdout.log");

        // Two lines, each 10 bytes + newline. Set max_bytes small so it rotates between lines.
        let input = b"aaaaaaaaaa\nbbbbbbbbbb\n".to_vec();
        let mut r: &[u8] = &input;
        log_copy_with_rotation(&mut r, &p, 15, 5, 0).unwrap();

        let base = p.to_string_lossy().to_string();
        let cur = fs::read(&p).unwrap();
        let rot1_gz = fs::read(format!("{base}.1.gz")).unwrap();

        // Current file should contain second line only.
        assert_eq!(cur, b"bbbbbbbbbb\n");
        // Rotated file exists (compressed) and is non-empty.
        assert!(!rot1_gz.is_empty());
    }
}


