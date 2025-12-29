# Launcher
The launcher is for you to launch processes using CLI.

It has many sub commands, and we will cover them well here.

## Sub commands

## start

Launches a program and wait until it dies. Exit with the same exit code as the proram. Rotate and compress the program's stdout|stderr if specified. 

Example:

`launcher start --resource "cpu=100m;memory=1G;io-weight=400;read_iops=422:2:200;read_iops=422:0:200;write_iops=422:2:200;write_iops=422:1:100;read_mbps=422:2:10;write_mbps=422:2:10;memory_swap=0G;" --env A=B --env c=d --env long_env=base64://xxxx --env long_env2=hex://afee --stdout /tmp/stdout.log --stderr /tmp/stderr.log --max-log-size=100m --max-log-files 10 --no-compress-logs --no-decoration --cgroot /sys/fs/cgroup --cgroup app/process1 --user john --group admin --disable-cg-subtree-control --cgroup-exact` 

It launches process with the resource specified, passing the environment, supporting bare env passing, or base64/hex passing, and supports stdout and stderr redirection. it also supports log rotation for stdout and stderr, only if the stdout and stderr files are specified. log rotation has to be enabled, default is 100M file plus 10 backup index. default compression is enabled. but can be turned off. default cgroot is /sys/fs/cgroup. the actual group is the relative path to the root. Create is it is not there already. run as the given user or group. if not specified, run as current user. 
the program run until the process dies. and exit with the same exit code. when the target process is actually killed, if we can exit launcher itself with that code, do it. if not, we can generate a pseudo exit code.

For resource spec, I think it is largely self explainatory. but I want to highlight that:

1. CPU supports XXm and 7.5 and 7 format.
2. Memory support kmgt for 10 based counting, kimigiti for 2 based counting, and optionally can suffix with a `b`. it is also case insensitive. kb = 1000, Ki=1024, MiB=1024*1024. Hope it is clear.
3. Swap use the same way.
4. iops specifier is `MAJ:MIN:<limit>`.  The read/write mbps is MiB per second.
5. the last `;` is optional. empty tokens are ignored. e.g. `a=b;;;c=d;` and `a=b;c=d` are the same. 
6. by default, the cg controllers enables all, if possible, and subtree is the same. however, if user disabled it by the `--disable-cg-subtree-control`, then it is disabled.

by default, we launch in the $target_cgroup/run. Unless, the target_cgroup is already ending with /run, or we have --cgroup-exact specified.

When we setup the cgroup, we always setup the cgroup.controllers to the same as its parent, and enable subtree_control to the same as the parent, trace back to the cgroup root group. If --disable-cg-subtree-control is set, when we setup cgroup, the subtree_control is always empty. For example:

--cgroup app/app1/app3, then app and app1 and app3 should be applied with the controls. also subtree control. the app will run in /app/app1/app3/run, the /app/app1/app3/run  will not have subtree_control but all /app/app1/app3 will have.

The resource flag reject deduplicate for each key, except iops and bandwidth flags. for iops and bandwidth flags, we should check and reject duplicate at per device as well. 
