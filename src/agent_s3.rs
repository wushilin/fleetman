use aws_config::BehaviorVersion;
use aws_sdk_s3::Client as S3Client;

use crate::agent_config::ObjectStorageConfig;

pub async fn create_s3_client(config: &ObjectStorageConfig) -> S3Client {
    let credentials = aws_credential_types::Credentials::new(
        &config.access_key,
        &config.secret_key,
        None,
        None,
        "static",
    );

    let mut s3_config_builder = aws_sdk_s3::config::Builder::new()
        .behavior_version(BehaviorVersion::latest())
        .region(aws_sdk_s3::config::Region::new("us-east-1"))
        .endpoint_url(&config.endpoint)
        .credentials_provider(credentials);

    if config.path_style.unwrap_or(false) {
        s3_config_builder = s3_config_builder.force_path_style(true);
    }

    S3Client::from_conf(s3_config_builder.build())
}


