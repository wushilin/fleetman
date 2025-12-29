use aws_sdk_s3::{
    config::{Builder, Credentials, Region},
    Client,
};

use crate::controller_config::BucketConfig;

pub async fn create_s3_client(config: &BucketConfig) -> Client {
    let credentials = Credentials::new(
        &config.access_key,
        &config.secret_key,
        None,
        None,
        "static",
    );

    let s3_config = Builder::new()
        .region(Region::new("us-east-1"))
        .endpoint_url(&config.endpoint)
        .credentials_provider(credentials)
        .force_path_style(config.path_style)
        .behavior_version_latest()
        .build();

    Client::from_conf(s3_config)
}


