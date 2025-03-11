use serde::Deserialize;
use std::collections::HashMap;

#[derive(Debug, Deserialize)]
pub struct Settings {
    pub cities: Vec<String>,
    pub minio: MinioConfig,
    pub api: ApiConfig,
}

#[derive(Debug, Deserialize)]
pub struct MinioConfig {
    pub endpoint: String,
    pub access_key: String,
    pub secret_key: String,
    pub bucket: String,
    pub region: String,
}

#[derive(Debug, Deserialize)]
pub struct ApiConfig {
    pub restaurant_url: String,
    pub vendor_url: String,
    pub review_url: String,
    pub headers: HashMap<String, String>,
    pub base_params: HashMap<String, String>,
}

impl Settings {
    pub fn new() -> Result<Self, config::ConfigError> {
        let settings = config::Config::builder()
            .add_source(config::File::with_name("config/default"))
            .add_source(config::Environment::with_prefix("APP"))
            .build()?;

        settings.try_deserialize()
    }
}