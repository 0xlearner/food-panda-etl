use serde::Deserialize;
use std::collections::HashMap;
use config::{Config, ConfigError};
use tracing::debug;

#[derive(Debug, Deserialize, Clone)]
pub struct Settings {
    pub cities: Vec<String>,
    pub minio: MinioConfig,
    pub api: ApiConfig,
}

#[derive(Debug, Deserialize, Clone)]
pub struct MinioConfig {
    pub endpoint: String,
    pub access_key: String,
    pub secret_key: String,
    pub bucket: String,
    pub region: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ApiConfig {
    pub headers: HashMap<String, String>,
}

impl Settings {
    pub fn new() -> Result<Self, ConfigError> {
        let builder = Config::builder()
            .add_source(config::File::with_name("config/default.yaml"))
            .add_source(config::Environment::with_prefix("APP"));

        // Build the configuration
        let config = builder.build()?;

        // Debug log the raw configuration
        if let Ok(headers) = config.get_table("api.headers") {
            debug!(
                ?headers,
                "Loaded API headers from configuration"
            );
        }

        // Try to deserialize the entire configuration
        let settings: Settings = config.try_deserialize()?;
        
        // Debug log the parsed headers
        debug!(
            headers = ?settings.api.headers,
            "Parsed API headers"
        );

        Ok(settings)
    }
}