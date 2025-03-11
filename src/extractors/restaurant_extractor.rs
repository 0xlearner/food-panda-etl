use crate::config::Settings;
use crate::models::restaurant::Restaurant;
use crate::utils::http_client::HttpClient;
use anyhow::Result;
use async_trait::async_trait;
use std::collections::HashMap;
use tracing::{info, error};

#[async_trait]
pub trait Extractor {
    async fn extract(&self, city_id: &str, offset: u32) -> Result<Vec<Restaurant>>;
}

pub struct RestaurantExtractor {
    client: HttpClient,
    settings: Settings,
}

impl RestaurantExtractor {
    pub fn new(settings: Settings) -> Result<Self> {
        let client = HttpClient::new()?;
        Ok(Self { client, settings })
    }
}

#[async_trait]
impl Extractor for RestaurantExtractor {
    async fn extract(&self, city_id: &str, offset: u32) -> Result<Vec<Restaurant>> {
        let mut params = self.settings.api.base_params.clone();
        params.insert("city_id".to_string(), city_id.to_string());
        params.insert("offset".to_string(), offset.to_string());
        
        let response = self.client
            .get(
                &self.settings.api.restaurant_url,
                self.settings.api.headers.clone(),
                params,
            )
            .await?;

        // Parse response and extract restaurants
        let restaurants = match response.get("data").and_then(|d| d.get("items")) {
            Some(items) => {
                serde_json::from_value(items.clone())?
            }
            None => {
                error!("Invalid response format");
                vec![]
            }
        };

        Ok(restaurants)
    }
}