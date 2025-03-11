use rquest::{Client, Error as RquestError};
use rquest_util::Emulation;
use std::collections::HashMap;
use tracing::{debug, error};

pub struct HttpClient {
    client: Client,
}

impl HttpClient {
    pub fn new() -> Result<Self, RquestError> {
        let client = Client::builder()
            .emulation(Emulation::Chrome133)
            .build()?;
        
        Ok(Self { client })
    }

    pub async fn get(
        &self,
        url: &str,
        headers: HashMap<String, String>,
        params: HashMap<String, String>,
    ) -> Result<serde_json::Value, RquestError> {
        debug!("Making GET request to: {}", url);
        
        let mut request = self.client.get(url);
        
        // Add headers
        for (key, value) in headers {
            request = request.header(&key, value);
        }
        
        // Add query parameters
        request = request.query(&params);
        
        let response = request.send().await?;
        
        if !response.status().is_success() {
            error!("Request failed with status: {}", response.status());
            return Err(RquestError::Status(response.status()));
        }
        
        let json = response.json().await?;
        Ok(json)
    }
}