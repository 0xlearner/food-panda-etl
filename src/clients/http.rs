use rquest::{Client, Response, RequestBuilder};
use rquest_util::Emulation;
use http::header::{HeaderMap, HeaderName, HeaderValue};
use http::StatusCode;
use crate::error::Result;
use crate::config::Settings;
use tracing::{error, debug};

pub struct HttpClient {
    client: Client,
    headers: HeaderMap,  // Store headers at struct level
}

impl HttpClient {
    pub fn new(settings: Settings, emulation: Emulation) -> Result<Self> {
        let mut headers = HeaderMap::new();
        
        // Add configured headers
        for (key, value) in settings.api.headers.iter() {
            if let (Ok(header_name), Ok(header_value)) = (
                HeaderName::from_bytes(key.as_bytes()),
                HeaderValue::from_str(value)
            ) {
                headers.insert(header_name, header_value);
                debug!(
                    header_key = key,
                    header_value = value,
                    "Adding header"
                );
            } else {
                error!(
                    header_key = key,
                    header_value = value,
                    "Invalid header value"
                );
            }
        }

        debug!(
            emulation = ?emulation,
            "Creating client with emulation"
        );

        let client = Client::builder()
            .emulation(emulation)
            .build()?;

        Ok(Self { 
            client,
            headers,
        })
    }

    pub fn get(&self, url: &str) -> RequestBuilder {
        let mut request = self.client.get(url);
        
        // Apply headers to each request
        for (key, value) in self.headers.iter() {
            request = request.header(key, value);
        }

        debug!(
            url = url,
            headers = ?self.headers,
            "Creating GET request with headers"
        );

        request
    }

    pub async fn send(&self, request: RequestBuilder) -> Result<Response> {
        // Build the request to inspect headers
        let built_request = request.try_clone()
            .expect("Failed to clone request")
            .build()?;
        
        debug!(
            url = %built_request.url(),
            headers = ?built_request.headers().iter()
                .map(|(k, v)| (k.as_str(), v.to_str().unwrap_or("invalid")))
                .collect::<Vec<_>>(),
            "Sending request"
        );
        
        let response = request.send().await?;
        
        debug!(
            status = response.status().as_u16(),
            url = %response.url(),
            response_headers = ?response.headers().iter()
                .map(|(k, v)| (k.as_str(), v.to_str().unwrap_or("invalid")))
                .collect::<Vec<_>>(),
            "Response received"
        );
    
        match response.status() {
            StatusCode::TOO_MANY_REQUESTS => {
                debug!("Rate limit exceeded");
                Err(crate::error::Error::RateLimit)
            },
            StatusCode::FORBIDDEN => {
                debug!(
                    url = %response.url(),
                    response_headers = ?response.headers().iter()
                        .map(|(k, v)| (k.as_str(), v.to_str().unwrap_or("invalid")))
                        .collect::<Vec<_>>(),
                    "Received 403 Forbidden"
                );
                Err(crate::error::Error::Forbidden)
            },
            _ => Ok(response)
        }
    }
}