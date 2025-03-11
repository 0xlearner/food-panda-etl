use rquest::Client;
use rquest_util::Emulation;
use serde::{Deserialize, Serialize};
use std::fs::File;
use chrono::Utc;
use anyhow::Result;

// Models
#[derive(Debug, Deserialize)]
struct VendorListResponse {
    data: VendorData,
}

#[derive(Debug, Deserialize)]
struct VendorData {
    items: Vec<VendorItem>,
}

#[derive(Debug, Deserialize)]
struct VendorItem {
    code: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Vendor {
    pub code: String,
    pub name: String,
    pub details: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
struct VendorDetailResponse {
    data: serde_json::Value,
}

async fn fetch_vendor_codes(client: &Client, city_id: &str) -> Result<Vec<String>> {
    let url = format!(
        "https://disco.deliveryhero.io/listing/api/v1/pandora/vendors?\
         configuration=&country=pk&city_id={}&include=&language_id=1&\
         sort=&offset=0&limit=48&vertical=restaurants",
        city_id
    );

    let mut request = client.get(&url);

    // Add headers
    request = request
        .header("perseus-client-id", "1737108613136.802524900772077665.hi5re1m8x0")
        .header("perseus-session-id", "1741709021414.887046100017242046.1lc8k6gi1m")
        .header("x-disco-client-id", "web")
        .header("x-fp-api-key", "volo");

    let response = request.send().await?;
    let body = response.bytes().await?;
    let vendor_list: VendorListResponse = serde_json::from_slice(&body)?;

    Ok(vendor_list.data.items.into_iter().map(|item| item.code).collect())
}

async fn fetch_vendor_details(client: &Client, code: &str) -> Result<serde_json::Value> {
    let url = format!(
        "https://pk.fd-api.com/api/v5/vendors/{}?\
         include=menus,bundles,multiple_discounts&language_id=1&\
         opening_type=delivery&basket_currency=PKR",
        code
    );

    let mut request = client.get(&url);

    // Add headers
    request = request
        .header("perseus-client-id", "1737108613136.802524900772077665.hi5re1m8x0")
        .header("perseus-session-id", "1741709021414.887046100017242046.1lc8k6gi1m")
        .header("x-disco-client-id", "web")
        .header("x-fp-api-key", "volo");

    let response = request.send().await?;
    let body = response.bytes().await?;
    let vendor_detail: VendorDetailResponse = serde_json::from_slice(&body)?;

    Ok(vendor_detail.data)
}

#[tokio::main]
async fn main() -> Result<()> {
    // Current timestamp
    let timestamp = Utc::now().format("%Y-%m-%d %H:%M:%S").to_string();
    let user_login = "xde719";

    println!("Starting extraction at: {}", timestamp);
    println!("User: {}", user_login);

    // Build the client
    let client = Client::builder()
        .emulation(Emulation::Firefox135)
        .build()?;

    // City ID to process
    let city_id = "69036";

    // Fetch vendor codes
    println!("Fetching vendor codes for city: {}", city_id);
    let vendor_codes = fetch_vendor_codes(&client, city_id).await?;
    println!("Found {} vendors", vendor_codes.len());

    // Create a vector to store all vendor data
    let mut vendors = Vec::new();

    // Process each vendor code
    for (index, code) in vendor_codes.iter().enumerate() {
        println!("Processing vendor {}/{}: {}", index + 1, vendor_codes.len(), code);
        
        match fetch_vendor_details(&client, code).await {
            Ok(details) => {
                let vendor = Vendor {
                    code: code.clone(),
                    name: details.get("name")
                        .and_then(|n| n.as_str())
                        .unwrap_or("Unknown")
                        .to_string(),
                    details: Some(details),
                };
                vendors.push(vendor);
            }
            Err(e) => {
                eprintln!("Error fetching details for vendor {}: {}", code, e);
            }
        }

        // Optional: Add a small delay between requests to avoid rate limiting
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    }

    // Save to JSON file
    let filename = format!("vendors_city_{}.json", city_id);
    let file = File::create(&filename)?;
    serde_json::to_writer_pretty(file, &vendors)?;

    println!("Successfully saved {} vendors to {}", vendors.len(), filename);
    Ok(())
}