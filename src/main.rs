use rquest::Client;
use rquest_util::Emulation;
use serde::{Deserialize, Serialize};
use std::fs::File;
use chrono::Utc;
use anyhow::Result;
use std::sync::Arc;

// Enhanced Models
#[derive(Debug, Deserialize)]
struct VendorListResponse {
    data: VendorData,
}

#[derive(Debug, Deserialize)]
struct VendorData {
    items: Vec<VendorItem>,
    returned_count: i32,      // Number of vendors in current page
    available_count: i32,     // Total available vendors
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
    pub batch_number: i32,  // Added batch number field
}

#[derive(Debug, Deserialize)]
struct VendorDetailResponse {
    data: serde_json::Value,
}

struct ClientPool {
    clients: Vec<Client>,
    current: std::sync::atomic::AtomicUsize,
}

impl ClientPool {
    fn new() -> Result<Self> {
        // Create clients with different emulations
        let emulations = vec![
            Emulation::Firefox136,
            Emulation::Chrome133,
            Emulation::Safari18_3,
            Emulation::Edge134,
        ];

        let clients = emulations.into_iter()
            .map(|emulation| {
                Client::builder()
                    .emulation(emulation)
                    .build()
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Self {
            clients,
            current: std::sync::atomic::AtomicUsize::new(0),
        })
    }

    fn next_client(&self) -> &Client {
        let current = self.current.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        &self.clients[current % self.clients.len()]
    }
}

async fn fetch_vendor_page(
    client_pool: &ClientPool, 
    city_id: &str, 
    offset: i32, 
    limit: i32
) -> Result<VendorListResponse> {
    let url = format!(
        "https://disco.deliveryhero.io/listing/api/v1/pandora/vendors?\
         configuration=&country=pk&city_id={}&include=&language_id=1&\
         sort=&offset={}&limit={}&vertical=restaurants",
        city_id, offset, limit
    );

    let client = client_pool.next_client();
    let mut request = client.get(&url);

    // Add headers
    request = request
        .header("perseus-client-id", "1737108613136.802524900772077665.hi5re1m8x0")
        .header("perseus-session-id", "1741721494639.068659692962093299.uzsw4zna3p")
        .header("x-disco-client-id", "web")
        .header("x-fp-api-key", "volo");

    let response = request.send().await?;
    println!("Vendor list API status code: {}", response.status());
    
    let body = response.bytes().await?;
    let vendor_list: VendorListResponse = serde_json::from_slice(&body)?;
    
    Ok(vendor_list)
}

async fn fetch_vendor_details(client_pool: &ClientPool, code: &str) -> Result<serde_json::Value> {
    let url = format!(
        "https://pk.fd-api.com/api/v5/vendors/{}?\
         include=menus,bundles,multiple_discounts&language_id=1&\
         opening_type=delivery&basket_currency=PKR",
        code
    );

    let client = client_pool.next_client();
    let mut request = client.get(&url);

    // Add headers
    request = request
        .header("perseus-client-id", "1737108613136.802524900772077665.hi5re1m8x0")
        .header("perseus-session-id", "1741721494639.068659692962093299.uzsw4zna3p")
        .header("x-fp-api-key", "volo")
        .header("x-pd-language-id", "1");

    let response = request.send().await?;
    println!("Vendor details API status code for {}: {}", code, response.status());
    
    let body = response.bytes().await?;
    let vendor_detail: VendorDetailResponse = serde_json::from_slice(&body)?;
    
    Ok(vendor_detail.data)
}

async fn process_vendor_batch(
    client_pool: &ClientPool,
    vendor_codes: Vec<String>,
    vendors: &mut Vec<Vendor>,
    batch_number: i32,
    total_batches: i32,
) -> Result<()> {
    println!("Processing batch {}/{}", batch_number, total_batches);

    for (index, code) in vendor_codes.iter().enumerate() {
        println!(
            "Batch {}/{} - Processing vendor {}/{}: {}", 
            batch_number, 
            total_batches,
            index + 1, 
            vendor_codes.len(), 
            code
        );
        
        match fetch_vendor_details(&client_pool, code).await {
            Ok(details) => {
                let vendor = Vendor {
                    code: code.clone(),
                    name: details.get("name")
                        .and_then(|n| n.as_str())
                        .unwrap_or("Unknown")
                        .to_string(),
                    details: Some(details),
                    batch_number,  // Added batch number
                };
                vendors.push(vendor);
            }
            Err(e) => {
                eprintln!("Error fetching details for vendor {}: {}", code, e);
            }
        }

        // Rate limiting delay
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    // Log start time and user
    let timestamp = Utc::now().format("%Y-%m-%d %H:%M:%S").to_string();
    let user_login = "xde719";

    println!("Starting extraction at: {}", timestamp);
    println!("User: {}", user_login);

    // Initialize client pool
    let client_pool = Arc::new(ClientPool::new()?);

    // City ID to process
    let city_id = "69036";
    
    // Get initial page to determine total count and page size
    let initial_response = fetch_vendor_page(&client_pool, city_id, 0, 48).await?;
    let total_vendors = initial_response.data.available_count;
    let page_size = initial_response.data.returned_count;
    let total_pages = (total_vendors as f32 / page_size as f32).ceil() as i32;

    println!(
        "Found {} total vendors, will process in {} pages of {} items each",
        total_vendors, total_pages, page_size
    );

    // Create a vector to store all vendor data
    let mut vendors = Vec::with_capacity(total_vendors as usize);

    // Process initial page vendors
    process_vendor_batch(
        &client_pool,
        initial_response.data.items.into_iter().map(|item| item.code).collect(),
        &mut vendors,
        1,
        total_pages
    ).await?;

    // Process remaining pages
    for page in 1..total_pages {
        let offset = page * page_size;
        
        println!("Fetching page {}/{} (offset: {})", page + 1, total_pages, offset);
        
        // Fetch vendor list page using the page_size from the API
        let vendor_list = fetch_vendor_page(&client_pool, city_id, offset, page_size).await?;
        
        // Process vendors in this page
        process_vendor_batch(
            &client_pool,
            vendor_list.data.items.into_iter().map(|item| item.code).collect(),
            &mut vendors,
            page + 1,
            total_pages
        ).await?;

        // Rate limiting delay between pages
        tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
    }

    // Save to JSON file
    let filename = format!("vendors_city_{}_{}_.json", city_id, timestamp.replace(" ", "_"));
    let file = File::create(&filename)?;
    serde_json::to_writer_pretty(file, &vendors)?;

    println!("Successfully saved {} vendors to {}", vendors.len(), filename);
    
    // Print summary
    println!("\nExtraction Summary:");
    println!("Timestamp: {}", timestamp);
    println!("User: {}", user_login);
    println!("City ID: {}", city_id);
    println!("Total Vendors Processed: {}", vendors.len());
    println!("Total Pages Processed: {}", total_pages);
    println!("Page Size: {}", page_size);
    println!("Output File: {}", filename);

    Ok(())
}