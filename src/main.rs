use std::sync::Arc;
use std::sync::Mutex;
use chrono::Utc;
use anyhow::Result;

use foodpanda_etl::config::Settings;
use foodpanda_etl::{fetch_vendor_page, process_vendor_batch_with_writer, upload_to_minio, sleep_with_jitter, ClientPool, JsonWriter};
#[tokio::main]
async fn main() -> Result<()> {
    let settings = Settings::new()?;
    
    let timestamp = Utc::now().format("%Y-%m-%d %H:%M:%S").to_string();
    let user_login = "xde719";

    println!("Starting extraction at: {}", timestamp);
    println!("User: {}", user_login);

    let client_pool = Arc::new(ClientPool::new()?);
    let city_id = "69036";

    let filename = format!("vendors_city_{}_{}_.json", city_id, timestamp.replace(" ", "_"));
    let json_writer = JsonWriter::new(&filename)?;
    let json_writer = Arc::new(Mutex::new(json_writer));
    
    // Get initial page to determine total count and page size
    let initial_response = fetch_vendor_page(&client_pool, city_id, 0, 48).await?;
    let total_vendors = initial_response.data.available_count;
    let page_size = initial_response.data.returned_count;
    let total_pages = (total_vendors as f32 / page_size as f32).ceil() as i32;

    println!(
        "Found {} total vendors, will process in {} pages of {} items each",
        total_vendors, total_pages, page_size
    );

    let processed_count = Arc::new(std::sync::atomic::AtomicI32::new(0));
    let start_time = std::time::Instant::now();

    // Process all pages
    for page in 0..total_pages {
        let offset = page * page_size;
        
        // Add delay between pages
        if page > 0 {
            sleep_with_jitter(2000, 1000).await;
        }
        
        let response = fetch_vendor_page(&client_pool, city_id, offset, page_size).await?;
        let vendor_codes: Vec<String> = response.data.items
            .into_iter()
            .map(|item| item.code)
            .collect();

        process_vendor_batch_with_writer(
            &client_pool,
            vendor_codes,
            &json_writer,
            page + 1,
            total_pages,
            &processed_count,
            total_vendors,
            start_time,
        ).await?;
    }

    // Finish writing the JSON file and get final count
    let final_count = {
        let writer = json_writer.lock().unwrap();
        writer.finish()?;
        writer.get_count()
    };

    let total_time = start_time.elapsed();

    // Print summary
    println!("\nExtraction Summary:");
    println!("Timestamp: {}", timestamp);
    println!("User: {}", user_login);
    println!("City ID: {}", city_id);
    println!("Total Vendors Processed: {}", final_count);
    println!("Total Pages Processed: {}", total_pages);
    println!("Page Size: {}", page_size);
    println!("Total Time: {:.2} minutes", total_time.as_secs_f64() / 60.0);
    println!("Average Speed: {:.1} vendors/second", final_count as f64 / total_time.as_secs_f64());
    println!("Output File: {}", filename);

    // After processing is complete, upload to MinIO
    println!("Uploading data to MinIO...");
    upload_to_minio(
        &filename,
        city_id,
        &settings.minio.endpoint,
        &settings.minio.access_key,
        &settings.minio.secret_key,
        &settings.minio.bucket,
        &settings.minio.region,
    ).await?;

    println!("Data upload complete!");

    Ok(())
}