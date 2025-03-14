use anyhow::Result;
use chrono::Utc;
use foodpanda_etl::upload_to_minio;  // Replace with your actual crate name
use foodpanda_etl::config::Settings;

#[tokio::main]
async fn main() -> Result<()> {
    let timestamp = Utc::now().format("%Y-%m-%d %H:%M:%S").to_string();
    let user_login = "xde719";
    let city_id = "69036";

    // Use the existing JSON file
    let json_filename = "vendors_city_69036_2025-03-12_21:49:20_.json";
    
    println!("Starting MinIO upload test at: {}", timestamp);
    println!("User: {}", user_login);
    println!("Using JSON file: {}", json_filename);

    // Load settings from config
    let settings = Settings::new()?;

    // Test upload to MinIO
    println!("Testing upload to MinIO...");
    upload_to_minio(
        json_filename,
        city_id,
        &settings.minio.endpoint,
        &settings.minio.access_key,
        &settings.minio.secret_key,
        &settings.minio.bucket,
        &settings.minio.region,
    ).await?;

    println!("Test completed successfully!");

    Ok(())
}