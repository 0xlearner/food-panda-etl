use rquest::Client;
use rquest_util::Emulation;
use serde::{Deserialize, Serialize};
use std::fs::File;
use anyhow::Result;
use std::io::{BufWriter, Write, Read};
use std::sync::Arc;
use std::sync::Mutex;
use rand::Rng;
use std::time::Duration;
use arrow::array::{StringArray, Int32Array, Int64Array};
use arrow::record_batch::RecordBatch;
use arrow::datatypes::{DataType, Field, Schema};
use parquet::arrow::ArrowWriter;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client as S3Client;
use aws_sdk_s3::config::{Credentials, Region};
use chrono::{Datelike, Utc};
use std::io::BufReader;
pub mod config;

pub async fn sleep_with_jitter(base_ms: u64, jitter_ms: u64) {
    let jitter = rand::rng().random_range(0..=jitter_ms);
    tokio::time::sleep(Duration::from_millis(base_ms + jitter)).await;
}

pub async fn retry_with_backoff<T, F, Fut>(
    mut retries: u32,
    base_delay_ms: u64,
    operation: F,
) -> Result<T>
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = Result<T>>,
{
    let mut delay = base_delay_ms;
    
    loop {
        match operation().await {
            Ok(result) => return Ok(result),
            Err(e) => {
                if retries == 0 {
                    return Err(e);
                }
                
                println!("Request failed, retrying in {}ms: {}", delay, e);
                sleep_with_jitter(delay, delay / 2).await;
                
                retries -= 1;
                delay *= 2; // Exponential backoff
            }
        }
    }
}

// Enhanced Models
#[derive(Debug, Deserialize)]
pub struct VendorListResponse {
    pub data: VendorData,
}

#[derive(Debug, Deserialize)]
pub struct VendorData {
    pub items: Vec<VendorItem>,
    pub returned_count: i32,      // Number of vendors in current page
    pub available_count: i32,     // Total available vendors
}

#[derive(Debug, Deserialize)]
pub struct VendorItem {
    pub code: String,
}

// Modify the Vendor struct to include the new fields
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Vendor {
    pub code: String,
    pub name: String,
    pub details: Option<serde_json::Value>,
    pub batch_number: i32,
    pub reviews: Option<Vec<serde_json::Value>>,
    pub ratings: Option<RatingsDistribution>,
    #[serde(with = "chrono::serde::ts_seconds")]
    pub extraction_started_at: chrono::DateTime<chrono::Utc>,
    #[serde(with = "chrono::serde::ts_seconds")]
    pub extraction_completed_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Deserialize)]
struct VendorDetailResponse {
    data: serde_json::Value,
}

// New models for reviews and ratings
#[derive(Debug, Clone, Serialize, Deserialize)]
struct ReviewsResponse {
    data: Vec<serde_json::Value>,  // Using Value since we want the entire data array
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RatingScore {
    pub count: i32,
    pub percentage: i32,
    pub score: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RatingsDistribution {
    #[serde(rename = "totalCount")]
    pub total_count: i32,
    #[serde(rename = "createdAt")]
    pub created_at: String,
    #[serde(rename = "updatedAt")]
    pub updated_at: String,
    pub ratings: Vec<RatingScore>,
}

pub struct ClientPool {
    pub clients: Vec<Client>,
    pub current: std::sync::atomic::AtomicUsize,
}

impl ClientPool {
    pub fn new() -> Result<Self> {
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

    pub fn next_client(&self) -> &Client {
        let current = self.current.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        &self.clients[current % self.clients.len()]
    }
}

pub async fn fetch_vendor_page(
    client_pool: &ClientPool, 
    city_id: &str, 
    offset: i32, 
    limit: i32
) -> Result<VendorListResponse> {
    retry_with_backoff(3, 1000, || async {
        let url = format!(
            "https://disco.deliveryhero.io/listing/api/v1/pandora/vendors?\
             configuration=&country=pk&city_id={}&include=&language_id=1&\
             sort=&offset={}&limit={}&vertical=restaurants",
            city_id, offset, limit
        );

        let client = client_pool.next_client();
        let mut request = client.get(&url);

        request = request
            .header("perseus-client-id", "1737108613136.802524900772077665.hi5re1m8x0")
            .header("perseus-session-id", "1741721494639.068659692962093299.uzsw4zna3p")
            .header("x-disco-client-id", "web")
            .header("x-fp-api-key", "volo");

        let response = request.send().await?;
        let status = response.status();
        
        if status == 429 {
            return Err(anyhow::anyhow!("Rate limited"));
        }
        
        println!("Vendor list API status code: {}", status);
        
        let body = response.bytes().await?;
        let vendor_list: VendorListResponse = serde_json::from_slice(&body)?;
        
        Ok(vendor_list)
    }).await
}

pub async fn fetch_vendor_details(client_pool: &ClientPool, code: &str) -> Result<serde_json::Value> {
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

// New functions to fetch reviews and ratings
pub async fn fetch_vendor_reviews(
    client_pool: &ClientPool,
    vendor_code: &str,
) -> Result<Vec<serde_json::Value>> {
    let url = format!(
        "https://reviews-api-pk.fd-api.com/reviews/vendor/{}?\
         global_entity_id=FP_PK&limit=30&created_at=desc&has_dish=true",
        vendor_code
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
    println!("Reviews API status code for {}: {}", vendor_code, response.status());
    
    let body = response.bytes().await?;
    let reviews: ReviewsResponse = serde_json::from_slice(&body)?;
    
    Ok(reviews.data)
}

pub async fn fetch_vendor_ratings(
    client_pool: &ClientPool,
    vendor_code: &str,
) -> Result<RatingsDistribution> {
    let url = format!(
        "https://reviews-api-pk.fd-api.com/ratings-distribution/vendor/{}?\
         global_entity_id=FP_PK",
        vendor_code
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
    println!("Ratings API status code for {}: {}", vendor_code, response.status());
    
    let body = response.bytes().await?;
    let ratings: RatingsDistribution = serde_json::from_slice(&body)?;
    
    Ok(ratings)
}

pub async fn process_vendor_batch_with_writer(
    client_pool: &ClientPool,
    vendor_codes: Vec<String>,
    json_writer: &Arc<Mutex<JsonWriter>>,
    batch_number: i32,
    total_batches: i32,
    processed_count: &Arc<std::sync::atomic::AtomicI32>,
    total_vendors: i32,
    start_time: std::time::Instant,
) -> Result<()> {
    println!("Processing batch {}/{}", batch_number, total_batches);

    for (index, code) in vendor_codes.iter().enumerate() {
        let current_count = processed_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let extraction_started_at = chrono::Utc::now();
        
        // Calculate progress and ETA
        let elapsed = start_time.elapsed();
        let items_per_second = current_count as f64 / elapsed.as_secs_f64().max(1.0);
        let remaining_items = total_vendors - current_count;
        let estimated_seconds_remaining = remaining_items as f64 / items_per_second;
        
        println!(
            "Batch {}/{} - Processing vendor {}/{}: {} (ETA: {:.1} minutes remaining)", 
            batch_number, 
            total_batches,
            index + 1, 
            vendor_codes.len(), 
            code,
            estimated_seconds_remaining / 60.0
        );
        
        // Add random delay between vendors (1.5 to 2.5 seconds)
        sleep_with_jitter(1500, 1000).await;
        
        let details_result = retry_with_backoff(3, 1000, || async {
            fetch_vendor_details(&client_pool, code).await
        }).await;
        
        match details_result {
            Ok(details) => {
                // Add delay before fetching reviews and ratings
                sleep_with_jitter(800, 400).await;
                
                let (reviews_result, ratings_result) = tokio::join!(
                    retry_with_backoff(2, 1000, || async {
                        fetch_vendor_reviews(&client_pool, code).await
                    }),
                    retry_with_backoff(2, 1000, || async {
                        fetch_vendor_ratings(&client_pool, code).await
                    })
                );
                
                let extraction_completed_at = chrono::Utc::now();
                
                let vendor = Vendor {
                    code: code.clone(),
                    name: details.get("name")
                        .and_then(|n| n.as_str())
                        .unwrap_or("Unknown")
                        .to_string(),
                    details: Some(details),
                    batch_number,
                    reviews: reviews_result.ok(),
                    ratings: ratings_result.ok(),
                    extraction_started_at,
                    extraction_completed_at,
                };
                
                if let Ok(writer) = json_writer.lock() {
                    if let Err(e) = writer.write_vendor(&vendor) {
                        eprintln!("Error writing vendor to file: {}", e);
                    }
                }
            }
            Err(e) => {
                eprintln!("Error fetching details for vendor {}: {}", code, e);
                continue;
            }
        }
    }

    Ok(())
}

pub struct JsonWriter {
    pub writer: Mutex<BufWriter<File>>,
    pub count: std::sync::atomic::AtomicUsize,
    pub first: std::sync::atomic::AtomicBool,
}

impl JsonWriter {
    pub fn new(filename: &str) -> Result<Self> {
        let file = File::create(filename)?;
        let mut writer = BufWriter::new(file);
        
        // Write the opening bracket
        writer.write_all(b"[\n")?;
        
        Ok(Self {
            writer: Mutex::new(writer),
            count: std::sync::atomic::AtomicUsize::new(0),
            first: std::sync::atomic::AtomicBool::new(true),
        })
    }

    pub fn write_vendor(&self, vendor: &Vendor) -> Result<()> {
        let mut writer = self.writer.lock().unwrap();
        
        // Add comma if not the first item
        if !self.first.load(std::sync::atomic::Ordering::SeqCst) {
            writer.write_all(b",\n")?;
        } else {
            // Set first to false after writing first item
            self.first.store(false, std::sync::atomic::Ordering::SeqCst);
        }
        
        // Validate JSON before writing
        let json_str = serde_json::to_string(&vendor)?;
        
        // Write the validated JSON
        writer.write_all(json_str.as_bytes())?;
        
        // Flush periodically (every 10 vendors)
        if self.count.fetch_add(1, std::sync::atomic::Ordering::SeqCst) % 10 == 0 {
            writer.flush()?;
        }
        
        Ok(())
    }

    pub fn finish(&self) -> Result<()> {
        let mut writer = self.writer.lock().unwrap();
        writer.write_all(b"\n]")?;
        writer.flush()?;
        Ok(())
    }

    pub fn get_count(&self) -> usize {
        self.count.load(std::sync::atomic::Ordering::SeqCst)
    }
}

pub async fn upload_to_minio(
    json_path: &str,
    city_id: &str,
    minio_endpoint: &str,
    access_key: &str,
    secret_key: &str,
    bucket: &str,
    region: &str,
) -> Result<()> {
    // Read and parse JSON file once
    println!("Reading and validating JSON file...");
    let file = File::open(json_path)?;
    let reader = BufReader::new(file);
    let vendors: Vec<Vendor> = serde_json::from_reader(reader)
        .map_err(|e| anyhow::anyhow!("Invalid JSON file: {}. Error at line {}, column {}", 
            e.to_string(), e.line(), e.column()))?;

    println!("JSON validation successful. Found {} vendors.", vendors.len());
    
    // Initialize S3 client
    let credentials = Credentials::new(
        access_key,
        secret_key,
        None,
        None,
        "static-credentials",
    );

    let region = Region::new(region.to_string());
    let s3_config = aws_sdk_s3::Config::builder()
        .behavior_version_latest() 
        .endpoint_url(minio_endpoint)
        .region(region)
        .credentials_provider(credentials)
        .force_path_style(true)
        .build();

    let s3_client = S3Client::from_conf(s3_config);

    // Pre-process string data
    let details_strings: Vec<Option<String>> = vendors.iter()
        .map(|v| v.details.as_ref()
            .map(|d| serde_json::to_string(d))
            .transpose()
            .unwrap_or(None))
        .collect();

    let reviews_strings: Vec<Option<String>> = vendors.iter()
        .map(|v| v.reviews.as_ref()
            .map(|r| serde_json::to_string(r))
            .transpose()
            .unwrap_or(None))
        .collect();

    let ratings_strings: Vec<Option<String>> = vendors.iter()
        .map(|v| v.ratings.as_ref()
            .map(|r| serde_json::to_string(r))
            .transpose()
            .unwrap_or(None))
        .collect();

    // Create Arrow arrays
    let codes: StringArray = vendors.iter()
        .map(|v| Some(v.code.as_str()))
        .collect();

    let names: StringArray = vendors.iter()
        .map(|v| Some(v.name.as_str()))
        .collect();

    let details: StringArray = details_strings.iter()
        .map(|opt| opt.as_deref())
        .collect();

    let batch_numbers: Int32Array = vendors.iter()
        .map(|v| Some(v.batch_number))
        .collect();

    let reviews: StringArray = reviews_strings.iter()
        .map(|opt| opt.as_deref())
        .collect();

    let ratings: StringArray = ratings_strings.iter()
        .map(|opt| opt.as_deref())
        .collect();

    let extraction_started_at: Int64Array = vendors.iter()
        .map(|v| Some(v.extraction_started_at.timestamp()))
        .collect();

    let extraction_completed_at: Int64Array = vendors.iter()
        .map(|v| Some(v.extraction_completed_at.timestamp()))
        .collect();

    // Create schema
    let schema = Arc::new(Schema::new(vec![
        Field::new("code", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("details", DataType::Utf8, true),
        Field::new("batch_number", DataType::Int32, false),
        Field::new("reviews", DataType::Utf8, true),
        Field::new("ratings", DataType::Utf8, true),
        Field::new("extraction_started_at", DataType::Int64, false),
        Field::new("extraction_completed_at", DataType::Int64, false),
    ]));

    // Create RecordBatch
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(codes),
            Arc::new(names),
            Arc::new(details),
            Arc::new(batch_numbers),
            Arc::new(reviews),
            Arc::new(ratings),
            Arc::new(extraction_started_at),
            Arc::new(extraction_completed_at),
        ],
    )?;

    // Create and write to temporary Parquet file
    let now = Utc::now();
    let temp_parquet = tempfile::NamedTempFile::new()?;
    
    let output_file = File::create(temp_parquet.path())?;
    let mut writer = ArrowWriter::try_new(output_file, schema, None)?;
    writer.write(&batch)?;
    writer.close()?;

    // Generate S3 key with partitioning
    let s3_key = format!(
        "city_id={}/year={}/month={:02}/day={:02}/vendors_{}.parquet",
        city_id,
        now.year(),
        now.month(),
        now.day(),
        now.timestamp()
    );

    // Upload to MinIO using multipart upload for large files
    println!("Uploading Parquet file to MinIO...");
    
    const CHUNK_SIZE: usize = 8 * 1024 * 1024; // 8MB chunks
    let file_size = temp_parquet.as_file().metadata()?.len() as usize;
    
    if file_size > CHUNK_SIZE {
        println!("Large file detected ({}MB), using multipart upload", file_size / 1024 / 1024);
        
        // Initialize multipart upload
        let create_multipart_res = s3_client
            .create_multipart_upload()
            .bucket(bucket)
            .key(&s3_key)
            .content_type("application/x-parquet")
            .send()
            .await?;
            
        let upload_id = create_multipart_res.upload_id()
            .ok_or_else(|| anyhow::anyhow!("Failed to get upload ID"))?;
            
        let mut part_number = 1;
        let mut completed_parts = Vec::new();
        let mut file = File::open(temp_parquet.path())?;
        
        // Upload parts
        loop {
            let mut buffer = vec![0; CHUNK_SIZE];
            let n = file.read(&mut buffer)?;
            if n == 0 { break; }
            buffer.truncate(n);
            
            let part_res = s3_client
                .upload_part()
                .bucket(bucket)
                .key(&s3_key)
                .upload_id(upload_id)
                .body(ByteStream::from(buffer))
                .part_number(part_number)
                .send()
                .await?;
                
            completed_parts.push(
                aws_sdk_s3::types::CompletedPart::builder()
                    .e_tag(part_res.e_tag.unwrap_or_default())
                    .part_number(part_number)
                    .build()
            );
            
            println!("Uploaded part {} of approximately {}", 
                part_number, 
                (file_size + CHUNK_SIZE - 1) / CHUNK_SIZE);
            
            part_number += 1;
        }
        
        // Complete multipart upload
        let completed_upload = aws_sdk_s3::types::CompletedMultipartUpload::builder()
            .set_parts(Some(completed_parts))
            .build();
            
        s3_client
            .complete_multipart_upload()
            .bucket(bucket)
            .key(&s3_key)
            .upload_id(upload_id)
            .multipart_upload(completed_upload)
            .send()
            .await?;
    } else {
        // Small file - use simple upload
        let body = ByteStream::from_path(temp_parquet.path()).await?;
        s3_client
            .put_object()
            .bucket(bucket)
            .key(&s3_key)
            .body(body)
            .content_type("application/x-parquet")
            .send()
            .await?;
    }

    println!("Successfully uploaded {} rows to s3://{}/{}", batch.num_rows(), bucket, s3_key);
    Ok(())
}
