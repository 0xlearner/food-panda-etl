use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::fs::File as TokioFile;
use tokio::io::{AsyncWriteExt, BufWriter as TokioBufWriter};
use crate::error::Result;
use crate::models::Vendor;

pub struct JsonWriter {
    writer: TokioBufWriter<TokioFile>,
    count: AtomicUsize,
    is_first: bool,
}

impl JsonWriter {
    pub async fn new(filename: &str) -> Result<Self> {
        let file = TokioFile::create(filename).await?;
        let mut writer = TokioBufWriter::new(file);
        writer.write_all(b"[\n").await?;
        
        Ok(Self {
            writer,
            count: AtomicUsize::new(0),
            is_first: true,
        })
    }

    pub async fn write_vendor(&mut self, vendor: &Vendor) -> Result<()> {
        if !self.is_first {
            self.writer.write_all(b",\n").await?;
        }
        self.is_first = false;

        let json = serde_json::to_vec(&vendor)?;
        self.writer.write_all(&json).await?;
        self.count.fetch_add(1, Ordering::SeqCst);
        self.writer.flush().await?;
        
        Ok(())
    }

    pub async fn finish(&mut self) -> Result<()> {
        self.writer.write_all(b"\n]").await?;
        self.writer.flush().await?;
        Ok(())
    }

    pub fn get_count(&self) -> usize {
        self.count.load(Ordering::SeqCst)
    }
}