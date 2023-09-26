use std::{io::Read, time::Duration};

use http::header::ETAG;
use rusty_s3::{
    actions::{CompleteMultipartUpload, CreateMultipartUpload, UploadPart},
    S3Action, UrlStyle,
};

use crate::{error::InternalError, Client, Result};

#[derive(Debug, Clone)]
pub struct Bucket {
    client: Client,
    bucket: rusty_s3::Bucket,
}

impl Bucket {
    pub fn new(client: Client, bucket: impl Into<String>) -> Result<Self> {
        Ok(Self {
            bucket: rusty_s3::Bucket::new(
                client.addr.clone(),
                UrlStyle::Path,
                bucket.into(),
                "minio",
            )?,
            client,
        })
    }

    pub fn create(&self) -> Result<Self> {
        let action = self.bucket.create_bucket(&self.client.cred);
        self.client.put(action)?;
        Ok(self.clone())
    }

    pub fn delete(&self) -> Result<()> {
        let action = self.bucket.delete_bucket(&self.client.cred);
        self.client.delete(action)?;
        Ok(())
    }

    pub fn put_object(&self, path: &str, content: &[u8]) -> Result<()> {
        let action = self.bucket.put_object(Some(&self.client.cred), path);
        self.client.put_with_body(action, content)?;
        Ok(())
    }

    pub fn delete_object(&self, path: &str) -> Result<()> {
        let action = self.bucket.delete_object(Some(&self.client.cred), path);
        self.client.delete(action)?;
        Ok(())
    }

    pub fn put_object_multipart(
        &self,
        path: &str,
        mut content: impl Read,
        part_size: usize,
    ) -> Result<()> {
        let duration = Duration::from_secs(60 * 60);
        let action = CreateMultipartUpload::new(&self.bucket, Some(&self.client.cred), path);
        let url = action.sign(duration);
        let resp = ureq::post(url.as_str()).call()?;

        let body = resp
            .into_string()
            .map_err(InternalError::S3ReturnedNonUtf8Payload)?;

        let multipart =
            CreateMultipartUpload::parse_response(&body).map_err(InternalError::BadS3Payload2)?;

        println!(
            "multipart upload created - upload id: {}",
            multipart.upload_id()
        );

        let mut etags = Vec::new();
        let mut buffer = vec![0u8; part_size];

        for part in 1..=10_000 {
            let mut buf = &mut buffer[..];
            let mut size = 0;

            while !buf.is_empty() {
                let read = content.read(buf)?;
                size += read;
                if read == 0 {
                    break;
                }
                buf = &mut buf[read..];
            }

            let buffer = &buffer[..size];
            if buffer.is_empty() {
                break;
            }

            let part_upload = UploadPart::new(
                &self.bucket,
                Some(&self.client.cred),
                path,
                part,
                multipart.upload_id(),
            );

            let url = part_upload.sign(duration);

            let resp = ureq::put(url.as_str()).send_bytes(buffer)?;
            let etag = resp
                .header(ETAG.as_str())
                .expect("every UploadPart request returns an Etag");
            etags.push(etag.trim_matches('"').to_string());
        }
        eprintln!("{etags:?}");

        let action = CompleteMultipartUpload::new(
            &self.bucket,
            Some(&self.client.cred),
            path,
            multipart.upload_id(),
            etags.iter().map(|s| s.as_ref()),
        );
        let url = action.sign(duration);

        let resp = ureq::post(url.as_str())
            .send_string(&dbg!(action.body()))
            .unwrap();
        let body = resp.into_string().unwrap();
        println!("it worked! {body}");
        Ok(())
    }
}
