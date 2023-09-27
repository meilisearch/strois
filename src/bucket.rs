use std::io::{BufReader, Read};

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
    /// Create a new bucket.
    /// /!\ this method doesn't create the bucket on S3. See [`Self::create`] for that.
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

    #[cfg(feature = "json")]
    pub fn get_object_json<T>(&self, path: &str) -> Result<T>
    where
        T: serde::de::DeserializeOwned,
    {
        let action = self.bucket.get_object(Some(&self.client.cred), path);
        let response = self.client.get(action)?;
        Ok(response.into_json()?)
    }

    pub fn get_object_string(&self, path: &str) -> Result<String> {
        let action = self.bucket.get_object(Some(&self.client.cred), path);
        let response = self.client.get(action)?;
        Ok(response.into_string()?)
    }

    pub fn get_object_bytes(&self, path: &str) -> Result<Vec<u8>> {
        let reader = self.get_object_reader(path)?;
        let mut reader = BufReader::new(reader);
        let mut buffer = Vec::new();
        reader.read_to_end(&mut buffer)?;
        Ok(buffer)
    }

    pub fn get_object_reader(&self, path: &str) -> Result<Box<dyn Read + Send + Sync + 'static>> {
        let action = self.bucket.get_object(Some(&self.client.cred), path);
        let response = self.client.get(action)?;
        Ok(response.into_reader())
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
        let duration = self.client.actions_expires_in;
        let action = CreateMultipartUpload::new(&self.bucket, Some(&self.client.cred), path);
        let url = action.sign(duration);
        let resp = ureq::post(url.as_str()).call()?;

        let body = resp
            .into_string()
            .map_err(InternalError::S3ReturnedNonUtf8Payload)?;

        let multipart =
            CreateMultipartUpload::parse_response(&body).map_err(InternalError::BadS3Payload)?;

        let mut etags = Vec::new();
        let mut buffer = vec![0u8; part_size];

        for part in 1.. {
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

        let action = CompleteMultipartUpload::new(
            &self.bucket,
            Some(&self.client.cred),
            path,
            multipart.upload_id(),
            etags.iter().map(|s| s.as_ref()),
        );
        let url = action.sign(duration);
        ureq::post(url.as_str()).send_string(&action.body())?;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::str::from_utf8;

    use super::*;

    #[derive(Debug, Clone)]
    struct TestBucket(pub Bucket);

    impl std::ops::Deref for TestBucket {
        type Target = Bucket;

        fn deref(&self) -> &Self::Target {
            &self.0
        }
    }

    impl Drop for TestBucket {
        fn drop(&mut self) {
            if let Err(e) = self.delete() {
                eprintln!("{e}");
            }
        }
    }

    fn new_bucket(name: Option<&str>) -> TestBucket {
        let client = Client::builder("http://127.0.0.1:9000")
            .unwrap()
            .key("minioadmin")
            .secret("minioadmin")
            .build();

        let bucket = if let Some(name) = name {
            client.bucket(name).unwrap().create().unwrap()
        } else {
            let uuid = uuid::Uuid::new_v4();
            client.bucket(uuid.to_string()).unwrap().create().unwrap()
        };

        TestBucket(bucket)
    }

    #[test]
    fn create_new_bucket() {
        let bucket = new_bucket(None);
        insta::assert_debug_snapshot!(bucket, @r###"
        TestBucket(
            Bucket {
                client: Client {
                    addr: Url {
                        scheme: "http",
                        cannot_be_a_base: false,
                        username: "",
                        password: None,
                        host: Some(
                            Ipv4(
                                127.0.0.1,
                            ),
                        ),
                        port: Some(
                            9000,
                        ),
                        path: "/",
                        query: None,
                        fragment: None,
                    },
                    cred: Credentials {
                        key: "minioadmin",
                    },
                    actions_expires_in: 3600s,
                    timeout: 60s,
                },
                bucket: Bucket {
                    base_url: Url {
                        scheme: "http",
                        cannot_be_a_base: false,
                        username: "",
                        password: None,
                        host: Some(
                            Ipv4(
                                127.0.0.1,
                            ),
                        ),
                        port: Some(
                            9000,
                        ),
                        path: "/3781df60-fccb-41e8-804c-bd0ac233bc4b/",
                        query: None,
                        fragment: None,
                    },
                    name: "3781df60-fccb-41e8-804c-bd0ac233bc4b",
                    region: "minio",
                },
            },
        )
        "###);
    }

    #[test]
    fn delete_bucket() {
        let bucket = new_bucket(None);
        let ret = bucket.delete();
        insta::assert_debug_snapshot!(ret, @r###"
        Ok(
            (),
        )
        "###);
    }

    #[test]
    fn put_get_delete_object() {
        let bucket = new_bucket(None);
        bucket.put_object("tamo", b"kero").unwrap();

        let content = bucket.get_object("tamo").unwrap();
        let content = from_utf8(&content).unwrap();

        insta::assert_display_snapshot!(content, @"kero");

        bucket.delete_object("tamo").unwrap();

        let ret = bucket.get_object("tamo").unwrap_err();
        insta::assert_display_snapshot!(ret, @"NoSuchKey: The specified key does not exist. on 5adaa4ae-e4e5-4254-b676-381046607655");
    }
}
