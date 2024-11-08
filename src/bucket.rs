use std::{
    fs::File,
    io::{BufReader, BufWriter, Read, Write},
    path::Path,
};

use http::header::ETAG;

use rusty_s3::{
    actions::{
        list_objects_v2::ListObjectsContent, CompleteMultipartUpload, CreateMultipartUpload,
        CreateMultipartUploadResponse, ListObjectsV2, ListObjectsV2Response, UploadPart,
    },
    UrlStyle,
};

use crate::{
    builder::MissingCred, error::InternalError, Builder, Client, Error, Result, S3ErrorCode,
    UserError,
};

#[derive(Debug, Clone)]
pub struct Bucket {
    client: Client,
    bucket: rusty_s3::Bucket,
}

impl Bucket {
    /// Create a new [`Builder`].
    /// It's currently missing its key and secret.
    ///
    /// # Example
    /// ```
    /// use strois::Builder;
    ///
    /// let bucket = Builder::new("http://localhost:9000")?
    ///     .key("minioadmin")
    ///     .secret("minioadmin")
    ///     .bucket("tamo");
    /// # Ok::<(), strois::Error>(())
    /// ```
    pub fn builder(url: impl AsRef<str>) -> Result<Builder<MissingCred>> {
        Builder::new(url)
    }

    /// Create a new [`Builder`] from a `Region`.
    /// It's currently missing its key and secret.
    ///
    /// # Example
    /// ```
    /// use strois::Builder;
    ///
    /// let bucket = Builder::new("us-east-1".parse()?)?
    ///     .key("minioadmin")
    ///     .secret("minioadmin")
    ///     .bucket("tamo");
    /// # Ok::<(), strois::Error>(())
    /// ```
    #[cfg(feature="aws-region")]
    pub fn region_builder(region: awsregion::Region) -> Builder<MissingCred> {
        Builder::new_region(region)
    }

    /// Create a new bucket.
    /// /!\ this method doesn't create the bucket on S3. See [`Self::create`] for that.
    pub fn new(client: Client, bucket: impl Into<String>, url_style: UrlStyle) -> Result<Self> {
        Ok(Self {
            bucket: rusty_s3::Bucket::new(
                client.addr.clone(),
                url_style,
                bucket.into(),
                client.region.clone(),
            )?,
            client,
        })
    }

    /// Create a new bucket on S3.
    ///
    /// # Example
    /// ```
    /// use strois::{Builder, Error, S3ErrorCode};
    ///
    /// let bucket = Builder::new("http://localhost:9000")?
    ///     .key("minioadmin")
    ///     .secret("minioadmin")
    ///     .with_url_path_style(true)
    ///     .bucket("tamo")?;
    ///
    /// match bucket.create() {
    ///   Ok(_) => (), // the bucket was created on S3
    ///   Err(Error::S3Error(error)) if matches!(error.code, S3ErrorCode::BucketAlreadyExists | S3ErrorCode::BucketAlreadyOwnedByYou) => (), // the bucket already exists.
    ///   Err(e) => return Err(e),
    /// }
    /// # Ok::<(), strois::Error>(())
    /// ```
    pub fn create(&self) -> Result<Self> {
        let action = self.bucket.create_bucket(&self.client.cred);
        self.client.put(action)?;
        Ok(self.clone())
    }

    /// Get or create a new bucket on S3.
    ///
    /// # Example
    /// ```
    /// use strois::{Builder};
    ///
    /// let bucket = Builder::new("http://localhost:9000")?
    ///     .key("minioadmin")
    ///     .secret("minioadmin")
    ///     .with_url_path_style(true)
    ///     .bucket("tamo")?
    ///     .get_or_create()?;
    ///
    /// # Ok::<(), strois::Error>(())
    /// ```
    pub fn get_or_create(&self) -> Result<Self> {
        match self.create() {
            Ok(bucket) => Ok(bucket),
            Err(Error::S3Error(e))
                if matches!(
                    e.code,
                    S3ErrorCode::BucketAlreadyExists | S3ErrorCode::BucketAlreadyOwnedByYou
                ) =>
            {
                Ok(self.clone())
            }
            e => e,
        }
    }

    /// Get or create a new bucket on S3.
    ///
    /// # Example
    /// ```
    /// use strois::{Builder, Error, S3ErrorCode};
    ///
    /// let bucket = Builder::new("http://localhost:9000")?
    ///     .key("minioadmin")
    ///     .secret("minioadmin")
    ///     .with_url_path_style(true)
    ///     .bucket("to-delete")?;
    ///
    /// match bucket.delete() {
    ///   Ok(_) => (), // the bucket was successfully deleted
    ///   Err(Error::S3Error(error)) if matches!(error.code, S3ErrorCode::NoSuchBucket) => (), // the bucket doesn't exists.
    ///   Err(e) => return Err(e),
    /// }
    ///
    /// # Ok::<(), strois::Error>(())
    /// ```
    pub fn delete(&self) -> Result<()> {
        let action = self.bucket.delete_bucket(&self.client.cred);
        self.client.delete(action)?;
        Ok(())
    }

    /// Get a json object and deserialize it on the fly.
    /// Returns an error if it can't be deserialized.
    ///
    /// # Example
    /// ```
    /// use strois::Builder;
    ///
    /// let bucket = Builder::new("http://localhost:9000")?
    ///     .key("minioadmin")
    ///     .secret("minioadmin")
    ///     .with_url_path_style(true)
    ///     .bucket("tamo")?
    ///     .get_or_create()?;
    ///
    /// bucket.put_object("tamo", "{ \"doggo\": \"golden retriever\" }")?;
    ///
    /// #[derive(serde::Deserialize)]
    /// struct Doggo {
    ///   doggo: String,
    /// }
    ///
    /// let tamo: Doggo = bucket.get_object_json("tamo")?;
    /// assert_eq!(tamo.doggo, "golden retriever");
    ///
    /// # Ok::<(), strois::Error>(())
    /// ```
    #[cfg(feature = "json")]
    pub fn get_object_json<T>(&self, path: impl AsRef<str>) -> Result<T>
    where
        T: serde::de::DeserializeOwned,
    {
        let action = self
            .bucket
            .get_object(Some(&self.client.cred), path.as_ref());
        let response = self.client.get(action)?;
        Ok(response.into_json()?)
    }

    /// Get an object as a string.
    /// Returns an error if it's not an utf-8 valid string.
    ///
    /// # Example
    /// ```
    /// use strois::Builder;
    ///
    /// let bucket = Builder::new("http://localhost:9000")?
    ///     .key("minioadmin")
    ///     .secret("minioadmin")
    ///     .with_url_path_style(true)
    ///     .bucket("tamo")?
    ///     .get_or_create()?;
    ///
    /// bucket.put_object("tamo", "kero")?;
    ///
    /// let tamo = bucket.get_object_string("tamo")?;
    /// assert_eq!(tamo, "kero");
    ///
    /// # Ok::<(), strois::Error>(())
    /// ```
    pub fn get_object_string(&self, path: impl AsRef<str>) -> Result<String> {
        let bytes = self.get_object_bytes(path)?;
        Ok(String::from_utf8(bytes).map_err(UserError::PayloadCouldNotBeConvertedToString)?)
    }

    /// Get an object as raw bytes.
    ///
    /// # Example
    /// ```
    /// use strois::Builder;
    ///
    /// let bucket = Builder::new("http://localhost:9000")?
    ///     .key("minioadmin")
    ///     .secret("minioadmin")
    ///     .with_url_path_style(true)
    ///     .bucket("tamo")?
    ///     .get_or_create()?;
    ///
    /// bucket.put_object("tamo", "kero")?;
    ///
    /// let tamo = bucket.get_object_bytes("tamo")?;
    /// assert_eq!(tamo, b"kero");
    ///
    /// # Ok::<(), strois::Error>(())
    /// ```
    pub fn get_object_bytes(&self, path: impl AsRef<str>) -> Result<Vec<u8>> {
        let reader = self.get_object_reader(path.as_ref())?;
        let mut reader = BufReader::new(reader);
        let mut buffer = Vec::new();
        reader.read_to_end(&mut buffer)?;
        Ok(buffer)
    }

    /// Get a reader over an object.
    ///
    /// # Example
    /// ```
    /// use strois::Builder;
    ///
    /// let bucket = Builder::new("http://localhost:9000")?
    ///     .key("minioadmin")
    ///     .secret("minioadmin")
    ///     .with_url_path_style(true)
    ///     .bucket("tamo")?
    ///     .get_or_create()?;
    ///
    /// bucket.put_object("tamo", "kero")?;
    ///
    /// let mut tamo = bucket.get_object_reader("tamo")?;
    /// let mut ret = String::new();
    /// tamo.read_to_string(&mut ret)?;
    /// assert_eq!(ret, "kero");
    ///
    /// # Ok::<(), strois::Error>(())
    /// ```
    pub fn get_object_reader(
        &self,
        path: impl AsRef<str>,
    ) -> Result<Box<dyn Read + Send + Sync + 'static>> {
        let action = self
            .bucket
            .get_object(Some(&self.client.cred), path.as_ref());
        let response = self.client.get(action)?;
        Ok(response.into_reader())
    }

    /// Download and write an object to a writer.
    ///
    /// # Example
    /// ```
    /// use strois::Builder;
    ///
    /// let bucket = Builder::new("http://localhost:9000")?
    ///     .key("minioadmin")
    ///     .secret("minioadmin")
    ///     .with_url_path_style(true)
    ///     .bucket("tamo")?
    ///     .get_or_create()?;
    ///
    /// bucket.put_object("tamo", "kero")?;
    ///
    /// let mut tamo: Vec<u8> = Vec::new();
    /// bucket.get_object_to_writer("tamo", &mut tamo)?;
    /// assert_eq!(tamo, b"kero");
    ///
    /// # Ok::<(), strois::Error>(())
    /// ```
    pub fn get_object_to_writer(&self, path: impl AsRef<str>, writer: impl Write) -> Result<u64> {
        let reader = self.get_object_reader(path)?;
        let mut reader = BufReader::new(reader);
        let mut writer = BufWriter::new(writer);
        let size = std::io::copy(&mut reader, &mut writer)?;
        Ok(size)
    }

    pub fn get_object_to_file(&self, path: impl AsRef<str>, file: impl AsRef<Path>) -> Result<u64> {
        let reader = self.get_object_reader(path)?;
        let mut reader = BufReader::new(reader);
        let file = File::open(file)?;
        let mut writer = BufWriter::new(file);
        let size = std::io::copy(&mut reader, &mut writer)?;
        Ok(size)
    }

    pub fn list_objects(&self, prefix: impl AsRef<str>) -> Result<ListObjectIterator> {
        let mut action = self.bucket.list_objects_v2(Some(&self.client.cred));
        action.with_prefix(prefix.as_ref());
        let response = self.client.get(action)?;
        let response = response.into_string()?;
        let response = match ListObjectsV2::parse_response(&response) {
            Ok(response) => response,
            Err(e) => return Err(InternalError::BadS3Payload(e).into()),
        };

        Ok(ListObjectIterator {
            current_bucket: response.contents.into_iter(),
            continuation_token: response.next_continuation_token,
            bucket: self.clone(),
        })
    }

    pub fn delete_object(&self, path: impl AsRef<str>) -> Result<()> {
        let action = self
            .bucket
            .delete_object(Some(&self.client.cred), path.as_ref());
        self.client.delete(action)?;
        Ok(())
    }

    pub fn put_object(&self, path: impl AsRef<str>, content: impl AsRef<[u8]>) -> Result<()> {
        let action = self
            .bucket
            .put_object(Some(&self.client.cred), path.as_ref());
        let content = content.as_ref();
        self.client.put_with_body(action, content, content.len())?;
        Ok(())
    }

    pub fn put_object_reader(
        &self,
        path: impl AsRef<str>,
        content: impl Read,
        length: usize,
    ) -> Result<()> {
        let action = self
            .bucket
            .put_object(Some(&self.client.cred), path.as_ref());
        self.client.put_with_body(action, content, length)?;
        Ok(())
    }

    pub fn starts_multipart<'a>(&'a self, path: &'a str) -> Result<Multipart> {
        let action = CreateMultipartUpload::new(&self.bucket, Some(&self.client.cred), path);
        let resp = self.client.post(action)?;
        let body = resp
            .into_string()
            .map_err(InternalError::S3ReturnedNonUtf8Payload)?;

        let multipart =
            CreateMultipartUpload::parse_response(&body).map_err(InternalError::BadS3Payload)?;

        Ok(Multipart {
            bucket: self,
            multipart,
            path,
            part: 1,
            etags: Vec::new(),
        })
    }

    pub fn put_object_multipart(
        &self,
        path: impl AsRef<str>,
        mut content: impl Read,
    ) -> Result<()> {
        let path = path.as_ref();
        let mut multipart = self.starts_multipart(path)?;

        let mut buffer = vec![0u8; self.client.multipart_size];

        loop {
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

            multipart.upload_part(buffer)?;
        }

        multipart.complete()
    }

    /// Put a file on S3.
    pub fn put_object_file(&self, path: impl AsRef<str>, file: impl AsRef<Path>) -> Result<()> {
        const MINIMAL_PUT_OBJECT_SIZE: u64 = 5 * 1024 * 1024; // 5MiB
        let file = File::open(file)?;
        let size = file.metadata()?.len();

        if size > MINIMAL_PUT_OBJECT_SIZE {
            let reader = BufReader::new(file);
            self.put_object_multipart(path, reader)?;
        } else {
            let reader = BufReader::new(file);
            self.put_object_reader(path, reader, size as usize)?;
        }

        Ok(())
    }
}

pub struct Multipart<'a> {
    bucket: &'a Bucket,
    multipart: CreateMultipartUploadResponse,
    path: &'a str,
    etags: Vec<String>,
    part: u16,
}

impl Multipart<'_> {
    pub fn upload_part(&mut self, buffer: impl AsRef<[u8]>) -> Result<()> {
        if self.part > 10_000 {
            return Err(UserError::TriedToSendMoreThan10000PartsInMultiPart.into());
        }
        let part_upload = UploadPart::new(
            &self.bucket.bucket,
            Some(&self.bucket.client.cred),
            self.path,
            self.part,
            self.multipart.upload_id(),
        );

        let buffer = buffer.as_ref();
        let response = self
            .bucket
            .client
            .put_with_body(part_upload, buffer, buffer.len())
            .unwrap();

        let etag = response.header(ETAG.as_str()).ok_or_else(|| {
            InternalError::MultipartMissingEtagHeader(response.headers_names().join(", "))
        })?;
        self.etags.push(etag.trim_matches('"').to_string());
        self.part += 1;

        Ok(())
    }

    pub fn complete(self) -> Result<()> {
        let action = CompleteMultipartUpload::new(
            &self.bucket.bucket,
            Some(&self.bucket.client.cred),
            self.path,
            self.multipart.upload_id(),
            self.etags.iter().map(|s| s.as_str()),
        );

        let body = action.clone().body();
        self.bucket
            .client
            .post_with_body(action, &mut body.as_bytes(), body.len())?;

        Ok(())
    }
}

pub struct ListObjectIterator {
    current_bucket: std::vec::IntoIter<ListObjectsContent>,
    continuation_token: Option<String>,
    bucket: Bucket,
}

impl Iterator for ListObjectIterator {
    type Item = Result<ListObjectsContent>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.current_bucket.next() {
            Some(ret) => Some(Ok(ret)),
            None => {
                let token = self.continuation_token.as_ref()?;
                let mut action = self
                    .bucket
                    .bucket
                    .list_objects_v2(Some(&self.bucket.client.cred));
                action.with_continuation_token(token);
                let response = match self.bucket.client.get(action) {
                    Ok(response) => response,
                    Err(e) => return Some(Err(e)),
                };
                let response = match response.into_string() {
                    Ok(response) => response,
                    Err(e) => return Some(Err(e.into())),
                };
                let response = match ListObjectsV2::parse_response(&response) {
                    Ok(response) => response,
                    Err(e) => return Some(Err(InternalError::BadS3Payload(e).into())),
                };
                let ListObjectsV2Response {
                    contents,
                    max_keys: _,
                    common_prefixes: _,
                    next_continuation_token,
                    start_after: _,
                    ..
                } = response;
                self.continuation_token = next_continuation_token;
                self.current_bucket = contents.into_iter();
                self.next()
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[derive(Debug, Clone)]
    struct TestBucket(pub Bucket);

    impl std::ops::Deref for TestBucket {
        type Target = Bucket;

        fn deref(&self) -> &Self::Target {
            &self.0
        }
    }

    impl std::ops::DerefMut for TestBucket {
        fn deref_mut(&mut self) -> &mut Self::Target {
            &mut self.0
        }
    }

    impl Drop for TestBucket {
        fn drop(&mut self) {
            if let Err(e) = self.delete() {
                eprintln!("{e}");
            }
        }
    }

    macro_rules! new_bucket {
        () => {{
            fn f() {}
            fn type_name_of<T>(_: T) -> &'static str {
                std::any::type_name::<T>()
            }
            let name = type_name_of(f);
            let name = &name[..name.len() - 3];
            let name = name.replace("::", "-");
            let name = name.replace("_", "-");
            new_bucket(Some(&format!("{name}")))
        }};
    }

    fn new_bucket(name: Option<&str>) -> TestBucket {
        let client = Client::builder("http://127.0.0.1:9000")
            .unwrap()
            .key("minioadmin")
            .secret("minioadmin")
            .with_url_path_style(true)
            .client();

        println!("Creating a bucket of name: {:?}", name);

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
        let bucket = new_bucket!();
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
                    region: "",
                    cred: Credentials {
                        key: "minioadmin",
                    },
                    url_style: Path,
                    actions_expires_in: 3600s,
                    timeout: 60s,
                    multipart_size: 52428800,
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
                        path: "/strois-bucket-test-create-new-bucket/",
                        query: None,
                        fragment: None,
                    },
                    name: "strois-bucket-test-create-new-bucket",
                    region: "",
                },
            },
        )
        "###);
    }

    #[test]
    fn delete_bucket() {
        let bucket = new_bucket!();
        let ret = bucket.delete();
        insta::assert_debug_snapshot!(ret, @r###"
        Ok(
            (),
        )
        "###);
    }

    #[test]
    fn put_get_delete_object() {
        let bucket = new_bucket!();
        bucket.put_object("tamo", b"kero").unwrap();

        let content = bucket.get_object_string("tamo").unwrap();

        insta::assert_display_snapshot!(content, @"kero");

        bucket.delete_object("tamo").unwrap();

        let ret = bucket.get_object_string("tamo").unwrap_err();
        insta::assert_display_snapshot!(ret, @r###"NoSuchKey: The specified key does not exist. on Some("strois-bucket-test-put-get-delete-object")"###);
    }

    #[test]
    fn put_multipart() {
        let mut bucket = new_bucket!();
        let mut payload = "tamo ".repeat(1024 * 1024); // 5MiB payload
        payload.push_str("tamo."); // 5Mib + 5 bytes

        // With the default part size that's only one part.
        bucket
            .put_object_multipart("tamo", &mut payload.as_bytes())
            .unwrap();

        let content = bucket.get_object_string("tamo").unwrap();
        assert_eq!(content, payload);
        bucket.delete_object("tamo").unwrap();

        bucket.client.multipart_size = 5 * 1024 * 1024; // 5MiB the minimum possible size for multipart.

        // This will create two parts
        bucket
            .put_object_multipart("tamo", &mut payload.as_bytes())
            .unwrap();
        let content = bucket.get_object_string("tamo").unwrap();
        assert_eq!(content, payload);
        bucket.delete_object("tamo").unwrap();
    }
}
