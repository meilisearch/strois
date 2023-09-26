use std::time::Duration;

use rusty_s3::{Credentials, S3Action};
use ureq::Response;
use url::Url;

use crate::{client_builder::MissingCred, Bucket, ClientBuilder, Result};

#[derive(Debug, Clone)]
pub struct Client {
    pub(crate) addr: Url,
    pub(crate) cred: Credentials,
    pub(crate) actions_expires_in: Duration,
    pub(crate) timeout: Duration,
}

impl Client {
    /// Create a new `ClientBuilder`.
    /// It's currently missing its key and secret.
    ///
    /// # Example
    /// ```
    /// use meilis3arch::ClientBuilder;
    ///
    /// let client = ClientBuilder::new("http://localhost:9000")?
    ///     .key("minio")
    ///     .secret("minio")
    ///     .build();
    /// # Ok::<(), meilis3arch::Error>(())
    /// ```
    ///
    pub fn builder(url: impl AsRef<str>) -> Result<ClientBuilder<MissingCred>> {
        ClientBuilder::new(url)
    }

    /// /!\ Do not create the bucket on the S3.
    pub fn bucket(&self, name: impl Into<String>) -> Result<Bucket> {
        Bucket::new(self.clone(), name)
    }

    pub(crate) fn put<'a>(&self, action: impl S3Action<'a>) -> Result<Response> {
        Ok(ureq::put(action.sign(self.actions_expires_in).as_str())
            .timeout(self.timeout)
            .call()?)
    }

    pub(crate) fn put_with_body<'a>(
        &self,
        action: impl S3Action<'a>,
        body: &[u8],
    ) -> Result<Response> {
        Ok(ureq::put(action.sign(self.actions_expires_in).as_str())
            .timeout(self.timeout)
            .send_bytes(body)?)
    }

    pub(crate) fn delete<'a>(&self, action: impl S3Action<'a>) -> Result<Response> {
        Ok(ureq::delete(action.sign(self.actions_expires_in).as_str())
            .timeout(self.timeout)
            .call()?)
    }
}

#[cfg(test)]
mod test {
    use crate::ClientBuilder;

    use super::*;
    use testcontainers::{clients::Cli, images::minio::MinIO};

    #[test]
    fn new_client() {
        let docker = Cli::docker();
        let image = docker.run(MinIO::default());

        let client = ClientBuilder::new("http://127.0.0.1:9000")
            .unwrap()
            .key("minio")
            .secret("minio")
            .build();
    }
}
