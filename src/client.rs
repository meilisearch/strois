use std::{io::Read, time::Duration};

use rusty_s3::{Credentials, S3Action, UrlStyle};
use ureq::Response;
use url::Url;

use crate::{builder::MissingCred, Bucket, Builder, Result};

#[derive(Debug, Clone)]
pub struct Client {
    pub(crate) addr: Url,
    pub(crate) region: String,
    pub(crate) cred: Credentials,
    pub(crate) url_style: UrlStyle,
    pub(crate) actions_expires_in: Duration,
    pub(crate) timeout: Duration,
    pub(crate) multipart_size: usize,
}

impl Client {
    /// Create a new [`Builder`].
    /// It's currently missing its key and secret.
    ///
    /// # Example
    /// ```
    /// use strois::Builder;
    ///
    /// let client = Builder::new("http://localhost:9000")?
    ///     .key("minioadmin")
    ///     .secret("minioadmin")
    ///     .client();
    /// # Ok::<(), strois::Error>(())
    /// ```
    ///
    pub fn builder(url: impl AsRef<str>) -> Result<Builder<MissingCred>> {
        Builder::new(url)
    }

    /// /!\ Do not create the bucket on the S3.
    pub fn bucket(&self, name: impl Into<String>) -> Result<Bucket> {
        Bucket::new(self.clone(), name, self.url_style)
    }

    pub(crate) fn post<'a>(&self, action: impl S3Action<'a>) -> Result<Response> {
        Ok(ureq::post(action.sign(self.actions_expires_in).as_str())
            .timeout(self.timeout)
            .call()?)
    }

    pub(crate) fn post_with_body<'a>(
        &self,
        action: impl S3Action<'a>,
        body: impl Read,
        length: usize,
    ) -> Result<Response> {
        Ok(ureq::post(action.sign(self.actions_expires_in).as_str())
            .timeout(self.timeout)
            .set(http::header::CONTENT_LENGTH.as_str(), &length.to_string())
            .send(body)?)
    }

    pub(crate) fn put<'a>(&self, action: impl S3Action<'a>) -> Result<Response> {
        Ok(ureq::put(action.sign(self.actions_expires_in).as_str())
            .timeout(self.timeout)
            .call()?)
    }

    pub(crate) fn put_with_body<'a>(
        &self,
        action: impl S3Action<'a>,
        body: impl Read,
        length: usize,
    ) -> Result<Response> {
        Ok(ureq::put(action.sign(self.actions_expires_in).as_str())
            .timeout(self.timeout)
            .set(http::header::CONTENT_LENGTH.as_str(), &length.to_string())
            .send(body)?)
    }

    pub(crate) fn get<'a>(&self, action: impl S3Action<'a>) -> Result<Response> {
        Ok(ureq::get(action.sign(self.actions_expires_in).as_str())
            .timeout(self.timeout)
            .call()?)
    }

    pub(crate) fn delete<'a>(&self, action: impl S3Action<'a>) -> Result<Response> {
        Ok(ureq::delete(action.sign(self.actions_expires_in).as_str())
            .timeout(self.timeout)
            .call()?)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn new_client() {
        let client = Client::builder("http://127.0.0.1:9000")
            .unwrap()
            .key("minioadmin")
            .secret("minioadmin")
            .client();

        insta::assert_debug_snapshot!(client, @r###"
        Client {
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
            url_style: VirtualHost,
            actions_expires_in: 3600s,
            timeout: 60s,
            multipart_size: 52428800,
        }
        "###);
    }
}
