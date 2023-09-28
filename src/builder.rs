use std::time::Duration;

use rusty_s3::Credentials;
use url::Url;

use crate::{Bucket, Client, Result};

pub struct MissingCred;
pub struct MissingSecret(String);
pub struct MissingKey(String);
pub struct Complete {
    key: String,
    secret: String,
}

pub struct Builder<State> {
    addr: Url,
    cred: State,
    token: Option<String>,
    actions_expires_in: Option<Duration>,
    timeout: Option<Duration>,
}

impl Builder<MissingCred> {
    /// Create a new `Builder`.
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
    /// If you try to call `.client()` before setting the key and secret it won't work.
    /// ```compile_fail
    /// use strois::Builder;
    ///
    /// let client = Builder::new("http://localhost:9000")?
    ///     .client();
    /// # Ok::<(), strois::Error>(())
    /// ```
    ///
    /// But if you only forgot the secret it should panic as well:
    /// ```compile_fail
    /// use strois::Builder;
    ///
    /// let client = Builder::new("http://localhost:9000")?
    ///     .secret("minioadmin")
    ///     .client();
    /// # Ok::<(), strois::Error>(())
    /// ```
    ///
    /// Same for the key:
    /// ```compile_fail
    /// use strois::Builder;
    ///
    /// let client = Builder::new("http://localhost:9000")?
    ///     .key("minioadmin")
    ///     .client();
    /// # Ok::<(), strois::Error>(())
    /// ```
    ///
    pub fn new(addr: impl AsRef<str>) -> Result<Self> {
        Ok(Self {
            addr: addr.as_ref().parse()?,
            cred: MissingCred,
            token: None,
            actions_expires_in: None,
            timeout: None,
        })
    }

    /// Set the key in the `Builder`.
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
    pub fn key(self, key: impl Into<String>) -> Builder<MissingSecret> {
        Builder {
            addr: self.addr,
            cred: MissingSecret(key.into()),
            token: self.token,
            actions_expires_in: self.actions_expires_in,
            timeout: self.timeout,
        }
    }

    /// Set the secret in the `Builder`.
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
    pub fn secret(self, secret: impl Into<String>) -> Builder<MissingKey> {
        Builder {
            addr: self.addr,
            cred: MissingKey(secret.into()),
            token: self.token,
            actions_expires_in: self.actions_expires_in,
            timeout: self.timeout,
        }
    }
}

impl Builder<MissingSecret> {
    pub fn secret(self, secret: impl Into<String>) -> Builder<Complete> {
        Builder {
            addr: self.addr,
            cred: Complete {
                key: self.cred.0,
                secret: secret.into(),
            },
            token: self.token,
            actions_expires_in: self.actions_expires_in,
            timeout: self.timeout,
        }
    }
}

impl Builder<MissingKey> {
    pub fn key(self, key: impl Into<String>) -> Builder<Complete> {
        Builder {
            addr: self.addr,
            cred: Complete {
                key: key.into(),
                secret: self.cred.0,
            },
            token: self.token,
            actions_expires_in: self.actions_expires_in,
            timeout: self.timeout,
        }
    }
}

impl Builder<Complete> {
    /// Create a new [`Client`] from the builder.
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
    pub fn client(self) -> Client {
        let cred = if let Some(token) = self.token {
            Credentials::new_with_token(self.cred.key, self.cred.secret, token)
        } else {
            Credentials::new(self.cred.key, self.cred.secret)
        };

        Client {
            addr: self.addr,
            cred,
            actions_expires_in: self
                .actions_expires_in
                .unwrap_or(Duration::from_secs(60 * 60)),
            timeout: self.timeout.unwrap_or(Duration::from_secs(60)),
        }
    }

    /// Create a new [`Bucket`] from the builder.
    ///
    /// # Example
    /// ```
    /// use strois::Builder;
    ///
    /// let client = Builder::new("http://localhost:9000")?
    ///     .key("minioadmin")
    ///     .secret("minioadmin")
    ///     .bucket("tamo");
    /// # Ok::<(), strois::Error>(())
    /// ```
    pub fn bucket(self, name: impl AsRef<str>) -> Result<Bucket> {
        self.client().bucket(name.as_ref())
    }
}

impl<T> Builder<T> {
    pub fn token(mut self, token: String) -> Self {
        self.token = Some(token);
        self
    }

    pub fn maybe_token(mut self, token: Option<String>) -> Self {
        self.token = token;
        self
    }

    pub fn actions_expires_in(mut self, actions_expires_in: Duration) -> Self {
        self.actions_expires_in = Some(actions_expires_in);
        self
    }

    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }
}
