use std::fmt;

use http::StatusCode;
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error(transparent)]
    UserError(#[from] UserError),
    #[error(transparent)]
    IoError(#[from] std::io::Error),
    #[error(transparent)]
    S3Error(Box<S3Error>),
    #[error(transparent)]
    InternalError(#[from] InternalError),
    #[error(transparent)]
    HttpError(Box<ureq::Error>),
    #[error(transparent)]
    RustyS3(#[from] rusty_s3::BucketError),
    #[error(transparent)]
    Url(#[from] url::ParseError),
}

impl From<S3Error> for Error {
    fn from(error: S3Error) -> Self {
        Error::S3Error(Box::new(error))
    }
}

impl From<ureq::Error> for Error {
    fn from(error: ureq::Error) -> Self {
        match error {
            ureq::Error::Status(code, response) => {
                let reader = response.into_reader();
                let mut error: S3Error = match serde_xml_rs::de::from_reader(reader) {
                    Ok(error) => error,
                    Err(e) => return Error::InternalError(InternalError::BadS3Payload(e)),
                };
                error.status_code = StatusCode::try_from(code).unwrap();
                Error::S3Error(Box::new(error))
            }
            e => Error::HttpError(Box::new(e)),
        }
    }
}

#[derive(Debug, Error)]
pub enum UserError {
    #[error("Bucket `{0}` already exists.`")]
    BucketAlreadyExists(String),
}

#[derive(Debug, Error)]
pub enum InternalError {
    #[error("S3 returned non utf8 payload, this shouldn't be possible: `{0}`.`")]
    S3ReturnedNonUtf8Payload(std::io::Error),
    #[error("Could not deserialize S3 payload: `{0}`.`")]
    BadS3Payload(serde_xml_rs::Error),
    #[error("Could not deserialize S3 payload: `{0}`.`")]
    BadS3Payload2(quick_xml::de::DeError),
}

#[derive(Debug, Error, Deserialize)]
#[serde(rename_all = "PascalCase")]
#[error("{code}: {message} on {bucket_name}")]
pub struct S3Error {
    #[serde(skip)]
    pub status_code: StatusCode,
    pub code: S3ErrorCode,
    pub message: String,
    pub bucket_name: String,
    pub resource: String,
    pub request_id: String,
    pub host_id: String,
}

#[derive(Debug, Error, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "PascalCase")]
pub enum S3ErrorCode {
    AccessDenied,
    AccountProblem,
    AllAccessDisabled,
    AmbiguousGrantByEmailAddress,
    AuthorizationHeaderMalformed,
    BadDigest,
    BucketAlreadyExists,
    BucketAlreadyOwnedByYou,
    BucketNotEmpty,
    CredentialsNotSupported,
    CrossLocationLoggingProhibited,
    EntityTooSmall,
    EntityTooLarge,
    ExpiredToken,
    IllegalVersioningConfigurationException,
    IncompleteBody,
    IncorrectNumberOfFilesInPostRequest,
    InlineDataTooLarge,
    InvalidAccessKeyId,
    InvalidAddressingHeader,
    InvalidArgument,
    InvalidBucketName,
    InvalidBucketState,
    InvalidDigest,
    InvalidLocationConstraint,
    InvalidObjectState,
    InvalidPart,
    InvalidPartOrder,
    InvalidPayer,
    InvalidPolicyDocument,
    InvalidRange,
    InvalidRequest,
    InvalidSecurity,
    InvalidSOAPRequest,
    InvalidStorageClass,
    InvalidTargetBucketForLogging,
    InvalidToken,
    InvalidURI,
    MalformedPOSTRequest,
    MalformedXML,
    MaxMessageLengthExceeded,
    MetadataTooLarge,
    MethodNotAllowed,
    MissingAttachment,
    MissingContentLength,
    MissingSecurityElement,
    MissingSecurityHeader,
    NoLoggingStatusForKey,
    NoSuchBucket,
    NoSuchBucketPolicy,
    NoSuchKey,
    NoSuchLifecycleConfiguration,
    NoSuchUpload,
    NoSuchVersion,
    NotImplemented,
    NotSignedUp,
    OperationAborted,
    PermanentRedirect,
    PreconditionFailed,
    Redirect,
    RestoreAlreadyInProgress,
    RequestIsNotMultiPartContent,
    RequestTimeout,
    RequestTimeTooSkewed,
    SignatureDoesNotMatch,
    ServiceUnavailable,
    SlowDown,
    TemporaryRedirect,
    TokenRefreshRequired,
    TooManyBuckets,
    UnexpectedContent,
    UnresolvableGrantByEmailAddress,
    UserKeyMustBeSpecified,
}

impl fmt::Display for S3ErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", serde_xml_rs::ser::to_string(self).unwrap())
    }
}
