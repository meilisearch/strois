use std::{fmt, io::BufReader, string::FromUtf8Error};

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
                let reader = BufReader::new(response.into_reader());
                let mut error: S3Error = match quick_xml::de::from_reader(reader) {
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
    #[error("Payload could not be converted to utf-8 string: `{0}`.")]
    PayloadCouldNotBeConvertedToString(FromUtf8Error),
}

#[derive(Debug, Error)]
pub enum InternalError {
    #[error("S3 returned non utf8 payload, this shouldn't be possible: `{0}`.`")]
    S3ReturnedNonUtf8Payload(std::io::Error),
    #[error("Could not deserialize S3 payload: `{0}`.`")]
    BadS3Payload(quick_xml::de::DeError),
}

#[derive(Debug, Error, Deserialize)]
#[serde(rename_all = "PascalCase")]
#[error("{code}: {message} on {bucket_name:?}")]
pub struct S3Error {
    #[serde(skip)]
    pub status_code: StatusCode,
    #[serde(with = "quick_xml::serde_helpers::text_content")]
    pub code: S3ErrorCode,
    pub message: String,
    pub bucket_name: Option<String>,
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
        let s = quick_xml::se::to_string(self).expect("This can't fail");
        write!(f, "{}", &s[1..s.len() - 2])
    }
}
