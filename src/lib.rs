mod bucket;
mod builder;
mod client;
mod error;

pub use bucket::Bucket;
pub use builder::Builder;
pub use client::Client;
pub use error::*;

pub type Result<T, E = Error> = std::result::Result<T, E>;
