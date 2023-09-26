mod bucket;
mod client;
mod client_builder;
mod error;

pub use bucket::Bucket;
pub use client::Client;
pub use client_builder::ClientBuilder;
pub use error::*;

pub type Result<T, E = Error> = std::result::Result<T, E>;
