Strois
======

An S3 synchronous SDK based on rusty_s3 and ureq.

## Usage

### Sending and retrieving a document on S3

```rust
use strois::{Builder, Error, S3ErrorCode};

let bucket = Builder::new("http://localhost:9000")?
    .key("minioadmin")
    .secret("minioadmin")
    .with_url_path_style()
    .bucket("tamo")?;

match bucket.create() {
    Ok(_) => (),
    Err(Error::S3Error(error)) if error.code == S3ErrorCode::BucketAlreadyExists || error.code == S3ErrorCode::BucketAlreadyOwnedByYou => (),
    e => return e.map(|_| ()),
}
bucket.put_object("tamo", b"kero")?;

let content = bucket.get_object_string("tamo")?;
assert_eq!(content, "kero");
# Ok::<(), strois::Error>(())
```
