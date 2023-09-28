use strois::{Client, Error, S3ErrorCode};

fn main() {
    let client = Client::builder("http://localhost:9000")
        .unwrap()
        .key("minioadmin")
        .secret("minioadmin")
        .client();
    let bucket = client.bucket("tamo").unwrap();
    match bucket.create() {
        Ok(_) => println!("created tamo"),
        Err(Error::S3Error(error)) if error.code == S3ErrorCode::BucketAlreadyExists => {
            println!("bucket already exists")
        }
        Err(Error::S3Error(error)) if error.code == S3ErrorCode::BucketAlreadyOwnedByYou => {
            println!("bucket already exists and is owned by you")
        }
        Err(e) => panic!("{e}"),
    };

    for element in bucket.list_objects("1000").unwrap() {
        println!("{:?}", element.unwrap());
    }
    // bucket.delete_object("tamo").unwrap();

    // bucket.delete().unwrap();
    println!("deleted tamo");
}
