use strois::Bucket;

fn main() {
    let bucket = Bucket::builder("http://localhost:9000")
        .unwrap()
        .key("minioadmin")
        .secret("minioadmin")
        .with_url_path_style(true)
        .bucket("tamo")
        .unwrap()
        .get_or_create()
        .unwrap();

    for element in bucket.list_objects("").unwrap() {
        let element = element.unwrap();
        println!("{:?} - {} bytes", element.key, element.size);
    }
}
