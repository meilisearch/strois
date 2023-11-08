use std::io::{stdin, stdout};

use clap::Parser;
use miette::{miette, Context, IntoDiagnostic, Result};
use strois::{Bucket, Error, S3ErrorCode, UserError};

pub fn get_styles() -> clap::builder::Styles {
    clap::builder::Styles::styled()
        .usage(
            anstyle::Style::new()
                .fg_color(Some(anstyle::Color::Ansi(anstyle::AnsiColor::Yellow)))
                .bold(),
        )
        .header(
            anstyle::Style::new()
                .bold()
                .underline()
                .fg_color(Some(anstyle::Color::Ansi(anstyle::AnsiColor::Yellow))),
        )
        .literal(
            anstyle::Style::new().fg_color(Some(anstyle::Color::Ansi(anstyle::AnsiColor::Green))),
        )
}

#[derive(Debug, Parser)]
#[clap(about = "Cli around S3")]
#[command(styles = get_styles())]
struct Options {
    /// The addr of the s3 server. By default we use the default addr of minio
    #[clap(global = true, long, short, default_value_t = String::from("http://localhost:9000"))]
    pub addr: String,

    /// The bucket name, by default `s3cli`
    #[clap(global = true, long, short, default_value_t = String::from("strois"))]
    pub bucket: String,

    /// The region, by default `eu-central-1`
    #[clap(global = true, long, default_value_t = String::from("eu-central-1"))]
    pub region: String,

    #[clap(flatten)]
    pub cred: Credential,

    /// The style of the url.
    /// Do you want your url to be: `http://bucket.url.com/`
    /// or `http://url.com/bucket/`.
    /// Notice that localhost doesn't work with the virtual host style.
    #[clap(global = true, long, default_value_t = false)]
    pub virtual_host_style: bool,

    /// The verbosity, the more `v` you use and the more verbose it gets.
    #[clap(global = true, short, action = clap::ArgAction::Count)]
    verbose: u8,

    #[clap(subcommand)]
    pub command: Command,
}

#[derive(Debug, Parser)]
struct Credential {
    /// Access key, set to `minioadmin` by default.
    #[clap(global = true, default_value_t = String::from("minioadmin"), long)]
    pub key: String,

    /// Secret key, set to `minioadmin` by default.
    #[clap(global = true, default_value_t = String::from("minioadmin"), long)]
    pub secret: String,

    /// Security token
    #[clap(global = true, long)]
    pub token: Option<String>,
}

#[derive(Debug, Parser)]
enum Command {
    /// List directory contents.
    #[clap(aliases = &["list", "l"])]
    Ls {
        /// List directory contents from the given path.
        path: Option<String>,
    },
    /// Print file.
    #[clap(aliases = &["bat"])]
    Cat {
        /// Path of the file to cat.
        file: String,
        /// Use it to send the raw data to stdout without any validation.
        #[clap(long, short, default_value_t = false)]
        raw: bool,
    },
    /// Remove directory entries.
    #[clap(aliases = &["rmdir"])]
    Rm {
        /// Path of the files to remove.
        paths: Vec<String>,
    },
    /// Write the content of stdin or argv to the specified path.
    /// The path must already exists. See the create command if you need to create a new node.
    #[clap(aliases = &["set"])]
    Write {
        /// Path of the file to write.
        path: String,
        /// Content to write in the file.
        content: Option<String>,
        /// Force:
        /// - If the file doesn't exsists create it as persistent.
        /// - If you don't send any content, erase the content of the file for nothing.
        #[clap(long, short, default_value_t = false)]
        force: bool,
    },
    /// Commands related to the buckets.
    #[clap(aliases = &["b"], subcommand)]
    Bucket(BucketCommand),
}

#[derive(Debug, Parser)]
enum BucketCommand {
    /// Create a bucket.
    Create {
        /// Doesn't return an error if the bucket already exists.
        #[clap(long, short, default_value_t = false)]
        ignore_if_exists: bool,
    },
    /// Delete a bucket.
    Delete {
        /// Doesn't return an error if the bucket doesn't exists.
        #[clap(long, short, default_value_t = false)]
        ignore_if_does_not_exists: bool,
    },
}

fn main() -> Result<()> {
    let opt = Options::parse();
    let mut log_builder = env_logger::Builder::new();
    let log_level = ["fatal", "error", "warn", "info", "debug", "trace"];
    log_builder.parse_filters(log_level[opt.verbose.clamp(0, log_level.len() as u8 - 1) as usize]);
    log_builder.init();

    let s3 = Bucket::builder(opt.addr)
        .into_diagnostic()?
        .key(opt.cred.key)
        .secret(opt.cred.secret)
        .maybe_token(opt.cred.token)
        .with_url_path_style(!opt.virtual_host_style)
        .bucket(opt.bucket)
        .into_diagnostic()?;

    match opt.command {
        Command::Ls { mut path } => {
            path.as_mut().map(sanitize_path);
            for child in s3.list_objects(path.unwrap_or_default()).into_diagnostic()? {
                print!("{} ", child.into_diagnostic()?.key);
            }
            println!();
        }
        Command::Cat { mut file, raw } => {
            sanitize_path(&mut file);
            if raw || atty::isnt(atty::Stream::Stdout){
                let mut stdout = stdout();
                s3.get_object_to_writer(&file, &mut stdout).into_diagnostic()?;
            } else {
                match s3.get_object_string(&file) {
                    Ok(s) => println!("{s}"),
                    Err(Error::UserError(UserError::PayloadCouldNotBeConvertedToString(e))) => return Err(e).into_diagnostic().wrap_err("Object contains non utf-8 character. To print it use the `--raw` flag."),
                    e => return e.into_diagnostic().map(drop),
                }
            }
        }
        Command::Rm { paths } => {
            for path in paths {
                let ret = || -> Result<()> {
                    s3.delete_object(&path).into_diagnostic()?;
                    Ok(())
                }();
                if let Err(e) = ret {
                    log::error!("`{}`: {}", path, e);
                }
            }
        }
        Command::Write {
            path,
            content,
            force,
        } => {
            match content {
                Some(content) => { s3.put_object(&path, content.as_bytes()).into_diagnostic()?;}
                None if atty::isnt(atty::Stream::Stdin) => {
                    let mut reader = stdin();
                    s3.put_object_multipart(path, &mut reader).into_diagnostic()?;
                }
                None if force => { s3.put_object(&path, []).into_diagnostic()?; }
                None => return Err(miette!("Did you forgot to pipe something in the command? If you wanted to reset the content of the file use `--force` or `-f`.")),
            }
        }
        Command::Bucket(command) => match command {
            BucketCommand::Create { ignore_if_exists } => {
                match s3.create() {
                    Ok(_) => (),
                    Err(Error::S3Error(e)) if ignore_if_exists && matches!(e.code, S3ErrorCode::BucketAlreadyExists | S3ErrorCode::BucketAlreadyOwnedByYou) => log::info!("Bucket already exists"),
                    e => return e.into_diagnostic().map(drop),
                }
            },
            BucketCommand::Delete { ignore_if_does_not_exists } => {
               match s3.delete() {
                    Ok(_) => (),
                    Err(Error::S3Error(e)) if ignore_if_does_not_exists && matches!(e.code, S3ErrorCode::NoSuchBucket) => log::info!("Bucket does not exists"),
                    e => return e.into_diagnostic().map(drop),
                }
            },
        },
    }
    Ok(())
}

fn sanitize_path(path: &mut String) {
    if path.starts_with('/') {
        log::warn!("Invalid path, trimming the `/` at the starts of your path");
        *path = path.trim_start_matches('/').to_string();
    }
}
