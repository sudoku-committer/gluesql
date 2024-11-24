#![deny(clippy::str_to_string)]

use object_store_opendal::OpendalStore;
use opendal::layers::LoggingLayer;
use opendal::services::S3;
use opendal::{Builder, ErrorKind, Operator};
use std::{
    sync::Arc,
    convert::AsRef,
    path::{Path, PathBuf},
};


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct S3Storage {
    pub bucket: &str,
    pub prefix: &str,
    pub store: Arc<OpendalStore>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct S3Row {
    pub key: Key,
    pub row: DataRow,
}

impl S3Storage {
    pub fn new(
        bucket: &str,
        region: Option<&str>,
        prefix: &str,
        no_credentials: bool,
        endpoint: Option<&str>,
        use_ssl: Option<bool>,
        server_side_encryption: Option<bool>,
    ) -> Result<Self> {
        let op = build(
            bucket.into(),
            region.into(),
            prefix.into(),
            no_credentials.into(),
            endpoint.into(),
            use_ssl.into(),
            server_side_encryption.into()
        )

        let store = Arc::new(OpendalStore::new(op))

        if let Err(e) = store.stat(prefix).await {
            if e.kind() == ErrorKind::NotFound {
                store.create_dir(prefix).await?
            }
        }

        Ok(Self {
            bucket: bucket.to_owned(),
            prefix: prefix.to_owned(),
            store: store,
        })
    }

    fn build(
        bucket: &str,
        region: Option<&str>,
        prefix: &str,
        no_credentials: bool,
        endpoint: Option<&str>,
        use_ssl: Option<bool>,
        server_side_encryption: Option<bool>,
    ) -> Result<Operator> {
        let mut builder = S3::default()
            .http_client(set_user_agent())
            .bucket(bucket)
            .root(prefix);

        if let Some(region) = region {
            builder = builder.region(region);
        }
        if no_credentials {
            builder = builder
                .disable_config_load()
                .disable_ec2_metadata()
                .allow_anonymous();
        }
        if let Some(endpoint) = endpoint {
            builder = builder.endpoint(&endpoint_resolver(endpoint, use_ssl)?);
        }
        if server_side_encryption.unwrap_or_default() {
            builder = builder.server_side_encryption_with_s3_key();
        }

        let op = Operator::new(builder)?
            .layer(LoggingLayer::default())
            .finish();
        Ok(op)
    }

    pub fn path<T: AsRef<Path>>(&self, table_name: T) -> PathBuf {
        let mut path = self.path.clone();
        path.push(table_name);
        path
    }

    pub fn data_path<T: AsRef<Path>>(&self, table_name: T, key: &Key) -> Result<PathBuf> {
        let mut path = self.path(table_name);
        let key = key.to_cmp_be_bytes()?.encode_hex::<String>();

        path.push(key);
        let path = path.with_extension("ron");

        Ok(path)
    }

    fn fetch_schema(&self, path: PathBuf) -> Result<Schema> {
        self.store.read(path)
            .map_storage_err()
            .and_then(|data| Schema::from_ddl(&data))
    }
}

pub trait ResultExt<T, E: ToString> {
    fn map_storage_err(self) -> Result<T, Error>;
}

impl<T, E: ToString> ResultExt<T, E> for std::result::Result<T, E> {
    fn map_storage_err(self) -> Result<T, Error> {
        self.map_err(|e| e.to_string()).map_err(Error::StorageMsg)
    }
}

impl AlterTable for S3Storage {}
impl Index for S3Storage {}
impl IndexMut for S3Storage {}
impl Transaction for S3Storage {}
impl Metadata for S3Storage {}
impl CustomFunction for S3Storage {}
impl CustomFunctionMut for S3Storage {}

