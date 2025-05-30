/*
 * Parseable Server (C) 2022 - 2024 Parseable, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

use clap::Parser;
use std::{env, fs, path::PathBuf};

use url::Url;

#[cfg(feature = "kafka")]
use crate::connectors::kafka::config::KafkaConfig;

use crate::{
    oidc::{self, OpenidConfig},
    option::{validation, Compression, Mode},
    storage::{AzureBlobConfig, FSConfig, S3Config},
};

/// Default username and password for Parseable server, used by default for local mode.
/// NOTE: obviously not recommended for production
pub const DEFAULT_USERNAME: &str = "admin";
pub const DEFAULT_PASSWORD: &str = "admin";

#[derive(Parser)]
#[command(
    name = "parseable",
    bin_name = "parseable",
    about = "Cloud Native, log analytics platform for modern applications.",
    long_about = r#"
Cloud Native, log analytics platform for modern applications.

Usage:
parseable [command] [options..]


Help:
parseable [command] --help

"#,
    arg_required_else_help = true,
    color = clap::ColorChoice::Always,
    version = env!("CARGO_PKG_VERSION"),
    propagate_version = true,
    next_line_help = false,
    help_template = r#"{name} v{version}
{about}
Join the community at https://logg.ing/community.

{all-args}
        "#,
    subcommand_required = true,
)]
pub struct Cli {
    #[command(subcommand)]
    pub storage: StorageOptions,
}

#[derive(Parser)]
pub enum StorageOptions {
    #[command(name = "local-store")]
    Local(LocalStoreArgs),

    #[command(name = "s3-store")]
    S3(S3StoreArgs),

    #[command(name = "blob-store")]
    Blob(BlobStoreArgs),
}

#[derive(Parser)]
pub struct LocalStoreArgs {
    #[command(flatten)]
    pub options: Options,
    #[command(flatten)]
    pub storage: FSConfig,
    #[cfg(feature = "kafka")]
    #[command(flatten)]
    pub kafka: KafkaConfig,
}

#[derive(Parser)]
pub struct S3StoreArgs {
    #[command(flatten)]
    pub options: Options,
    #[command(flatten)]
    pub storage: S3Config,
    #[cfg(feature = "kafka")]
    #[command(flatten)]
    pub kafka: KafkaConfig,
}

#[derive(Parser)]
pub struct BlobStoreArgs {
    #[command(flatten)]
    pub options: Options,
    #[command(flatten)]
    pub storage: AzureBlobConfig,
    #[cfg(feature = "kafka")]
    #[command(flatten)]
    pub kafka: KafkaConfig,
}

#[derive(Parser, Debug, Default)]
pub struct Options {
    // Authentication
    #[arg(long, env = "P_USERNAME", help = "Admin username to be set for this Parseable server", default_value = DEFAULT_USERNAME)]
    pub username: String,

    #[arg(long, env = "P_PASSWORD", help = "Admin password to be set for this Parseable server", default_value = DEFAULT_PASSWORD)]
    pub password: String,

    // Server configuration
    #[arg(
        long,
        env = "P_ADDR",
        default_value = "0.0.0.0:8000",
        value_parser = validation::socket_addr,
        help = "Address and port for Parseable HTTP(s) server"
    )]
    pub address: String,

    #[arg(
        long = "origin",
        env = "P_ORIGIN_URI",
        value_parser = validation::url,
        help = "Parseable server global domain address"
    )]
    pub domain_address: Option<Url>,

    #[arg(
        long,
        env = "P_MODE",
        default_value = "all",
        value_parser = validation::mode,
        help = "Mode of operation"
    )]
    pub mode: Mode,

    #[arg(
        long,
        env = "P_CORS",
        default_value = "true",
        help = "Enable/Disable CORS, default disabled"
    )]
    pub cors: bool,

    #[arg(
        long,
        env = "P_CHECK_UPDATE",
        default_value = "true",
        help = "Enable/Disable checking for new Parseable release"
    )]
    pub check_update: bool,

    #[arg(
        long,
        env = "P_SEND_ANONYMOUS_USAGE_DATA",
        default_value = "true",
        help = "Enable/Disable anonymous telemetry data collection"
    )]
    pub send_analytics: bool,

    #[arg(
        long,
        env = "P_MASK_PII",
        default_value = "false",
        help = "mask PII data when sending to Prism"
    )]
    pub mask_pii: bool,

    // TLS/Security
    #[arg(
        long,
        env = "P_TLS_CERT_PATH",
        value_parser = validation::file_path,
        help = "Local path on this device where certificate file is located. Required to enable TLS"
    )]
    pub tls_cert_path: Option<PathBuf>,

    #[arg(
        long,
        env = "P_TLS_KEY_PATH",
        value_parser = validation::file_path,
        help = "Local path on this device where private key file is located. Required to enable TLS"
    )]
    pub tls_key_path: Option<PathBuf>,

    #[arg(
        long,
        env = "P_TRUSTED_CA_CERTS_DIR",
        value_parser = validation::canonicalize_path,
        help = "Local path on this device where all trusted certificates are located"
    )]
    pub trusted_ca_certs_path: Option<PathBuf>,

    // Storage configuration
    #[arg(
        long,
        env = "P_STAGING_DIR",
        default_value = "./staging",
        value_parser = validation::canonicalize_path,
        help = "Local path on this device to be used as landing point for incoming events"
    )]
    pub local_staging_path: PathBuf,

    #[arg(
        long = "hot-tier-path",
        env = "P_HOT_TIER_DIR",
        value_parser = validation::canonicalize_path,
        help = "Local path on this device to be used for hot tier data"
    )]
    pub hot_tier_storage_path: Option<PathBuf>,

    //TODO: remove this when smart cache is implemented
    #[arg(
        long = "index-storage-path",
        env = "P_INDEX_DIR",
        value_parser = validation::canonicalize_path,
        help = "Local path on this indexer used for indexing"
    )]
    pub index_storage_path: Option<PathBuf>,

    #[arg(
        long,
        env = "P_MAX_DISK_USAGE_PERCENT",
        default_value = "80.0",
        value_parser = validation::validate_disk_usage,
        help = "Maximum allowed disk usage in percentage e.g 90.0 for 90%"
    )]
    pub max_disk_usage: f64,

    // Service ports
    #[arg(
        long,
        env = "P_GRPC_PORT",
        default_value = "8001",
        help = "Port for gRPC server"
    )]
    pub grpc_port: u16,

    #[arg(
        long,
        env = "P_FLIGHT_PORT",
        default_value = "8002",
        help = "Port for Arrow Flight Querying Engine"
    )]
    pub flight_port: u16,

    // Performance settings
    #[arg(
        long,
        long = "livetail-capacity",
        env = "P_LIVETAIL_CAPACITY",
        default_value = "1000",
        help = "Number of rows in livetail channel"
    )]
    pub livetail_channel_capacity: usize,

    #[arg(
        long,
        long = "query-mempool-size",
        env = "P_QUERY_MEMORY_LIMIT",
        help = "Set a fixed memory limit for query in GiB"
    )]
    pub query_memory_pool_size: Option<usize>,
    // reduced the max row group size from 1048576
    // smaller row groups help in faster query performance in multi threaded query
    #[arg(
        long,
        env = "P_PARQUET_ROW_GROUP_SIZE",
        default_value = "262144",
        help = "Number of rows in a row group"
    )]
    pub row_group_size: usize,

    #[arg(
        long,
        env = "P_EXECUTION_BATCH_SIZE",
        default_value = "20000",
        help = "batch size for query execution"
    )]
    pub execution_batch_size: usize,

    #[arg(
        long = "compression-algo",
        env = "P_PARQUET_COMPRESSION_ALGO",
        default_value = "lz4_raw",
        value_parser = validation::compression,
        help = "Parquet compression algorithm"
    )]
    pub parquet_compression: Compression,

    // Integration features
    #[arg(
        long,
        env = "P_OPENAI_API_KEY",
        help = "OpenAI key to enable llm features"
    )]
    pub open_ai_key: Option<String>,

    #[arg(
        long,
        env = "P_INGESTOR_ENDPOINT",
        default_value = "",
        help = "URL to connect to this specific ingestor. Default is the address of the server"
    )]
    pub ingestor_endpoint: String,

    #[arg(
        long,
        env = "P_INDEXER_ENDPOINT",
        default_value = "",
        help = "URL to connect to this specific indexer. Default is the address of the server"
    )]
    pub indexer_endpoint: String,

    #[command(flatten)]
    pub oidc: Option<OidcConfig>,

    // Audit logging
    #[arg(
        long,
        env = "P_AUDIT_LOGGER",
        value_parser = validation::url,
        help = "Audit logger endpoint"
    )]
    pub audit_logger: Option<Url>,

    #[arg(long, env = "P_AUDIT_USERNAME", help = "Audit logger username")]
    pub audit_username: Option<String>,

    #[arg(long, env = "P_AUDIT_PASSWORD", help = "Audit logger password")]
    pub audit_password: Option<String>,

    #[arg(long, env = "P_MS_CLARITY_TAG", help = "Tag for MS Clarity")]
    pub ms_clarity_tag: Option<String>,
}

#[derive(Parser, Debug)]
pub struct OidcConfig {
    #[arg(
        long = "oidc-client",
        name = "oidc-client",
        env = "P_OIDC_CLIENT_ID",
        required = false,
        help = "Client id for OIDC provider"
    )]
    pub client_id: String,

    #[arg(
        long = "oidc-client-secret",
        name = "oidc-client-secret",
        env = "P_OIDC_CLIENT_SECRET",
        required = false,
        help = "Client secret for OIDC provider"
    )]
    pub secret: String,

    #[arg(
        long = "oidc-issuer",
        name = "oidc-issuer",
        env = "P_OIDC_ISSUER",
        required = false,
        value_parser = validation::url,
        help = "OIDC provider's host address"
    )]
    pub issuer: Url,
}

impl Options {
    pub fn local_stream_data_path(&self, stream_name: &str) -> PathBuf {
        self.local_staging_path.join(stream_name)
    }

    pub fn get_scheme(&self) -> String {
        if self.tls_cert_path.is_some() && self.tls_key_path.is_some() {
            "https".to_string()
        } else {
            "http".to_string()
        }
    }

    pub fn openid(&self) -> Option<OpenidConfig> {
        let OidcConfig {
            secret,
            client_id,
            issuer,
        } = self.oidc.as_ref()?;
        let origin = if let Some(url) = self.domain_address.clone() {
            oidc::Origin::Production(url)
        } else {
            oidc::Origin::Local {
                socket_addr: self.address.clone(),
                https: self.tls_cert_path.is_some() && self.tls_key_path.is_some(),
            }
        };
        Some(OpenidConfig {
            id: client_id.clone(),
            secret: secret.clone(),
            issuer: issuer.clone(),
            origin,
        })
    }

    pub fn is_default_creds(&self) -> bool {
        self.username == DEFAULT_USERNAME && self.password == DEFAULT_PASSWORD
    }

    /// Path to staging directory, ensures that it exists or panics
    pub fn staging_dir(&self) -> &PathBuf {
        fs::create_dir_all(&self.local_staging_path)
            .expect("Should be able to create dir if doesn't exist");

        &self.local_staging_path
    }

    /// Path to index directory, ensures that it exists or returns the PathBuf
    pub fn index_dir(&self) -> Option<&PathBuf> {
        if let Some(path) = &self.index_storage_path {
            fs::create_dir_all(path)
                .expect("Should be able to create index directory if it doesn't exist");
            Some(path)
        } else {
            None
        }
    }

    /// TODO: refactor and document
    pub fn get_url(&self, mode: Mode) -> Url {
        let (endpoint, env_var) = match mode {
            Mode::Ingest => {
                if self.ingestor_endpoint.is_empty() {
                    return format!(
                        "{}://{}",
                        self.get_scheme(),
                        self.address
                    )
                    .parse::<Url>() // if the value was improperly set, this will panic before hand
                    .unwrap_or_else(|err| {
                        panic!("{err}, failed to parse `{}` as Url. Please set the environment variable `P_ADDR` to `<ip address>:<port>` without the scheme (e.g., 192.168.1.1:8000). Please refer to the documentation: https://logg.ing/env for more details.", self.address)
                    });
                }
                (&self.ingestor_endpoint, "P_INGESTOR_ENDPOINT")
            }
            Mode::Index => {
                if self.indexer_endpoint.is_empty() {
                    return format!(
                        "{}://{}",
                        self.get_scheme(),
                        self.address
                    )
                    .parse::<Url>() // if the value was improperly set, this will panic before hand
                    .unwrap_or_else(|err| {
                        panic!("{err}, failed to parse `{}` as Url. Please set the environment variable `P_ADDR` to `<ip address>:<port>` without the scheme (e.g., 192.168.1.1:8000). Please refer to the documentation: https://logg.ing/env for more details.", self.address)
                    });
                }
                (&self.indexer_endpoint, "P_INDEXER_ENDPOINT")
            }
            _ => panic!("Invalid mode"),
        };

        if endpoint.starts_with("http") {
            panic!("Invalid value `{}`, please set the environement variable `{env_var}` to `<ip address / DNS>:<port>` without the scheme (e.g., 192.168.1.1:8000 or example.com:8000). Please refer to the documentation: https://logg.ing/env for more details.", endpoint);
        }

        let addr_from_env = endpoint.split(':').collect::<Vec<&str>>();

        if addr_from_env.len() != 2 {
            panic!("Invalid value `{}`, please set the environement variable `{env_var}` to `<ip address / DNS>:<port>` without the scheme (e.g., 192.168.1.1:8000 or example.com:8000). Please refer to the documentation: https://logg.ing/env for more details.", endpoint);
        }

        let mut hostname = addr_from_env[0].to_string();
        let mut port = addr_from_env[1].to_string();

        // if the env var value fits the pattern $VAR_NAME:$VAR_NAME
        // fetch the value from the specified env vars
        if hostname.starts_with('$') {
            let var_hostname = hostname[1..].to_string();
            hostname = env::var(&var_hostname).unwrap_or_default();

            if hostname.is_empty() {
                panic!("The environement variable `{}` is not set, please set as <ip address / DNS> without the scheme (e.g., 192.168.1.1 or example.com). Please refer to the documentation: https://logg.ing/env for more details.", var_hostname);
            }
            if hostname.starts_with("http") {
                panic!("Invalid value `{}`, please set the environement variable `{}` to `<ip address / DNS>` without the scheme (e.g., 192.168.1.1 or example.com). Please refer to the documentation: https://logg.ing/env for more details.", hostname, var_hostname);
            } else {
                hostname = format!("{}://{}", self.get_scheme(), hostname);
            }
        }

        if port.starts_with('$') {
            let var_port = port[1..].to_string();
            port = env::var(&var_port).unwrap_or_default();

            if port.is_empty() {
                panic!(
                    "Port is not set in the environement variable `{}`. Please refer to the documentation: https://logg.ing/env for more details.",
                    var_port
                );
            }
        }

        format!("{}://{}:{}", self.get_scheme(), hostname, port)
            .parse::<Url>()
            .expect("Valid URL")
    }
}
