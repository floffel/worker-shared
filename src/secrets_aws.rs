//! AWS Secrets Manager adapter, behind the `secrets-aws` feature.
//!
//! [`SecretsManagerFetcher`] implements [`SecretFetcher`] over the SDK client.
//! It maps the SDK's error surface onto this crate's [`Error`] so the cache can
//! tell "retry me" (throttling) from "this will never work" (wrong ARN,
//! missing permission, binary secret).
//!
//! The SDK client is passed in rather than built here: constructing one reads
//! the ambient AWS configuration, and a library should let its caller own that
//! decision. `Client::new(&aws_config::load_from_env().await)` in the Lambda
//! entry point is the usual wiring.
//!
//! ```no_run
//! use worker_shared::secrets::{CacheConfig, SecretCache};
//! use worker_shared::secrets_aws::SecretsManagerFetcher;
//! # async fn demo(client: aws_sdk_secretsmanager::Client) -> Result<(), worker_shared::Error> {
//! let cache = SecretCache::new(
//!     SecretsManagerFetcher::new(client),
//!     CacheConfig::default(),
//! )?;
//!
//! let password = cache.get("prod/db/password").await?;
//! println!("{}", password.len()); // never print the value itself
//! # Ok(())
//! # }
//! ```

use aws_sdk_secretsmanager::error::ProvideErrorMetadata;
use aws_sdk_secretsmanager::Client;

use crate::error::Error;
use crate::secrets::SecretFetcher;

/// The service name reported in [`Error::Throttled`] and [`Error::Upstream`].
const SERVICE: &str = "secretsmanager";

/// Fetches secret values from AWS Secrets Manager.
///
/// ```no_run
/// use worker_shared::secrets_aws::SecretsManagerFetcher;
///
/// # async fn demo(client: aws_sdk_secretsmanager::Client) {
/// let fetcher = SecretsManagerFetcher::new(client);
/// assert_eq!(format!("{fetcher:?}"), "SecretsManagerFetcher");
/// # }
/// ```
#[derive(Clone)]
pub struct SecretsManagerFetcher {
    client: Client,
}

impl std::fmt::Debug for SecretsManagerFetcher {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The SDK client's Debug prints endpoint configuration, which is noise
        // here and must never gain credentials by accident.
        formatter.write_str("SecretsManagerFetcher")
    }
}

impl SecretsManagerFetcher {
    /// Wrap an SDK client.
    ///
    /// ```no_run
    /// use worker_shared::secrets_aws::SecretsManagerFetcher;
    ///
    /// # async fn demo(client: aws_sdk_secretsmanager::Client) {
    /// let fetcher = SecretsManagerFetcher::new(client);
    /// let _client = fetcher.client();
    /// # }
    /// ```
    #[must_use]
    pub fn new(client: Client) -> Self {
        Self { client }
    }

    /// The wrapped client.
    ///
    /// ```no_run
    /// use worker_shared::secrets_aws::SecretsManagerFetcher;
    ///
    /// # async fn demo(client: aws_sdk_secretsmanager::Client) {
    /// assert!(SecretsManagerFetcher::new(client).client().config().region().is_some());
    /// # }
    /// ```
    #[must_use]
    pub fn client(&self) -> &Client {
        &self.client
    }
}

#[async_trait::async_trait]
impl SecretFetcher for SecretsManagerFetcher {
    async fn fetch(&self, id: &str) -> Result<String, Error> {
        let response = self
            .client
            .get_secret_value()
            .secret_id(id)
            .send()
            .await
            .map_err(|err| map_sdk_error(id, &err))?;

        response
            .secret_string()
            .map(str::to_string)
            .ok_or_else(|| Error::Secret {
                id: id.to_string(),
                // Binary secrets are legal in Secrets Manager but cannot be an
                // API key or password; treat them as a configuration mistake.
                message: "secret has a binary value, expected a string".to_string(),
            })
    }
}

fn map_sdk_error<E>(id: &str, err: &aws_sdk_secretsmanager::error::SdkError<E>) -> Error
where
    E: ProvideErrorMetadata + std::fmt::Debug,
{
    let code = err
        .as_service_error()
        .and_then(ProvideErrorMetadata::code)
        .map(str::to_string);

    match code.as_deref() {
        // Rejected before processing: retrying cannot duplicate anything.
        Some("ThrottlingException" | "TooManyRequestsException") => Error::Throttled {
            service: SERVICE,
            retry_after: None,
        },
        Some("ResourceNotFoundException") => Error::Secret {
            id: id.to_string(),
            message: "secret not found".to_string(),
        },
        Some("AccessDeniedException") => Error::Secret {
            id: id.to_string(),
            message: "access denied".to_string(),
        },
        Some("DecryptionFailure") => Error::Secret {
            id: id.to_string(),
            message: "secret could not be decrypted with the configured KMS key".to_string(),
        },
        Some(other) => Error::Secret {
            id: id.to_string(),
            message: format!("Secrets Manager error {other}"),
        },
        // No service error: a transport or configuration failure. Ambiguous is
        // wrong here — the request never reached the service — but the id is
        // what the operator needs to see.
        None => Error::Secret {
            id: id.to_string(),
            message: format!("request failed: {err}"),
        },
    }
}
