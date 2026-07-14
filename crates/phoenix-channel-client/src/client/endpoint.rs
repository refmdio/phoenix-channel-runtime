use std::rc::Rc;

use base64::{Engine as _, engine::general_purpose::STANDARD_NO_PAD};
use futures::future::LocalBoxFuture;
use phoenix_channel_runtime::V2_PROTOCOL_VERSION;
use thiserror::Error;
use url::Url;

use super::ConnectContext;

const AUTH_TOKEN_PREFIX: &str = "base64url.bearer.phx.";

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ConnectionConfig {
    pub params: Vec<(String, String)>,
    pub auth_token: Option<String>,
}

impl ConnectionConfig {
    pub fn param(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.params.push((name.into(), value.into()));
        self
    }

    pub fn auth_token(mut self, token: impl Into<String>) -> Self {
        self.auth_token = Some(token.into());
        self
    }
}

pub type ConnectionConfigLoader =
    Rc<dyn Fn(ConnectContext) -> LocalBoxFuture<'static, Result<ConnectionConfig, String>>>;

pub fn static_connection_config(config: ConnectionConfig) -> ConnectionConfigLoader {
    Rc::new(move |_| {
        let config = config.clone();
        Box::pin(async move { Ok(config) })
    })
}

#[derive(Clone)]
pub struct Endpoint {
    url: Url,
    config_loader: ConnectionConfigLoader,
}

impl Endpoint {
    pub fn new(url: impl AsRef<str>) -> Result<Self, EndpointError> {
        let mut url = Url::parse(url.as_ref())?;
        match url.scheme() {
            "ws" | "wss" => {}
            scheme => return Err(EndpointError::UnsupportedScheme(scheme.to_owned())),
        }
        if !url.path().trim_end_matches('/').ends_with("/websocket") {
            let path = format!("{}/websocket", url.path().trim_end_matches('/'));
            url.set_path(&path);
        }
        Ok(Self {
            url,
            config_loader: static_connection_config(ConnectionConfig::default()),
        })
    }

    pub fn connection_config(mut self, config: ConnectionConfig) -> Self {
        self.config_loader = static_connection_config(config);
        self
    }

    pub fn connection_config_loader(mut self, loader: ConnectionConfigLoader) -> Self {
        self.config_loader = loader;
        self
    }

    pub async fn resolve(
        &self,
        context: ConnectContext,
    ) -> Result<ResolvedEndpoint, EndpointError> {
        let config = (self.config_loader)(context)
            .await
            .map_err(EndpointError::Config)?;
        let mut url = self.url.clone();
        let existing = url
            .query_pairs()
            .filter(|(name, _)| name != "vsn")
            .map(|(name, value)| (name.into_owned(), value.into_owned()))
            .collect::<Vec<_>>();
        url.set_query(None);
        {
            let mut query = url.query_pairs_mut();
            for (name, value) in existing.into_iter().chain(config.params) {
                query.append_pair(&name, &value);
            }
            query.append_pair("vsn", V2_PROTOCOL_VERSION);
        }

        let protocols = config.auth_token.map_or_else(Vec::new, |token| {
            vec![
                "phoenix".to_owned(),
                format!("{AUTH_TOKEN_PREFIX}{}", STANDARD_NO_PAD.encode(token)),
            ]
        });
        Ok(ResolvedEndpoint {
            url: url.into(),
            protocols,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResolvedEndpoint {
    pub url: String,
    pub protocols: Vec<String>,
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum EndpointError {
    #[error("invalid endpoint URL: {0}")]
    InvalidUrl(String),
    #[error("unsupported endpoint URL scheme: {0}")]
    UnsupportedScheme(String),
    #[error("connection configuration failed: {0}")]
    Config(String),
}

impl From<url::ParseError> for EndpointError {
    fn from(error: url::ParseError) -> Self {
        Self::InvalidUrl(error.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolves_the_websocket_path_version_params_and_auth_protocol() {
        futures::executor::block_on(async {
            let endpoint = Endpoint::new("wss://example.test/socket?existing=yes")
                .unwrap()
                .connection_config(
                    ConnectionConfig::default()
                        .param("user_id", "7")
                        .auth_token("1234"),
                );

            let resolved = endpoint
                .resolve(ConnectContext { attempt: 0 })
                .await
                .unwrap();

            assert_eq!(
                resolved.url,
                "wss://example.test/socket/websocket?existing=yes&user_id=7&vsn=2.0.0"
            );
            assert_eq!(
                resolved.protocols,
                ["phoenix", "base64url.bearer.phx.MTIzNA"]
            );
        });
    }

    #[test]
    fn reloads_connection_configuration_for_every_attempt() {
        futures::executor::block_on(async {
            let endpoint = Endpoint::new("ws://example.test/socket/websocket?vsn=1.0.0")
                .unwrap()
                .connection_config_loader(Rc::new(|context| {
                    Box::pin(async move {
                        Ok(ConnectionConfig::default()
                            .param("attempt", context.attempt.to_string()))
                    })
                }));

            assert_eq!(
                endpoint
                    .resolve(ConnectContext { attempt: 3 })
                    .await
                    .unwrap()
                    .url,
                "ws://example.test/socket/websocket?attempt=3&vsn=2.0.0"
            );
        });
    }
}
