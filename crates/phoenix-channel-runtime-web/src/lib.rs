//! Browser WebSocket transport for `wasm32-unknown-unknown`.

#![forbid(unsafe_code)]

#[cfg(target_arch = "wasm32")]
mod web {
    use std::{pin::Pin, time::Duration};

    use futures::{Sink, SinkExt, StreamExt, future::poll_fn};
    use gloo_net::websocket::{Message, State, WebSocketError, futures::WebSocket};
    use gloo_timers::future::TimeoutFuture;
    use phoenix_channel_client::{ConnectContext, Connector, Endpoint, ResolvedEndpoint, Timer};
    use phoenix_channel_runtime::{
        Transport, TransportClose, TransportError, TransportErrorKind, TransportEvent, WireMessage,
    };

    pub struct WebTransport {
        inner: WebSocket,
    }

    #[derive(Clone)]
    pub struct WebConnector {
        endpoint: WebEndpoint,
    }

    #[derive(Clone)]
    enum WebEndpoint {
        Url(String),
        Phoenix(Endpoint),
    }

    impl WebConnector {
        pub fn new(url: impl Into<String>) -> Self {
            Self {
                endpoint: WebEndpoint::Url(url.into()),
            }
        }

        pub fn from_endpoint(endpoint: Endpoint) -> Self {
            Self {
                endpoint: WebEndpoint::Phoenix(endpoint),
            }
        }
    }

    impl Connector for WebConnector {
        fn connect(
            &self,
            context: ConnectContext,
        ) -> futures::future::LocalBoxFuture<'static, Result<Box<dyn Transport>, TransportError>>
        {
            let endpoint = self.endpoint.clone();
            Box::pin(async move {
                let endpoint = match endpoint {
                    WebEndpoint::Url(url) => ResolvedEndpoint {
                        url,
                        protocols: Vec::new(),
                    },
                    WebEndpoint::Phoenix(endpoint) => {
                        endpoint.resolve(context).await.map_err(|error| {
                            TransportError::with_kind(
                                TransportErrorKind::Connect,
                                error.to_string(),
                            )
                        })?
                    }
                };
                let transport = WebTransport::connect_resolved(endpoint).await?;
                Ok(Box::new(transport) as Box<dyn Transport>)
            })
        }
    }

    #[derive(Clone, Copy, Debug, Default)]
    pub struct WebTimer;

    impl Timer for WebTimer {
        fn sleep(&self, duration: Duration) -> futures::future::LocalBoxFuture<'static, ()> {
            let milliseconds = duration.as_millis().min(u128::from(u32::MAX)) as u32;
            Box::pin(TimeoutFuture::new(milliseconds))
        }
    }

    impl WebTransport {
        pub fn connect(url: &str) -> Result<Self, TransportError> {
            let inner = WebSocket::open(url).map_err(|error| {
                TransportError::with_kind(TransportErrorKind::Connect, format!("{error:?}"))
            })?;
            Ok(Self { inner })
        }

        async fn connect_resolved(endpoint: ResolvedEndpoint) -> Result<Self, TransportError> {
            let mut inner = if endpoint.protocols.is_empty() {
                WebSocket::open(&endpoint.url)
            } else {
                WebSocket::open_with_protocols(&endpoint.url, &endpoint.protocols)
            }
            .map_err(|error| {
                TransportError::with_kind(TransportErrorKind::Connect, format!("{error:?}"))
            })?;
            poll_fn(|context| Pin::new(&mut inner).poll_ready(context))
                .await
                .map_err(|error| {
                    TransportError::with_kind(TransportErrorKind::Connect, error.to_string())
                })?;
            if !matches!(inner.state(), State::Open) {
                return Err(TransportError::with_kind(
                    TransportErrorKind::Connect,
                    "WebSocket closed before the open event",
                ));
            }
            Ok(Self { inner })
        }
    }

    impl Transport for WebTransport {
        fn send<'a>(
            &'a mut self,
            message: WireMessage,
        ) -> futures::future::LocalBoxFuture<'a, Result<(), TransportError>> {
            Box::pin(async move {
                let message = match message {
                    WireMessage::Text(text) => Message::Text(text),
                    WireMessage::Binary(bytes) => Message::Bytes(bytes),
                };
                self.inner.send(message).await.map_err(|error| {
                    TransportError::with_kind(TransportErrorKind::Send, format!("{error:?}"))
                })
            })
        }

        fn receive<'a>(
            &'a mut self,
        ) -> futures::future::LocalBoxFuture<'a, Result<TransportEvent, TransportError>> {
            Box::pin(async move {
                let Some(message) = self.inner.next().await else {
                    return Ok(TransportEvent::Closed(TransportClose::connection_ended()));
                };
                match message {
                    Ok(Message::Text(text)) => Ok(TransportEvent::Message(WireMessage::Text(text))),
                    Ok(Message::Bytes(bytes)) => {
                        Ok(TransportEvent::Message(WireMessage::Binary(bytes)))
                    }
                    Err(WebSocketError::ConnectionClose(close)) => Ok(TransportEvent::Closed(
                        TransportClose::new(Some(close.code), close.reason, close.was_clean),
                    )),
                    Err(error) => Err(TransportError::with_kind(
                        TransportErrorKind::Receive,
                        error.to_string(),
                    )),
                }
            })
        }

        fn close<'a>(
            &'a mut self,
        ) -> futures::future::LocalBoxFuture<'a, Result<(), TransportError>> {
            Box::pin(async move {
                SinkExt::close(&mut self.inner).await.map_err(|error| {
                    TransportError::with_kind(TransportErrorKind::Close, format!("{error:?}"))
                })
            })
        }
    }
}

#[cfg(target_arch = "wasm32")]
pub use web::{WebConnector, WebTimer, WebTransport};
