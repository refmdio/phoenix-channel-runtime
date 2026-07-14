//! Browser WebSocket transport for `wasm32-unknown-unknown`.

#![forbid(unsafe_code)]

#[cfg(target_arch = "wasm32")]
mod web {
    use std::time::Duration;

    use futures::{SinkExt, StreamExt};
    use gloo_net::websocket::{Message, futures::WebSocket};
    use gloo_timers::future::TimeoutFuture;
    use phoenix_channel_client::{Connector, Timer};
    use phoenix_channel_runtime::{Transport, TransportError, WireMessage};

    pub struct WebTransport {
        inner: WebSocket,
    }

    #[derive(Clone, Debug)]
    pub struct WebConnector {
        url: String,
    }

    impl WebConnector {
        pub fn new(url: impl Into<String>) -> Self {
            Self { url: url.into() }
        }
    }

    impl Connector for WebConnector {
        fn connect(
            &self,
        ) -> futures::future::LocalBoxFuture<'static, Result<Box<dyn Transport>, TransportError>>
        {
            let result = WebTransport::connect(&self.url)
                .map(|transport| Box::new(transport) as Box<dyn Transport>);
            Box::pin(async move { result })
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
            let inner =
                WebSocket::open(url).map_err(|error| TransportError::new(format!("{error:?}")))?;
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
                self.inner
                    .send(message)
                    .await
                    .map_err(|error| TransportError::new(format!("{error:?}")))
            })
        }

        fn receive<'a>(
            &'a mut self,
        ) -> futures::future::LocalBoxFuture<'a, Result<Option<WireMessage>, TransportError>>
        {
            Box::pin(async move {
                let Some(message) = self.inner.next().await else {
                    return Ok(None);
                };
                let message = message.map_err(|error| TransportError::new(format!("{error:?}")))?;
                Ok(Some(match message {
                    Message::Text(text) => WireMessage::Text(text),
                    Message::Bytes(bytes) => WireMessage::Binary(bytes),
                }))
            })
        }

        fn close<'a>(
            &'a mut self,
        ) -> futures::future::LocalBoxFuture<'a, Result<(), TransportError>> {
            Box::pin(async move {
                SinkExt::close(&mut self.inner)
                    .await
                    .map_err(|error| TransportError::new(format!("{error:?}")))
            })
        }
    }
}

#[cfg(target_arch = "wasm32")]
pub use web::{WebConnector, WebTimer, WebTransport};
