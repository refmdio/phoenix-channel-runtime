use std::rc::Rc;

use super::{ChannelEvent, SocketEvent, TelemetryEvent, TelemetryHook};

pub fn tracing_telemetry_hook() -> TelemetryHook {
    Rc::new(|event| match event {
        TelemetryEvent::Socket(socket) => match socket {
            SocketEvent::Connected => {
                tracing::info!(target: "phoenix_channels", "socket connected")
            }
            SocketEvent::Disconnected { reason } => {
                tracing::warn!(target: "phoenix_channels", %reason, "socket disconnected")
            }
            SocketEvent::Connecting { attempt } => {
                tracing::debug!(target: "phoenix_channels", attempt, "socket connecting")
            }
            SocketEvent::ReconnectScheduled { attempt, delay } => tracing::info!(
                target: "phoenix_channels",
                attempt,
                delay_ms = delay.as_millis(),
                "socket reconnect scheduled"
            ),
            SocketEvent::ReconnectStopped { attempt, reason } => tracing::warn!(
                target: "phoenix_channels",
                attempt,
                %reason,
                "socket reconnect stopped"
            ),
            SocketEvent::Closed => tracing::info!(target: "phoenix_channels", "socket closed"),
            SocketEvent::Lagged { dropped } => tracing::warn!(
                target: "phoenix_channels",
                dropped,
                "socket event subscriber lagged"
            ),
        },
        TelemetryEvent::Channel { topic, event } => match event {
            ChannelEvent::Lagged { dropped } => tracing::warn!(
                target: "phoenix_channels",
                topic,
                dropped,
                "channel event subscriber lagged"
            ),
            _ => tracing::debug!(
                target: "phoenix_channels",
                topic,
                event = ?event,
                "channel event"
            ),
        },
        TelemetryEvent::FrameSent {
            topic,
            event,
            binary,
            bytes,
        } => tracing::trace!(
            target: "phoenix_channels",
            topic,
            event,
            binary,
            bytes,
            "frame sent"
        ),
        TelemetryEvent::FrameReceived {
            topic,
            event,
            binary,
            bytes,
        } => tracing::trace!(
            target: "phoenix_channels",
            topic,
            event,
            binary,
            bytes,
            "frame received"
        ),
        TelemetryEvent::ConnectionAttemptFinished {
            attempt,
            duration,
            connected,
        } => tracing::info!(
            target: "phoenix_channels",
            attempt,
            duration_ms = duration.as_millis(),
            connected,
            "connection attempt finished"
        ),
        TelemetryEvent::CallCompleted {
            topic,
            event,
            outcome,
            duration,
        } => tracing::debug!(
            target: "phoenix_channels",
            topic,
            event,
            outcome = ?outcome,
            duration_ms = duration.as_millis(),
            "channel call completed"
        ),
    })
}
