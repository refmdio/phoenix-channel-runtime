use std::collections::{BTreeMap, BTreeSet};

use serde_json::{Map, Value};
use thiserror::Error;

use crate::Frame;

#[derive(Clone, Debug, Default, PartialEq)]
pub struct PresenceState(pub BTreeMap<String, Presence>);

impl PresenceState {
    pub fn from_value(value: &Value) -> Result<Self, PresenceError> {
        let state = value.as_object().ok_or(PresenceError::InvalidState)?;
        state
            .iter()
            .map(|(key, presence)| Ok((key.clone(), Presence::from_value(key, presence)?)))
            .collect::<Result<BTreeMap<_, _>, _>>()
            .map(Self)
    }

    pub fn get(&self, key: &str) -> Option<&Presence> {
        self.0.get(key)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&str, &Presence)> {
        self.0.iter().map(|(key, value)| (key.as_str(), value))
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct Presence {
    pub metas: Vec<Map<String, Value>>,
    pub fields: Map<String, Value>,
}

impl Presence {
    fn from_value(key: &str, value: &Value) -> Result<Self, PresenceError> {
        let mut fields = value
            .as_object()
            .cloned()
            .ok_or_else(|| PresenceError::InvalidPresence(key.to_owned()))?;
        let metas = fields
            .remove("metas")
            .and_then(|value| value.as_array().cloned())
            .ok_or_else(|| PresenceError::InvalidMetas(key.to_owned()))?
            .into_iter()
            .map(|meta| {
                let meta = meta
                    .as_object()
                    .cloned()
                    .ok_or_else(|| PresenceError::InvalidMeta(key.to_owned()))?;
                meta_ref(key, &meta)?;
                Ok(meta)
            })
            .collect::<Result<Vec<_>, PresenceError>>()?;
        Ok(Self { metas, fields })
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct PresenceDiff {
    pub joins: PresenceState,
    pub leaves: PresenceState,
}

impl PresenceDiff {
    pub fn from_value(value: &Value) -> Result<Self, PresenceError> {
        let diff = value.as_object().ok_or(PresenceError::InvalidDiff)?;
        let joins = diff.get("joins").ok_or(PresenceError::InvalidDiff)?;
        let leaves = diff.get("leaves").ok_or(PresenceError::InvalidDiff)?;
        Ok(Self {
            joins: PresenceState::from_value(joins)?,
            leaves: PresenceState::from_value(leaves)?,
        })
    }

    fn extend(&mut self, other: Self) {
        extend_state(&mut self.joins, other.joins);
        extend_state(&mut self.leaves, other.leaves);
    }
}

fn extend_state(target: &mut PresenceState, source: PresenceState) {
    for (key, presence) in source.0 {
        if let Some(existing) = target.0.get_mut(&key) {
            existing.metas.extend(presence.metas);
        } else {
            target.0.insert(key, presence);
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum PresenceUpdate {
    Ignored,
    Pending,
    Synced(PresenceDiff),
}

#[derive(Clone, Debug)]
pub struct PresenceTracker {
    state: PresenceState,
    join_ref: Option<String>,
    pending_diffs: Vec<PresenceDiff>,
    state_event: String,
    diff_event: String,
}

impl PresenceTracker {
    pub fn new() -> Self {
        Self {
            state: PresenceState::default(),
            join_ref: None,
            pending_diffs: Vec::new(),
            state_event: "presence_state".into(),
            diff_event: "presence_diff".into(),
        }
    }

    pub fn with_events(state_event: impl Into<String>, diff_event: impl Into<String>) -> Self {
        Self {
            state: PresenceState::default(),
            join_ref: None,
            pending_diffs: Vec::new(),
            state_event: state_event.into(),
            diff_event: diff_event.into(),
        }
    }

    pub fn state(&self) -> &PresenceState {
        &self.state
    }

    pub fn reset(&mut self) {
        self.join_ref = None;
        self.pending_diffs.clear();
    }

    pub fn apply(&mut self, frame: &Frame) -> Result<PresenceUpdate, PresenceError> {
        if frame.event != self.state_event && frame.event != self.diff_event {
            return Ok(PresenceUpdate::Ignored);
        }
        let payload = frame
            .payload
            .as_json()
            .ok_or(PresenceError::BinaryPayload)?;
        if frame.event == self.state_event {
            let new_state = PresenceState::from_value(payload)?;
            let mut changes = sync_state(&mut self.state, new_state)?;
            for diff in self.pending_diffs.drain(..) {
                changes.extend(sync_diff(&mut self.state, diff)?);
            }
            self.join_ref = frame.join_ref.clone();
            Ok(PresenceUpdate::Synced(changes))
        } else if frame.event == self.diff_event {
            let diff = PresenceDiff::from_value(payload)?;
            if self.join_ref.is_none() || self.join_ref != frame.join_ref {
                self.pending_diffs.push(diff);
                Ok(PresenceUpdate::Pending)
            } else {
                Ok(PresenceUpdate::Synced(sync_diff(&mut self.state, diff)?))
            }
        } else {
            unreachable!("presence event names were checked before decoding")
        }
    }
}

impl Default for PresenceTracker {
    fn default() -> Self {
        Self::new()
    }
}

pub fn sync_state(
    state: &mut PresenceState,
    new_state: PresenceState,
) -> Result<PresenceDiff, PresenceError> {
    let mut joins = PresenceState::default();
    let mut leaves = PresenceState::default();
    for (key, current) in &state.0 {
        if !new_state.0.contains_key(key) {
            leaves.0.insert(key.clone(), current.clone());
        }
    }
    for (key, new_presence) in &new_state.0 {
        if let Some(current) = state.0.get(key) {
            let current_refs = refs(key, &current.metas)?;
            let new_refs = refs(key, &new_presence.metas)?;
            let joined_metas = new_presence
                .metas
                .iter()
                .filter(|meta| !current_refs.contains(meta_ref(key, meta).unwrap_or_default()))
                .cloned()
                .collect::<Vec<_>>();
            let left_metas = current
                .metas
                .iter()
                .filter(|meta| !new_refs.contains(meta_ref(key, meta).unwrap_or_default()))
                .cloned()
                .collect::<Vec<_>>();
            if !joined_metas.is_empty() {
                let mut joined = new_presence.clone();
                joined.metas = joined_metas;
                joins.0.insert(key.clone(), joined);
            }
            if !left_metas.is_empty() {
                let mut left = current.clone();
                left.metas = left_metas;
                leaves.0.insert(key.clone(), left);
            }
        } else {
            joins.0.insert(key.clone(), new_presence.clone());
        }
    }
    let changes = PresenceDiff { joins, leaves };
    sync_diff(state, changes.clone())?;
    Ok(changes)
}

pub fn sync_diff(
    state: &mut PresenceState,
    diff: PresenceDiff,
) -> Result<PresenceDiff, PresenceError> {
    for (key, joined) in &diff.joins.0 {
        let joined_refs = refs(key, &joined.metas)?;
        let mut merged = joined.clone();
        if let Some(current) = state.0.get(key) {
            let mut existing = current
                .metas
                .iter()
                .filter(|meta| !joined_refs.contains(meta_ref(key, meta).unwrap_or_default()))
                .cloned()
                .collect::<Vec<_>>();
            existing.extend(merged.metas);
            merged.metas = existing;
        }
        state.0.insert(key.clone(), merged);
    }
    for (key, left) in &diff.leaves.0 {
        let left_refs = refs(key, &left.metas)?;
        if let Some(current) = state.0.get_mut(key) {
            current
                .metas
                .retain(|meta| !left_refs.contains(meta_ref(key, meta).unwrap_or_default()));
            if current.metas.is_empty() {
                state.0.remove(key);
            }
        }
    }
    Ok(diff)
}

fn refs(key: &str, metas: &[Map<String, Value>]) -> Result<BTreeSet<String>, PresenceError> {
    metas
        .iter()
        .map(|meta| meta_ref(key, meta).map(str::to_owned))
        .collect()
}

fn meta_ref<'a>(key: &str, meta: &'a Map<String, Value>) -> Result<&'a str, PresenceError> {
    meta.get("phx_ref")
        .and_then(Value::as_str)
        .ok_or_else(|| PresenceError::MissingReference(key.to_owned()))
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum PresenceError {
    #[error("presence state must be an object")]
    InvalidState,
    #[error("presence diff must contain joins and leaves objects")]
    InvalidDiff,
    #[error("presence entry for {0} must be an object")]
    InvalidPresence(String),
    #[error("presence entry for {0} must contain a metas array")]
    InvalidMetas(String),
    #[error("presence meta for {0} must be an object")]
    InvalidMeta(String),
    #[error("presence meta for {0} must contain a string phx_ref")]
    MissingReference(String),
    #[error("presence events cannot use binary payloads")]
    BinaryPayload,
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn frame(join_ref: &str, event: &str, payload: Value) -> Frame {
        Frame::new(Some(join_ref.into()), None, "room:lobby", event, payload)
    }

    #[test]
    fn queues_diffs_until_state_and_tracks_meta_references() {
        let mut tracker = PresenceTracker::new();
        let pending = frame(
            "1",
            "presence_diff",
            json!({
                "joins": {"u1": {"metas": [{"phx_ref": "a", "online_at": 1}]}},
                "leaves": {}
            }),
        );
        assert_eq!(tracker.apply(&pending).unwrap(), PresenceUpdate::Pending);

        let state = frame("1", "presence_state", json!({}));
        let PresenceUpdate::Synced(changes) = tracker.apply(&state).unwrap() else {
            panic!("expected a presence sync");
        };
        assert!(changes.joins.get("u1").is_some());
        assert_eq!(tracker.state().get("u1").unwrap().metas.len(), 1);

        let leave = frame(
            "1",
            "presence_diff",
            json!({
                "joins": {},
                "leaves": {"u1": {"metas": [{"phx_ref": "a"}]}}
            }),
        );
        tracker.apply(&leave).unwrap();
        assert!(tracker.state().is_empty());
    }

    #[test]
    fn replaces_state_and_reports_only_changed_metas() {
        let mut current = PresenceState::from_value(&json!({
            "u1": {"metas": [{"phx_ref": "a"}, {"phx_ref": "b"}]}
        }))
        .unwrap();
        let next = PresenceState::from_value(&json!({
            "u1": {"metas": [{"phx_ref": "b"}, {"phx_ref": "c"}]}
        }))
        .unwrap();
        let diff = sync_state(&mut current, next).unwrap();
        assert_eq!(diff.joins.get("u1").unwrap().metas.len(), 1);
        assert_eq!(diff.leaves.get("u1").unwrap().metas.len(), 1);
        assert_eq!(current.get("u1").unwrap().metas.len(), 2);
    }
}
