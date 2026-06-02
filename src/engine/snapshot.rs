/// In-memory snapshot of latest tag values.
///
/// Per D002, latest values come from `tag.last_*` columns, but the running
/// poller also maintains a concurrent-friendly snapshot so status reads
/// never hit SQLite.

use crate::types::*;
use std::collections::HashMap;
use parking_lot::RwLock;

/// Thread-safe in-memory tag value cache.
pub struct Snapshot {
    inner: RwLock<SnapshotInner>,
}

struct SnapshotInner {
    tags: HashMap<i64, TagSnapshot>,       // tag_id -> snapshot
    by_name: HashMap<(i64, String), i64>,  // (plc_id, name) -> tag_id
}

impl Snapshot {
    pub fn new() -> Self {
        Snapshot {
            inner: RwLock::new(SnapshotInner {
                tags: HashMap::new(),
                by_name: HashMap::new(),
            }),
        }
    }

    /// Update a tag's value in the snapshot.
    pub fn update(&self, tag_id: i64, plc_id: i64, name: &str, address: &str, data_type: DataType, vi: Option<i64>, vf: Option<f64>, ts: TimestampUs) {
        let mut inner = self.inner.write();
        inner.tags.insert(tag_id, TagSnapshot {
            tag_id,
            plc_id,
            name: name.to_string(),
            address: address.to_string(),
            data_type,
            last_value_i: vi,
            last_value_f: vf,
            last_ts: Some(ts),
        });
        inner.by_name.insert((plc_id, name.to_string()), tag_id);
    }

    /// Get a tag snapshot by tag ID.
    pub fn get_by_id(&self, tag_id: i64) -> Option<TagSnapshot> {
        self.inner.read().tags.get(&tag_id).cloned()
    }

    /// Get a tag snapshot by PLC ID and tag name.
    pub fn get_by_name(&self, plc_id: i64, name: &str) -> Option<TagSnapshot> {
        let inner = self.inner.read();
        inner.by_name.get(&(plc_id, name.to_string())).and_then(|id| inner.tags.get(id).cloned())
    }

    /// Get all snapshots for a PLC.
    pub fn get_by_plc(&self, plc_id: i64) -> Vec<TagSnapshot> {
        self.inner
            .read()
            .tags
            .values()
            .filter(|t| t.plc_id == plc_id)
            .cloned()
            .collect()
    }

    /// Remove a tag from the snapshot.
    pub fn remove(&self, tag_id: i64) {
        let mut inner = self.inner.write();
        if let Some(tag) = inner.tags.remove(&tag_id) {
            inner.by_name.remove(&(tag.plc_id, tag.name));
        }
    }

    /// Total number of tracked tags.
    pub fn len(&self) -> usize {
        self.inner.read().tags.len()
    }

    /// Clear all entries.
    pub fn clear(&self) {
        let mut inner = self.inner.write();
        inner.tags.clear();
        inner.by_name.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_update_and_get() {
        let snap = Snapshot::new();
        snap.update(1, 1, "motor.speed", "DB1.DBW0", DataType::I16, Some(42), None, 1000);
        let tag = snap.get_by_id(1).unwrap();
        assert_eq!(tag.last_value_i, Some(42));
        assert_eq!(tag.name, "motor.speed");
    }

    #[test]
    fn test_get_by_name() {
        let snap = Snapshot::new();
        snap.update(1, 1, "motor.speed", "DB1.DBW0", DataType::I16, Some(42), None, 1000);
        let tag = snap.get_by_name(1, "motor.speed").unwrap();
        assert_eq!(tag.last_value_i, Some(42));
    }

    #[test]
    fn test_remove() {
        let snap = Snapshot::new();
        snap.update(1, 1, "motor.speed", "DB1.DBW0", DataType::I16, Some(42), None, 1000);
        snap.remove(1);
        assert!(snap.get_by_id(1).is_none());
    }

    #[test]
    fn test_get_by_plc() {
        let snap = Snapshot::new();
        snap.update(1, 1, "a", "", DataType::I16, None, None, 0);
        snap.update(2, 1, "b", "", DataType::I16, None, None, 0);
        snap.update(3, 2, "c", "", DataType::I16, None, None, 0);
        assert_eq!(snap.get_by_plc(1).len(), 2);
        assert_eq!(snap.get_by_plc(2).len(), 1);
    }
}
