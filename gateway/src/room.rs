//! A room's name and the Felix resources that belong to it.

/// A room name the gateway accepts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Room(String);

impl Room {
    /// `None` unless `name` passes [`valid_name`].
    pub(crate) fn parse(name: &str) -> Option<Self> {
        valid_name(name).then(|| Self(name.to_string()))
    }

    pub(crate) fn name(&self) -> &str {
        &self.0
    }

    /// The durable edit log.
    pub(crate) fn ops(&self) -> String {
        format!("canvas.ops.{}", self.0)
    }

    /// The in-memory cursor feed.
    pub(crate) fn presence(&self) -> String {
        format!("canvas.presence.{}", self.0)
    }

    /// Per-session op sequence counters, keyed by session.
    pub(crate) fn seq(&self) -> String {
        format!("canvas.seq.{}", self.0)
    }

    /// The snapshot, under [`SNAPSHOT_KEY`].
    pub(crate) fn snapshots(&self) -> String {
        format!("canvas.snap.{}", self.0)
    }

    /// Who is in the room, one entry per session that expires unless refreshed.
    pub(crate) fn members(&self) -> String {
        format!("canvas.members.{}", self.0)
    }

    /// Every permission a session in this room needs, as Felix writes them in
    /// a token: `<action>:<object>`. Each room has its own streams and caches
    /// because Felix authorizes a cache as a whole, never one key of it.
    pub(crate) fn grants(&self, tenant: &str, namespace: &str) -> Vec<String> {
        let stream = |name: String| format!("stream:{tenant}/{namespace}/{name}");
        let cache = |name: String| format!("cache:{tenant}/{namespace}/{name}");
        vec![
            format!("stream.publish:{}", stream(self.ops())),
            format!("stream.subscribe:{}", stream(self.ops())),
            format!("stream.publish:{}", stream(self.presence())),
            format!("stream.subscribe:{}", stream(self.presence())),
            // Counters authorize as cache writes.
            format!("cache.write:{}", cache(self.seq())),
            format!("cache.read:{}", cache(self.snapshots())),
            format!("cache.read:{}", cache(self.members())),
            format!("cache.write:{}", cache(self.members())),
        ]
    }
}

/// The key the snapshotter writes a room's snapshot under.
pub(crate) const SNAPSHOT_KEY: &str = "latest";

pub(crate) const BAD_NAME: &str = "a name is 1 to 64 ASCII letters, digits, '-' or '_'";

/// Room names and counter keys end up inside Felix stream, cache and RBAC
/// names, so they are kept to an alphabet with no separators or wildcards.
pub(crate) fn valid_name(name: &str) -> bool {
    (1..=64).contains(&name.len())
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_that_could_reach_another_resource_are_refused() {
        for bad in ["", "a.b", "a/b", "a:b", "*", "lobby*", &"x".repeat(65)] {
            assert!(Room::parse(bad).is_none(), "{bad:?}");
        }
        assert_eq!(Room::parse("studio-2_b").unwrap().name(), "studio-2_b");
    }

    #[test]
    fn a_room_needs_its_own_streams_and_caches_only() {
        let grants = Room::parse("lobby").unwrap().grants("canvas", "default");
        assert_eq!(
            grants,
            [
                "stream.publish:stream:canvas/default/canvas.ops.lobby",
                "stream.subscribe:stream:canvas/default/canvas.ops.lobby",
                "stream.publish:stream:canvas/default/canvas.presence.lobby",
                "stream.subscribe:stream:canvas/default/canvas.presence.lobby",
                "cache.write:cache:canvas/default/canvas.seq.lobby",
                "cache.read:cache:canvas/default/canvas.snap.lobby",
                "cache.read:cache:canvas/default/canvas.members.lobby",
                "cache.write:cache:canvas/default/canvas.members.lobby",
            ]
        );
    }
}
