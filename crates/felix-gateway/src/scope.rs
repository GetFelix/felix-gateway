//! The scope file, and the Felix streams, caches and counters one scope owns.
//! Every Felix name the gateway uses comes from this file.

use std::collections::{BTreeMap, HashSet};
use std::fmt::Debug;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, ensure};
use serde::Deserialize;

/// Where a scope's value goes in a resource name.
const PLACEHOLDER: &str = "{scope}";

/// Keys of `join` that cannot name the scope.
const RESERVED_FIELDS: [&str; 4] = ["type", "token", "protocol", "features"];

pub(crate) const BAD_NAME: &str = "a name is 1 to 64 ASCII letters, digits, '-' or '_'";

/// The scope file: what one scope holds and what a session may do with it.
/// `docs/protocol.md` describes the format.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScopeConfig {
    /// Whether a browser may slow its own connection with `throttle`.
    #[serde(default)]
    pub(crate) allow_throttle: bool,
    pub(crate) scope: ScopeSpec,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ScopeSpec {
    /// The key `join` carries the scope's value under, such as `room`.
    pub(crate) field: String,
    #[serde(default)]
    pub(crate) streams: Vec<Resource<StreamAction>>,
    #[serde(default)]
    pub(crate) caches: Vec<Resource<CacheAction>>,
    #[serde(default)]
    pub(crate) counters: Vec<Resource<CounterAction>>,
}

/// One stream, cache or counter, addressed by `alias` on the wire.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Resource<A> {
    pub(crate) alias: String,
    /// The Felix name, with `{scope}` where the scope's value goes.
    name: String,
    actions: Vec<A>,
    /// A session opens without it when its sign-in does not reach it.
    #[serde(default)]
    optional: bool,
    /// Streams only: each publish is prefixed with the publisher's principal.
    #[serde(default)]
    pub(crate) stamp_sender: bool,
    /// Caches only: how long a written entry lasts without another write.
    ttl_s: Option<u64>,
}

impl<A> Resource<A> {
    pub(crate) fn ttl(&self) -> Option<Duration> {
        self.ttl_s.map(Duration::from_secs)
    }
}

/// What a session may do to one kind of resource, and the Felix permission
/// each action needs.
pub(crate) trait Action: Copy + PartialEq + Debug {
    /// The resource kind, as messages and the scope file name it.
    const KIND: &'static str;
    /// The kind of Felix object it is.
    const OBJECT: &'static str;
    fn permission(self) -> &'static str;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum StreamAction {
    Publish,
    Subscribe,
}

impl Action for StreamAction {
    const KIND: &'static str = "stream";
    const OBJECT: &'static str = "stream";
    fn permission(self) -> &'static str {
        match self {
            Self::Publish => "stream.publish",
            Self::Subscribe => "stream.subscribe",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum CacheAction {
    Read,
    Write,
    Watch,
}

impl Action for CacheAction {
    const KIND: &'static str = "cache";
    const OBJECT: &'static str = "cache";
    fn permission(self) -> &'static str {
        match self {
            Self::Read | Self::Watch => "cache.read",
            Self::Write => "cache.write",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum CounterAction {
    Add,
}

impl Action for CounterAction {
    const KIND: &'static str = "counter";
    // Felix keeps counters in caches and authorizes an add as a cache write.
    const OBJECT: &'static str = "cache";
    fn permission(self) -> &'static str {
        "cache.write"
    }
}

impl ScopeConfig {
    /// Read and check the scope file at `path`.
    ///
    /// # Errors
    /// When the file cannot be read, does not parse, or breaks a rule in
    /// `docs/protocol.md`.
    pub fn load(path: &Path) -> Result<Self> {
        let text =
            std::fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
        Self::parse(&text).with_context(|| format!("scope file {}", path.display()))
    }

    /// Parse and check a scope file's text.
    ///
    /// # Errors
    /// As for [`ScopeConfig::load`], past reading the file.
    pub fn parse(text: &str) -> Result<Self> {
        let config: Self = toml::from_str(text)?;
        let field = &config.scope.field;
        ensure!(
            valid_name(field) && !RESERVED_FIELDS.contains(&field.as_str()),
            "field {field:?}: {BAD_NAME}, and not one of {RESERVED_FIELDS:?}"
        );
        check(&config.scope.streams)?;
        check(&config.scope.caches)?;
        check(&config.scope.counters)?;
        Ok(config)
    }

    /// The Felix name of stream `alias` in `scope`, if the file declares one.
    pub fn stream_name(&self, alias: &str, scope: &str) -> Option<String> {
        let stream = self.scope.streams.iter().find(|s| s.alias == alias)?;
        Some(stream.name.replace(PLACEHOLDER, scope))
    }

    /// How long an entry of each cache with a TTL lasts, in milliseconds.
    pub(crate) fn cache_ttl_ms(&self) -> BTreeMap<String, u64> {
        self.scope
            .caches
            .iter()
            .filter_map(|cache| Some((cache.alias.clone(), cache.ttl_s? * 1000)))
            .collect()
    }
}

fn check<A: Action>(resources: &[Resource<A>]) -> Result<()> {
    let mut aliases = HashSet::new();
    for resource in resources {
        let kind = A::KIND;
        let alias = &resource.alias;
        ensure!(valid_name(alias), "{kind} {alias:?}: {BAD_NAME}");
        ensure!(aliases.insert(alias), "{kind} {alias:?} is declared twice");
        ensure!(
            resource.name.contains(PLACEHOLDER),
            "{kind} {alias:?}: the name must contain {PLACEHOLDER}, or every scope would share it"
        );
        ensure!(!resource.actions.is_empty(), "{kind} {alias:?}: no actions");
        for (i, action) in resource.actions.iter().enumerate() {
            ensure!(
                !resource.actions[..i].contains(action),
                "{kind} {alias:?}: {action:?} is listed twice"
            );
        }
        ensure!(
            !resource.stamp_sender || kind == StreamAction::KIND,
            "{kind} {alias:?}: only a stream can stamp_sender"
        );
        ensure!(
            resource.ttl_s.is_none() || kind == CacheAction::KIND,
            "{kind} {alias:?}: only a cache has a ttl_s"
        );
        ensure!(
            resource.ttl_s != Some(0),
            "{kind} {alias:?}: ttl_s must be above 0"
        );
    }
    Ok(())
}

/// Scope values and keys end up inside Felix stream, cache and RBAC names, so
/// they are kept to an alphabet with no separators or wildcards.
pub(crate) fn valid_name(name: &str) -> bool {
    (1..=64).contains(&name.len())
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
}

/// One permission a session in a scope asks for, as Felix writes it in a
/// token: `<action>:<object>`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Permission {
    pub(crate) grant: String,
    /// The alias of the optional resource it is for; `None` when required.
    pub(crate) optional: Option<String>,
}

/// A resource resolved in one scope, for an action it allows.
pub(crate) struct Bound<'a, A> {
    pub(crate) resource: &'a Resource<A>,
    /// The Felix name.
    pub(crate) name: String,
}

/// One value of the scope, such as `lobby` when the field is `room`.
#[derive(Debug, Clone)]
pub(crate) struct Scope {
    config: Arc<ScopeConfig>,
    value: String,
}

impl Scope {
    /// `None` unless `value` passes [`valid_name`].
    pub(crate) fn parse(config: &Arc<ScopeConfig>, value: &str) -> Option<Self> {
        valid_name(value).then(|| Self {
            config: Arc::clone(config),
            value: value.to_string(),
        })
    }

    pub(crate) fn config(&self) -> &ScopeConfig {
        &self.config
    }

    pub(crate) fn value(&self) -> &str {
        &self.value
    }

    pub(crate) fn stream(
        &self,
        alias: &str,
        action: StreamAction,
    ) -> Result<Bound<'_, StreamAction>, String> {
        self.bind(&self.config.scope.streams, alias, action)
    }

    pub(crate) fn cache(
        &self,
        alias: &str,
        action: CacheAction,
    ) -> Result<Bound<'_, CacheAction>, String> {
        self.bind(&self.config.scope.caches, alias, action)
    }

    pub(crate) fn counter(
        &self,
        alias: &str,
        action: CounterAction,
    ) -> Result<Bound<'_, CounterAction>, String> {
        self.bind(&self.config.scope.counters, alias, action)
    }

    /// `resources`' `alias` with its Felix name, or why a session cannot use
    /// it for `action`.
    fn bind<'a, A: Action>(
        &self,
        resources: &'a [Resource<A>],
        alias: &str,
        action: A,
    ) -> Result<Bound<'a, A>, String> {
        let kind = A::KIND;
        let resource = resources
            .iter()
            .find(|resource| resource.alias == alias)
            .ok_or_else(|| format!("there is no {kind} {alias:?}"))?;
        if !resource.actions.contains(&action) {
            return Err(format!("the {kind} {alias:?} does not allow {action:?}"));
        }
        Ok(Bound {
            resource,
            name: resource.name.replace(PLACEHOLDER, &self.value),
        })
    }

    /// Every permission a session in this scope asks for. Each scope has its
    /// own resources because Felix authorizes a cache as a whole, never one
    /// key of it.
    pub(crate) fn permissions(&self, tenant: &str, namespace: &str) -> Vec<Permission> {
        let mut permissions = Vec::new();
        let spec = &self.config.scope;
        self.add_permissions(&mut permissions, &spec.streams, tenant, namespace);
        self.add_permissions(&mut permissions, &spec.caches, tenant, namespace);
        self.add_permissions(&mut permissions, &spec.counters, tenant, namespace);
        permissions
    }

    fn add_permissions<A: Action>(
        &self,
        permissions: &mut Vec<Permission>,
        resources: &[Resource<A>],
        tenant: &str,
        namespace: &str,
    ) {
        for resource in resources {
            let name = resource.name.replace(PLACEHOLDER, &self.value);
            for action in &resource.actions {
                let permission = Permission {
                    grant: format!(
                        "{}:{}:{tenant}/{namespace}/{name}",
                        action.permission(),
                        A::OBJECT
                    ),
                    optional: resource.optional.then(|| resource.alias.clone()),
                };
                // A cache's read and watch need the same grant.
                if !permissions.contains(&permission) {
                    permissions.push(permission);
                }
            }
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    /// A scope with streams, caches and a counter, plus an optional stream.
    pub(crate) const EXAMPLE: &str = r#"
        allow_throttle = true

        [scope]
        field = "room"

        [[scope.streams]]
        alias = "ops"
        name = "app.ops.{scope}"
        actions = ["publish", "subscribe"]

        [[scope.streams]]
        alias = "input"
        name = "app.input.{scope}"
        actions = ["publish"]
        optional = true
        stamp_sender = true

        [[scope.caches]]
        alias = "members"
        name = "app.members.{scope}"
        actions = ["read", "write", "watch"]
        ttl_s = 30

        [[scope.counters]]
        alias = "seq"
        name = "app.seq.{scope}"
        actions = ["add"]
    "#;

    pub(crate) fn lobby() -> Scope {
        let config = Arc::new(ScopeConfig::parse(EXAMPLE).unwrap());
        Scope::parse(&config, "lobby").unwrap()
    }

    #[test]
    fn values_that_could_reach_another_resource_are_refused() {
        let config = Arc::new(ScopeConfig::parse(EXAMPLE).unwrap());
        for bad in ["", "a.b", "a/b", "a:b", "*", "lobby*", &"x".repeat(65)] {
            assert!(Scope::parse(&config, bad).is_none(), "{bad:?}");
        }
        assert_eq!(
            Scope::parse(&config, "studio-2_b").unwrap().value(),
            "studio-2_b"
        );
    }

    #[test]
    fn a_scope_asks_for_its_own_resources_only() {
        let permissions = lobby().permissions("t", "default");
        let shown: Vec<(&str, Option<&str>)> = permissions
            .iter()
            .map(|p| (p.grant.as_str(), p.optional.as_deref()))
            .collect();
        assert_eq!(
            shown,
            [
                ("stream.publish:stream:t/default/app.ops.lobby", None),
                ("stream.subscribe:stream:t/default/app.ops.lobby", None),
                (
                    "stream.publish:stream:t/default/app.input.lobby",
                    Some("input")
                ),
                ("cache.read:cache:t/default/app.members.lobby", None),
                ("cache.write:cache:t/default/app.members.lobby", None),
                ("cache.write:cache:t/default/app.seq.lobby", None),
            ]
        );
    }

    #[test]
    fn binds_an_alias_only_for_actions_it_allows() {
        let lobby = lobby();
        let ops = lobby.stream("ops", StreamAction::Subscribe).unwrap();
        assert_eq!(ops.name, "app.ops.lobby");
        assert!(lobby.stream("input", StreamAction::Subscribe).is_err());
        assert!(lobby.stream("chat", StreamAction::Publish).is_err());
        assert!(lobby.cache("ops", CacheAction::Read).is_err());
        let members = lobby.cache("members", CacheAction::Watch).unwrap();
        assert_eq!(members.resource.ttl(), Some(Duration::from_secs(30)));
        assert_eq!(
            lobby.config().cache_ttl_ms(),
            BTreeMap::from([("members".to_string(), 30_000)])
        );
    }

    #[test]
    fn rejects_a_file_that_breaks_a_rule() {
        let stream = |extra: &str| {
            format!(
                "[scope]\nfield = \"room\"\n[[scope.streams]]\nalias = \"ops\"\n\
                 name = \"a.{{scope}}\"\nactions = [\"publish\"]\n{extra}"
            )
        };
        assert!(ScopeConfig::parse(&stream("")).is_ok());
        for bad in [
            "[scope]\nfield = \"token\"".to_string(),
            "[scope]\nfield = \"a.b\"".to_string(),
            "[scope]\nfield = \"room\"\nextra = 1".to_string(),
            stream("ttl_s = 5"),
            stream("actions = []").replace("actions = [\"publish\"]\n", ""),
            stream("").replace("{scope}", "all"),
            stream("").replace("\"publish\"", "\"publish\", \"publish\""),
            stream("").replace("\"publish\"", "\"read\""),
            stream(
                "[[scope.streams]]\nalias = \"ops\"\nname = \"b.{scope}\"\nactions = [\"publish\"]",
            ),
            stream("")
                .replace("streams", "counters")
                .replace("publish", "add")
                + "stamp_sender = true",
        ] {
            assert!(ScopeConfig::parse(&bad).is_err(), "{bad}");
        }
    }
}
