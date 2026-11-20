//! Credentials the control plane has issued to a session, kept so the same
//! sign-in can rejoin while the control plane cannot be reached.
//!
//! Nothing is granted that the control plane had not already granted. An entry
//! is the answer to one exchange of one ID token for one scope, found again
//! only by presenting that same ID token for that same scope, and it stops
//! counting at the earliest of the ID token's expiry, the Felix token's expiry
//! and a cap after the control plane issued it.
//! Reuse happens only when the control plane cannot be reached. Any answer
//! from it, a refusal included, deletes the entry. See
//! `docs/configuration.md#when-the-control-plane-is-down`.

use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use sha2::{Digest, Sha256};

use super::{Grant, claims};
use crate::scope::Scope;

/// A credential with less than this left is not reused: the broker would
/// refuse it before the session did anything with it.
pub(super) const MIN_REMAINING: u64 = 30;

/// Entries kept. When full, the one that expires first goes.
const MAX_ENTRIES: usize = 10_000;

struct Held {
    felix_token: String,
    principal: String,
    missing: Vec<String>,
    /// The token delegated to the gateway, with shared connections.
    delegated: Option<(String, u64)>,
    felix_exp: u64,
    id_exp: u64,
    /// When the control plane last issued the token, which the cap counts from.
    obtained: u64,
}

pub(crate) struct Rejoin {
    entries: Mutex<HashMap<String, Held>>,
    /// The longest a grant may be reused after the control plane issued it.
    max: u64,
    /// Counts every [`Self::forget`], so an exchange that began before one is
    /// not kept after it.
    forgets: AtomicU64,
}

impl Rejoin {
    pub(crate) fn new(max: Duration) -> Self {
        Self {
            entries: Mutex::default(),
            max: max.as_secs(),
            forgets: AtomicU64::new(0),
        }
    }

    fn expires(&self, held: &Held) -> u64 {
        held.felix_exp
            .min(held.id_exp)
            .min(held.obtained.saturating_add(self.max))
    }

    /// What an ID token and scope are looked up by. The token itself is not
    /// kept.
    ///
    /// The key starts with a hash of the ID token alone, then a hash of the
    /// scope, so [`Self::forget`] can find every scope one ID token holds.
    pub(crate) fn key(id_token: &str, scope: &Scope) -> String {
        let hex = |hash: Sha256| -> String {
            hash.finalize().iter().map(|b| format!("{b:02x}")).collect()
        };
        let token = hex(Sha256::new().chain_update(id_token.as_bytes()));
        let mut hash = Sha256::new();
        for part in [scope.namespace(), scope.value()] {
            hash.update((part.len() as u64).to_be_bytes());
            hash.update(part.as_bytes());
        }
        format!("{token}:{}", hex(hash))
    }

    /// Keep the answer to an exchange. A token without an `exp`, or an ID token
    /// without one, is not kept: nothing bounds how long it could be reused.
    /// Read before asking the control plane, and pass to [`Self::remember`].
    pub(crate) fn forgotten(&self) -> u64 {
        self.forgets.load(Ordering::SeqCst)
    }

    pub(crate) fn remember(&self, key: &str, id_token: &str, grant: &Grant, now: u64, seen: u64) {
        let (Some(felix_exp), Some(id_exp)) = (
            claims(&grant.felix_token).and_then(|c| c.exp),
            claims(id_token).and_then(|c| c.exp),
        ) else {
            return;
        };
        let held = Held {
            felix_token: grant.felix_token.clone(),
            principal: grant.principal.clone(),
            missing: grant.missing.clone(),
            delegated: None,
            felix_exp,
            id_exp,
            obtained: now,
        };
        let mut entries = self.entries.lock().unwrap_or_else(|e| e.into_inner());
        // A refusal landed while this exchange was in flight: its answer may
        // predate the revocation, so it is not kept.
        if self.forgets.load(Ordering::SeqCst) != seen {
            return;
        }
        entries.retain(|_, held| self.expires(held) > now);
        if entries.len() >= MAX_ENTRIES
            && let Some(soonest) = entries
                .iter()
                .min_by_key(|(_, held)| self.expires(held))
                .map(|(key, _)| key.clone())
        {
            entries.remove(&soonest);
        }
        entries.insert(key.to_string(), held);
    }

    /// Note the delegated token a shared session connected with.
    pub(crate) fn remember_delegated(&self, key: &str, delegated: &str) {
        let Some(exp) = claims(delegated).and_then(|c| c.exp) else {
            return;
        };
        if let Some(held) = self
            .entries
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get_mut(key)
        {
            held.delegated = Some((delegated.to_string(), exp));
        }
    }

    /// Note the tokens a session's refresh obtained. The refresh ran RBAC
    /// again, so this is as current as a new exchange. The ID token's expiry
    /// is unchanged.
    pub(crate) fn refreshed(
        &self,
        key: &str,
        felix_token: &str,
        delegated: Option<&str>,
        now: u64,
    ) {
        let Some(felix_exp) = claims(felix_token).and_then(|c| c.exp) else {
            return;
        };
        if let Some(held) = self
            .entries
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get_mut(key)
        {
            held.felix_token = felix_token.to_string();
            held.felix_exp = felix_exp;
            held.obtained = now;
            held.delegated =
                delegated.and_then(|token| Some((token.to_string(), claims(token)?.exp?)));
        }
    }

    /// Drop what is kept for `key`, and for every other scope the same ID
    /// token holds: the control plane refused the sign-in, so what it granted
    /// before no longer stands in for it.
    pub(crate) fn forget(&self, key: &str) {
        let mut entries = self.entries.lock().unwrap_or_else(|e| e.into_inner());
        self.forgets.fetch_add(1, Ordering::SeqCst);
        let token = key.split_once(':').map_or(key, |(token, _)| token);
        entries
            .retain(|other, _| other.split_once(':').map_or(other.as_str(), |(t, _)| t) != token);
    }

    /// The grant kept for `key`, if it is still good for at least
    /// [`MIN_REMAINING`] seconds. It has no refresh token, so a session on it
    /// ends when the token does.
    pub(crate) fn recall(&self, key: &str, now: u64) -> Option<Grant> {
        let mut entries = self.entries.lock().unwrap_or_else(|e| e.into_inner());
        let held = entries.get(key)?;
        if self.expires(held) < now + MIN_REMAINING {
            entries.remove(key);
            return None;
        }
        let delegated = held
            .delegated
            .as_ref()
            .filter(|(_, exp)| *exp >= now + MIN_REMAINING)
            .map(|(token, _)| token.clone());
        Some(Grant {
            felix_token: held.felix_token.clone(),
            refresh_token: None,
            principal: held.principal.clone(),
            missing: held.missing.clone(),
            key: key.to_string(),
            delegated,
        })
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.entries.lock().unwrap().len()
    }
}
