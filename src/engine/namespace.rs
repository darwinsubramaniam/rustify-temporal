//! Namespaces: register, describe, list, update, deprecate, and the namespace-state rules every
//! other operation follows. Behaviour follows the Go server's frontend namespace handler for a
//! single cluster named "active" with global namespaces and archival not configured. A "default"
//! namespace always exists.

use std::collections::HashMap;

use prost::Message;

use super::*;
use crate::proto::temporal::api::replication::v1 as replication;

pub const DEFAULT_RETENTION_MS: i64 = 3 * 24 * 3600 * 1000;
/// The Go server's `system.namespaceMinRetentionLocal` default.
pub const MIN_RETENTION_MS: i64 = 3600 * 1000;
/// The Go server's `frontend.maxBadBinaries` default.
pub const MAX_BAD_BINARIES: usize = 10;
pub const CLUSTER_NAME: &str = "active";
pub const DEFAULT_PAGE_SIZE: usize = 100;

use enums::NamespaceState as NsState;

/// States in which most operations are allowed (the Go server's default).
pub const ALLOW_DEFAULT: &[NsState] = &[NsState::Registered, NsState::Deprecated];
/// Starting new workflows needs a registered (not deprecated) namespace.
pub const ALLOW_START: &[NsState] = &[NsState::Registered];

fn invalid_state(ns: &str, state: i32, allowed: &[NsState]) -> Status {
    let st = NsState::try_from(state).unwrap_or(NsState::Unspecified);
    let names: Vec<&str> = allowed.iter().map(|s| s.as_str_name()).collect();
    status_with_detail(
        tonic::Code::FailedPrecondition,
        &format!(
            "Namespace has invalid state: {}. Must be {}.",
            st.as_str_name(),
            names.join(" or ")
        ),
        "temporal.api.errordetails.v1.NamespaceInvalidStateFailure",
        &errordetails::NamespaceInvalidStateFailure {
            namespace: ns.to_string(),
            state,
            allowed_states: allowed.iter().map(|s| *s as i32).collect(),
        },
    )
}

fn validate_retention(d: &Option<prost_types::Duration>) -> R<i64> {
    let ms = dur_ms(d);
    if ms < MIN_RETENTION_MS {
        return Err(invalid("A valid retention period is not set on request."));
    }
    Ok(ms)
}

fn validate_cluster_name(name: &str) -> R<()> {
    if name != CLUSTER_NAME {
        return Err(invalid(format!("Invalid cluster name: {name}")));
    }
    Ok(())
}

/// A local namespace must be active in this cluster and replicated only to it.
fn validate_local_replication(active: &str, clusters: &[String]) -> R<()> {
    validate_cluster_name(active)?;
    for c in clusters {
        validate_cluster_name(c)?;
    }
    if clusters.len() != 1 || clusters[0] != active {
        return Err(invalid("Invalid local namespace clusters"));
    }
    Ok(())
}

/// Allowed namespace state changes through UpdateNamespace (the Go server's validateStateUpdate).
fn validate_state_update(old: i32, new: i32) -> R<()> {
    if new == NsState::Unspecified as i32 || new == old {
        return Ok(());
    }
    let ok = match NsState::try_from(old).unwrap_or(NsState::Unspecified) {
        NsState::Registered => new == NsState::Deleted as i32 || new == NsState::Deprecated as i32,
        NsState::Deprecated => new == NsState::Deleted as i32,
        _ => false,
    };
    if ok {
        Ok(())
    } else {
        Err(invalid("Invalid namespace state update."))
    }
}

fn build_desc(
    name: &str,
    id: &str,
    description: &str,
    owner: &str,
    retention_ms: i64,
    data: HashMap<String, String>,
) -> wsv1::DescribeNamespaceResponse {
    wsv1::DescribeNamespaceResponse {
        namespace_info: Some(namespace_pb::NamespaceInfo {
            name: name.to_string(),
            state: NsState::Registered as i32,
            description: description.to_string(),
            owner_email: owner.to_string(),
            data,
            id: id.to_string(),
            capabilities: Some(namespace_pb::namespace_info::Capabilities::default()),
            ..Default::default()
        }),
        config: Some(namespace_pb::NamespaceConfig {
            workflow_execution_retention_ttl: Some(dur_from_ms(retention_ms)),
            bad_binaries: Some(namespace_pb::BadBinaries::default()),
            history_archival_state: enums::ArchivalState::Disabled as i32,
            visibility_archival_state: enums::ArchivalState::Disabled as i32,
            ..Default::default()
        }),
        replication_config: Some(replication::NamespaceReplicationConfig {
            active_cluster_name: CLUSTER_NAME.into(),
            clusters: vec![replication::ClusterReplicationConfig {
                cluster_name: CLUSTER_NAME.into(),
                ..Default::default()
            }],
            state: enums::ReplicationState::Normal as i32,
        }),
        failover_version: 0,
        is_global_namespace: false,
        ..Default::default()
    }
}

impl Inner {
    pub(super) fn load_namespaces(&mut self) -> anyhow::Result<()> {
        for b in self.store.load_namespaces()? {
            let d = wsv1::DescribeNamespaceResponse::decode(b.as_slice())?;
            let info = d.namespace_info.clone().unwrap_or_default();
            self.namespaces.insert(
                info.name.clone(),
                NsEntry {
                    id: info.id.clone(),
                    desc: d,
                },
            );
        }
        if !self.namespaces.contains_key("default") {
            self.create_namespace(
                "default",
                "Default namespace",
                "",
                DEFAULT_RETENTION_MS,
                Default::default(),
            )
            .map_err(|e| anyhow::anyhow!(e.message().to_string()))?;
        }
        Ok(())
    }

    fn create_namespace(
        &mut self,
        name: &str,
        description: &str,
        owner: &str,
        retention_ms: i64,
        data: HashMap<String, String>,
    ) -> R<()> {
        let id = new_id();
        let d = build_desc(name, &id, description, owner, retention_ms, data);
        self.save_namespace_desc(name, &id, d)
    }

    fn save_namespace_desc(&mut self, name: &str, id: &str, d: wsv1::DescribeNamespaceResponse) -> R<()> {
        self.store
            .save_namespace(name, id, &d.encode_to_vec())
            .map_err(internal)?;
        self.namespaces.insert(
            name.to_string(),
            NsEntry {
                id: id.to_string(),
                desc: d,
            },
        );
        Ok(())
    }

    pub fn ns_id(&self, ns: &str) -> R<String> {
        self.namespaces
            .get(ns)
            .map(|n| n.id.clone())
            .ok_or_else(|| namespace_not_found(ns))
    }

    fn ns_state(&self, ns: &str) -> R<i32> {
        self.namespaces
            .get(ns)
            .map(|n| n.desc.namespace_info.as_ref().map(|i| i.state).unwrap_or(0))
            .ok_or_else(|| namespace_not_found(ns))
    }

    /// The namespace exists and is in one of the allowed states (the Go server's namespace validator).
    pub fn check_ns(&self, ns: &str, allowed: &[NsState]) -> R<()> {
        if ns.is_empty() {
            return Err(invalid("Namespace is not set on request."));
        }
        let state = self.ns_state(ns)?;
        if allowed.iter().any(|s| *s as i32 == state) {
            Ok(())
        } else {
            Err(invalid_state(ns, state, allowed))
        }
    }

    /// Whether a worker build (binary checksum) is marked bad in the namespace.
    pub fn is_bad_binary(&self, ns: &str, checksum: &str) -> bool {
        !checksum.is_empty()
            && self
                .namespaces
                .get(ns)
                .and_then(|n| n.desc.config.as_ref())
                .and_then(|c| c.bad_binaries.as_ref())
                .is_some_and(|b| b.binaries.contains_key(checksum))
    }

    pub fn register_namespace(&mut self, r: wsv1::RegisterNamespaceRequest) -> R<()> {
        if r.namespace.is_empty() {
            return Err(invalid("Namespace is not set on request."));
        }
        if r.is_global_namespace {
            return Err(invalid("Cannot register global namespace when not enabled"));
        }
        let retention = validate_retention(&r.workflow_execution_retention_period)?;
        if self.namespaces.contains_key(&r.namespace) {
            return Err(status_with_detail(
                tonic::Code::AlreadyExists,
                &format!("Namespace \"{}\" already exists", r.namespace),
                "temporal.api.errordetails.v1.NamespaceAlreadyExistsFailure",
                &errordetails::NamespaceAlreadyExistsFailure {},
            ));
        }
        let active = if r.active_cluster_name.is_empty() {
            CLUSTER_NAME.to_string()
        } else {
            r.active_cluster_name.clone()
        };
        let mut clusters: Vec<String> = r.clusters.iter().map(|c| c.cluster_name.clone()).collect();
        if clusters.is_empty() {
            clusters.push(active.clone());
        }
        validate_local_replication(&active, &clusters)?;
        // Archival isn't configured on this server, so (like the Go server in that case) the
        // archival fields are accepted and archival stays disabled.
        self.create_namespace(&r.namespace, &r.description, &r.owner_email, retention, r.data)
    }

    pub fn describe_namespace(&self, name: &str, id: &str) -> R<wsv1::DescribeNamespaceResponse> {
        if !name.is_empty() {
            return self
                .namespaces
                .get(name)
                .map(|n| n.desc.clone())
                .ok_or_else(|| namespace_not_found(name));
        }
        if !id.is_empty() {
            return self
                .namespaces
                .values()
                .find(|n| n.id == id)
                .map(|n| n.desc.clone())
                .ok_or_else(|| namespace_not_found(id));
        }
        Err(invalid("Namespace is not set on request."))
    }

    /// Namespaces in name order, a page at a time; deleted ones only when asked for.
    pub fn list_namespaces(&self, r: &wsv1::ListNamespacesRequest) -> R<wsv1::ListNamespacesResponse> {
        let include_deleted = r.namespace_filter.as_ref().is_some_and(|f| f.include_deleted);
        let mut all: Vec<&NsEntry> = self
            .namespaces
            .values()
            .filter(|n| {
                include_deleted || n.desc.namespace_info.as_ref().map(|i| i.state) != Some(NsState::Deleted as i32)
            })
            .collect();
        all.sort_by(|a, b| {
            let an = a.desc.namespace_info.as_ref().map(|i| i.name.as_str()).unwrap_or("");
            let bn = b.desc.namespace_info.as_ref().map(|i| i.name.as_str()).unwrap_or("");
            an.cmp(bn)
        });
        let size = if r.page_size > 0 {
            r.page_size as usize
        } else {
            DEFAULT_PAGE_SIZE
        };
        let offset = if r.next_page_token.is_empty() {
            0
        } else {
            std::str::from_utf8(&r.next_page_token)
                .ok()
                .and_then(|s| s.parse::<usize>().ok())
                .ok_or_else(|| invalid("Invalid next page token."))?
        };
        let page: Vec<_> = all.iter().skip(offset).take(size).map(|n| n.desc.clone()).collect();
        let next = if offset + page.len() < all.len() {
            (offset + page.len()).to_string().into_bytes()
        } else {
            vec![]
        };
        Ok(wsv1::ListNamespacesResponse {
            namespaces: page,
            next_page_token: next,
        })
    }

    pub fn update_namespace(&mut self, r: wsv1::UpdateNamespaceRequest) -> R<wsv1::UpdateNamespaceResponse> {
        let entry = self
            .namespaces
            .get(&r.namespace)
            .ok_or_else(|| namespace_not_found(&r.namespace))?;
        let id = entry.id.clone();
        // Work on a copy so a rejected update changes nothing.
        let mut d = entry.desc.clone();
        let info = d.namespace_info.get_or_insert_with(Default::default);
        if let Some(u) = r.update_info {
            if !u.description.is_empty() {
                info.description = u.description;
            }
            if !u.owner_email.is_empty() {
                info.owner_email = u.owner_email;
            }
            info.data.extend(u.data);
            validate_state_update(info.state, u.state)?;
            if u.state != NsState::Unspecified as i32 {
                info.state = u.state;
            }
        }
        let current_state = info.state;
        let cfg = d.config.get_or_insert_with(Default::default);
        if let Some(c) = r.config {
            if c.workflow_execution_retention_ttl.is_some() {
                validate_retention(&c.workflow_execution_retention_ttl)?;
                cfg.workflow_execution_retention_ttl = c.workflow_execution_retention_ttl;
            }
            if let Some(bb) = c.bad_binaries {
                let mine = cfg.bad_binaries.get_or_insert_with(Default::default);
                for (checksum, mut b) in bb.binaries {
                    b.create_time = Some(now_ts());
                    mine.binaries.insert(checksum, b);
                }
                if mine.binaries.len() > MAX_BAD_BINARIES {
                    return Err(invalid(format!(
                        "Total resetBinaries cannot exceed the max limit: {MAX_BAD_BINARIES}"
                    )));
                }
            }
            cfg.custom_search_attribute_aliases
                .extend(c.custom_search_attribute_aliases);
            // Archival isn't configured on this server: archival fields are accepted and ignored.
        }
        if !r.delete_bad_binary.is_empty() {
            let mine = cfg.bad_binaries.get_or_insert_with(Default::default);
            if mine.binaries.remove(&r.delete_bad_binary).is_none() {
                return Err(invalid(format!(
                    "Bad binary checksum {} doesn't exists.",
                    r.delete_bad_binary
                )));
            }
        }
        let rc = d.replication_config.get_or_insert_with(Default::default);
        if let Some(u) = r.replication_config {
            if !u.clusters.is_empty() {
                rc.clusters = u.clusters.clone();
            }
            if !u.active_cluster_name.is_empty() {
                rc.active_cluster_name = u.active_cluster_name.clone();
            }
            if u.state != enums::ReplicationState::Unspecified as i32 && u.state != rc.state {
                if current_state != NsState::Registered as i32 {
                    let st = NsState::try_from(current_state).unwrap_or(NsState::Unspecified);
                    return Err(invalid(format!(
                        "update ReplicationState is only supported when namespace is in NAMESPACE_STATE_REGISTERED or NAMESPACE_STATE_SOFT_DELETED state, current state: {}",
                        st.as_str_name()
                    )));
                }
                if u.state == enums::ReplicationState::Handover as i32 {
                    return Err(invalid(
                        "REPLICATION_STATE_HANDOVER can only be set for global namespace",
                    ));
                }
                rc.state = u.state;
            }
        }
        if r.promote_namespace {
            return Err(invalid(format!(
                "global namespace is not enabled on this cluster, cannot update global namespace or promote local namespace: {}",
                r.namespace
            )));
        }
        let clusters: Vec<String> = rc.clusters.iter().map(|c| c.cluster_name.clone()).collect();
        validate_local_replication(&rc.active_cluster_name, &clusters)?;
        self.save_namespace_desc(&r.namespace, &id, d.clone())?;
        Ok(wsv1::UpdateNamespaceResponse {
            namespace_info: d.namespace_info,
            config: d.config,
            replication_config: d.replication_config,
            failover_version: d.failover_version,
            is_global_namespace: d.is_global_namespace,
        })
    }

    /// Mark a namespace deprecated: running workflows carry on, new ones can't start.
    pub fn deprecate_namespace(&mut self, ns: &str) -> R<()> {
        self.check_ns(ns, ALLOW_DEFAULT)?;
        let entry = self.namespaces.get(ns).expect("checked");
        let (id, mut d) = (entry.id.clone(), entry.desc.clone());
        d.namespace_info.get_or_insert_with(Default::default).state = NsState::Deprecated as i32;
        self.save_namespace_desc(ns, &id, d)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn state_updates_follow_the_go_server() {
        let (reg, dep, del) = (
            NsState::Registered as i32,
            NsState::Deprecated as i32,
            NsState::Deleted as i32,
        );
        assert!(validate_state_update(reg, dep).is_ok());
        assert!(validate_state_update(reg, del).is_ok());
        assert!(validate_state_update(dep, del).is_ok());
        assert!(validate_state_update(dep, reg).is_err());
        assert!(validate_state_update(del, reg).is_err());
        assert!(validate_state_update(reg, 0).is_ok());
    }

    #[test]
    fn local_replication_is_single_cluster() {
        assert!(validate_local_replication("active", &["active".into()]).is_ok());
        assert!(validate_local_replication("other", &["other".into()]).is_err());
        assert!(validate_local_replication("active", &["active".into(), "active".into()]).is_err());
    }

    #[test]
    fn retention_minimum_is_one_hour() {
        assert!(validate_retention(&None).is_err());
        assert!(validate_retention(&Some(dur_from_ms(59 * 60 * 1000))).is_err());
        assert!(validate_retention(&Some(dur_from_ms(3600 * 1000))).is_ok());
    }
}
