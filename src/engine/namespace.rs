//! Namespaces: register, describe, list, update. A "default" namespace always exists.

use prost::Message;

use super::*;

pub const DEFAULT_RETENTION_MS: i64 = 3 * 24 * 3600 * 1000;

fn build_desc(name: &str, id: &str, description: &str, owner: &str, retention_ms: i64, data: std::collections::HashMap<String, String>) -> wsv1::DescribeNamespaceResponse {
    wsv1::DescribeNamespaceResponse {
        namespace_info: Some(namespace_pb::NamespaceInfo {
            name: name.to_string(),
            state: enums::NamespaceState::Registered as i32,
            description: description.to_string(),
            owner_email: owner.to_string(),
            data,
            id: id.to_string(),
            capabilities: Some(namespace_pb::namespace_info::Capabilities::default()),
            ..Default::default()
        }),
        config: Some(namespace_pb::NamespaceConfig {
            workflow_execution_retention_ttl: Some(dur_from_ms(retention_ms)),
            history_archival_state: enums::ArchivalState::Disabled as i32,
            visibility_archival_state: enums::ArchivalState::Disabled as i32,
            ..Default::default()
        }),
        replication_config: Some(crate::proto::temporal::api::replication::v1::NamespaceReplicationConfig {
            active_cluster_name: "active".into(),
            clusters: vec![crate::proto::temporal::api::replication::v1::ClusterReplicationConfig {
                cluster_name: "active".into(),
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
            self.namespaces.insert(info.name.clone(), NsEntry { id: info.id.clone(), desc: d });
        }
        if !self.namespaces.contains_key("default") {
            self.create_namespace("default", "Default namespace", "", DEFAULT_RETENTION_MS, Default::default())
                .map_err(|e| anyhow::anyhow!(e.message().to_string()))?;
        }
        Ok(())
    }

    fn create_namespace(&mut self, name: &str, description: &str, owner: &str, retention_ms: i64, data: std::collections::HashMap<String, String>) -> R<()> {
        let id = new_id();
        let d = build_desc(name, &id, description, owner, retention_ms, data);
        self.store.save_namespace(name, &id, &d.encode_to_vec()).map_err(internal)?;
        self.namespaces.insert(name.to_string(), NsEntry { id, desc: d });
        Ok(())
    }

    pub fn ns_id(&self, ns: &str) -> R<String> {
        self.namespaces.get(ns).map(|n| n.id.clone()).ok_or_else(|| namespace_not_found(ns))
    }

    pub fn register_namespace(&mut self, r: wsv1::RegisterNamespaceRequest) -> R<()> {
        if r.namespace.is_empty() {
            return Err(invalid("Namespace is not set on request."));
        }
        if self.namespaces.contains_key(&r.namespace) {
            return Err(status_with_detail(
                tonic::Code::AlreadyExists,
                "Namespace already exists.",
                "temporal.api.errordetails.v1.NamespaceAlreadyExistsFailure",
                &errordetails::NamespaceAlreadyExistsFailure {},
            ));
        }
        let mut ret = dur_ms(&r.workflow_execution_retention_period);
        if ret <= 0 {
            ret = DEFAULT_RETENTION_MS;
        }
        self.create_namespace(&r.namespace, &r.description, &r.owner_email, ret, r.data)
    }

    pub fn describe_namespace(&self, name: &str, id: &str) -> R<wsv1::DescribeNamespaceResponse> {
        if !name.is_empty() {
            return self.namespaces.get(name).map(|n| n.desc.clone()).ok_or_else(|| namespace_not_found(name));
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

    pub fn list_namespaces(&self) -> Vec<wsv1::DescribeNamespaceResponse> {
        let mut v: Vec<_> = self.namespaces.values().map(|n| n.desc.clone()).collect();
        v.sort_by(|a, b| {
            let an = a.namespace_info.as_ref().map(|i| i.name.clone()).unwrap_or_default();
            let bn = b.namespace_info.as_ref().map(|i| i.name.clone()).unwrap_or_default();
            an.cmp(&bn)
        });
        v
    }

    pub fn update_namespace(&mut self, r: wsv1::UpdateNamespaceRequest) -> R<wsv1::UpdateNamespaceResponse> {
        let entry = self.namespaces.get_mut(&r.namespace).ok_or_else(|| namespace_not_found(&r.namespace))?;
        let d = &mut entry.desc;
        if let Some(u) = r.update_info {
            let info = d.namespace_info.get_or_insert_with(Default::default);
            if !u.description.is_empty() {
                info.description = u.description;
            }
            if !u.owner_email.is_empty() {
                info.owner_email = u.owner_email;
            }
            info.data.extend(u.data);
        }
        if let Some(c) = r.config {
            let cfg = d.config.get_or_insert_with(Default::default);
            if dur_ms(&c.workflow_execution_retention_ttl) > 0 {
                cfg.workflow_execution_retention_ttl = c.workflow_execution_retention_ttl;
            }
            cfg.custom_search_attribute_aliases.extend(c.custom_search_attribute_aliases);
        }
        let d = d.clone();
        let id = entry.id.clone();
        self.store.save_namespace(&r.namespace, &id, &d.encode_to_vec()).map_err(internal)?;
        Ok(wsv1::UpdateNamespaceResponse {
            namespace_info: d.namespace_info,
            config: d.config,
            replication_config: d.replication_config,
            failover_version: 0,
            is_global_namespace: false,
        })
    }
}
