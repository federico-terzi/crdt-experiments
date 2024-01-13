use bytes::{Buf, BufMut, Bytes, BytesMut};
use bytes_varint::{VarIntSupport, VarIntSupportMut};
use rustc_hash::{FxHashMap, FxHashSet};
use thiserror::Error;

use crate::{serde::Serializable, ClientId, GlobalClient, GlobalClientId};

#[derive(Clone)]
pub struct ClientRegistry {
    clients: Vec<GlobalClient>,

    current_global: GlobalClientId,
    current_local: ClientId,

    local_to_global_cache: FxHashMap<ClientId, GlobalClientId>,
    global_to_local_cache: FxHashMap<GlobalClientId, ClientId>,
}

// TODO: tests
impl ClientRegistry {
    pub fn new(global_client_id: GlobalClientId, timestamp: u64) -> Self {
        let mut registry = Self {
            clients: vec![GlobalClient {
                created_at: timestamp,
                global_id: global_client_id.clone(),
            }],
            current_global: global_client_id,
            current_local: 0,

            local_to_global_cache: FxHashMap::default(),
            global_to_local_cache: FxHashMap::default(),
        };

        registry.rebuild_caches();

        registry
    }

    pub fn from_buffer(
        global_client_id: GlobalClientId,
        timestamp: u64,
        buffer: Bytes,
    ) -> Result<(Self, Option<ClientRemappings>), ClientRegistryError> {
        let loaded_clients = Self::deserialize_clients(buffer)?;
        let mut registry = Self::new(global_client_id, timestamp);
        let remappings = registry.register_clients(&loaded_clients);
        Ok((registry, remappings))
    }

    fn deserialize_clients(buffer: Bytes) -> Result<Vec<GlobalClient>, ClientRegistryError> {
        let mut buffer = Bytes::from(buffer);
        let clients_len = buffer.get_u32_varint().map_err(|_| {
            ClientRegistryError::SerializationError("error reading clients_len".to_string())
        })?;

        let mut clients = Vec::new();
        for _ in 0..clients_len {
            let created_at = buffer.get_u64_varint().map_err(|_| {
                ClientRegistryError::SerializationError("error reading created_at".to_string())
            })?;

            let global_id_len = buffer.get_u32_varint().map_err(|_| {
                ClientRegistryError::SerializationError("error reading global_id_len".to_string())
            })?;

            let global_id_bytes = buffer.copy_to_bytes(global_id_len as usize);
            let global_id = String::from_utf8(global_id_bytes.to_vec()).map_err(|_| {
                ClientRegistryError::SerializationError("error reading global_id".to_string())
            })?;

            clients.push(GlobalClient {
                created_at,
                global_id,
            });
        }

        Ok(clients)
    }

    pub fn get_clients(&self) -> &[GlobalClient] {
        return &self.clients;
    }

    pub fn register_clients(&mut self, clients: &[GlobalClient]) -> Option<ClientRemappings> {
        if !self.has_unknown_clients(clients) {
            return None;
        }

        let new_clients = Self::merge_clients(&self.clients, clients);

        let remappings = if self.requires_remapping(&new_clients) {
            Some(self.build_remappings(&new_clients))
        } else {
            None
        };

        self.clients = new_clients;
        self.rebuild_caches();
        self.current_local = self.global_to_local_cache[&self.current_global];

        remappings
    }

    fn has_unknown_clients(&self, clients: &[GlobalClient]) -> bool {
        for client in clients {
            if !self.global_to_local_cache.contains_key(&client.global_id) {
                return true;
            }
        }

        false
    }

    fn merge_clients(clients_a: &[GlobalClient], clients_b: &[GlobalClient]) -> Vec<GlobalClient> {
        let mut all_clients = Vec::new();
        all_clients.extend(clients_a);
        all_clients.extend(clients_b);
        all_clients.sort_by(|a, b| {
            if a.created_at == b.created_at {
                return a.global_id.cmp(&b.global_id);
            } else {
                return a.created_at.cmp(&b.created_at);
            }
        });

        let mut visited_clients = FxHashSet::default();
        let mut new_clients = Vec::new();
        for client in all_clients {
            if visited_clients.contains(&client.global_id) {
                continue;
            }

            visited_clients.insert(&client.global_id);
            new_clients.push(client.clone());
        }

        return new_clients;
    }

    fn requires_remapping(&self, new_clients: &[GlobalClient]) -> bool {
        for (local_id, local_client) in self.clients.iter().enumerate() {
            let corresponding_client = new_clients.get(local_id);
            if let Some(corresponding_client) = corresponding_client {
                if corresponding_client.global_id != local_client.global_id {
                    return true;
                }
            } else {
                return true;
            }
        }

        false
    }

    fn build_remappings(&self, new_clients: &[GlobalClient]) -> ClientRemappings {
        let mut remappings = FxHashMap::default();

        let mut new_clients_global_to_local = FxHashMap::default();
        for (new_client_local_id, new_client) in new_clients.iter().enumerate() {
            new_clients_global_to_local.insert(&new_client.global_id, new_client_local_id);
        }

        for (local_id, local_client) in self.clients.iter().enumerate() {
            let new_client_local_id = new_clients_global_to_local[&local_client.global_id];
            if local_id == new_client_local_id {
                continue;
            }

            remappings.insert(local_id as ClientId, new_client_local_id as ClientId);
        }

        remappings
    }

    fn rebuild_caches(&mut self) {
        self.local_to_global_cache.clear();
        self.global_to_local_cache.clear();

        for (local_id, client) in self.clients.iter().enumerate() {
            self.local_to_global_cache
                .insert(local_id as ClientId, client.global_id.clone());
            self.global_to_local_cache
                .insert(client.global_id.clone(), local_id as ClientId);
        }
    }

    pub fn get_current_id(&self) -> ClientId {
        self.current_local
    }
}

impl Serializable for ClientRegistry {
    fn serialize(&self) -> Result<Vec<u8>, crate::serde::SerializationError> {
        let mut buf = BytesMut::new();

        let clients_len: u32 = self
            .clients
            .len()
            .try_into()
            .expect("client registry too large");

        buf.put_u32_varint(clients_len);

        for client in self.clients.iter() {
            // The local id is implicitly the array position

            let created_at: u64 = client.created_at;
            buf.put_u64_varint(created_at);

            let global_id_len: u32 = client
                .global_id
                .len()
                .try_into()
                .expect("client global ID too large");

            buf.put_u32_varint(global_id_len);
            buf.put_slice(client.global_id.as_bytes());
        }

        Ok(buf.to_vec())
    }
}

#[derive(Error, Debug)]
pub enum ClientRegistryError {
    #[error("serialization error: {0}")]
    SerializationError(String),
}

pub type PreviousClientId = ClientId;
pub type NewClientId = ClientId;
pub type ClientRemappings = FxHashMap<PreviousClientId, NewClientId>;

pub trait ClientRemappable {
    fn remap_client_ids(&mut self, mappings: &ClientRemappings);
}
