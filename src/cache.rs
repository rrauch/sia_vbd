use crate::vbd::{Block, BlockId, Cluster, ClusterId, Snapshot, SnapshotId};
use anyhow::anyhow;
use foyer::{
    AdmitAllPicker, DirectFsDeviceOptions, Engine, FifoPicker, HybridCache, HybridCacheBuilder,
    LargeEngineOptions,
};
use serde::{Deserialize, Serialize};
use std::future::Future;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

pub(crate) trait Cachable {
    fn id(&self) -> &impl CacheId;

    fn into_value(self) -> CacheValue;
}

impl Cachable for Block {
    fn id(&self) -> &impl CacheId {
        self.content_id()
    }

    fn into_value(self) -> CacheValue {
        CacheValue::Block(self)
    }
}

impl Cachable for Cluster {
    fn id(&self) -> &impl CacheId {
        self.content_id()
    }

    fn into_value(self) -> CacheValue {
        CacheValue::Cluster(self)
    }
}

impl Cachable for Snapshot {
    fn id(&self) -> &impl CacheId {
        self.content_id()
    }

    fn into_value(self) -> CacheValue {
        CacheValue::Snapshot(self)
    }
}

pub(crate) trait CacheId {
    type Value: Cachable + Send;

    fn key(&self) -> CacheKey;

    fn convert(v: CacheValue) -> Option<Self::Value>;
}

impl CacheId for BlockId {
    type Value = Block;

    fn key(&self) -> CacheKey {
        CacheKey::Block(self.clone())
    }

    fn convert(v: CacheValue) -> Option<Self::Value> {
        match v {
            CacheValue::Block(block) => Some(block),
            _ => None,
        }
    }
}

impl CacheId for ClusterId {
    type Value = Cluster;

    fn key(&self) -> CacheKey {
        CacheKey::Cluster(self.clone())
    }

    fn convert(v: CacheValue) -> Option<Self::Value> {
        match v {
            CacheValue::Cluster(cluster) => Some(cluster),
            _ => None,
        }
    }
}

impl CacheId for SnapshotId {
    type Value = Snapshot;

    fn key(&self) -> CacheKey {
        CacheKey::Snapshot(self.clone())
    }

    fn convert(v: CacheValue) -> Option<Self::Value> {
        match v {
            CacheValue::Snapshot(snapshot) => Some(snapshot),
            _ => None,
        }
    }
}

#[derive(Clone, PartialEq, Eq, Hash, Serialize, Deserialize, Debug)]
pub(crate) enum CacheKey {
    #[serde(rename = "B")]
    Block(BlockId),
    #[serde(rename = "C")]
    Cluster(ClusterId),
    #[serde(rename = "S")]
    Snapshot(SnapshotId),
}

#[derive(Clone, Serialize, Deserialize)]
pub(crate) enum CacheValue {
    Block(Block),
    Cluster(Cluster),
    Snapshot(Snapshot),
}

impl CacheValue {
    fn weigh(&self) -> usize {
        match self {
            Self::Block(block) => block.len() as usize + block.content_id().as_ref().len() + 16,
            Self::Cluster(cluster) => {
                cluster.content_id().as_ref().len() * (cluster.len() + 1) + 16
            }
            Self::Snapshot(snapshot) => {
                snapshot.content_id().as_ref().len() * (snapshot.len() + 1) + 16
            }
        }
    }
}

struct OptionExpiry {
    some_expiration: Duration,
    none_expiration: Duration,
}

impl moka::Expiry<CacheKey, Option<CacheValue>> for OptionExpiry {
    fn expire_after_create(
        &self,
        _key: &CacheKey,
        value: &Option<CacheValue>,
        _created_at: Instant,
    ) -> Option<Duration> {
        match value {
            Some(_) => Some(self.some_expiration),
            None => Some(self.none_expiration),
        }
    }
}

pub struct Cache {
    inner: HybridCache<CacheKey, CacheValue>,
    moka: moka::future::Cache<CacheKey, Option<CacheValue>>,
}

impl Cache {
    pub async fn new(
        max_memory_use: usize,
        max_disk_use: u64,
        disk_path: impl AsRef<Path>,
    ) -> anyhow::Result<Self> {
        let admission_picker = Arc::new(AdmitAllPicker::default());
        let disk_path = disk_path.as_ref();
        let inner = HybridCacheBuilder::new()
            .with_name("sia_vbd_cache")
            .memory(1)
            .with_shards(1)
            .with_weighter(|_, value: &CacheValue| value.weigh())
            .with_hash_builder(ahash::RandomState::with_seeds(
                3942754392483298543,
                2006766938398453335,
                11,
                568403945374598573,
            ))
            .storage(Engine::Large)
            .with_large_object_disk_cache_options(
                LargeEngineOptions::new()
                    .with_reinsertion_picker(admission_picker.clone())
                    .with_eviction_pickers(vec![Box::<FifoPicker>::default()]),
            )
            .with_admission_picker(admission_picker)
            .with_device_options(
                DirectFsDeviceOptions::new(disk_path).with_capacity(max_disk_use as usize),
            )
            .build()
            .await?;

        let moka = moka::future::Cache::builder()
            .max_capacity(max_memory_use as u64)
            .weigher(|_, v: &Option<CacheValue>| {
                v.as_ref().map(|v| v.weigh() as u32).unwrap_or_else(|| 1)
            })
            .expire_after(OptionExpiry {
                some_expiration: Duration::from_secs(86400),
                none_expiration: Duration::from_secs(10),
            })
            .build();

        Ok(Self { inner, moka })
    }

    pub async fn invalidate<T: CacheId, I: Iterator<Item = T>>(&self, ids: I) {
        for id in ids.into_iter() {
            let key = id.key();
            self.inner.remove(&key);
            self.moka.invalidate(&key).await;
        }
    }

    pub async fn insert<T: Cachable, I: Iterator<Item = T>>(
        &self,
        values: I,
    ) -> anyhow::Result<()> {
        for v in values.into_iter() {
            let key = v.id().key();
            let value = v.into_value();
            self.inner.insert(key.clone(), value);
            self.moka.invalidate(&key).await;
        }
        Ok(())
    }

    pub async fn get<T: CacheId>(
        &self,
        id: &T,
        fetch: impl Future<Output = anyhow::Result<Option<T::Value>>> + Send,
    ) -> anyhow::Result<Option<T::Value>> {
        let key = id.key();
        let inner = &self.inner;
        Ok(self
            .moka
            .try_get_with(key.clone(), async move {
                match inner.get(&key).await.map(|e| e.map(|e| e.value().clone())) {
                    Ok(Some(v)) => Ok(Some(v)),
                    Err(err) => Err(err),
                    Ok(None) => match fetch.await.map(|v| v.map(|v| v.into_value())) {
                        Ok(Some(v)) => {
                            inner.insert(key, v.clone());
                            Ok(Some(v))
                        }
                        Ok(None) => Ok(None),
                        Err(err) => Err(err),
                    },
                }
            })
            .await
            .map_err(|e| anyhow!("error retrieving entry: {}", e))?
            .map(|v| T::convert(v))
            .flatten())
    }

    pub async fn close(&mut self) -> anyhow::Result<()> {
        self.inner.close().await
    }
}
