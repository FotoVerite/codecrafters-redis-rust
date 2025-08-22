use std::{
    collections::{BTreeMap, HashMap, HashSet},
    net::SocketAddr,
};

use ordered_float::OrderedFloat;

use crate::{
    resp::RespValue,
    shared_store::{
        shared_store::{Entry, RedisValue, Store},
        zrank,
    },
};

#[derive(Debug, Clone)]
pub struct Zrank {
    data: BTreeMap<OrderedFloat<f64>, HashSet<String>>,
    pub(crate) reverse_map: HashMap<String, f64>,
}

impl Zrank {
    pub fn new() -> Self {
        Self {
            data: BTreeMap::new(),
            reverse_map: HashMap::new(),
        }
    }
}

impl Store {
    pub async fn zadd(&self, key: String, rank: f64, value: String) -> anyhow::Result<i64> {
        let mut keyspace = self.keyspace.write().await;
        if let Some(entry) = keyspace.get_mut(&key) {
            match &mut entry.value {
                RedisValue::ZRank(zrank) => {
                    if let Some(old_rank) = zrank.reverse_map.get(&value) {
                        if old_rank != &rank {
                            zrank
                                .data
                                .get_mut(&OrderedFloat(*old_rank))
                                .unwrap()
                                .remove(&value);
                        }
                    }
                    zrank.reverse_map.insert(value.clone(), rank);
                    let data = zrank.data.entry(OrderedFloat(rank)).or_default();
                    let added = data.insert(value.clone());
                    if added {
                        Ok(1)
                    } else {
                        Ok(0)
                    }
                }
                _ => Ok(0),
            }
        } else {
            let mut zrank = Zrank::new();
            let mut set = HashSet::new();
            set.insert(value);
            zrank.data.insert(OrderedFloat(rank), set);
            let entry: Entry = Entry::new(RedisValue::ZRank(zrank), None);
            keyspace.insert(key, entry);
            Ok(1)
        }
    }

    pub async fn zrank_command(&self, key: String, rank: f64) -> anyhow::Result<Option<usize>> {
        let mut keyspace = self.keyspace.read().await;
        if let Some(entry) = keyspace.get(&key) {
            match &entry.value {
                RedisValue::ZRank(zrank) => {
                    if let Some(data) = zrank.data.get(&OrderedFloat(rank)) {
                        return Ok(Some(data.len()));
                    };
                    return Ok(None);
                }
                _ => return Ok(None),
            }
        }
        Ok(None)
    }

    // pub async fn zrange(&self, key: String, rank: f64) -> anyhow::Result<Vec<String>> {
    //     let mut keyspace = self.keyspace.read().await;
    //     if let Some(entry) = keyspace.get(&key) {
    //          match &entry.value {
    //             RedisValue::ZRank(zrank) => {
    //                 zrank.data.ran
    //                 return Ok(vec![])
    //             }
    //             _ => return Ok(vec![])
    //          }
    //     }
    //     Ok(vec![])
    // }

    pub async fn zcard(&self, key: String) -> anyhow::Result<i64> {
        let mut keyspace = self.keyspace.read().await;
        if let Some(entry) = keyspace.get(&key) {
            match &entry.value {
                RedisValue::ZRank(zrank) => {
                    let mut ret = 0;
                    for (_, entries) in &zrank.data {
                        ret += entries.len();
                    }
                    return Ok(ret as i64);
                }
                _ => return Ok(0),
            }
        }
        Ok(0)
    }

    pub async fn zscore(&self, key: String, value: String) -> anyhow::Result<Option<f64>> {
        let mut keyspace = self.keyspace.read().await;
        if let Some(entry) = keyspace.get(&key) {
            match &entry.value {
                RedisValue::ZRank(zrank) => {
                    let ret = zrank.reverse_map.get(&value).copied();
                    return Ok(ret);
                }
                _ => return Ok(None),
            }
        }
        Ok(None)
    }

    pub async fn zrem(&self, key: String, value: String) -> anyhow::Result<Option<i64>> {
        let mut keyspace = self.keyspace.write().await;
        if let Some(entry) = keyspace.get_mut(&key) {
            match &mut entry.value {
                RedisValue::ZRank(zrank) => {
                    if let Some(rank) = zrank.reverse_map.get(&value) {
                        let data = zrank.data.get_mut(&OrderedFloat(*rank)).unwrap();
                        data.remove(&value);
                        return Ok(Some(1i64));
                    }
                }
                _ => return Ok(None),
            }
        }
        Ok(None)
    }
}
