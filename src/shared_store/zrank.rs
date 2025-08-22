use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use ordered_float::OrderedFloat;

use crate::shared_store::shared_store::{Entry, RedisValue, Store};

#[derive(Debug, Clone)]
pub struct Zrank {
    data: BTreeMap<OrderedFloat<f64>, BTreeSet<String>>,
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
        let mut old_value = false;
        if let Some(entry) = keyspace.get_mut(&key) {
            match &mut entry.value {
                RedisValue::ZRank(zrank) => {
                    if let Some(old_rank) = zrank.reverse_map.get(&value) {
                        if old_rank != &rank {
                            old_value = true;
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
                    if added && !old_value {
                        Ok(1)
                    } else {
                        Ok(0)
                    }
                }
                _ => Ok(0),
            }
        } else {
            let mut zrank = Zrank::new();
            zrank.reverse_map.insert(value.clone(), rank);
            let mut set = BTreeSet::new();
            set.insert(value);
            zrank.data.insert(OrderedFloat(rank), set);
            let entry: Entry = Entry::new(RedisValue::ZRank(zrank), None);
            keyspace.insert(key, entry);
            Ok(1)
        }
    }

    pub async fn zrank_command(&self, key: String, value: String) -> anyhow::Result<Option<usize>> {
        let keyspace = self.keyspace.read().await;
        if let Some(entry) = keyspace.get(&key) {
            match &entry.value {
                RedisValue::ZRank(zrank) => {
                    if !zrank.reverse_map.contains_key(&value) {
                        return Ok(None);
                    }
                    let target_score = *zrank.reverse_map.get(&value).unwrap();
                    let mut rank = 0;
                    for (r, values) in zrank.data.iter() {
                        if r != &OrderedFloat(target_score) {
                            rank += values.len();
                        } else {
                            let mut sorted_members: Vec<_> = values.iter().collect();
                            sorted_members.sort(); // lexicographical
                            for m in sorted_members {
                                if m == &value {
                                    return Ok(Some(rank)); // found the rank
                                }
                                rank += 1;
                            }
                        }
                    }

                    return Ok(None);
                }
                _ => return Ok(None),
            }
        }
        Ok(None)
    }

    pub async fn zrange(&self, key: String, start: i64, stop: i64) -> anyhow::Result<Vec<String>> {
        let mut keyspace = self.keyspace.read().await;
        if let Some(entry) = keyspace.get(&key) {
            match &entry.value {
                RedisValue::ZRank(zrank) => {
                    let len = zrank.data.len();
                    let mut members: Vec<String> = zrank
                        .data
                        .iter()
                        .flat_map(|(_, set)| {
                            let mut v: Vec<String> = set.iter().cloned().collect();
                            v.sort(); // sort members with same score lexicographically
                            v
                        })
                        .collect();
                    if len == 0 {
                        return Ok(vec![]);
                    }

                    let start = normalize_index(start, len);
                    let mut stop = normalize_index(stop, len);

                    if start > stop {
                        return Ok(vec![]);
                    }

                    if stop >= len {
                        stop = len - 1;
                    }

                    // Grab the slice and map to just the member strings
                    let range = members[start..=stop].to_vec();

                    return Ok(range);
                }
                _ => return Ok(vec![]),
            }
        }
        Ok(vec![])
    }

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

fn normalize_index(idx: i64, len: usize) -> usize {
    if idx < 0 {
        let abs = (-idx) as usize;
        if abs > len {
            0
        } else {
            len - abs
        }
    } else if idx as usize >= len {
        len
    } else {
        idx as usize
    }
}
