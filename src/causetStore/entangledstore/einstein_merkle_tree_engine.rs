//Copyright WHTCORPS INC 2021-2023 LICENSED WITH APACHE 2.0.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::BTreeMap;
use std::collections::Bound::{self, Excluded, Included, Unbounded};
use std::default::Default;
use std::fmt::{self, Debug, Display, Formatter};
use std::ops::RangeBounds;
use std::sync::{Arc, RwLock};

use einstein_merkle_embedded_engine::EinsteinMerkleembedded_engine;
use einstein_merkle_embedded_engine_promises::{BraneName, IterOptions, ReadOptions, BRANE_DEFAULT, BRANE_LOCK, BRANE_WRITE};
use eekvproto::kvrpcpb::Context;
use txn_types::{Key, Value};

use crate::causetStorage::kv::{
    Callback as embedded_engineCallback, CbContext, Cursor, embedded_engine, Error as embedded_engineError,
    ErrorInner as embedded_engineErrorInner, Iterator, Modify, Result as embedded_engineResult, ScanMode, Snapshot,
    WriteData,
};
use EinsteinDB_util::time::ThreadReadId;

type RwLockTree = RwLock<BTreeMap<Key, Value>>;

/// The BTreeembedded_engine(based on `BTreeMap`) is in memory and only used in tests and benchmarks.
/// Note: The `snapshot()` and `async_snapshot()` methods are fake, the returned snapshot is not isolated,
/// they will be affected by the later modifies.
#[derive(Clone)]
pub struct BTreeembedded_engine {
    brane_names: Vec<BraneName>,
    brane_contents: Vec<Arc<RwLockTree>>,
}

impl BTreeembedded_engine {
    pub fn new(cfs: &[BraneName]) -> Self {
        let mut brane_names = vec![];
        let mut brane_contents = vec![];

        // create default brane if missing
        if cfs.iter().find(|&&c| c == BRANE_DEFAULT).is_none() {
            brane_names.push(BRANE_DEFAULT);
            brane_contents.push(Arc::new(RwLock::new(BTreeMap::new())))
        }

        for brane in cfs.iter() {
            brane_names.push(*brane);
            brane_contents.push(Arc::new(RwLock::new(BTreeMap::new())))
        }

        Self {
            brane_names,
            brane_contents,
        }
    }

    pub fn get_brane(&self, brane: BraneName) -> Arc<RwLockTree> {
        let index = self
            .brane_names
            .iter()
            .position(|&c| c == brane)
            .expect("BRANE not exist!");
        self.brane_contents[index].clone()
    }
}

impl Default for BTreeembedded_engine {
    fn default() -> Self {
        let cfs = &[BRANE_WRITE, BRANE_DEFAULT, BRANE_LOCK];
        Self::new(cfs)
    }
}

impl embedded_engine for BTreeembedded_engine {
    type Snap = BTreeembedded_engineSnapshot;

    fn kv_embedded_engine(&self) -> EinsteinMerkleembedded_engine {
        unimplemented!();
    }

    fn snapshot_on_kv_embedded_engine(&self, _: &[u8], _: &[u8]) -> embedded_engineResult<Self::Snap> {
        unimplemented!();
    }

    fn modify_on_kv_embedded_engine(&self, _: Vec<Modify>) -> embedded_engineResult<()> {
        unimplemented!();
    }

    fn async_write(
        &self,
        _ctx: &Context,
        batch: WriteData,
        cb: embedded_engineCallback<()>,
    ) -> embedded_engineResult<()> {
        if batch.modifies.is_empty() {
            return Err(embedded_engineError::from(embedded_engineErrorInner::EmptyRequest));
        }
        cb((CbContext::new(), write_modifies(&self, batch.modifies)));

        Ok(())
    }

    /// warning: It returns a fake snapshot whose content will be affected by the later modifies!
    fn async_snapshot(
        &self,
        _ctx: &Context,
        _: Option<ThreadReadId>,
        cb: embedded_engineCallback<Self::Snap>,
    ) -> embedded_engineResult<()> {
        cb((CbContext::new(), Ok(BTreeembedded_engineSnapshot::new(&self))));
        Ok(())
    }
}

impl Display for BTreeembedded_engine {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "BTreeembedded_engine",)
    }
}

impl Debug for BTreeembedded_engine {
    // TODO: Provide more debug info.
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "BTreeembedded_engine",)
    }
}

pub struct BTreeembedded_engineIterator {
    tree: Arc<RwLockTree>,
    cur_key: Option<Key>,
    cur_value: Option<Value>,
    valid: bool,
    bounds: (Bound<Key>, Bound<Key>),
}

impl BTreeembedded_engineIterator {
    pub fn new(tree: Arc<RwLockTree>, iter_opt: IterOptions) -> BTreeembedded_engineIterator {
        let lower_bound = match iter_opt.lower_bound() {
            None => Unbounded,
            Some(key) => Included(Key::from_raw(key)),
        };

        let upper_bound = match iter_opt.upper_bound() {
            None => Unbounded,
            Some(key) => Excluded(Key::from_raw(key)),
        };
        let bounds = (lower_bound, upper_bound);
        Self {
            tree,
            cur_key: None,
            cur_value: None,
            valid: false,
            bounds,
        }
    }

    /// In general, there are 2 endpoints in a range, the left one and the right one.
    /// This method will seek to the left one if left is `true`, else seek to the right one.
    /// Returns true when the endpoint is valid, which means the endpoint exist and in `self.bounds`.
    fn seek_to_range_endpoint(&mut self, range: (Bound<Key>, Bound<Key>), left: bool) -> bool {
        let tree = self.tree.read().unwrap();
        let mut range = tree.range(range);
        let item = if left {
            range.next() // move to the left endpoint
        } else {
            range.next_back() // move to the right endpoint
        };
        match item {
            Some((k, v)) if self.bounds.contains(k) => {
                self.cur_key = Some(k.clone());
                self.cur_value = Some(v.clone());
                self.valid = true;
            }
            _ => {
                self.valid = false;
            }
        }
        self.valid().unwrap()
    }
}

impl Iterator for BTreeembedded_engineIterator {
    fn next(&mut self) -> embedded_engineResult<bool> {
        let range = (Excluded(self.cur_key.clone().unwrap()), Unbounded);
        Ok(self.seek_to_range_endpoint(range, true))
    }

    fn prev(&mut self) -> embedded_engineResult<bool> {
        let range = (Unbounded, Excluded(self.cur_key.clone().unwrap()));
        Ok(self.seek_to_range_endpoint(range, false))
    }

    fn seek(&mut self, key: &Key) -> embedded_engineResult<bool> {
        let range = (Included(key.clone()), Unbounded);
        Ok(self.seek_to_range_endpoint(range, true))
    }

    fn seek_for_prev(&mut self, key: &Key) -> embedded_engineResult<bool> {
        let range = (Unbounded, Included(key.clone()));
        Ok(self.seek_to_range_endpoint(range, false))
    }

    fn seek_to_first(&mut self) -> embedded_engineResult<bool> {
        let range = (self.bounds.0.clone(), self.bounds.1.clone());
        Ok(self.seek_to_range_endpoint(range, true))
    }

    fn seek_to_last(&mut self) -> embedded_engineResult<bool> {
        let range = (self.bounds.0.clone(), self.bounds.1.clone());
        Ok(self.seek_to_range_endpoint(range, false))
    }

    #[inline]
    fn valid(&self) -> embedded_engineResult<bool> {
        Ok(self.valid)
    }

    fn key(&self) -> &[u8] {
        assert!(self.valid().unwrap());
        self.cur_key.as_ref().unwrap().as_encoded()
    }

    fn value(&self) -> &[u8] {
        assert!(self.valid().unwrap());
        self.cur_value.as_ref().unwrap().as_slice()
    }
}

impl Snapshot for BTreeembedded_engineSnapshot {
    type Iter = BTreeembedded_engineIterator;

    fn get(&self, key: &Key) -> embedded_engineResult<Option<Value>> {
        self.get_brane(BRANE_DEFAULT, key)
    }
    fn get_brane(&self, brane: BraneName, key: &Key) -> embedded_engineResult<Option<Value>> {
        let tree_cf = self.inner_embedded_engine.get_brane(brane);
        let tree = tree_cf.read().unwrap();
        let v = tree.get(key);
        match v {
            None => Ok(None),
            Some(v) => Ok(Some(v.clone())),
        }
    }
    fn get_brane_opt(&self, _: ReadOptions, brane: BraneName, key: &Key) -> embedded_engineResult<Option<Value>> {
        self.get_brane(brane, key)
    }
    fn iter(&self, iter_opt: IterOptions, mode: ScanMode) -> embedded_engineResult<Cursor<Self::Iter>> {
        self.iter_cf(BRANE_DEFAULT, iter_opt, mode)
    }
    #[inline]
    fn iter_cf(
        &self,
        brane: BraneName,
        iter_opt: IterOptions,
        mode: ScanMode,
    ) -> embedded_engineResult<Cursor<Self::Iter>> {
        let tree = self.inner_embedded_engine.get_brane(brane);

        Ok(Cursor::new(BTreeembedded_engineIterator::new(tree, iter_opt), mode))
    }
}

#[derive(Debug, Clone)]
pub struct BTreeembedded_engineSnapshot {
    inner_embedded_engine: Arc<BTreeembedded_engine>,
}

impl BTreeembedded_engineSnapshot {
    pub fn new(embedded_engine: &BTreeembedded_engine) -> Self {
        Self {
            inner_embedded_engine: Arc::new(embedded_engine.clone()),
        }
    }
}

fn write_modifies(embedded_engine: &BTreeembedded_engine, modifies: Vec<Modify>) -> embedded_engineResult<()> {
    for rev in modifies {
        match rev {
            Modify::Delete(brane, k) => {
                let brane_tree = embedded_engine.get_brane(brane);
                brane_tree.write().unwrap().remove(&k);
            }
            Modify::Put(brane, k, v) => {
                let brane_tree = embedded_engine.get_brane(brane);
                brane_tree.write().unwrap().insert(k, v);
            }

            Modify::DeleteRange(_cf, _start_key, _end_key, _notify_only) => unimplemented!(),
        };
    }
    Ok(())
}

#[cfg(test)]
pub mod tests {
    use super::super::tests::*;
    use super::super::CfStatistics;
    use super::*;

    #[test]
    fn test_btree_embedded_engine() {
        let embedded_engine = BTreeembedded_engine::new(TEST_embedded_engine_CFS);
        test_base_curd_options(&embedded_engine)
    }

    #[test]
    fn test_linear_of_btree_embedded_engine() {
        let embedded_engine = BTreeembedded_engine::default();
        test_linear(&embedded_engine);
    }

    #[test]
    fn test_statistic_of_btree_embedded_engine() {
        let embedded_engine = BTreeembedded_engine::default();
        test_cfs_statistics(&embedded_engine);
    }

    #[test]
    fn test_bounds_of_btree_embedded_engine() {
        let embedded_engine = BTreeembedded_engine::default();
        let test_data = vec![
            (b"a1".to_vec(), b"v1".to_vec()),
            (b"a3".to_vec(), b"v3".to_vec()),
            (b"a5".to_vec(), b"v5".to_vec()),
            (b"a7".to_vec(), b"v7".to_vec()),
        ];
        for (k, v) in &test_data {
            must_put(&embedded_engine, k.as_slice(), v.as_slice());
        }
        let snap = embedded_engine.snapshot(&Context::default()).unwrap();
        let mut statistics = CfStatistics::default();

        // lower bound > upper bound, seek() returns false.
        let mut iter_op = IterOptions::default();
        iter_op.set_lower_bound(b"a7", 0);
        iter_op.set_upper_bound(b"a3", 0);
        let mut cursor = snap.iter(iter_op, ScanMode::Forward).unwrap();
        assert!(!cursor.seek(&Key::from_raw(b"a5"), &mut statistics).unwrap());

        let mut iter_op = IterOptions::default();
        iter_op.set_lower_bound(b"a3", 0);
        iter_op.set_upper_bound(b"a7", 0);
        let mut cursor = snap.iter(iter_op, ScanMode::Forward).unwrap();

        assert!(cursor.seek(&Key::from_raw(b"a5"), &mut statistics).unwrap());
        assert!(!cursor.seek(&Key::from_raw(b"a8"), &mut statistics).unwrap());
        assert!(!cursor.seek(&Key::from_raw(b"a0"), &mut statistics).unwrap());

        assert!(cursor.seek_to_last(&mut statistics));

        let mut ret = vec![];
        loop {
            ret.push((
                Key::from_encoded(cursor.key(&mut statistics).to_vec())
                    .to_raw()
                    .unwrap(),
                cursor.value(&mut statistics).to_vec(),
            ));

            if !cursor.prev(&mut statistics) {
                break;
            }
        }
        ret.sort();
        assert_eq!(ret, test_data[1..3].to_vec());
    }

    #[test]
    fn test_iterator() {
        let embedded_engine = BTreeembedded_engine::default();
        let test_data = vec![
            (b"a1".to_vec(), b"v1".to_vec()),
            (b"a3".to_vec(), b"v3".to_vec()),
            (b"a5".to_vec(), b"v5".to_vec()),
            (b"a7".to_vec(), b"v7".to_vec()),
        ];
        for (k, v) in &test_data {
            must_put(&embedded_engine, k.as_slice(), v.as_slice());
        }

        let iter_op = IterOptions::default();
        let tree = embedded_engine.get_brane(BRANE_DEFAULT);
        let mut iter = BTreeembedded_engineIterator::new(tree, iter_op);
        assert!(!iter.valid().unwrap());

        assert!(iter.seek_to_first().unwrap());
        assert_eq!(iter.key(), Key::from_raw(b"a1").as_encoded().as_slice());
        assert!(!iter.prev().unwrap());
        assert!(iter.next().unwrap());
        assert_eq!(iter.key(), Key::from_raw(b"a3").as_encoded().as_slice());

        assert!(iter.seek(&Key::from_raw(b"a1")).unwrap());
        assert_eq!(iter.key(), Key::from_raw(b"a1").as_encoded().as_slice());

        assert!(iter.seek_to_last().unwrap());
        assert_eq!(iter.key(), Key::from_raw(b"a7").as_encoded().as_slice());
        assert!(!iter.next().unwrap());
        assert!(iter.prev().unwrap());
        assert_eq!(iter.key(), Key::from_raw(b"a5").as_encoded().as_slice());

        assert!(iter.seek(&Key::from_raw(b"a7")).unwrap());
        assert_eq!(iter.key(), Key::from_raw(b"a7").as_encoded().as_slice());

        assert!(!iter.seek_for_prev(&Key::from_raw(b"a0")).unwrap());

        assert!(iter.seek_for_prev(&Key::from_raw(b"a1")).unwrap());
        assert_eq!(iter.key(), Key::from_raw(b"a1").as_encoded().as_slice());

        assert!(iter.seek_for_prev(&Key::from_raw(b"a8")).unwrap());
        assert_eq!(iter.key(), Key::from_raw(b"a7").as_encoded().as_slice());
    }

    #[test]
    fn test_get_not_exist_cf() {
        let embedded_engine = BTreeembedded_engine::new(&[]);
        assert!(::panic_hook::recover_safe(|| embedded_engine.get_brane("not_exist_cf")).is_err());
    }
}
