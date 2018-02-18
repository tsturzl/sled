use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use super::*;

#[derive(Debug, Default)]
pub struct Mvcc {
    inner: RwLock<HashMap<Key, Arc<RwLock<Chain>>>>,
}

impl Mvcc {
    pub(super) fn get(&self, k: &Key) -> Option<Arc<RwLock<Chain>>> {
        let inner = self.inner.read().unwrap();
        inner.get(k).cloned()
    }

    pub(super) fn insert(&self, k: Key, chain: Chain) -> Result<(), ()> {
        let mut inner = self.inner.write().unwrap();
        if inner.contains_key(&k) {
            return Err(());
        }
        inner.insert(k, Arc::new(RwLock::new(chain)));
        Ok(())
    }
}
