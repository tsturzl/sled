use super::*;

use epoch::pin;
use pagecache::PageGet;

/// An iterator over keys and values in a `Tree`.
pub struct Iter<'a> {
    pub(super) id: PageID,
    pub(super) inner:
        &'a PageCache<BLinkMaterializer, Frag, Vec<(PageID, PageID)>>,
    pub(super) last_key: Bound,
    pub(super) broken: Option<Error<()>>,
    pub(super) done: bool,
    // TODO we have to refactor this in light of pages being deleted
}

impl<'a> Iterator for Iter<'a> {
    type Item = DbResult<(Vec<u8>, Vec<u8>), ()>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        } else if let Some(broken) = self.broken.take() {
            self.done = true;
            return Some(Err(broken));
        };

        let guard = pin();
        loop {
            let res = self.inner.get(self.id, &guard);

            let node = match res {
                Ok(PageGet::Materialized(Frag::Base(base, _), _)) => {
                    base
                }
                Err(e) => {
                    // TODO(when implementing merge support) this could
                    // be None if the node was removed since the last
                    // iteration, and we need to just get the inner
                    // node again...
                    error!("iteration failed: {:?}", e);
                    self.done = true;
                    return Some(Err(e.danger_cast()));
                }
                other => panic!(
                    "the pagecache returned an unexpected value \
                     to the Tree iterator: {:?}",
                    other
                ),
            };

            let prefix = node.lo.inner();
            for (k, v) in
                node.data.leaf().expect("node should be a leaf")
            {
                let decoded_k = prefix_decode(prefix, &*k);
                if Bound::Inclusive(decoded_k.clone()) > self.last_key
                {
                    self.last_key =
                        Bound::Inclusive(decoded_k.to_vec());

                    let val: Value = match v {
                        InlineOrPtr::Inline(val) => val,
                        InlineOrPtr::Ptr(pids) => {
                            let mut pages = vec![];
                            for pid in pids {
                                let get_cursor = self.inner
                                    .get(pid, &guard)
                                    .map_err(|e| e.danger_cast());

                                if get_cursor.is_err() {
                                    return Some(
                                        get_cursor.map(|_| {
                                            (vec![], vec![])
                                        }),
                                    );
                                }

                                let get_cursor = get_cursor.unwrap();

                                let data = match get_cursor {
                                    PageGet::Materialized(
                                        Frag::PartialValue(data),
                                        _,
                                    ) => data,
                                    _broken => {
                                        return Some(Err(Error::ReportableBug(format!("got non-base node while reassembling fragmented page!"))));
                                    }
                                };

                                pages.push(data);
                            }
                            pages.concat()
                        }
                    };

                    let ret = Ok((decoded_k, val));
                    return Some(ret);
                }
            }
            match node.next {
                Some(id) => self.id = id,
                None => return None,
            }
        }
    }
}
