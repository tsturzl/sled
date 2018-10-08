use super::*;
use std::cell::Cell;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Node {
    pub(crate) id: PageId,
    pub(crate) data: Data,
    pub(crate) next: Option<PageId>,
    pub(crate) lo: Bound,
    pub(crate) hi: Bound,
}

impl Node {
    pub(crate) fn apply(
        &mut self,
        frag: &Frag,
        merge_operator: Option<usize>,
        cmp_operator: Option<usize>,
    ) {
        use self::Frag::*;

        match *frag {
            Set(ref k, ref v) => {
                let decoded_k = prefix_decode(self.lo.inner(), k);
                if Bound::Inclusive(decoded_k) < self.hi {
                    let cmp_fn_ptr = cmp_operator.expect(
                        "must have a comparison operator set",
                    );
                    unsafe {
                        let cmp_fn = std::mem::transmute(cmp_fn_ptr);
                        self.set_leaf(k.clone(), v.clone(), cmp_fn);
                    }
                } else {
                    panic!("tried to consolidate set at key <= hi")
                }
            }
            Merge(ref k, ref v) => {
                let decoded_k = prefix_decode(self.lo.inner(), k);
                if Bound::Inclusive(decoded_k) < self.hi {
                    let merge_fn_ptr = merge_operator
                        .expect("must have a merge operator set");
                    let cmp_fn_ptr = cmp_operator.expect(
                        "must have a comparison operator set",
                    );
                    unsafe {
                        let merge_fn: MergeOperator =
                            std::mem::transmute(merge_fn_ptr);
                        let cmp_fn = std::mem::transmute(cmp_fn_ptr);
                        self.merge_leaf(
                            k.clone(),
                            v.clone(),
                            merge_fn,
                            cmp_fn,
                        );
                    }
                } else {
                    panic!("tried to consolidate set at key <= hi")
                }
            }
            ChildSplit(ref child_split) => {
                self.child_split(child_split);
            }
            ParentSplit(ref parent_split) => {
                let cmp_fn_ptr = cmp_operator
                    .expect("must have a comparison operator set");
                unsafe {
                    let cmp_fn = std::mem::transmute(cmp_fn_ptr);
                    self.parent_split(parent_split, cmp_fn)
                }
            }
            Del(ref k) => {
                let decoded_k = prefix_decode(self.lo.inner(), k);
                if Bound::Inclusive(decoded_k) < self.hi {
                    let cmp_fn_ptr = cmp_operator.expect(
                        "must have a comparison operator set",
                    );
                    unsafe {
                        let cmp_fn = std::mem::transmute(cmp_fn_ptr);
                        self.del_leaf(k, cmp_fn);
                    }
                } else {
                    panic!("tried to consolidate del at key <= hi")
                }
            }
            Base(_, _) => {
                panic!("encountered base page in middle of chain")
            }
        }
    }

    pub(crate) fn set_leaf(
        &mut self,
        key: Key,
        val: Value,
        cmp_fn: CmpOperator,
    ) {
        if let Data::Leaf(ref mut records) = self.data {
            let search =
                records.binary_search_by(|&(ref k, ref _v)| {
                    cmp_fn(k, &*key)
                });
            if let Ok(idx) = search {
                records.push((key, val));
                records.swap_remove(idx);
            } else {
                records.push((key, val));
                records.sort_unstable_by(|a, b| cmp_fn(&*a.0, &*b.0));
            }
        } else {
            panic!("tried to Set a value to an index");
        }
    }

    pub(crate) fn merge_leaf(
        &mut self,
        key: Key,
        val: Value,
        merge_fn: MergeOperator,
        cmp_fn: CmpOperator,
    ) {
        if let Data::Leaf(ref mut records) = self.data {
            let search =
                records.binary_search_by(|&(ref k, ref _v)| {
                    cmp_fn(k, &*key)
                });

            let decoded_k = prefix_decode(self.lo.inner(), &key);
            if let Ok(idx) = search {
                let new = merge_fn(
                    &*decoded_k,
                    Some(&records[idx].1),
                    &val,
                );
                if let Some(new) = new {
                    records.push((key, new));
                    records.swap_remove(idx);
                } else {
                    records.remove(idx);
                }
            } else {
                let new = merge_fn(&*decoded_k, None, &val);
                if let Some(new) = new {
                    records.push((key, new));
                    records.sort_unstable_by(|a, b| {
                        cmp_fn(&*a.0, &*b.0)
                    });
                }
            }
        } else {
            panic!("tried to Merge a value to an index");
        }
    }

    pub(crate) fn child_split(&mut self, cs: &ChildSplit) {
        self.data.drop_gte(&cs.at, self.lo.inner());
        self.hi = Bound::Exclusive(cs.at.inner().to_vec());
        self.next = Some(cs.to);
    }

    pub(crate) fn parent_split(
        &mut self,
        ps: &ParentSplit,
        cmp_fn: CmpOperator,
    ) {
        if let Data::Index(ref mut ptrs) = self.data {
            let encoded_sep =
                prefix_encode(self.lo.inner(), ps.at.inner());
            ptrs.push((encoded_sep, ps.to));
            ptrs.sort_unstable_by(|a, b| cmp_fn(&*a.0, &*b.0));
        } else {
            panic!("tried to attach a ParentSplit to a Leaf chain");
        }
    }

    pub(crate) fn del_leaf(
        &mut self,
        key: KeyRef<'_>,
        cmp_fn: CmpOperator,
    ) {
        if let Data::Leaf(ref mut records) = self.data {
            let search =
                records.binary_search_by(|&(ref k, ref _v)| {
                    cmp_fn(k, &*key)
                });
            if let Ok(idx) = search {
                records.remove(idx);
            }
        } else {
            panic!("tried to attach a Del to an Index chain");
        }
    }

    pub(crate) fn should_split(&self, fanout: u8) -> bool {
        self.data.len() > fanout as usize
    }

    pub(crate) fn split(&self, id: PageId) -> Node {
        let (split, right_data) = self.data.split(self.lo.inner());
        Node {
            id: id,
            data: right_data,
            next: self.next,
            lo: Bound::Inclusive(split),
            hi: self.hi.clone(),
        }
    }
}
