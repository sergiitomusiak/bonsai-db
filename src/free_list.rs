use crate::{
    format::{read_u64, read_vec_u64, write_u64, write_slice_u64},
    node::Address,
    tx::TransactionId,
};

use anyhow::Result;
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::io::{Read, Write};
use std::mem::size_of;

#[derive(Debug, Default)]
pub(crate) struct FreeList {
    free: BTreeSet<Address>,
    pending: BTreeMap<TransactionId, BTreeSet<Address>>,
    cache: HashSet<Address>,
}

impl FreeList {
    fn allocate(&mut self, required_pages: usize) -> Option<Address> {
        let required_pages= required_pages as u64;
        let mut previous_page_id: Option<Address> = None;
        let mut initial_page_id = *self.free.first()?;

        for page_id in self.free.iter() {
            assert!(*page_id > 1, "invalid page allocation: {page_id}");

            let restart_initial_page = previous_page_id
                .map(|previous_page_id| page_id-previous_page_id != 1)
                .unwrap_or(false);

            if restart_initial_page {
                initial_page_id = *page_id;
            }

            if page_id-initial_page_id+1 == required_pages as Address {
                // Remove found pages from free list
                for i in 0..required_pages {
                    self.free.remove(&(initial_page_id+i));
                }
                return Some(initial_page_id);
            }
            previous_page_id = Some(*page_id);
        }

        None
    }

    pub(crate) fn read<R: Read>(reader: &mut R) -> Result<Self> {
        let free_len = read_u64(reader)?;
        let free = read_vec_u64(reader, free_len as usize)?;
        let free = BTreeSet::from_iter(free);
        Ok(Self {
            free,
            pending: BTreeMap::new(),
            cache: HashSet::new(),
        })
    }

    pub(crate) fn write<W: Write>(&self, writer: &mut W) -> Result<()> {
        let data = self.copy_all();
        write_u64(writer, data.len() as u64)?;
        write_slice_u64(writer, &data)?;
        Ok(())
    }

    fn free(&mut self, transaction_id: TransactionId, page_id: Address, page_overflow: u16) {
        // assert!(page_id > 1, "freeing page id must be greater than 1");
        let pending = self.pending.entry(transaction_id).or_default();
        let overflow = page_id + page_overflow as u64;
        for page_id in page_id..=overflow {
            let inserted = self.cache.insert(page_id);
            assert!(inserted, "page {page_id} is already free");
            pending.insert(page_id);
        }
    }

    fn release(&mut self, transaction_id: TransactionId) {
        let txs = self.pending
            .range(..=transaction_id)
            .map(|(tx_id, _)| *tx_id).collect::<Vec<_>>();

        for tx_id in txs {
            let pages = self
                .pending
                .remove(&tx_id)
                .expect("pending transactions");

            self.free.extend(pages);
        }
    }

    fn size(&self) -> usize {
        size_of::<u64>() * (self.pages_len() + 1)
    }

    fn pages_len(&self) -> usize {
        self.free.len() + self.pending_pages_len()
    }

    fn pending_pages_len(&self) -> usize {
        self.pending.iter().map(|(_, pages)| pages.len()).sum()
    }

    fn copy_all(&self) -> Vec<u64> {
        let pending = self
            .pending
            .iter()
            .flat_map(|(_, pages)| pages.iter().map(|page_id| *page_id))
            .collect::<BTreeSet<_>>();

        self.free.union(&pending).map(|page_id| *page_id).collect()
    }

    fn rollback(&mut self, transaction_id: TransactionId) {
        let Some(pages) = self.pending.remove(&transaction_id)
            else { return; };

        for page in pages {
            self.cache.remove(&page);
        }
    }

    fn is_page_freed(&self, page_id: Address) -> bool {
        self.cache.contains(&page_id)
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;

    macro_rules! free_list {
        ($($x:expr),+ $(,)?) => {
            BTreeSet::from([$($x),+])
        };
    }

    const FREE_LIST_DATA: &[u8] = &[
        // len
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03,
        // 16
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10,
        // 32
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x20,
        // 48
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x30,
    ];

    #[test]
    fn allocates_multiple_pages_at_the_start() {
        let mut free_list = FreeList::default();
        free_list.free = free_list![2, 3, 4, 5, 11, 13, 15, 16, 17, 18];
        let page_id = free_list.allocate(4);
        assert_eq!(page_id, Some(2));
        assert_eq!(free_list.free, free_list![11, 13, 15, 16, 17, 18]);
    }

    #[test]
    fn allocates_multiple_pages_at_the_middle() {
        let mut free_list = FreeList::default();
        free_list.free = free_list![2, 11, 13, 15, 16, 17, 18];
        let page_id = free_list.allocate(3);
        assert_eq!(page_id, Some(15));
        assert_eq!(free_list.free, free_list![2, 11, 13, 18]);
    }

    #[test]
    fn allocates_multiple_pages_at_the_end() {
        let mut free_list = FreeList::default();
        free_list.free = free_list![2, 11, 13, 15, 16, 17, 18];
        let page_id = free_list.allocate(4);
        assert_eq!(page_id, Some(15));
        assert_eq!(free_list.free, free_list![2, 11, 13]);
    }

    #[test]
    fn allocates_one_page() {
        let mut free_list = FreeList::default();
        free_list.free = free_list![2, 11, 13, 15, 16, 17, 18];
        let page_id = free_list.allocate(1);
        assert_eq!(page_id, Some(2));
        assert_eq!(free_list.free, free_list![11, 13, 15, 16, 17, 18]);
    }

    #[test]
    fn cannot_allocates_when_page_runs_are_too_small() {
        let mut free_list = FreeList::default();
        free_list.free = free_list![2, 11, 13, 15, 16, 17, 18];
        let page_id = free_list.allocate(10);
        assert_eq!(page_id, None);
        assert_eq!(free_list.free, free_list![2, 11, 13, 15, 16, 17, 18]);
    }

    #[test]
    fn cannot_allocates_when_no_free_pages() {
        let mut free_list = FreeList::default();
        let page_id = free_list.allocate(1);
        assert_eq!(page_id, None);
        assert!(free_list.free.is_empty());
    }

    #[test]
    fn reads_free_list() {
        let mut reader = Cursor::new(FREE_LIST_DATA);
        let free_list = FreeList::read(&mut reader).unwrap();
        assert_eq!(free_list.free, free_list![16, 32, 48]);
        assert!(free_list.pending.is_empty());
        assert!(free_list.cache.is_empty());
    }

    #[test]
    fn writes_free_list() {
        let mut free_list = FreeList::default();
        free_list.free = free_list![16, 32, 48];
        let mut writer = Cursor::new(Vec::new());
        free_list.write(&mut writer).unwrap();
        assert_eq!(writer.into_inner(), FREE_LIST_DATA);
    }
}