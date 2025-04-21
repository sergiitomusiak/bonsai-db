use crate::{
    format::{read_vec_u64, write_slice_u64},
    node::{Address, NodeHeader, FREELIST_NODE},
    tx::TransactionId,
};

use anyhow::Result;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::io::{Read, Write};
use std::mem::size_of;

#[derive(Debug, Default)]
pub struct FreeList {
    pub free: BTreeSet<Address>,
    pub allocated_by: HashMap<Address, TransactionId>,
    pub pending_allocated: BTreeSet<Address>,
    pub pending_free: BTreeMap<TransactionId, BTreeSet<Address>>,
}

impl FreeList {
    pub fn allocate(&mut self, required_pages: u64, page_size: u64) -> Option<Address> {
        let mut previous_page_address: Option<Address> = None;
        let mut initial_page_address = *self.free.first()?;

        for page_address in self.free.difference(&self.pending_allocated) {
            assert_eq!(page_address % page_size, 0, "invalid page address");
            let restart_initial_page = previous_page_address
                .map(|previous_page_address| page_address - previous_page_address != page_size)
                .unwrap_or(false);

            if restart_initial_page {
                initial_page_address = *page_address;
            }

            if ((page_address - initial_page_address) / page_size) + 1 == required_pages as Address {
                // Remove found pages from free list
                // println!("ALLOCATED PAGES: {required_pages:?}");
                for i in 0..required_pages {
                    self.free.remove(&(initial_page_address + i * page_size));
                    self.pending_allocated
                        .insert(initial_page_address + i * page_size);
                }
                return Some(initial_page_address);
            }
            previous_page_address = Some(*page_address);
        }
        // println!("NO {required_pages:?} PAGES IN FREE LIST");
        None
    }

    pub fn register_allocation(&mut self, page_address: Address, transaction_id: TransactionId) {
        let replaced = self.allocated_by.insert(page_address, transaction_id).is_some();
        assert!(!replaced, "page address already registered");
    }

    pub fn read<R: Read>(reader: &mut R) -> Result<(NodeHeader, Self)> {
        let header = NodeHeader::read(reader)?;
        let free = read_vec_u64(reader, header.internal_nodes_len as usize)?;
        let free = BTreeSet::from_iter(free);
        let node = Self {
            free,
            allocated_by: HashMap::new(),
            pending_allocated: BTreeSet::new(),
            pending_free: BTreeMap::new(),
        };
        // println!("FREE LIST: {node:?}");
        Ok((header, node))
    }

    pub fn write<W: Write>(&self, writer: &mut W, page_size: u32) -> Result<NodeHeader> {
        let page_size = page_size as u64;
        let data = self.copy_all();
        let data_size = NodeHeader::size() + (self.size()) as u64;
        assert_eq!(data.len()*8, self.size());
        let overflow_len = if data_size <= page_size {
            0
        } else {
            (data_size - page_size).div_ceil(page_size)
        };
        let header = NodeHeader {
            flags: FREELIST_NODE,
            internal_nodes_len: data.len() as u64,
            overflow_len,
        };
        header.write(writer)?;
        write_slice_u64(writer, &data)?;
        Ok(header)
    }

    pub fn free(
        &mut self,
        transaction_id: TransactionId,
        page_start_address: Address,
        page_overflow: u64,
        page_size: u64,
    ) {
        let pending = self.pending_free.entry(transaction_id).or_default();
        let page_end_addess = page_start_address + (page_overflow + 1) * page_size;
        let mut page_address = page_start_address;
        // println!("FREEING PAGE WITH OVERFLOW: {page_overflow:?}");
        while page_address < page_end_addess {
            pending.insert(page_address);
            page_address += page_size;
        }
    }

    pub fn release(&mut self, min_tx: TransactionId, max_tx: TransactionId) -> Vec<Address> {
        let txs = self
            .pending_free
            .range(..=min_tx)
            .map(|(tx_id, _)| *tx_id)
            .collect::<Vec<_>>();

        // Release pages freed in earliest snapshot
        let mut freed: Vec<Address> = Vec::new();
        for tx_id in txs {
            let pages = self
                .pending_free
                .remove(&tx_id)
                .expect("pending transactions");

            freed.extend(&pages);
            self.free.extend(pages);
        }

        // Release pages freed by write transactions
        // that were allocated after latest snapshot
        for (_, pages) in self.pending_free.iter_mut() {
            pages.retain(|page_address| {
                let allocated_by = self
                    .allocated_by
                    .get(page_address)
                    .unwrap_or(&0);

                let remove = *allocated_by > max_tx;
                if remove {
                    freed.push(*page_address);
                }
                !remove
            });
        }

        self.pending_free.retain(|_, pages| !pages.is_empty());
        // println!("RELEASING PAGES COUNT: {:?}", freed.len());

        for page in freed.iter() {
            self.allocated_by.remove(page);
        }
        freed
    }

    pub fn size(&self) -> usize {
        size_of::<u64>() * self.pages_len()
    }

    pub fn pages_len(&self) -> usize {
        self.free.len() + self.pending_pages_len()
    }

    pub fn pending_pages_len(&self) -> usize {
        self.pending_free.values().map(|pages| pages.len()).sum()
    }

    pub fn summary(&self) -> String {
        format!(
            "free={}, pending_free={}, pending_allocations={}, free_summary=[{}]",
            self.free.len(),
            self.pending_free.len(),
            self.pending_allocated.len(),
            self.free_summary().unwrap_or(String::new()),
        )
    }

    pub fn free_summary(&self) -> Option<String> {
        let page_size = 1 << 12;
        let mut previous_page_address: Option<Address> = None;
        let mut initial_page_address = *self.free.first()?;

        let mut res = String::new();
        for page_address in self.free.difference(&self.pending_allocated) {
            assert_eq!(page_address % page_size, 0, "invalid page address");
            let restart_initial_page = previous_page_address
                .map(|previous_page_address| page_address - previous_page_address != page_size)
                .unwrap_or(false);

            if restart_initial_page {
                let item = format!("({:?}-{:?},len={:?},gap={:?}),",
                    initial_page_address, previous_page_address.unwrap(),
                    (previous_page_address.unwrap()-initial_page_address),
                    (page_address-previous_page_address.unwrap()),
                );
                res.push_str(&item);
                initial_page_address = *page_address;
            }

            previous_page_address = Some(*page_address);
        }
        Some(res)
    }

    fn copy_all(&self) -> Vec<u64> {
        let pending = self
            .pending_free
            .iter()
            .flat_map(|(_, pages)| pages.iter().copied())
            .collect::<BTreeSet<_>>();

        self.free.union(&pending).copied().collect()
    }

    pub fn commit_allocations(&mut self) {
        self.pending_allocated.clear();
    }

    pub fn rollback(&mut self, transaction_id: TransactionId) {
        let Some(_) = self.pending_free.remove(&transaction_id) else {
            return;
        };
        self.free.extend(self.pending_allocated.iter());
        for allocated in self.pending_allocated.iter() {
            let removed = self.allocated_by.remove(allocated).is_some();
            assert!(removed, "allocated page was not registered");
        }
        self.pending_allocated.clear();
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
        // header
        0x00, 0x03, // flags
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, // len=3
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // overflow
        // contents
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10, // 16
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x20, // 32
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x30, // 48
    ];

    #[test]
    fn allocates_multiple_pages_at_the_start() {
        let mut free_list = FreeList::default();
        free_list.free = free_list![20, 30, 40, 50, 110, 130, 150, 160, 170, 180];
        let page_address = free_list.allocate(4, 10);
        assert_eq!(page_address, Some(20));
        assert_eq!(free_list.free, free_list![110, 130, 150, 160, 170, 180]);
    }

    #[test]
    fn allocates_multiple_pages_at_the_middle() {
        let mut free_list = FreeList::default();
        free_list.free = free_list![20, 110, 130, 150, 160, 170, 180];
        let page_address = free_list.allocate(3, 10);
        assert_eq!(page_address, Some(150));
        assert_eq!(free_list.free, free_list![20, 110, 130, 180]);
    }

    #[test]
    fn allocates_multiple_pages_at_the_end() {
        let mut free_list = FreeList::default();
        free_list.free = free_list![20, 110, 130, 150, 160, 170, 180];
        let page_address = free_list.allocate(4, 10);
        assert_eq!(page_address, Some(150));
        assert_eq!(free_list.free, free_list![20, 110, 130]);
    }

    #[test]
    fn allocates_one_page() {
        let mut free_list = FreeList::default();
        free_list.free = free_list![20, 110, 130, 150, 160, 170, 180];
        let page_address = free_list.allocate(1, 10);
        assert_eq!(page_address, Some(20));
        assert_eq!(free_list.free, free_list![110, 130, 150, 160, 170, 180]);
    }

    #[test]
    fn cannot_allocates_when_page_runs_are_too_small() {
        let mut free_list = FreeList::default();
        free_list.free = free_list![20, 110, 130, 150, 160, 170, 180];
        let page_address = free_list.allocate(10, 10);
        assert_eq!(page_address, None);
        assert_eq!(free_list.free, free_list![20, 110, 130, 150, 160, 170, 180]);
    }

    #[test]
    fn cannot_allocates_when_no_free_pages() {
        let mut free_list = FreeList::default();
        let page_address = free_list.allocate(1, 10);
        assert_eq!(page_address, None);
        assert!(free_list.free.is_empty());
    }

    #[test]
    fn reads_free_list() {
        let mut reader = Cursor::new(FREE_LIST_DATA);
        let (header, free_list) = FreeList::read(&mut reader).unwrap();
        assert_eq!(
            header,
            NodeHeader {
                flags: FREELIST_NODE,
                internal_nodes_len: 3,
                overflow_len: 0
            }
        );
        assert_eq!(free_list.free, free_list![16, 32, 48]);
        assert!(free_list.pending_free.is_empty());
    }

    #[test]
    fn writes_free_list() {
        let mut free_list = FreeList::default();
        free_list.free = free_list![16, 32, 48];
        let mut writer = Cursor::new(Vec::new());
        free_list.write(&mut writer, 128).unwrap();
        assert_eq!(writer.into_inner(), FREE_LIST_DATA);
    }
}
