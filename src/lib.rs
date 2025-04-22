pub mod cursor;
pub mod free_list;
pub mod node;

mod format;
mod tx;

use anyhow::{anyhow, Result};
use free_list::FreeList;
use node::{Address, InternalNodes, MetaNode, NodeHeader, NodeManager};
use std::{
    collections::{btree_map::Entry, BTreeMap}, io::{Seek, Write}, path::Path, sync::{Arc, Condvar, Mutex}
};
use tx::{ReadTransaction, TransactionId, WriteTransaction};

const MIN_PAGE_SIZE: usize = 1 << 7;

#[derive(Debug)]
pub struct Options {
    pub max_files: u16,
    pub page_size: u32,
    pub cache_size: u64,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            max_files: 16,
            page_size: 4 << 10,  // 4KiB
            cache_size: 100 << 20, // 1MiB
        }
    }
}

#[derive(Clone)]
pub struct Database {
    internal: Arc<DatabaseInternal>,
}

impl Database {
    pub fn open(file_path: impl AsRef<Path>, options: Options) -> Result<Self> {
        // open or create file
        let internal = if !std::fs::exists(file_path.as_ref())? {
            Self::write_initial_state(file_path, &options)?
        } else {
            Self::read_state(file_path, &options)?
        };

        Ok(Self {
            internal: Arc::new(internal),
        })
    }

    pub fn begin_write(&self) -> WriteTransaction {
        self.internal.begin_write()
    }

    pub fn begin_read(&self) -> ReadTransaction {
        self.internal.begin_read()
    }

    fn write_initial_state(
        file_path: impl AsRef<Path>,
        options: &Options,
    ) -> Result<DatabaseInternal> {
        if (options.page_size as usize) < MIN_PAGE_SIZE {
            return Err(anyhow!(
                "page size is too small: {}. must be at least {}",
                options.page_size,
                MIN_PAGE_SIZE,
            ));
        }

        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(file_path.as_ref())?;

        let initial_alignment = options.page_size as u64
            * (MetaNode::page_size() * 2).div_ceil(options.page_size as u64);

        let free_list_address = initial_alignment;
        let root_node_address = initial_alignment + options.page_size as u64;
        let end_address = root_node_address + options.page_size as u64;
        let meta_nodes = [
            MetaNode {
                page_size: options.page_size,
                root_node: root_node_address,
                free_list_node: free_list_address,
                transaction_id: 0,
                end_address,
            },
            MetaNode {
                page_size: options.page_size,
                root_node: root_node_address,
                free_list_node: free_list_address,
                transaction_id: 1,
                end_address,
            },
        ];
        for (i, meta_node) in meta_nodes.iter().enumerate() {
            file.seek(std::io::SeekFrom::Start(i as u64 * MetaNode::page_size()))?;
            meta_node.write(&mut file)?;
        }

        // write free list node
        let free_list = FreeList::default();
        file.seek(std::io::SeekFrom::Start(free_list_address))?;
        let free_list_header = free_list.write(&mut file, options.page_size)?;

        // write root node
        let node = InternalNodes::Leaf(Vec::new());
        file.seek(std::io::SeekFrom::Start(root_node_address))?;
        node.write(&mut file, options.page_size as u64)?;

        file.set_len(end_address)?;
        file.flush()?;

        let write_state = WriteState {
            free_list,
            free_list_node_address: free_list_address,
            free_list_header,
            meta_nodes,
        };

        let reader_meta = write_state.meta().clone();

        Ok(DatabaseInternal {
            node_manager: NodeManager::new(
                file_path,
                options.max_files as usize,
                options.page_size,
                options.cache_size,
            ),
            write_state: Mutex::new(Some(write_state)),
            write_state_condvar: Condvar::new(),
            read_state: Mutex::new(ReadState {
                meta_node: reader_meta,
                transactions: BTreeMap::new(),
            }),
            page_size: options.page_size,
        })
    }

    fn read_state(file_path: impl AsRef<Path>, options: &Options) -> Result<DatabaseInternal> {
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(false)
            .open(file_path.as_ref())?;

        file.seek(std::io::SeekFrom::Start(0))?;
        let meta_node0 = MetaNode::read(&mut file);
        file.seek(std::io::SeekFrom::Start(MetaNode::page_size()))?;
        let meta_node1 = MetaNode::read(&mut file);

        let meta_node = match (meta_node0, meta_node1) {
            (Ok(meta_node0), Ok(meta_node1)) => {
                if meta_node0.transaction_id < meta_node1.transaction_id {
                    Ok(meta_node1)
                } else {
                    Ok(meta_node0)
                }
            }
            (Ok(meta_node0), _) => Ok(meta_node0),
            (_, Ok(meta_node1)) => Ok(meta_node1),
            (e @ Err(_), _) => e,
        }?;

        if options.page_size != meta_node.page_size {
            return Err(anyhow!(
                "unexpected page size: {}. actual database page size is {}",
                options.page_size,
                meta_node.page_size,
            ));
        }

        file.seek(std::io::SeekFrom::Start(meta_node.free_list_node))?;
        let (free_list_header, free_list) = FreeList::read(&mut file)?;

        Ok(DatabaseInternal {
            node_manager: NodeManager::new(
                file_path,
                options.max_files as usize,
                meta_node.page_size,
                options.cache_size,
            ),
            page_size: meta_node.page_size,
            write_state: Mutex::new(Some(WriteState {
                free_list_header,
                free_list,
                free_list_node_address: meta_node.free_list_node,
                meta_nodes: [meta_node.clone(), meta_node.clone()],
            })),
            write_state_condvar: Condvar::new(),
            read_state: Mutex::new(ReadState {
                meta_node,
                transactions: BTreeMap::new(),
            }),
        })
    }
}

pub struct DatabaseInternal {
    pub node_manager: NodeManager,
    pub write_state: Mutex<Option<WriteState>>,
    pub write_state_condvar: Condvar,
    pub read_state: Mutex<ReadState>,
    pub page_size: u32,
}

impl DatabaseInternal {
    pub fn begin_write(self: &Arc<Self>) -> WriteTransaction {
        let writer = self.take_write_state();
        WriteTransaction::new(self.clone(), writer)
    }

    pub fn begin_read(self: &Arc<Self>) -> ReadTransaction {
        let mut read_state_lock = self.read_state.lock().expect("read state lock");
        let root_node = read_state_lock.meta_node.root_node;
        let transaction_id = read_state_lock.meta_node.transaction_id;
        *read_state_lock.transactions
            .entry(transaction_id)
            .or_default() += 1;

        ReadTransaction::new(self.clone(), root_node, transaction_id)
    }

    pub fn take_write_state(&self) -> WriteState {
        let mut write_state_lock = self.write_state.lock().expect("writer lock");
        loop {
            if let Some(mut write_state) = write_state_lock.take() {
                let read_state_lock = self.read_state.lock().expect("read state lock");
                let min_transaction_id = read_state_lock
                    .transactions
                    .first_key_value()
                    .map(|(transaction_id, _)| *transaction_id)
                    .unwrap_or(TransactionId::MAX);

                let max_transaction_id = read_state_lock
                .transactions
                .last_key_value()
                .map(|(transaction_id, _)| *transaction_id)
                .unwrap_or(0);

                if min_transaction_id > 0 {
                    let freed = write_state.free_list.release(min_transaction_id, max_transaction_id);
                    self.node_manager.invalidate_nodes_cache(freed);
                }

                return write_state;
            } else {
                write_state_lock = self
                    .write_state_condvar
                    .wait(write_state_lock)
                    .expect("writer cond var");
            }
        }
    }

    pub fn release_writer(&self, writer: WriteState) {
        let mut write_state_lock = self.write_state.lock().expect("transaction state lock");
        assert!(write_state_lock.is_none(), "there must be only one writer token");
        let mut read_state_lock = self.read_state.lock().expect("read state lock");
        read_state_lock.meta_node = writer.meta().clone();
        *write_state_lock = Some(writer);
        self.write_state_condvar.notify_one();
    }

    pub fn release_reader(&self, transaction_id: TransactionId) {
        let mut read_state_lock = self.read_state.lock().expect("transaction state lock");
        match read_state_lock.transactions.entry(transaction_id) {
            Entry::Occupied(mut entry) => {
                assert!(*entry.get() > 0, "missing read transaction_id entry");
                if *entry.get() == 1 {
                    entry.remove();
                } else {
                    *entry.get_mut() -= 1;
                }
            }
            Entry::Vacant(_) => panic!("missing entries for transaction"),
        }
    }
}

pub struct ReadState {
    pub meta_node: MetaNode,
    pub transactions: BTreeMap<TransactionId, usize>,
}

#[derive(Debug)]
pub struct WriteState {
    pub free_list_header: NodeHeader,
    pub free_list: FreeList,
    pub free_list_node_address: Address,
    pub meta_nodes: [MetaNode; 2],
}

impl WriteState {
    pub fn meta_mut(&mut self) -> &mut MetaNode {
        if self.meta_nodes[0].transaction_id < self.meta_nodes[1].transaction_id {
            &mut self.meta_nodes[1]
        } else {
            &mut self.meta_nodes[0]
        }
    }

    pub fn meta(&self) -> &MetaNode {
        if self.meta_nodes[0].transaction_id < self.meta_nodes[1].transaction_id {
            &self.meta_nodes[1]
        } else {
            &self.meta_nodes[0]
        }
    }
}
