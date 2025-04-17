pub mod cursor;
pub mod free_list;
pub mod node;

mod format;
mod tx;

use anyhow::{anyhow, Result};
use free_list::FreeList;
use node::{Address, InternalNodes, MetaNode, NodeHeader, NodeManager};
use std::{
    collections::{btree_map::Entry, BTreeMap}, io::{Seek, Write}, path::Path, sync::{Arc, Condvar, Mutex, RwLock}
};
use tx::{ReadTransaction, TransactionId, WriteTransaction};

pub type Key = Vec<u8>;
pub type Value = Vec<u8>;

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
            cache_size: 1 << 20, // 1MiB
        }
    }
}

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
        let meta_nodes = [
            MetaNode {
                page_size: options.page_size,
                root_node: root_node_address,
                free_list_node: free_list_address,
                transaction_id: 0,
            },
            MetaNode {
                page_size: options.page_size,
                root_node: root_node_address,
                free_list_node: free_list_address,
                transaction_id: 1,
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
            transaction_state: Mutex::new(TransactionState {
                write_state: Some(write_state),
                read_state: ReadState {
                    meta_node: reader_meta,
                    transactions: BTreeMap::new(),
                },
            }),
            transaction_state_condvar: Condvar::new(),
            page_size: options.page_size,
            // writer: Mutex::new(Some(writer)),
            // writer_cond_var: Condvar::new(),
            // reader_meta: RwLock::new(reader_meta),
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
            transaction_state: Mutex::new(TransactionState {
                write_state: Some(WriteState {
                    free_list_header,
                    free_list,
                    free_list_node_address: meta_node.free_list_node,
                    meta_nodes: [meta_node.clone(), meta_node.clone()],
                }),
                read_state: ReadState {
                    meta_node,
                    transactions: BTreeMap::new(),
                },
            }),
            transaction_state_condvar: Condvar::new(),
            // writer: Mutex::new(Some(WriteState {
            //     free_list_header,
            //     free_list,
            //     free_list_node_address: meta_node.free_list_node,
            //     meta_nodes: [meta_node.clone(), meta_node.clone()],
            // })),
            // writer_cond_var: Condvar::new(),
            // reader_meta: RwLock::new(meta_node),
        })
    }
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

pub struct DatabaseInternal {
    pub node_manager: NodeManager,
    // pub writer: Mutex<Option<WriteState>>,
    // pub writer_cond_var: Condvar,
    pub transaction_state: Mutex<TransactionState>,
    pub transaction_state_condvar: Condvar,
    pub page_size: u32,
    // pub reader_meta: RwLock<MetaNode>,
}

impl DatabaseInternal {
    pub fn begin_write(self: &Arc<Self>) -> WriteTransaction {
        let writer = self.take_write_state();
        // TODO: Release pending free pages, and invalidate cache
        WriteTransaction::new(self.clone(), writer)
    }

    pub fn begin_read(self: &Arc<Self>) -> ReadTransaction {
        let mut transaction_state_lock = self.transaction_state
            .lock()
            .expect("transaction state lock");

        let root_node = transaction_state_lock.read_state.meta_node.root_node;
        let transaction_id = transaction_state_lock.read_state.meta_node.transaction_id;
        *transaction_state_lock.read_state.transactions
            .entry(transaction_id)
            .or_default() += 1;

        ReadTransaction::new(self.clone(), root_node, transaction_id)
    }

    pub fn take_write_state(&self) -> WriteState {
        let mut transaction_state_lock = self.transaction_state.lock().expect("writer lock");
        loop {
            if let Some(mut write_state) = transaction_state_lock.write_state.take() {
                let min_transaction_id = transaction_state_lock
                    .read_state
                    .transactions
                    .first_key_value()
                    .map(|(transaction_id, _)| *transaction_id)
                    .unwrap_or(TransactionId::MAX);

                if min_transaction_id > 0 {
                    write_state.free_list.release(min_transaction_id-1);
                }

                return write_state;
            } else {
                transaction_state_lock = self
                    .transaction_state_condvar
                    .wait(transaction_state_lock)
                    .expect("writer cond var");
            }
        }
    }

    pub fn release_writer(&self, writer: WriteState) {
        let mut transaction_state_lock = self.transaction_state.lock().expect("transaction state lock");
        assert!(transaction_state_lock.write_state.is_none(), "there must be only one writer token");
        transaction_state_lock.read_state.meta_node = writer.meta().clone();
        transaction_state_lock.write_state = Some(writer);
        self.transaction_state_condvar.notify_one();
    }

    pub fn release_reader(&self, transaction_id: TransactionId) {
        let mut transaction_state_lock = self.transaction_state.lock().expect("transaction state lock");
        match transaction_state_lock.read_state.transactions.entry(transaction_id) {
            Entry::Occupied(mut entry) => {
                if *entry.get() == 0 {
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

pub struct TransactionState {
    pub write_state: Option<WriteState>,
    pub read_state: ReadState,
}