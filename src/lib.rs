pub mod cursor;
pub mod free_list;
pub mod node;

mod format;
mod tx;

use anyhow::{anyhow, Result};
use free_list::FreeList;
use node::{Address, InternalNodes, MetaNode, NodeHeader, NodeManager};
use std::{
    io::{Seek, Write},
    path::Path,
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicU64, Ordering},
    },
};
use tx::WriteTransaction;

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

        Ok(DatabaseInternal {
            node_manager: NodeManager::new(
                file_path,
                options.max_files as usize,
                options.page_size,
                options.cache_size,
            ),
            writer: Mutex::new(Some(Writer {
                free_list,
                free_list_node_address: free_list_address,
                free_list_header,
                meta_nodes,
            })),
            writer_cond_var: Condvar::new(),
            page_size: options.page_size,
            root_node: AtomicU64::new(root_node_address),
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
        let root_node_address = meta_node.root_node;
        Ok(DatabaseInternal {
            node_manager: NodeManager::new(
                file_path,
                options.max_files as usize,
                meta_node.page_size,
                options.cache_size,
            ),
            page_size: meta_node.page_size,
            writer: Mutex::new(Some(Writer {
                free_list_header,
                free_list,
                free_list_node_address: meta_node.free_list_node,
                meta_nodes: [meta_node.clone(), meta_node],
            })),
            writer_cond_var: Condvar::new(),
            root_node: AtomicU64::new(root_node_address),
        })
    }
}

#[derive(Debug)]
pub struct Writer {
    pub free_list_header: NodeHeader,
    pub free_list: FreeList,
    pub free_list_node_address: Address,
    pub meta_nodes: [MetaNode; 2],
}

impl Writer {
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
    pub writer: Mutex<Option<Writer>>,
    pub writer_cond_var: Condvar,
    pub page_size: u32,
    pub root_node: std::sync::atomic::AtomicU64,
}

impl DatabaseInternal {
    pub fn begin_write(self: &Arc<Self>) -> WriteTransaction {
        let writer = self.take_writer();
        WriteTransaction::new(self.clone(), writer)
    }

    pub fn take_writer(&self) -> Writer {
        let mut writer_lock = self.writer.lock().expect("writer lock");

        loop {
            if let Some(writer) = writer_lock.take() {
                return writer;
            } else {
                writer_lock = self
                    .writer_cond_var
                    .wait(writer_lock)
                    .expect("writer cond var");
            }
        }
    }

    pub fn release_writer(&self, writer: Writer) {
        let root_node = writer.meta().root_node;
        let mut writer_lock = self.writer.lock().expect("files lock");
        assert!(writer_lock.is_none(), "there must be only one writer token");
        *writer_lock = Some(writer);
        self.writer_cond_var.notify_one();
        self.root_node.store(root_node, Ordering::Release);
    }
}
