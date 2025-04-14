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
    sync::{Arc, Condvar, Mutex},
};
use tx::WriteTransaction;

pub type Key = Vec<u8>;
pub type Value = Vec<u8>;

const MIN_PAGE_SIZE: usize = 1 << 10;

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

    // fn read(&self) -> ReadTransaction {
    //     todo!()
    // }

    // fn write(&self) -> WriteTransaction {
    //     todo!()
    // }

    // fn sync(&self) -> Result<()> {
    //     todo!()
    // }

    // fn close(&self) -> Result<()> {
    //     todo!()
    // }

    pub fn begin_write(&self) -> WriteTransaction {
        self.internal.begin_write()
    }

    fn write_initial_state(
        file_path: impl AsRef<Path>,
        options: &Options,
    ) -> Result<DatabaseInternal> {
        // if (options.page_size as usize) < MIN_PAGE_SIZE {
        //     return Err(anyhow!("page size is too small: {}. must be at least {}",
        //         options.page_size, MIN_PAGE_SIZE,
        //     ));
        // }

        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(file_path.as_ref())?;

        let freelist_address = 0;
        let root_node_address = 1;

        // write meta nodes
        let meta_nodes = [
            MetaNode {
                page_size: options.page_size,
                root_node: root_node_address,
                freelist_node: freelist_address,
                transaction_id: 0,
            },
            MetaNode {
                page_size: options.page_size,
                root_node: root_node_address,
                freelist_node: freelist_address,
                transaction_id: 1,
            },
        ];
        for (i, meta_node) in meta_nodes.iter().enumerate() {
            file.seek(std::io::SeekFrom::Start(
                i as u64 * MetaNode::page_size() as u64,
            ))?;
            // let mut page_writer = LimitedWriter::new(&mut file, i * MetaNode::page_size() as u64, MetaNode::page_size() as usize)?;
            meta_node.write(&mut file)?;
        }

        let initial_alignment = options.page_size as u64
            * ((MetaNode::page_size() * 2 + options.page_size as u64 - 1)
                / (options.page_size as u64));

        // write free list node
        let freelist = FreeList::default();
        file.seek(std::io::SeekFrom::Start(
            initial_alignment + freelist_address,
        ))?;
        // let mut page_writer = LimitedWriter::new(&mut file, (initial_page_alignment + free_list_page_id) * options.page_size as u64 , options.page_size as usize)?;
        let freelist_header = freelist.write(&mut file, options.page_size)?;

        // write root node
        let node = InternalNodes::Leaf(Vec::new());
        file.seek(std::io::SeekFrom::Start(
            initial_alignment + root_node_address * options.page_size as u64,
        ))?;
        // let mut page_writer = LimitedWriter::new(&mut file, (initial_page_alignment + root_node_page_id) * options.page_size as u64, options.page_size as usize)?;
        node.write(&mut file, options.page_size as usize)?;

        file.flush()?;

        Ok(DatabaseInternal {
            node_manager: NodeManager::new(
                file_path,
                options.max_files as usize,
                options.page_size,
                initial_alignment,
            ),
            root_node_address,
            writer: Mutex::new(Some(Writer { freelist, freelist_node_address: freelist_address, freelist_header })),
            writer_cond_var: Condvar::new(),
            meta_nodes,
            page_size: options.page_size,
        })
    }

    fn read_state(file_path: impl AsRef<Path>, options: &Options) -> Result<DatabaseInternal> {
        // read meta nodes
        // validate and pick valid node with highest transaction id
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(false)
            .open(file_path.as_ref())?;

        // let meta_nodes = [MetaNode; 2];
        // read first meta page to determine page size
        // let mut page_reader = LimitedReader::new(&mut file, 0, MetaNode::size())?;
        file.seek(std::io::SeekFrom::Start(0))?;
        let meta_node0 = MetaNode::read(&mut file);

        // let mut page_reader = LimitedReader::new(&mut file, MetaNode::page_size() as u64, MetaNode::size())?;
        file.seek(std::io::SeekFrom::Start(MetaNode::page_size() as u64))?;
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

        let initial_alignment = meta_node.page_size as u64
            * ((MetaNode::page_size() * 2 + meta_node.page_size as u64 - 1)
                / (meta_node.page_size as u64));

        // read free list
        file.seek(std::io::SeekFrom::Start(
            initial_alignment + meta_node.freelist_node * meta_node.page_size as u64,
        ))?;
        let (freelist_header, freelist) = FreeList::read(&mut file)?;

        Ok(DatabaseInternal {
            node_manager: NodeManager::new(
                file_path,
                options.max_files as usize,
                meta_node.page_size,
                initial_alignment,
            ),
            root_node_address: meta_node.root_node,
            writer: Mutex::new(Some(Writer { freelist_header, freelist, freelist_node_address: meta_node.freelist_node })),
            writer_cond_var: Condvar::new(),
            page_size: meta_node.page_size,
            meta_nodes: [meta_node.clone(), meta_node],
        })
    }
}

#[derive(Debug)]
pub struct Writer {
    pub freelist_header: NodeHeader,
    pub freelist: FreeList,
    pub freelist_node_address: Address,
}

pub struct DatabaseInternal {
    pub node_manager: NodeManager,
    pub root_node_address: Address,
    pub writer: Mutex<Option<Writer>>,
    pub writer_cond_var: Condvar,
    pub meta_nodes: [MetaNode; 2],
    pub page_size: u32,
    // initial_page_alignment: u64,
    // meta_node: MetaNode,
    // free_list: FreeList,
}

impl DatabaseInternal {
    pub fn begin_write(self: &Arc<Self>) -> WriteTransaction {
        let writer = self.take_writer();
        WriteTransaction::new(self.clone(), self.root_node_address, writer)
    }

    pub fn meta(&self) -> MetaNode {
        if self.meta_nodes[0].transaction_id >= self.meta_nodes[0].transaction_id {
            self.meta_nodes[0].clone()
        } else {
            self.meta_nodes[1].clone()
        }
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
        let mut writer_lock = self.writer.lock().expect("files lock");
        assert!(
            writer_lock.is_none(),
            "there must be only one writer token"
        );
        *writer_lock = Some(writer);
        self.writer_cond_var.notify_one();
    }
}
