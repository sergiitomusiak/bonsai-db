pub mod cursor;
pub mod node;
pub mod free_list;

mod format;
mod tx;

use node::{NodeId, NodeManager};
use tx::WriteTransaction;
use std::sync::Arc;

pub type Key = Vec<u8>;
pub type Value = Vec<u8>;

// const MIN_PAGE_SIZE: usize = 1 << 10;

#[derive(Debug)]
pub struct Options {
    pub max_read_files: u16,
    pub page_size: u32,
    pub cache_size: u64,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            max_read_files: 16,
            page_size: 4 << 10, // 4KiB
            cache_size: 1 << 20, // 1MiB
        }
    }
}

// pub struct Database {
//     state: Arc<DatabaseState>,
// }

// impl Database {
//     // pub fn open(file_path: impl AsRef<Path>, options: Options) -> Result<Self> {
//     //     // open or create file
//     //     let database_state = if !std::fs::exists(file_path.as_ref())? {
//     //         Self::write_initial_state(file_path, &options)?
//     //     } else {
//     //         Self::read_state(file_path, &options)?
//     //     };

//     //     Ok(Self {
//     //         state: Arc::new(database_state),
//     //     })
//     // }

//     // fn read(&self) -> ReadTransaction {
//     //     todo!()
//     // }

//     // fn write(&self) -> WriteTransaction {
//     //     todo!()
//     // }

//     // fn sync(&self) -> Result<()> {
//     //     todo!()
//     // }

//     // fn close(&self) -> Result<()> {
//     //     todo!()
//     // }

//     // fn write_initial_state(file_path: impl AsRef<Path>, options: &Options) -> Result<DatabaseState> {
//     //     if (options.page_size as usize) < MIN_PAGE_SIZE {
//     //         return Err(anyhow!("page size is too small: {}. must be at least {}",
//     //             options.page_size, MIN_PAGE_SIZE,
//     //         ));
//     //     }

//     //     let mut file = std::fs::OpenOptions::new()
//     //         .read(true)
//     //         .write(true)
//     //         .create_new(true)
//     //         .open(file_path.as_ref())?;

//     //     let free_list_page_id = 0;
//     //     let root_node_page_id = 1;

//     //     // write meta nodes
//     //     for i in 0..2 {
//     //         let meta_node = MetaNode {
//     //             page_size: options.page_size,
//     //             root_node: root_node_page_id,
//     //             freelist_node: free_list_page_id,
//     //             transaction_id: i,
//     //         };
//     //         let mut page_writer = LimitedWriter::new(&mut file, i * MetaNode::page_size() as u64, MetaNode::page_size() as usize)?;
//     //         meta_node.write(&mut page_writer)?;
//     //     }

//     //     let initial_page_alignment = (MetaNode::page_size()*2 + options.page_size as u64 - 1) / (options.page_size as u64);

//     //     // write free list node
//     //     let free_list = FreeList::default();
//     //     let mut page_writer = LimitedWriter::new(&mut file, (initial_page_alignment + free_list_page_id) * options.page_size as u64 , options.page_size as usize)?;
//     //     free_list.write(&mut page_writer)?;

//     //     // write root node
//     //     let node = InternalNodes::Leaf(Vec::new());
//     //     let mut page_writer = LimitedWriter::new(&mut file, (initial_page_alignment + root_node_page_id) * options.page_size as u64, options.page_size as usize)?;
//     //     node.write(&mut page_writer, options.page_size as usize)?;

//     //     Ok(DatabaseState {
//     //         initial_page_alignment,
//     //         meta_node: MetaNode {
//     //             page_size: options.page_size,
//     //             root_node: root_node_page_id,
//     //             freelist_node: free_list_page_id,
//     //             transaction_id: 1,
//     //         },
//     //         free_list,
//     //         node_manager: NodeManager::new(
//     //             file_path,
//     //             options.max_read_files as usize,
//     //             options.page_size,
//     //             options.cache_size as u32,
//     //         ),
//     //         page_size: options.page_size,
//     //     })
//     // }

//     // fn read_state(file_path: impl AsRef<Path>, options: &Options) -> Result<DatabaseState> {
//     //     // read meta nodes
//     //     // validate and pick valid node with highest transaction id
//     //     let mut file = std::fs::OpenOptions::new()
//     //         .read(true)
//     //         .write(true)
//     //         .create(false)
//     //         .open(file_path.as_ref())?;

//     //     // read first meta page to determine page size
//     //     let mut page_reader = LimitedReader::new(&mut file, 0, MetaNode::size())?;
//     //     let meta_node0 = MetaNode::read(&mut page_reader);

//     //     let mut page_reader = LimitedReader::new(&mut file, MetaNode::page_size() as u64, MetaNode::size())?;
//     //     let meta_node1 = MetaNode::read(&mut page_reader);

//     //     let meta_node = match (meta_node0, meta_node1) {
//     //         (Ok(meta_node0), Ok(meta_node1)) => {
//     //             if meta_node0.transaction_id < meta_node1.transaction_id {
//     //                 Ok(meta_node1)
//     //             } else {
//     //                 Ok(meta_node0)
//     //             }
//     //         }
//     //         (Ok(meta_node0), _) => Ok(meta_node0),
//     //         (_, Ok(meta_node1)) => Ok(meta_node1),
//     //         (e@Err(_), _) => e,
//     //     }?;

//     //     if options.page_size != meta_node.page_size {
//     //         return Err(anyhow!("unexpected page size: {}. actual database page size is {}",
//     //             options.page_size, meta_node.page_size,
//     //         ));
//     //     }

//     //     let initial_page_alignment = (MetaNode::page_size()*2 + meta_node.page_size as u64 - 1) / (meta_node.page_size as u64);

//     //     // read free list
//     //     let mut page_reader = LimitedReader::new(&mut file, (initial_page_alignment + meta_node.freelist_node) * meta_node.page_size as u64, usize::MAX)?;
//     //     let free_list = FreeList::read(&mut page_reader)?;

//     //     Ok(DatabaseState {
//     //         initial_page_alignment,
//     //         meta_node,
//     //         free_list,
//     //         node_manager: NodeManager::new(
//     //             file_path,
//     //             options.max_read_files as usize,
//     //             options.page_size,
//     //             options.cache_size as u32,
//     //         ),
//     //         page_size: options.page_size,
//     //     })
//     // }
// }

pub struct DatabaseState {
    pub node_manager: NodeManager,
    pub root_node_id: NodeId,
    // page_size: u32,
    // initial_page_alignment: u64,
    // meta_node: MetaNode,
    // free_list: FreeList,
}

impl DatabaseState {
    pub fn begin_write(self: &Arc<Self>) -> WriteTransaction {
        WriteTransaction::new(self.clone(), self.root_node_id)
    }
}