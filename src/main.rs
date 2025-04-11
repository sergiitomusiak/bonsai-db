use anyhow::Result;
use bonsai_db::{
    node::{BranchInternalNode, InternalNodes, LeafInternalNode, NodeId, NodeManager}, DatabaseState,
};
use std::{collections::BTreeMap, sync::Arc};

fn setup_test_for_cursor() -> Result<()> {
    let path = "./my.db";
    std::fs::File::create(&path)?;

    let node_manager = NodeManager::new("./my.db", 10, 128);

    // Root
    let node = InternalNodes::Branch(vec![
        BranchInternalNode {
            key: b"key10".to_vec(),
            node_id: NodeId::Address(1),
        },
        BranchInternalNode {
            key: b"key20".to_vec(),
            node_id: NodeId::Address(2),
        },
        BranchInternalNode {
            key: b"key30".to_vec(),
            node_id: NodeId::Address(3),
        },
    ]);
    node_manager.write_node(0, &node)?;

    // Child1
    let node = InternalNodes::Leaf(vec![
        LeafInternalNode {
            key: b"key10".to_vec(),
            value: b"value10".to_vec(),
        },
        LeafInternalNode {
            key: b"key12".to_vec(),
            value: b"value12".to_vec(),
        },
    ]);
    node_manager.write_node(1, &node)?;

    // Child2
    let node = InternalNodes::Leaf(vec![
        LeafInternalNode {
            key: b"key20".to_vec(),
            value: b"value20".to_vec(),
        },
        LeafInternalNode {
            key: b"key22".to_vec(),
            value: b"value22".to_vec(),
        },
    ]);
    node_manager.write_node(2, &node)?;

    // Child3
    let node = InternalNodes::Leaf(vec![
        LeafInternalNode {
            key: b"key30".to_vec(),
            value: b"value30".to_vec(),
        },
        LeafInternalNode {
            key: b"key32".to_vec(),
            value: b"value32".to_vec(),
        },
    ]);
    node_manager.write_node(3, &node)?;

    Ok(())
}

fn create_test_database() -> Result<Arc<DatabaseState>> {
    let node_manager = NodeManager::new("./my.db", 10, 128);
    Ok(Arc::new(DatabaseState {
        node_manager,
        root_node_id: NodeId::Address(0),
    }))
}

fn run_basic_cursor_test() -> Result<()> {
    println!("\nrun_basic_cursor_test\n");
    // let node_manager = NodeManager::new("./my.db", 10, 128, 1024);
    let db = create_test_database()?;
    let tx = db.begin_write();
    let mut cursor = tx.cursor()?;
    //let mut cursor = Cursor::new(0, Arc::new(node_manager))?;
    while cursor.is_valid() {
        println!(
            "{:?} = {:?}",
            String::from_utf8_lossy(cursor.key()),
            String::from_utf8_lossy(cursor.value()),
        );
        cursor.next()?;
    }
    Ok(())
}

fn run_basic_cursor_reverse_test() -> Result<()> {
    println!("\nrun_basic_cursor_reverse_test\n");
    // let node_manager = NodeManager::new("./my.db", 10, 128, 1024);
    // let mut cursor = Cursor::new(NodeId::Address(0), Arc::new(node_manager))?;
    let db = create_test_database()?;
    let tx = db.begin_write();
    let mut cursor = tx.cursor()?;
    cursor.last()?;
    while cursor.is_valid() {
        println!(
            "{:?} = {:?}",
            String::from_utf8_lossy(cursor.key()),
            String::from_utf8_lossy(cursor.value()),
        );
        if !cursor.prev()? {
            break;
        }
    }
    Ok(())
}

fn run_cursor_seek() -> Result<()> {
    println!("\nrun_cursor_seek\n");
    let seeks = [
        "key0", "key10", "key11", "key12", "key13",
        "key2", "key20", "key21", "key22", "key23",
        "key3", "key30", "key31", "key32", "key33",
        "key4",
    ];

    for seek in seeks {
        let db = create_test_database()?;
        let tx = db.begin_write();
        let mut cursor = tx.cursor()?;
        //let mut cursor = Cursor::new(NodeId::Address(0), node_manager.clone())?;
        cursor.seek(seek.as_bytes())?;
        if cursor.is_valid() {
            println!(
                "Seek = {:?}; Current key {:?} = {:?}",
                seek,
                String::from_utf8_lossy(cursor.key()),
                String::from_utf8_lossy(cursor.value()),
            );
        } else {
            println!("Seek = {:?}; Invalidated cursor", seek);
        }
    }

    Ok(())
}

fn run_get_put_test() -> Result<()> {
    println!("\nrun_get_put_test\n");
    let db = create_test_database()?;
    let mut tx = db.begin_write();

    for i in 0..30 {
        let key = format!("key0000_{i}");
        let value = format!("value_{i}");
        tx.put(key.as_bytes(), value.as_bytes())?;
    }

    // tx.put(b"key_00000", b"value0a")?;
    // tx.put(b"key_00010a", b"value10a")?;
    // tx.put(b"key_00012a", b"value12a")?;
    // tx.put(b"key_00020a", b"value20a")?;
    // tx.put(b"key_00022a", b"value22a")?;
    // tx.put(b"key_00030a", b"value20a")?;
    // tx.put(b"key_00032a", b"value22a")?;
    // tx.put(b"key_00040a", b"value40a")?;

    tx.remove(b"key0")?;
    tx.remove(b"key10")?;
    tx.remove(b"key12")?;
    tx.remove(b"key20")?;
    tx.remove(b"key22")?;
    tx.remove(b"key30")?;
    tx.remove(b"key32")?;
    tx.remove(b"key40")?;

    tx.traverse();
    tx.merge()?;
    println!("\n======\n");
    tx.traverse();
    tx.split()?;
    println!("\n======\n");
    tx.traverse();

    println!("\n======\n");
    let mut cursor = tx.cursor()?;
    //let mut cursor = Cursor::new(0, Arc::new(node_manager))?;
    while cursor.is_valid() {
        println!(
            "{:?} = {:?}",
            String::from_utf8_lossy(cursor.key()),
            String::from_utf8_lossy(cursor.value()),
        );
        cursor.next()?;
    }

    Ok(())
}

fn main() {
    setup_test_for_cursor().expect("setup test for cursor");
    run_basic_cursor_test().expect("basic cursor test");
    run_basic_cursor_reverse_test().expect("basic cursor reverse test");
    run_cursor_seek().expect("cursor seek");
    run_get_put_test().expect("get put");
    // let node_manager = NodeManager::new("./my.db", 10, 128, 1024);

    let mut m = BTreeMap::new();
    m.insert(10, "test");

    // let node = InternalNodes::Leaf(vec![
    //     LeafInternalNode {
    //         key: b"hello".to_vec(),
    //         value: b"world".to_vec(),
    //     },
    //     LeafInternalNode {
    //         key: b"hey".to_vec(),
    //         value: b"man".to_vec(),
    //     },
    // ]);

    // node_manager.write_node(0, &node).expect("write");
    // node_manager.write_node(1, &node).expect("write");

    // let node = node_manager.read_node(1).expect("read");
    // let InternalNodes::Leaf(nodes) = node else {
    //     panic!("unexpected node type");
    // };

    // for n in nodes {
    //     println!("{:?} {:?}", String::from_utf8_lossy(&n.key), String::from_utf8_lossy(&n.value));
    // }

    // let _db = Database::open("./my.db", Options::default())
    //     .expect("open database");

    //let f = std::fs::File::create("test.txt").unwrap();
    //f.try_clone()
    // let mut bucket = Bucket::new();
    // for i in 0..50 {
    //     bucket.put(format!("{:03}", i), format!("value_{i}"));
    // }

    // for i in 0..100 {
    //     let value = bucket.get(format!("{:03}", i));
    //     println!("key = {i}, value = {value:?}");
    // }

    // println!("{bucket:#?}");

    // let value = 40;
    // let data = vec![10, 20, 30];
    // let res = data.binary_search_by(|item| item.cmp(&value));
    // println!("{res:?}");
}

// const INITIAL_ROOT_NODE_ID: NodeId = 0;
// const MAX_INTERNAL_NODES: usize = 30;

// type TransactionId = u64;
// type PageId = u64;
// type NodeId = usize;
// type Key = String;
// type Value = String;

// // type Key = Vec<u8>;
// // type Value = Vec<u8>;

// #[derive(Debug, Clone, Copy, PartialEq, Eq)]
// enum NodeType {
//     Branch,
//     Leaf,
// }

// #[derive(Debug)]
// struct Node {
//     parent_node_id: Option<NodeId>,
//     node_type: NodeType,
//     internal_nodes: Vec<InternalNode>,
// }

// impl Node {
//     fn new(node_type: NodeType) -> Self {
//         Self {
//             parent_node_id: None,
//             node_type,
//             internal_nodes: Vec::new(),
//         }
//     }

//     fn is_leaf(&self) -> bool {
//         self.node_type == NodeType::Leaf
//     }

//     fn child_index(&self, node_id: NodeId) -> Option<usize> {
//         self.internal_nodes
//             .iter()
//             .enumerate()
//             .find(|(_, internal_node)| internal_node.node_id == Some(node_id))
//             .map(|item| item.0)
//     }
// }

// #[derive(Debug)]
// struct InternalNode {
//     node_id: Option<NodeId>,
//     key: Key,
//     value: Value,
// }

// #[derive(Debug)]
// struct Bucket {
//     root_node_id: NodeId,
//     next_node_id: NodeId,
//     nodes: HashMap<NodeId, Node>,
// }

// impl Bucket {
//     fn new() -> Self {
//         Self {
//             root_node_id: INITIAL_ROOT_NODE_ID,
//             next_node_id: INITIAL_ROOT_NODE_ID + 1,
//             nodes: HashMap::from([(INITIAL_ROOT_NODE_ID, Node{
//                 node_type: NodeType::Leaf,
//                 parent_node_id: None,
//                 internal_nodes: Vec::new(),
//             })]),
//         }
//     }

//     fn get(&self, key: Key) -> Option<Value> {
//         let (node_id, index, exact_match) = self.seek(&key);
//         if exact_match {
//             Some(self.nodes[&node_id].internal_nodes[index].value.clone())
//         } else {
//             None
//         }
//     }

//     fn put(&mut self, key: Key, value: Value) {
//         let (node_id, index, exact_match) = self.seek(&key);
//         let node = self.nodes.get_mut(&node_id).expect("get mut node");
//         if exact_match {
//             node.internal_nodes[index].key = value;
//         } else {
//             node.internal_nodes.insert(index, InternalNode {
//                 node_id: None,
//                 key,
//                 value,
//             });
//         }
//         self.split(node_id);
//     }

//     fn remove(&mut self, key: Key) {
//         let (node_id, index, exact_match) = self.seek(&key);
//         let node = self.nodes.get_mut(&node_id).expect("get mut node");
//         if !exact_match {
//             return;
//         }
//         node.internal_nodes.remove(index);
//         self.merge(node_id);
//     }

//     fn merge(&mut self, mut node_id: NodeId) {
//         // 1. merge with smaller sibling, and remove current node
//         // 2. take nodes from either left or right sibling.
//         // 3.
//     }

//     fn split(&mut self, mut node_id: NodeId) {
//         if self.nodes[&node_id].internal_nodes.len() <= MAX_INTERNAL_NODES {
//             return;
//         }

//         loop {
//             if self.nodes[&node_id].internal_nodes.len() <= MAX_INTERNAL_NODES {
//                 return;
//             }

//             let split_off_at = self.nodes
//                 .get_mut(&node_id)
//                 .expect("get mut node")
//                 .internal_nodes.len() / 2;

//             let new_internal_nodes = self
//                 .nodes
//                 .get_mut(&node_id)
//                 .expect("get mut node")
//                 .internal_nodes.split_off(split_off_at);

//             let new_node = Node {
//                 node_type: self.nodes[&node_id].node_type,
//                 parent_node_id: self.nodes[&node_id].parent_node_id,
//                 internal_nodes: new_internal_nodes,
//             };

//             let parent_node_id = new_node.parent_node_id;
//             let new_node_id = self.next_id();
//             let new_node_key = new_node.internal_nodes[0].key.clone();
//             self.nodes.insert(new_node_id, new_node);

//             node_id = if let Some(parent_node_id) = parent_node_id {
//                 let child_index = self.nodes[&parent_node_id]
//                     .child_index(node_id)
//                     .expect("child index");

//                 self.nodes.get_mut(&parent_node_id).expect("parent node")
//                     .internal_nodes
//                     .insert(child_index + 1, InternalNode {
//                         node_id: Some(new_node_id),
//                         key: new_node_key,
//                         value: String::new(),
//                     });
//                 parent_node_id
//             } else {
//                 let parent_node_id = self.next_id();
//                 let new_parent_node = Node {
//                     node_type: NodeType::Branch,
//                     parent_node_id: None,
//                     internal_nodes: vec![
//                         InternalNode {
//                             node_id: Some(node_id),
//                             key: self.nodes[&node_id].internal_nodes[0].key.clone(),
//                             value: String::new(),
//                         },
//                         InternalNode {
//                             node_id: Some(new_node_id),
//                             key: new_node_key,
//                             value: String::new(),
//                         },
//                     ],
//                 };
//                 self.nodes.insert(parent_node_id, new_parent_node);
//                 self.root_node_id = parent_node_id;
//                 self.nodes.get_mut(&node_id)
//                     .expect("get mut node")
//                     .parent_node_id = Some(parent_node_id);

//                 self.nodes.get_mut(&new_node_id)
//                     .expect("get mut node")
//                     .parent_node_id = Some(parent_node_id);

//                 parent_node_id
//             };
//         }
//     }

//     fn seek(&self, key: &Key) -> (NodeId, usize, bool) {
//         let mut node_id = self.root_node_id;
//         loop {
//             // Get current node
//             let node = &self.nodes[&node_id];
//             let mut exact_match = false;
//             let mut index = node
//                 .internal_nodes
//                 .binary_search_by(|internal_node| {
//                     let res = internal_node.key.cmp(&key);
//                     if res == Ordering::Equal {
//                         exact_match = true;
//                     }
//                     res
//                 })
//                 .unwrap_or_else(identity);

//             if node.is_leaf() {
//                 return (node_id, index, exact_match);
//             }

//             if !exact_match && index > 0 {
//                 index -=1;
//             }

//             node_id = node.internal_nodes[index].node_id.expect("branch node id");
//         }
//     }

//     fn next_id(&mut self) -> NodeId {
//         let node_id = self.next_node_id;
//         self.next_node_id += 1;
//         node_id
//     }
// }