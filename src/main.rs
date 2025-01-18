use bonsai_db::{Database, Options};

fn main() {
    let _db = Database::open("./my.db", Options::default())
        .expect("open database");

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