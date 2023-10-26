use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::{Mutex, MutexGuard};
use crate::{Iter, NodeData, NodeId, OpenMode, TreeFileError};
use crate::TreeFileError::{NonExistingFiles, NonExistingNode, FileIOError, LogicError};
use crate::OpenMode::{TruncateCreate, OpenCreate, MustExist};
use crate::utils::{add_and_subtract, create_file, open_file};


const NODE_LENGTH: usize = 40;
const MAP_LENGTH: usize = 10;
const NODE_CHILD_META_LENGTH: usize = 16;
const NODE_CHILD_META_OFFSET: u64 = 24;

struct ChildrenMeta {
    first_child_pos: u64,
    n_children: u32,
    max_children: u32,
}

struct ChildMap {
    node_pos: u64,
    key: u16,
}

struct ChildrenMaps {
    key_hit: Option<ChildMap>,
    child_maps: Vec<ChildMap>,
}

struct FileData {
    node_file: File,
    map_file: File,
    n_nodes: usize,
}
pub struct TreeMap {
    guarded: Mutex<FileData>,
}

impl TreeMap {
    pub fn new(path: &str, max_top_children: u32, open_mode: OpenMode, file_prefix: Option<u8>) -> Result<TreeMap, TreeFileError> {
        let prefix = if let Some(p) = file_prefix {format!("{:03}.", p)} else {String::new()};
        let node_path = format!("{}/{}treemap.nodes.bin", path, prefix);
        let map_path = format!("{}/{}treemap.map.bin", path, prefix);

        let exists = Path::new(&node_path).is_file() && Path::new(&map_path).is_file();

        let (node_file, map_file) = match open_mode {
            TruncateCreate => (create_file(&node_path)?, create_file(&map_path)?),
            OpenCreate if exists => (open_file(&node_path)?, open_file(&map_path)?),
            OpenCreate => (create_file(&node_path)?, create_file(&map_path)?),
            MustExist if exists => (open_file(&node_path)?, open_file(&map_path)?),
            MustExist => { return Err(NonExistingFiles) },
        };

        let tree = TreeMap {
            guarded: Mutex::new(FileData {
                node_file,
                map_file,
                n_nodes: 0,
            }),
        };

        {
            let mut lock = tree.guarded.lock().unwrap();
            count_nodes(&mut lock)?;
            if lock.n_nodes == 0 {
                add_node(&mut lock, u64::MAX, 0, 0, max_top_children)?;
            }
        }

        Ok(tree)
    }

    pub fn get_top(&self) -> NodeId {
        0
    }

    pub fn len(&self) -> usize {
        let lock = self.guarded.lock().unwrap();
        lock.n_nodes
    }

    pub fn get_node(&self, node: NodeId) -> Result<NodeData, TreeFileError> {
        let mut lock = self.guarded.lock().unwrap();
        check_presence(&mut lock, node)?;

        get_node(&mut lock, node_id_to_pos(node))
    }

    pub fn add_child(&mut self, node: NodeId, key: u16, hits: u64, score: u64, max_children: u32) -> Result<NodeId, TreeFileError> {
        let mut lock = self.guarded.lock().unwrap();
        check_presence(&mut lock, node)?;

        let parent_pos = node_id_to_pos(node);
        let child_pos = expected_node_pos(&mut lock);

        let mut children_meta = get_node_child_meta(&mut lock, parent_pos)?;

        if children_meta.n_children == 0 {
            new_children_child_mappings(&mut lock, parent_pos, key, child_pos, &mut children_meta)?;
        } else {
            update_children_child_mappings(&mut lock, parent_pos, key, child_pos, &mut children_meta)?;
        }

        add_node(&mut lock, parent_pos, hits, score, max_children)?;

        Ok(pos_to_node_id(child_pos))
    }

    pub fn get_child(&self, node: NodeId, key: u16) -> Result<Option<NodeData>, TreeFileError> {
        let mut lock = self.guarded.lock().unwrap();
        check_presence(&mut lock, node)?;

        let parent_pos = node_id_to_pos(node);
        let children_meta = get_node_child_meta(&mut lock, parent_pos)?;
        if children_meta.n_children == 0 {
            return Ok(None);
        }

        let res = get_children_maps(&mut lock, key, &children_meta)?;

        if let Some(c) = res.key_hit {
            Ok(Some(get_node(&mut lock, c.node_pos)?))
        } else {
            Ok(None)
        }
        // match res.get(&key) {
        //     Some(&node_pos) => {
        //         Ok(Some(get_node(&mut lock, node_pos)?))
        //     },
        //     None => {
        //         Ok(None)
        //     }
        // }
    }

    pub fn get_parent(&self, node: NodeId) -> Result<Option<NodeData>, TreeFileError> {
        let mut lock = self.guarded.lock().unwrap();
        check_presence(&mut lock, node)?;

        let node_data = get_node(&mut lock, node_id_to_pos(node))?;
        match node_data.parent {
            Some(node_id) => {
                let parent_pos = node_id_to_pos(node_id);
                Ok(Some(get_node(&mut lock, parent_pos)?))
            },
            None => Ok(None)
        }
    }

    pub fn update_node_add(&self, node: NodeId, hits: i64, score: i64) -> Result<(), TreeFileError> {
        let mut lock = self.guarded.lock().unwrap();
        check_presence(&mut lock, node)?;

        let mut node_data = get_node(&mut lock, node_id_to_pos(node))?;
        node_data.hits = add_and_subtract(node_data.hits, hits)?;
        node_data.score = add_and_subtract(node_data.score, score)?;
        update_node(&mut lock, &node_data)?;

        Ok(())
    }

    pub fn get_child_iter(&self, node: NodeId) -> Iter {
        let mut iter = Iter {
            key_vals: Vec::new(),
        };

        let mut lock = self.guarded.lock().unwrap();
        if let Err(_) = check_presence(&mut lock, node) {
            return iter;
        }

        let node_pos = node_id_to_pos(node);
        let children_meta = get_node_child_meta(&mut lock, node_pos).unwrap();
        iter.key_vals = get_children_vec(&mut lock, &children_meta).unwrap();

        iter
    }
}

impl Drop for TreeMap {
    fn drop(&mut self) {
        let mut lock = self.guarded.lock().unwrap();
        let _ = lock.node_file.flush();
        let _ = lock.map_file.flush();
    }
}

fn count_nodes(lock: &mut MutexGuard<FileData>) -> Result<(), TreeFileError> {
    lock.node_file.sync_all().unwrap();
    let metadata = lock.node_file.metadata().unwrap();
    lock.n_nodes = (metadata.len() / NODE_LENGTH as u64) as usize;

    Ok(())
}

fn new_children_child_mappings(lock: &mut MutexGuard<FileData>, parent_pos: u64, key: u16, child_pos: u64, children_meta: &mut ChildrenMeta) -> Result<(), TreeFileError> {
    if children_meta.max_children == 0 {
        return Err(LogicError {
            msg: String::from("trying to add more children than allowed for parent")
        });
    }

    let new_child_map = ChildMap{
        node_pos: child_pos,
        key,
    };
    children_meta.n_children = 1;
    children_meta.first_child_pos = add_child_map(lock, new_child_map, children_meta.max_children)?;
    update_node_child_meta(lock, parent_pos, &children_meta)?;

    Ok(())
}

fn update_children_child_mappings(lock: &mut MutexGuard<FileData>, parent_pos: u64, key: u16, child_pos: u64, children_meta: &mut ChildrenMeta) -> Result<(), TreeFileError> {
    let mut res = get_children_maps(lock, key, children_meta)?;
    if let Some(_) = res.key_hit {
        return Err(LogicError {
            msg: String::from("key already present, would turn existing child node to a ghost node")
        });
    } else {
        res.child_maps.push(ChildMap{ node_pos: child_pos, key })
    }
    // if let Some(_) = res.insert(key, child_pos) {
    //     return Err(LogicError {
    //         msg: String::from("key already present, would turn existing child node to a ghost node")
    //     });
    // }

    let new_children_len = res.child_maps.len() as u32;
    if new_children_len > children_meta.max_children {
        return Err(LogicError {
            msg: String::from("trying to add more children than allowed for parent")
        });
    }

    update_children_maps(lock, res.child_maps, children_meta)?;

    if new_children_len != children_meta.n_children {
        children_meta.n_children = new_children_len;
        update_node_child_meta(lock, parent_pos, &children_meta)?;
    }

    Ok(())
}

fn get_node(lock: &mut MutexGuard<FileData>, node_pos: u64) -> Result<NodeData, TreeFileError> {
    let mut buf = [0u8;NODE_LENGTH];
    let _ = lock.node_file.seek(SeekFrom::Start(node_pos)).unwrap();
    lock.node_file.read_exact(&mut buf).map_err(|e| FileIOError {
        msg: String::from(format!("while reading from node file: {}", e))
    })?;

    let parent_pos = u64::from_le_bytes(buf[0..8].try_into().unwrap());
    let hits = u64::from_le_bytes(buf[8..16].try_into().unwrap());
    let score = u64::from_le_bytes(buf[16..24].try_into().unwrap());
    let first_child_pos = u64::from_le_bytes(buf[24..32].try_into().unwrap());
    let n_children = u32::from_le_bytes(buf[32..36].try_into().unwrap());
    let max_children = u32::from_le_bytes(buf[36..40].try_into().unwrap());

    Ok(NodeData{
        node_id: pos_to_node_id(node_pos),
        node_pos,
        parent: if parent_pos == u64::MAX {None} else {Some(pos_to_node_id(parent_pos))},
        hits,
        score,
        first_child_pos,
        n_children,
        max_children,
    })
}

fn add_node(lock: &mut MutexGuard<FileData>, parent_pos: u64, hits: u64, score: u64, max_children: u32) -> Result<u64, TreeFileError> {
    let node_pos = lock.node_file.seek(SeekFrom::End(0)).unwrap();
    let node_data = NodeData {
        node_id: 0,
        node_pos,
        parent: None,
        hits,
        score,
        first_child_pos: 0,
        n_children: 0,
        max_children,
    };
    let buf = node_to_buf(parent_pos, &node_data);
    lock.node_file.write_all(&buf).map_err(|e| FileIOError {
        msg: String::from(format!("while writing to node file: {}", e))
    })?;
    lock.n_nodes += 1;

    Ok(node_pos)
}

fn update_node(lock: &mut MutexGuard<FileData>, node_data: &NodeData) -> Result<(), TreeFileError> {
    lock.node_file.seek(SeekFrom::Start(node_data.node_pos)).unwrap();
    let parent_pos = if let Some(p) = node_data.parent {
        node_id_to_pos(p)
    } else {u64::MAX};

    let buf = node_to_buf(parent_pos, node_data);
    lock.node_file.write_all(&buf).map_err(|e| FileIOError {
        msg: String::from(format!("while writing to node file: {}", e))
    })?;

    Ok(())
}

fn expected_node_pos(lock: &mut MutexGuard<FileData>) -> u64 {
    lock.node_file.seek(SeekFrom::End(0)).unwrap()
}

fn get_node_child_meta(lock: &mut MutexGuard<FileData>, node_pos: u64) -> Result<ChildrenMeta, TreeFileError> {
    lock.node_file.seek(SeekFrom::Start(node_pos + NODE_CHILD_META_OFFSET)).unwrap();
    let mut buf = [0u8;NODE_CHILD_META_LENGTH];
    lock.node_file.read_exact(&mut buf).map_err(|e| FileIOError {
        msg: String::from(format!("while reading from node file: {}", e))
    })?;

    Ok(ChildrenMeta{
        first_child_pos: u64::from_le_bytes(buf[0..8].try_into().unwrap()),
        n_children: u32::from_le_bytes(buf[8..12].try_into().unwrap()),
        max_children: u32::from_le_bytes(buf[12..16].try_into().unwrap()),
    })
}

fn update_node_child_meta(lock: &mut MutexGuard<FileData>, node_pos: u64, children_meta: &ChildrenMeta) -> Result<(), TreeFileError> {
    lock.node_file.seek(SeekFrom::Start(node_pos + NODE_CHILD_META_OFFSET)).unwrap();
    let buf = node_children_to_buf(children_meta.first_child_pos, children_meta.n_children, children_meta.max_children);
    lock.node_file.write_all(&buf).map_err(|e| FileIOError {
        msg: String::from(format!("while writing to node file: {}", e))
    })?;

    Ok(())
}

fn get_children_maps(lock: &mut MutexGuard<FileData>, key: u16, children_meta: &ChildrenMeta) -> Result<ChildrenMaps, TreeFileError> {
    lock.map_file.seek(SeekFrom::Start(children_meta.first_child_pos)).unwrap();
    let mut buf = vec![0u8;MAP_LENGTH * children_meta.max_children as usize];
    lock.map_file.read_exact(&mut buf).map_err(|e| FileIOError {
        msg: String::from(format!("while reading from map file: {}", e))
    })?;

    let mut child_no: usize = 0;
    //let mut res: HashMap<u16, u64> = HashMap::new();
    let mut children_maps = ChildrenMaps { key_hit: None, child_maps: Vec::new() };
    while child_no < children_meta.n_children as usize {
        let offset = MAP_LENGTH * child_no;
        let node_pos = u64::from_le_bytes(buf[0+offset..8+offset].try_into().unwrap());
        let child_key = u16::from_le_bytes(buf[8+offset..10+offset].try_into().unwrap());
        if child_key == key {
            children_maps.key_hit = Some(ChildMap{ node_pos, key });
        }
        children_maps.child_maps.push(ChildMap{ node_pos, key: child_key });
        //res.insert(key, node_pos);
        child_no += 1;
    }

    Ok(children_maps)
}

fn get_children_vec(lock: &mut MutexGuard<FileData>, children_meta: &ChildrenMeta) -> Result<Vec<(u16, NodeId)>, TreeFileError> {
    lock.map_file.seek(SeekFrom::Start(children_meta.first_child_pos)).unwrap();
    let mut buf = vec![0u8;MAP_LENGTH * children_meta.max_children as usize];
    lock.map_file.read_exact(&mut buf).map_err(|e| FileIOError {
        msg: String::from(format!("while reading from map file: {}", e))
    })?;

    let mut child_no: usize = 0;
    let mut res: Vec<(u16, NodeId)> = Vec::new();
    while child_no < children_meta.n_children as usize {
        let offset = MAP_LENGTH * child_no;
        let node_pos = u64::from_le_bytes(buf[0+offset..8+offset].try_into().unwrap());
        let key = u16::from_le_bytes(buf[8+offset..10+offset].try_into().unwrap());
        res.push((key, pos_to_node_id(node_pos)));
        child_no += 1;
    }

    Ok(res)
}

fn update_children_maps(lock: &mut MutexGuard<FileData>, children_maps: Vec<ChildMap>, children_meta: &ChildrenMeta) -> Result<(), TreeFileError> {
    lock.map_file.seek(SeekFrom::Start(children_meta.first_child_pos)).unwrap();
    let buf = children_to_buf(children_maps, children_meta.max_children);
    lock.map_file.write_all(&buf).map_err(|e| FileIOError {
        msg: String::from(format!("while writing to map file: {}", e))
    })?;

    Ok(())
}

fn add_child_map(lock: &mut MutexGuard<FileData>, child_map: ChildMap, max_children: u32) -> Result<u64, TreeFileError> {
    let buf = children_to_buf(Vec::from([child_map]),max_children);
    let children_pos = lock.map_file.seek(SeekFrom::End(0)).unwrap();
    lock.map_file.write_all(&buf).map_err(|e| FileIOError {
         msg: String::from(format!("while writing to map file: {}", e))
    })?;

    Ok(children_pos)
}

fn check_presence(lock: &mut MutexGuard<FileData>, node: NodeId) -> Result<(), TreeFileError> {
    if node >= lock.n_nodes {
        Err(NonExistingNode)
    } else {
        Ok(())
    }
}

fn pos_to_node_id(pos: u64) -> NodeId {
    (pos / NODE_LENGTH as u64) as NodeId
}

fn node_id_to_pos(node_id: NodeId) -> u64 {
    node_id as u64 * NODE_LENGTH as u64
}

fn node_to_buf(parent_pos: u64, node_data: &NodeData) -> [u8;NODE_LENGTH] {
    // |parent 8 |hits 8|score 8|children pos 8|children_len 4|max_children 4|
    let mut buf = [0u8;NODE_LENGTH];
    let mut offset: usize = 0;

    parent_pos.to_le_bytes().iter().for_each(|v| {
        buf[offset] = *v;
        offset += 1;
    });
    node_data.hits.to_le_bytes().iter().for_each(|v| {
        buf[offset] = *v;
        offset += 1;
    });
    node_data.score.to_le_bytes().iter().for_each(|v| {
        buf[offset] = *v;
        offset += 1;
    });
    node_data.first_child_pos.to_le_bytes().iter().for_each(|v| {
        buf[offset] = *v;
        offset += 1;
    });
    node_data.n_children.to_le_bytes().iter().for_each(|v| {
        buf[offset] = *v;
        offset += 1;
    });
    node_data.max_children.to_le_bytes().iter().for_each(|v| {
        buf[offset] = *v;
        offset += 1;
    });

    buf
}

fn children_to_buf(children: Vec<ChildMap>, max_children: u32) -> Vec<u8> {
    // |node 8|key 2| * max_children
    let mut buf = vec![255u8;MAP_LENGTH * max_children as usize];
    let mut offset: usize = 0;

    for child in children {
        child.node_pos.to_le_bytes().iter().for_each(|v| {
            buf[offset] = *v;
            offset += 1;
        });
        child.key.to_le_bytes().iter().for_each(|v| {
            buf[offset] = *v;
            offset += 1;
        });
    }

    buf
}

fn node_children_to_buf(children_pos: u64, children_len: u32, children_max: u32) -> [u8;NODE_CHILD_META_LENGTH] {
    // |children 8 |children_len 4|max_children 4|
    let mut buf = [0u8;NODE_CHILD_META_LENGTH];
    let mut offset: usize = 0;

    children_pos.to_le_bytes().iter().for_each(|v| {
        buf[offset] = *v;
        offset += 1;
    });
    children_len.to_le_bytes().iter().for_each(|v| {
        buf[offset] = *v;
        offset += 1;
    });
    children_max.to_le_bytes().iter().for_each(|v| {
        buf[offset] = *v;
        offset += 1;
    });

    buf
}
