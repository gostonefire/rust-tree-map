use std::collections::HashMap;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::{Mutex, MutexGuard};
use crate::{Iter, NodeData, NodeId, OpenMode, TreeFileError};
use crate::TreeFileError::{FileIOError, LogicError, NonExistingFiles};
use crate::OpenMode::{TruncateCreate, OpenCreate, MustExist};
use crate::tree_map::TreeMap;
use crate::utils::{add_and_subtract, create_file, open_file};

const MASTER_MIN_LENGTH: usize = 24;

struct MasterData {
    path: String,
    master_file: File,
    trees: HashMap<u8, TreeMap>,
    max_top_children: u32,
    hits: u64,
    score: u64,
}

pub struct MultiFileTreeMap<F> 
    where F: Fn(u16) -> u8
{
    guarded: Mutex<MasterData>,
    splitter: F,
    open_mode: OpenMode,
}

impl<F> MultiFileTreeMap<F>
    where F: Fn(u16) -> u8
{
    pub fn new(path: &str, max_top_children: u32, open_mode: OpenMode, splitter: F) -> Result<MultiFileTreeMap<F>, TreeFileError> {
        let file_path = format!("{}/multifile_treemap.bin", path);

        let exists = Path::new(&file_path).is_file();

        let master_file = match open_mode.clone() {
            TruncateCreate => create_file(&file_path)?,
            OpenCreate if exists => open_file(&file_path)?,
            OpenCreate => create_file(&file_path)?,
            MustExist if exists => open_file(&file_path)?,
            MustExist => { return Err(NonExistingFiles); },
        };

        let tree = MultiFileTreeMap {
            guarded: Mutex::new(MasterData {
                path: String::from(path),
                master_file,
                trees: HashMap::new(),
                max_top_children,
                hits: 0,
                score: 0,
            }),
            splitter,
            open_mode: open_mode.clone(),
        };

        {
            let mut lock = tree.guarded.lock().unwrap();
            load_master_data(&mut lock, open_mode)?;
            save_master_data(&mut lock)?;
        }


        Ok(tree)
    }

    pub fn get_top(&self) -> NodeId {
        0
    }

    pub fn len(&self) -> usize {
        let lock = self.guarded.lock().unwrap();
        let len = lock.trees.values().map(|t| t.len()).sum::<usize>();
        len + 1
    }

    pub fn get_node(&mut self, node: NodeId) -> Result<NodeData, TreeFileError> {
        let mut lock = self.guarded.lock().unwrap();

        if node == self.get_top() {
            return Ok(self.get_top_node_data(&mut lock));
        }

        let tree_selector = self.get_selector(node, None)?;

        manage_trees_and_execute(&mut lock, tree_selector, MustExist, |t| {
            t.get_node(node_from_selector_node(node))
        }).map(|mut n| {
            n.node_id = selector_node_from_node(n.node_id, tree_selector);
            n
        })
    }

    pub fn add_child(&mut self, node: NodeId, key: u16, hits: u64, score: u64, max_children: u32) -> Result<NodeId, TreeFileError> {
        let tree_selector = self.get_selector(node, Some(key))?;
        let mut lock = self.guarded.lock().unwrap();

        manage_trees_and_execute(&mut lock, tree_selector, self.open_mode.clone(), |t| {
            t.add_child(node_from_selector_node(node), key, hits, score, max_children)
        }).map(|n| selector_node_from_node(n, tree_selector))
    }

    pub fn get_child(&mut self, node: NodeId, key: u16) -> Result<Option<NodeData>, TreeFileError> {
        let tree_selector = self.get_selector(node, Some(key))?;
        let mut lock = self.guarded.lock().unwrap();

        manage_trees_and_execute(&mut lock, tree_selector, MustExist, |t| {
            t.get_child(node_from_selector_node(node), key)
        }).map_or_else(|e| match e {
            NonExistingFiles => Ok(None),
            _ => Err(e),
        }, |n| {
            Ok(n.map(|mut nd| {
                nd.node_id = selector_node_from_node(nd.node_id, tree_selector);
                nd
            }))
        })

        //     .map_or_else(|e| match e {
        //     NonExistingFiles => Ok(None),
        //     _ => Err(e),
        // }, |v| Ok(v))

    }

    pub fn get_parent(&mut self, node: NodeId) -> Result<Option<NodeData>, TreeFileError> {
        if node == self.get_top() {
            return Ok(None);
        }

        let tree_selector = self.get_selector(node, None)?;
        let mut lock = self.guarded.lock().unwrap();

        manage_trees_and_execute(&mut lock, tree_selector, MustExist, |t| {
            t.get_parent(node_from_selector_node(node))
        }).map(|n| {
            n.map(|mut nd| {
                if nd.node_id == self.get_top() {
                    self.get_top_node_data(&mut lock)
                } else {
                    nd.node_id = selector_node_from_node(nd.node_id, tree_selector);
                    nd
                }
            })
        })
    }

    pub fn update_node_add(&mut self, node: NodeId, hits: i64, score: i64) -> Result<(), TreeFileError> {
        let mut lock = self.guarded.lock().unwrap();

        if node == self.get_top() {
            lock.hits = add_and_subtract(lock.hits, hits)?;
            lock.score = add_and_subtract(lock.score, score)?;
            return save_master_data(&mut lock);
        }

        let tree_selector = self.get_selector(node, None)?;

        manage_trees_and_execute(&mut lock, tree_selector, MustExist, |t| {
            t.update_node_add(node_from_selector_node(node), hits, score)
        })
    }

    pub fn get_child_iter(&mut self, node: NodeId) -> Iter {
        let mut lock = self.guarded.lock().unwrap();

        let mut iter = Iter {
            key_vals: Vec::new(),
        };

        if node == self.get_top() {
            lock.trees.values()
                .for_each(|t| {
                t.get_child_iter(t.get_top()).for_each(|t| {
                    let tree_selector = self.get_selector(node, Some(t.0)).unwrap();
                    let node = selector_node_from_node(t.1, tree_selector);
                    iter.key_vals.push((t.0, node));
                })
            });

            return iter;
        }

        let tree_selector = self.get_selector(node, None).unwrap();

        manage_trees_and_execute(&mut lock, tree_selector, MustExist, |t| {
            Ok(t.get_child_iter(node_from_selector_node(node)))
        }).expect("non existing tree files for the child iterator")
    }

    fn get_selector(&self, node: NodeId, key: Option<u16>) -> Result<u8, TreeFileError> {
        match key {
            Some(k) if node == self.get_top() => {
                Ok((self.splitter)(k))
            },
            Some(_) => {
                Ok(selector_from_selector_node(node))
            }
            None if node != self.get_top() => {
                Ok(selector_from_selector_node(node))
            },
            None => {
                Err(LogicError {msg: String::from("Top node given, but no key to select files from")})
            }
        }
    }

    fn get_top_node_data(&self, lock: &mut MutexGuard<MasterData>) -> NodeData {
        NodeData {
            node_id: self.get_top(),
            node_pos: 0,
            parent: None,
            hits: lock.hits,
            score: lock.score,
            first_child_pos: 0,
            n_children: lock.trees.len() as u32,
            max_children: lock.max_top_children,
        }
    }
}

fn manage_trees_and_execute<F, T>(lock: &mut MutexGuard<MasterData>, tree_selector: u8, open_mode: OpenMode, func: F) -> Result<T, TreeFileError>
    where F: Fn(&mut TreeMap) -> Result<T, TreeFileError>
{
    loop {
        match lock.trees.get_mut(&tree_selector) {
            Some(tree) => {
                return (func)(tree);
            },
            None => {
                add_tree(lock, tree_selector, open_mode.clone())?;
            }
        }
    }
}

fn add_tree(lock: &mut MutexGuard<MasterData>, tree_selector: u8, open_mode: OpenMode) -> Result<(), TreeFileError> {

    if lock.trees.len() >= lock.max_top_children as usize {
        return Err(LogicError {msg: String::from("Error, trying to add more children than allowed for parent") });
    }

    let tree = TreeMap::new(&lock.path, lock.max_top_children, open_mode, Some(tree_selector))?;
    let _ = &lock.trees.insert(tree_selector, tree);

    save_master_data(lock)
}

fn load_master_data(lock: &mut MutexGuard<MasterData>, open_mode: OpenMode) -> Result<(), TreeFileError> {
    lock.master_file.seek(SeekFrom::Start(0)).unwrap();
    let mut buf: Vec<u8> = Vec::new();
    lock.master_file.read_to_end(&mut buf).map_err(|e| FileIOError {
        msg: String::from(format!("Error while reading from master file: {}", e))
    })?;

    match open_mode {
        MustExist if buf.len() < MASTER_MIN_LENGTH => {
            return Err(LogicError {msg: String::from("Error, no master data in master file")});
        },
        _ => {
            if buf.len() >= MASTER_MIN_LENGTH {
                lock.max_top_children = u32::from_le_bytes(buf[0..4].try_into().unwrap());
                let n_children = u32::from_le_bytes(buf[4..8].try_into().unwrap());
                lock.hits = u64::from_le_bytes(buf[8..16].try_into().unwrap());
                lock.score = u64::from_le_bytes(buf[16..24].try_into().unwrap());

                if buf.len() < (n_children as usize + MASTER_MIN_LENGTH) {
                    return Err(LogicError {msg: String::from("Error, to few trees in master file")});
                }

                for offset in 0..n_children as usize {
                    let tree_selector = buf[MASTER_MIN_LENGTH+offset];
                    let tree = TreeMap::new(&lock.path, 0, open_mode.clone(), Some(tree_selector))?;
                    let _ = lock.trees.insert(tree_selector, tree);
                }
            }
        }
    }

    Ok(())
}

fn save_master_data(lock: &mut MutexGuard<MasterData>) -> Result<(), TreeFileError> {
    let mut buf: Vec<u8> = Vec::new();
    lock.max_top_children.to_le_bytes().iter().for_each(|v| buf.push(*v));
    (lock.trees.len() as u32).to_le_bytes().iter().for_each(|v| buf.push(*v));
    lock.hits.to_le_bytes().iter().for_each(|v| buf.push(*v));
    lock.score.to_le_bytes().iter().for_each(|v| buf.push(*v));

    lock.trees.keys().for_each(|v| buf.push(*v));

    lock.master_file.seek(SeekFrom::Start(0)).unwrap();
    lock.master_file.write_all(&buf).map_err(|e| FileIOError {
        msg: String::from(format!("Error while writing to master file: {}", e))
    })?;

    Ok(())
}

fn selector_from_selector_node(node: NodeId) -> u8 {
    (node & 0b11111111) as u8
}

fn selector_node_from_node(node: NodeId, selector: u8) -> NodeId {
    (node << 8) + selector as NodeId
}

fn node_from_selector_node(node: NodeId) -> NodeId {
    node >> 8
}