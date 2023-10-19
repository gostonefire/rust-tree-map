use std::collections::HashMap;
use std::fs::{read_dir, remove_file};
use rust_tree_map;
use rust_tree_map::NodeId;
use rust_tree_map::OpenMode::{MustExist, OpenCreate, TruncateCreate};
use rust_tree_map::tree_map::TreeMap;

const MAP_PATH: &str = "tests/test_data";

fn remove_files(tree_map: TreeMap) {
    drop(tree_map);

    for entry in read_dir(MAP_PATH).unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();
        if path.is_file() {
            remove_file(path).unwrap();
        }
    }
}

#[test]
fn creates_a_new_tree() {
    let res = TreeMap::new(MAP_PATH, 2, TruncateCreate, None);
    assert!(res.is_ok(), "tree not created");

    if let Ok(ref t) = res {
        assert_eq!(t.len(), 1, "it shall always have length of 1 from start, got {}", t.len());
        assert_eq!(t.get_top(), 0, "top node shall always have node id 0 (zero)");
    }

    remove_files(res.unwrap());

    let res = TreeMap::new(MAP_PATH, 2, OpenCreate, None);
    assert!(res.is_ok(), "tree not created");

    remove_files(res.unwrap());

    let res = TreeMap::new(MAP_PATH, 2, MustExist, None);
    assert!(res.is_err(), "tree created");

}

#[test]
fn can_add_children() {
    let mut res = TreeMap::new(MAP_PATH, 2, TruncateCreate, None);
    assert!(res.is_ok(), "tree not created");

    if let Ok(ref mut t) = res {
        let child1 = t.add_child(t.get_top(), 10, 100, 1000, 2).unwrap();
        assert_eq!(child1, 1, "first child shall get node id 1, got {}", child1);

        let child2 = t.add_child(t.get_top(), 15, 200, 2000, 2).unwrap();
        assert_eq!(child2, 2, "second child shall get node id 2, got {}", child2);

        let child3 = t.add_child(t.get_top(), 20, 300, 3000, 2);
        assert!(child3.is_err(), "third child shall fail");
    }

    remove_files(res.unwrap());
}

#[test]
fn can_get_children() {
    let mut res = TreeMap::new(MAP_PATH, 3, TruncateCreate, None);
    assert!(res.is_ok(), "tree not created");

    if let Ok(ref mut t) = res {
        let _child1 = t.add_child(t.get_top(), 10, 100, 1000, 2).unwrap();
        let _child2 = t.add_child(t.get_top(), 15, 200, 2000, 2).unwrap();
        let _child3 = t.add_child(t.get_top(), 20, 300, 3000, 2).unwrap();

        let res = t.get_child(t.get_top(), 10);
        assert!(res.is_ok(), "could not get child node");

        if let Ok(no) = res {
            assert!(no.is_some(), "could not get child via key");

            if let Some(n) = no {
                assert_eq!(n.node_id, 1, "first child shall have node id 1, got {}", n.node_id);
                assert_eq!(n.hits, 100, "should have 100 hits, got {}", n.hits);
                assert_eq!(n.score, 1000, "should have score 1000, got {}", n.score);
                assert!(n.parent.is_some(), "got no parent for child");
                if let Some(p) = n.parent {
                    assert_eq!(p, t.get_top(), "should have node id 0 as parent, got {}", p);
                }
            }
        }

        let mut comp: HashMap<(u16, NodeId), ()> = HashMap::new();
        comp.insert((10, 1), ());
        comp.insert((15, 2), ());
        comp.insert((20, 3), ());

        for child in t.get_child_iter(t.get_top()) {
            let cr = comp.remove(&child);
            assert!(cr.is_some(), "item from iterator not in accordance with child");
        }
        assert_eq!(comp.len(), 0, "iterator should have returned all children, but omitted {}", comp.len());
    }

    remove_files(res.unwrap());
}

#[test]
fn can_get_node() {
    let mut res = TreeMap::new(MAP_PATH, 3, TruncateCreate, None);
    assert!(res.is_ok(), "tree not created");

    if let Ok(ref mut t) = res {
        let _child1 = t.add_child(t.get_top(), 10, 100, 1000, 2).unwrap();
        let _child2 = t.add_child(t.get_top(), 15, 200, 2000, 2).unwrap();
        let child3 = t.add_child(t.get_top(), 20, 300, 3000, 2).unwrap();

        let res = t.get_node(child3);
        assert!(res.is_ok(), "could not get node");

        if let Ok(nd) = res {
            assert_eq!(nd.node_id, 3, "should have node id 3, got {}", nd.node_id);
            assert_eq!(nd.hits, 300, "should have 300 hits, got {}", nd.hits);
            assert_eq!(nd.score, 3000, "should have score 3000, got {}", nd.score);
            assert!(nd.parent.is_some(), "got no parent for child");
            if let Some(p) = nd.parent {
                assert_eq!(p, t.get_top(), "should have node id 0 as parent, got {}", p);
            }
        }
    }

    remove_files(res.unwrap());
}

#[test]
fn can_get_parent() {
    let mut res = TreeMap::new(MAP_PATH, 3, TruncateCreate, None);
    assert!(res.is_ok(), "tree not created");

    if let Ok(ref mut t) = res {
        let _child1 = t.add_child(t.get_top(), 10, 100, 1000, 2).unwrap();
        let _child2 = t.add_child(t.get_top(), 15, 200, 2000, 2).unwrap();
        let child3 = t.add_child(t.get_top(), 20, 300, 3000, 2).unwrap();

        let res = t.get_parent(child3);
        assert!(res.is_ok(), "could not get parent");

        if let Ok(no) = res {
            assert!(no.is_some(), "no parent found");

            if let Some(n) = no {
                assert_eq!(n.node_id, 0, "should have parent node id 0 (zero), got {}", n.node_id);
                assert_eq!(n.hits, 0, "should have 0 (zero) hits, got {}", n.hits);
                assert_eq!(n.score, 0, "should have score 0 (zero), got {}", n.score);
                assert!(n.parent.is_none(), "got parent for parent");
            }
        }
    }

    remove_files(res.unwrap());
}

#[test]
fn can_update_add_node() {
    let mut res = TreeMap::new(MAP_PATH, 3, TruncateCreate, None);
    assert!(res.is_ok(), "tree not created");

    if let Ok(ref mut t) = res {
        let _child1 = t.add_child(t.get_top(), 10, 100, 1000, 2).unwrap();
        let _child2 = t.add_child(t.get_top(), 15, 200, 2000, 2).unwrap();
        let child3 = t.add_child(t.get_top(), 20, 300, 3000, 2).unwrap();

        let res = t.update_node_add(child3, 30, 300);
        assert!(res.is_ok(), "could not update node");

        let res = t.get_node(child3);
        assert!(res.is_ok(), "could not get updated node");

        if let Ok(nd) = res {
            assert_eq!(nd.hits, 330, "should have 300 hits, got {}", nd.hits);
            assert_eq!(nd.score, 3300, "should have score 3000, got {}", nd.score);
        }
    }

    remove_files(res.unwrap());
}