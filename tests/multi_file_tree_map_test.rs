use std::collections::HashMap;
use std::fs::{read_dir, remove_file};
use rust_tree_map::multi_file_tree_map::MultiFileTreeMap;
use rust_tree_map::NodeId;
use rust_tree_map::OpenMode::{TruncateCreate, OpenCreate, MustExist};

const MAP_PATH: &str = "tests/test_data";

fn remove_files<F>(tree_map: MultiFileTreeMap<F>)
    where F: Fn(u16) -> u8
{
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
fn create_a_new_tree() {
    let splitter: fn(u16) -> u8 = |k| {(k >> 8) as u8};
    //let key1 = ((10 << 8) + 1) as u16;

    let mut res = MultiFileTreeMap::new(MAP_PATH, 2, TruncateCreate, splitter);
    assert!(res.is_ok(), "tree not created");

    if let Ok(ref mut t) = res {
        let top = t.get_top();
        assert_eq!(top, 0, "top node shall always have node id 0 (zero)");

        let node = t.get_node(top);
        assert!(node.is_ok(), "could not get node");

        if let Ok(nd) = node {
            assert_eq!(nd.node_id, 0, "should have node id 0");
            assert_eq!(nd.hits, 0, "should have 0 hits");
            assert_eq!(nd.score, 0, "should have score 0");
            assert!(nd.parent.is_none(), "got parent for top node");
            assert_eq!(nd.max_children, 2, "should have max children 2");
            assert_eq!(nd.n_children, 0, "should have 0 children")
        }
    }

    remove_files(res.unwrap());
}

#[test]
fn open_existing_tree() {
    let splitter: fn(u16) -> u8 = |k| {(k >> 8) as u8};

    let res = MultiFileTreeMap::new(MAP_PATH, 2, TruncateCreate, splitter);
    assert!(res.is_ok(), "tree not created");

    drop(res.unwrap());

    let mut res = MultiFileTreeMap::new(MAP_PATH, 10, OpenCreate, splitter);
    assert!(res.is_ok(), "tree not created");

    if let Ok(ref mut t) = res {
        let node = t.get_node(t.get_top());
        assert!(node.is_ok(), "could not get node");

        if let Ok(nd) = node {
            assert_eq!(nd.node_id, 0, "should have node id 0");
            assert_eq!(nd.hits, 0, "should have 0 hits");
            assert_eq!(nd.score, 0, "should have score 0");
            assert!(nd.parent.is_none(), "got parent for top node");
            assert_eq!(nd.max_children, 2, "should have max children 2");
            assert_eq!(nd.n_children, 0, "should have 0 children")
        }
    }

    drop(res.unwrap());

    let mut res = MultiFileTreeMap::new(MAP_PATH, 10, MustExist, splitter);
    assert!(res.is_ok(), "tree not created");

    if let Ok(ref mut t) = res {
        let node = t.get_node(t.get_top());
        assert!(node.is_ok(), "could not get node");

        if let Ok(nd) = node {
            assert_eq!(nd.node_id, 0, "should have node id 0");
            assert_eq!(nd.hits, 0, "should have 0 hits");
            assert_eq!(nd.score, 0, "should have score 0");
            assert!(nd.parent.is_none(), "got parent for top node");
            assert_eq!(nd.max_children, 2, "should have max children 2");
            assert_eq!(nd.n_children, 0, "should have 0 children")
        }
    }

    remove_files(res.unwrap());

    let res = MultiFileTreeMap::new(MAP_PATH, 10, MustExist, splitter);
    assert!(res.is_err(), "tree created");

}

#[test]
fn can_add_children() {
    let splitter: fn(u16) -> u8 = |k| {(k >> 8) as u8};
    let key1 = ((10 << 8) + 1) as u16;
    let key2 = ((15 << 8) + 1) as u16;
    let key3 = ((20 << 8) + 1) as u16;
    let key4 = ((30 << 8) + 1) as u16;
    let key5 = ((40 << 8) + 1) as u16;
    let key6 = ((50 << 8) + 1) as u16;

    let mut res = MultiFileTreeMap::new(MAP_PATH, 2, TruncateCreate, splitter);
    assert!(res.is_ok(), "tree not created");

    if let Ok(ref mut t) = res {
        let child1 = t.add_child(t.get_top(), key1, 100, 1000, 2).unwrap();
        assert_eq!(child1, 266, "first child shall get node id 266");
        // 266 is local node id 1 shifted left by 8 plus selector 10 from key1

        let child2 = t.add_child(t.get_top(), key2, 200, 2000, 2).unwrap();
        assert_eq!(child2, 271, "second child shall get node id 271");
        // 271 is local node id 1 shifted left by 8 plus selector 15 from key2

        let child3 = t.add_child(t.get_top(), key3, 300, 3000, 2);
        assert!(child3.is_err(), "third child shall fail");

        let child21 = t.add_child(child2, key4, 200, 2000, 2).unwrap();
        assert_eq!(child21, 527, "first sub child shall get node id 527");
        // 527 is local node id 2 shifted left by 8 plus selector 15 (which comes from selector part from child2)

        let child22 = t.add_child(child2, key5, 200, 2000, 2).unwrap();
        assert_eq!(child22, 783, "second sub child shall get node id 783");
        // 783 is local node id 3 shifted left by 8 plus selector 15 (which comes from selector part from child2)

        let child23 = t.add_child(child2, key6, 200, 2000, 2);
        assert!(child23.is_err(), "third sub child shall fail");
    }

    remove_files(res.unwrap());
}

#[test]
fn can_get_children() {
    let splitter: fn(u16) -> u8 = |k| {(k >> 8) as u8};
    let key1 = ((10 << 8) + 1) as u16;
    let key2 = ((15 << 8) + 1) as u16;
    let key3 = ((20 << 8) + 1) as u16;

    let mut res = MultiFileTreeMap::new(MAP_PATH, 3, TruncateCreate, splitter);
    assert!(res.is_ok(), "tree not created");

    if let Ok(ref mut t) = res {
        let _child1 = t.add_child(t.get_top(), key1, 100, 1000, 2).unwrap();
        let _child2 = t.add_child(t.get_top(), key2, 200, 2000, 2).unwrap();
        let _child3 = t.add_child(t.get_top(), key3, 300, 3000, 2).unwrap();

        let res = t.get_child(t.get_top(), key1);
        assert!(res.is_ok(), "could not get child node");

        if let Ok(no) = res {
            assert!(no.is_some(), "could not get child via key");

            if let Some(n) = no {
                assert_eq!(n.node_id, 266, "first child shall have node id 266");
                assert_eq!(n.hits, 100, "should have 100 hits");
                assert_eq!(n.score, 1000, "should have score 1000");
                assert!(n.parent.is_some(), "got no parent for child");
                if let Some(p) = n.parent {
                    assert_eq!(p, t.get_top(), "should have node id 0 as parent");
                }
            }
        }

        let mut comp: HashMap<(u16, NodeId), ()> = HashMap::new();
        comp.insert((key1, 266), ());
        comp.insert((key2, 271), ());
        comp.insert((key3, 276), ());

        for child in t.get_child_iter(t.get_top()) {
            let cr = comp.remove(&child);
            assert!(cr.is_some(), "item from iterator not in accordance with child");
        }
        assert_eq!(comp.len(), 0, "iterator should have returned all children");
    }

    remove_files(res.unwrap());
}

#[test]
fn can_get_none_for_get_child_with_no_file() {
    let splitter: fn(u16) -> u8 = |k| {(k >> 8) as u8};
    let key1 = ((10 << 8) + 1) as u16;
    let key2 = ((15 << 8) + 1) as u16;

    let mut res = MultiFileTreeMap::new(MAP_PATH, 2, TruncateCreate, splitter);
    assert!(res.is_ok(), "tree not created");

    if let Ok(ref mut t) = res {
        let child1 = t.add_child(t.get_top(), key1, 100, 1000, 2).unwrap();
        assert_eq!(child1, 266, "first child shall get node id 266");
        // 266 is local node id 1 shifted left by 8 plus selector 10 from key1

        let child2_nd = t.get_child(t.get_top(), key2);
        assert!(child2_nd.is_ok(), "should not return error");

        if let Some(_nd) = child2_nd.unwrap() {
            assert!(false, "should not return data");
        }

        let child2 = t.add_child(t.get_top(), key2, 200, 2000, 2).unwrap();
        assert_eq!(child2, 271, "second child shall get node id 271");
        // 271 is local node id 1 shifted left by 8 plus selector 15 from key2

        let child2_nd = t.get_child(t.get_top(), key2);
        assert!(res.is_ok(), "should not return error");

        if let None = child2_nd.unwrap() {
            assert!(false, "should not return none");
        }
    }

    remove_files(res.unwrap());
}

#[test]
fn can_get_node() {
    let splitter: fn(u16) -> u8 = |k| {(k >> 8) as u8};
    let key1 = ((10 << 8) + 1) as u16;
    let key2 = ((15 << 8) + 1) as u16;
    let key3 = ((20 << 8) + 1) as u16;

    let mut res = MultiFileTreeMap::new(MAP_PATH, 3, TruncateCreate, splitter);
    assert!(res.is_ok(), "tree not created");

    if let Ok(ref mut t) = res {
        let _child1 = t.add_child(t.get_top(), key1, 100, 1000, 2).unwrap();
        let _child2 = t.add_child(t.get_top(), key2, 200, 2000, 2).unwrap();
        let child3 = t.add_child(t.get_top(), key3, 300, 3000, 2).unwrap();

        let res = t.get_node(child3);
        assert!(res.is_ok(), "could not get node");

        if let Ok(nd) = res {
            assert_eq!(nd.node_id, 276, "should have node id 276");
            assert_eq!(nd.hits, 300, "should have 300 hits");
            assert_eq!(nd.score, 3000, "should have score 3000");
            assert!(nd.parent.is_some(), "got no parent for child");
            if let Some(p) = nd.parent {
                assert_eq!(p, t.get_top(), "should have node id 0 as parent");
            }
        }
    }

    remove_files(res.unwrap());
}

#[test]
fn can_get_parent() {
    let splitter: fn(u16) -> u8 = |k| {(k >> 8) as u8};
    let key1 = ((10 << 8) + 1) as u16;
    let key2 = ((15 << 8) + 1) as u16;
    let key3 = ((20 << 8) + 1) as u16;

    let mut res = MultiFileTreeMap::new(MAP_PATH, 3, TruncateCreate, splitter);
    assert!(res.is_ok(), "tree not created");

    if let Ok(ref mut t) = res {
        let _child1 = t.add_child(t.get_top(), key1, 100, 1000, 2).unwrap();
        let _child2 = t.add_child(t.get_top(), key2, 200, 2000, 2).unwrap();
        let child3 = t.add_child(t.get_top(), key3, 300, 3000, 2).unwrap();

        let res = t.get_parent(child3);
        assert!(res.is_ok(), "could not get parent");

        if let Ok(no) = res {
            assert!(no.is_some(), "no parent found");

            if let Some(n) = no {
                assert_eq!(n.node_id, 0, "should have parent node id 0 (zero)");
                assert_eq!(n.hits, 0, "should have 0 (zero) hits");
                assert_eq!(n.score, 0, "should have score 0 (zero)");
                assert!(n.parent.is_none(), "got parent for parent");
            }
        }
    }

    remove_files(res.unwrap());
}

#[test]
fn can_update_add_node() {
    let splitter: fn(u16) -> u8 = |k| {(k >> 8) as u8};
    let key1 = ((10 << 8) + 1) as u16;
    let key2 = ((15 << 8) + 1) as u16;
    let key3 = ((20 << 8) + 1) as u16;

    let mut res = MultiFileTreeMap::new(MAP_PATH, 3, TruncateCreate, splitter);
    assert!(res.is_ok(), "tree not created");

    if let Ok(ref mut t) = res {
        let _child1 = t.add_child(t.get_top(), key1, 100, 1000, 2).unwrap();
        let _child2 = t.add_child(t.get_top(), key2, 200, 2000, 2).unwrap();
        let child3 = t.add_child(t.get_top(), key3, 300, 3000, 2).unwrap();

        let res = t.update_node_add(child3, 30, 300);
        assert!(res.is_ok(), "could not update node");

        let res = t.get_node(child3);
        assert!(res.is_ok(), "could not get updated node");

        if let Ok(nd) = res {
            assert_eq!(nd.hits, 330, "should have 300 hits");
            assert_eq!(nd.score, 3300, "should have score 3000");
        }

        let res = t.update_node_add(t.get_top(), 50, 500);
        assert!(res.is_ok(), "could not update top node");

        let res = t.get_node(t.get_top());
        assert!(res.is_ok(), "could not get updated top node");

        if let Ok(nd) = res {
            assert_eq!(nd.hits, 50, "should have 50 hits");
            assert_eq!(nd.score, 500, "should have score 500");
        }

    }

    remove_files(res.unwrap());
}