use anyhow::Result;
use bonsai_db::{cursor::Cursor, Database, Options};

fn options() -> Options {
    Options {
        max_files: 10,
        page_size: 1 << 7,
        cache_size: 1 << 30,
    }
}

fn setup_test_for_cursor() -> Result<()> {
    let path = "./my.db";
    let res = std::fs::remove_file(path);
    println!("Remove file: {res:?}");
    let _db = Database::open(path, options())?;
    Ok(())
}

fn create_test_database() -> Result<Database> {
    Database::open("./my.db", options())
}

fn run_basic_cursor_test() -> Result<()> {
    println!("\nrun_basic_cursor_test\n");
    let db = create_test_database()?;
    let mut tx = db.begin_write();
    tx.traverse();
    Ok(())
}

fn run_basic_cursor_reverse_test() -> Result<()> {
    println!("\nrun_basic_cursor_reverse_test\n");
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
        if !cursor.prev_entry()? {
            break;
        }
    }
    Ok(())
}

fn display_cursor(cursor: &mut Cursor<'_>) -> Result<()> {
    let mut len = 0;
    while cursor.is_valid() {
        let key_len = String::from_utf8_lossy(cursor.key()).len();
        let val_len = String::from_utf8_lossy(cursor.value()).len();
        len += key_len + val_len;
        println!(
            "{:?} = {:?}",
            String::from_utf8_lossy(cursor.key()),
            String::from_utf8_lossy(cursor.value()),
        );
        cursor.next_entry()?;
    }
    println!("READ TOTAL LEN: {:?}", len);
    Ok(())
}

fn run_cursor_seek() -> Result<()> {
    println!("\nrun_cursor_seek\n");
    let seeks = [
        "key0", "key10", "key11", "key12", "key13", "key2", "key20", "key21", "key22", "key23",
        "key3", "key30", "key31", "key32", "key33", "key4",
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

    println!("\n==============================\n");
    tx.traverse();
    tx.merge()?;
    println!("\n====== Merge\n");
    tx.traverse();
    tx.split()?;
    println!("\n====== Split\n");
    tx.traverse();

    for i in 0..30 {
        let key = format!("key0000_{i}", i = i * 10);
        let value = format!("value_{i}");
        tx.put(key.as_bytes(), value.as_bytes())?;
    }

    println!("\n==============================\n");
    tx.traverse();
    tx.merge()?;
    println!("\n====== Merge\n");
    tx.traverse();
    tx.split()?;
    println!("\n====== Split\n");
    tx.traverse();

    Ok(())
}

fn run_tx_test() -> Result<()> {
    let db = create_test_database()?;
    // let mut tx = db.begin_write();
    // for i in 0..100 {
    //     let key = format!("KEY_{i}");
    //     let value = format!("value_{:0>50?}", i);
    //     tx.put(key.as_bytes(), value.as_bytes())?;
    // }
    // // tx.traverse();
    // tx.commit()?;

    // let db2 = db.clone();
    // std::thread::spawn(move || {
    //     let mut txs = Vec::new();
    //     for _ in 0..100 {
    //         let rtx = db2.begin_read();
    //         // println!("ReadOnly Cursor");
    //         let mut cursor = rtx.cursor().unwrap();
    //         display_cursor(&mut cursor).unwrap();
    //         std::thread::sleep(std::time::Duration::from_millis(1000));
    //         txs.push(rtx);
    //     }
    // });

    // // let db = create_test_database()?;
    // for t in 0..10 {
    //     let mut tx = db.begin_write();
    //     for i in 100..200 {
    //         if t % 2 == 0 {
    //             let key = format!("KEY_{i}");
    //             let value = format!("VALUE_{:0>50?}", t*i);
    //             tx.put(key.as_bytes(), value.as_bytes())?;
    //         } else {
    //             let key = format!("KEY_{i}");
    //             tx.remove(key.as_bytes())?;
    //         }

    //         std::thread::sleep(std::time::Duration::from_millis(10));

    //         if i % 50 == 0 {
    //             tx.commit()?;
    //             tx = db.begin_write();
    //         }
    //     }
    // }

    // let tx = db.begin_write();
    // tx.commit()?;

    // let rtx = db.begin_read();
    // let mut cursor = rtx.cursor().unwrap();
    // display_cursor(&mut cursor).unwrap();
    // println!("TRAVERSING 2");
    // let mut cursor = tx.cursor()?;
    // display_cursor(&mut cursor)?;

    // println!("TRAVERSING 1");
    // let db = create_test_database()?;

    // let mut cursor = rtx.cursor()?;
    // display_cursor(&mut cursor)?;
    // let mut tx = db.begi();

    // println!("TRAVERSING 2");
    // // let db = create_test_database()?;
    let tx = db.begin_read();
    let mut cursor = tx.cursor()?;
    display_cursor(&mut cursor)?;

    Ok(())
}

fn shrink() -> Result<()> {
    let db = create_test_database()?;
    let mut tx = db.begin_write();
    let s = 0;
    let n = 150;
    let m = 80;
    for i in s..n {
        // let key = format!("KEY_{:0>50?}", i);
        // let value = format!("value_{:0>50?}", i);

        let key = format!("KEY_{:?}", i);
        let value = format!("value_{:?}", i);
        tx.put(key.as_bytes(), value.as_bytes())?;

        if i % m == 0 {
            println!("<<<====COMMIT==== {i}");
        }

        if i % m == 0 {
            // println!("\n\n======= BEFORE COMMIT");
            // tx.traverse();
            tx.commit()?;
            tx = db.begin_write();
            // println!("\n\n======= AFTER COMMIT");
            // tx.traverse();
        }
    }

    // println!("\n\n======= BEFORE LAST COMMIT");
    // tx.traverse();
    tx.commit()?;

    println!("<<<<DELETION>>>>");
    let mut tx = db.begin_write();
    // println!("\n\n======= BEFORE FIRST DELETION");
    // tx.traverse();

    for i in s..n {
        // let key = format!("KEY_{:0>50?}", i);
        // if i == 100 {
        //     println!("======= BEFORE REMOVE");
        //     tx.traverse();
        // }

        let key = format!("KEY_{:?}", i);
        tx.remove(key.as_bytes())?;

        // if i == 100 {
        //     println!("======= BEFORE COMMIT");
        //     tx.traverse();
        // }

        if i % m == 0 {
            println!("<<<====COMMIT==== {i}");
        }

        if i % m == 0 {
            // println!("\n\n======= BEFORE COMMIT");
            // tx.traverse();
            tx.commit()?;
            tx = db.begin_write();
            // println!("\n\n======= AFTER COMMIT");
            // tx.traverse();
        }
    }
    // tx.traverse();
    tx.commit()?;

    let mut tx = db.begin_write();
    // tx.put(b"sample", b"sample")?;
    println!("\n\n====== FINAL TRAVERSE");
    tx.traverse();
    tx.rollback()?;

    let db = create_test_database()?;
    let tx = db.begin_read();
    let mut c = tx.cursor()?;
    display_cursor(&mut c)?;

    Ok(())
}

fn run_large() -> Result<()> {
    let db = create_test_database()?;
    let mut tx = db.begin_write();
    for i in 0..10_000_000 {
        let key = format!("KEY_{:?}", i);
        let value = format!("value_{:0>50?}", i);
        tx.put(key.as_bytes(), value.as_bytes())?;

        if i % 100_000 == 0 {
            // tx.traverse();
            tx.commit()?;
            tx = db.begin_write();
        }

        if i % 100_000 == 0 {
            println!("<<<====COMMIT==== {i}");
        }
    }
    // tx.traverse();
    tx.commit()?;
    Ok(())
}

fn eval_large() -> Result<()> {
    let db = create_test_database()?;
    let rtx = db.begin_read();
    // println!("ReadOnly Cursor");
    let mut cursor = rtx.cursor().unwrap();
    for i in 0..1_000_000 {
        if !cursor.is_valid() {
            return Err(anyhow::anyhow!("invalid cursor"));
        }

        let key = format!("KEY_{:0>50?}", i);
        let value = format!("value_{:0>50?}", i);

        assert_eq!(cursor.key(), key.as_bytes(), "keys at {i} are not equal");
        assert_eq!(cursor.value(), value.as_bytes(), "values at {i} are not equal");

        if i % 100_000 == 0 {
            println!("CHECKS {i}");
        }

        cursor.next_entry()?;
    }

    Ok(())
}

fn rm() {
    let res = std::fs::remove_file("./my.db");
    println!("Remove file: {res:?}");
}

fn bug_repr() -> Result<()> {
    rm();
    shrink()?;
    shrink()?;
    Ok(())
}

fn main() {
    // setup_test_for_cursor().expect("setup test for cursor");
    // run_basic_cursor_test().expect("basic cursor test");
    // run_basic_cursor_reverse_test().expect("basic cursor reverse test");
    // run_cursor_seek().expect("cursor seek");
    // run_basic_cursor_test().expect("cursor test 1");
    // run_tx_test().expect("run tx");
    // run_basic_cursor_test().expect("cursor test 1");
    // run_large().expect("run large");
    // shrink().expect("shrink");
    // shrink().expect("shrink");
    // eval_large().expect("eval large");
    bug_repr().expect("bug repr");
}
