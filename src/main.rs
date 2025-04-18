use anyhow::Result;
use bonsai_db::{cursor::Cursor, Database, Options};

fn options() -> Options {
    Options {
        max_files: 10,
        page_size: 1 << 12,
        cache_size: 1 << 20,
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
    while cursor.is_valid() {
        println!(
            "{:?} = {:?}",
            String::from_utf8_lossy(cursor.key()),
            String::from_utf8_lossy(cursor.value()),
        );
        cursor.next_entry()?;
    }
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
    let mut tx = db.begin_write();
    for i in 0..100 {
        let key = format!("KEY_{i}");
        let value = format!("value_{:0>50?}", i);
        tx.put(key.as_bytes(), value.as_bytes())?;
    }
    // tx.traverse();
    tx.commit()?;

    let db2 = db.clone();
    std::thread::spawn(move || {
        let rtx = db2.begin_read();
        for _ in 0..10 {
            // println!("ReadOnly Cursor");
            let mut cursor = rtx.cursor().unwrap();
            display_cursor(&mut cursor).unwrap();
        }
        std::thread::sleep(std::time::Duration::from_millis(1));
        drop(rtx);
    });

    // let db = create_test_database()?;
    for t in 0..10 {
        let mut tx = db.begin_write();
        for i in 100..200 {
            let key = format!("KEY_{i}");
            let value = format!("VALUE_{:0>50?}", t*i);
            tx.put(key.as_bytes(), value.as_bytes())?;
            std::thread::sleep(std::time::Duration::from_millis(1));
            // println!("Written value");

            if i % 50 == 0 {
                tx.commit()?;
                tx = db.begin_write();
            }
        }
    }

    let tx = db.begin_write();
    tx.commit()?;

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
    // let tx = db.begin_read();
    // let mut cursor = tx.cursor()?;
    // display_cursor(&mut cursor)?;

    Ok(())
}

fn main() {
    // setup_test_for_cursor().expect("setup test for cursor");
    // run_basic_cursor_test().expect("basic cursor test");
    // run_basic_cursor_reverse_test().expect("basic cursor reverse test");
    // run_cursor_seek().expect("cursor seek");
    // run_basic_cursor_test().expect("cursor test 1");
    run_tx_test().expect("run tx");
    // run_basic_cursor_test().expect("cursor test 1");
}
