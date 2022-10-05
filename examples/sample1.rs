extern crate rust_client;

use rust_client::get_value;
use rust_client::griddb::ContainerInfo::*;
use rust_client::griddb::StoreFactory::*;
use rust_client::griddb::Type::*;
use rust_client::griddb::Value::*;
use rust_client::gsvec;
use std::env;

fn main() {
    // get default factory
    let factory = StoreFactory::get_instance();
    let args: Vec<_> = env::args().collect();
    let properties = vec![
        ("notification_address", args[1].as_str()),
        ("notification_port", args[2].as_str()),
        ("cluster_name", args[3].as_str()),
        ("user", args[4].as_str()),
        ("password", args[5].as_str()),
    ];
    // get gridstore function
    let store = match factory.get_store(properties) {
        Ok(result) => result,
        Err(error) => panic!("Error factory get_store() with error code: {:?}", error),
    };

    let colinfo = ContainerInfo::ContainerInfo(
        "col01",
        vec![
            ("name", Type::String),
            ("status", Type::Bool),
            ("count", Type::Long),
            ("lob", Type::Blob),
        ],
        ContainerType::Collection,
        true,
    );

    store.drop_container("col01");
    let con = match store.put_container(&colinfo, false) {
        Ok(result) => result,
        Err(error) => panic!("Error store put_container() with error code: {:?}", error),
    };
    con.set_auto_commit(false);
    con.create_index("name", IndexType::Default);
    // Create row for get and set
    let blob1 = vec![65, 66, 67, 68, 69, 70, 71, 72, 73, 74];
    let blob2 = vec![65, 66, 67, 68, 69, 70, 71, 72, 73, 74];
    con.put(gsvec!["name01".to_string(), false, 100i64, blob1]);
    con.put(gsvec!["name02".to_string(), false, 100i64, blob2]);
    con.remove("name02");
    con.commit();

    // container get row
    let _row3 = match con.get("name01") {
        Ok(result) => result,
        Err(error) => panic!("Error container get row with error code: {:?}", error),
    };

    // container execute query
    let query = match con.query("select *") {
        Ok(result) => result,
        Err(error) => panic!("Error container query data with error code: {:?}", error),
    };
    let row_set = match query.fetch() {
        Ok(result) => result,
        Err(error) => panic!("Error query fetch() data with error code: {:?}", error),
    };
    while row_set.has_next() {
        let row_data = match row_set.next() {
            Ok(result) => result,
            Err(error) => panic!("Error row set next() row with error code: {:?}", error),
        };
        let name: String = get_value![row_data[0]];
        let active: bool = get_value![row_data[1]];
        let count: i64 = get_value![row_data[2]];
        let blob_data: Vec<u8> = get_value![row_data[3]];
        let tup_query = (name, active, count, blob_data);
        println!(
            "Person: name={0} status={1} count={2} lob=[{3}]",
            tup_query.0,
            tup_query.1,
            tup_query.2,
            String::from_utf8(tup_query.3).expect("Expect Utf8 String")
        );
    }
}
