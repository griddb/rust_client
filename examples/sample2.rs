extern crate griddb_rust;

use chrono::Utc;
use griddb_rust::get_value;
use griddb_rust::griddb::ContainerInfo::*;
use griddb_rust::griddb::StoreFactory::*;
use griddb_rust::griddb::Type::*;
use griddb_rust::griddb::Value::*;
use griddb_rust::gsvec;
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
        "point01",
        vec![
            ("timestamp", Type::Timestamp),
            ("active", Type::Bool),
            ("voltage", Type::Double),
        ],
        ContainerType::TimeSeries,
        true,
    );
    let con = match store.put_container(&colinfo, false) {
        Ok(result) => result,
        Err(error) => panic!("Error store put_container() with error code: {:?}", error),
    };
    let timestamp: Timestamp = Timestamp {
        value: Utc::now().timestamp_millis(),
    };
    con.put(gsvec![timestamp, false, 100.0f64]);

    // container execute query
    let query = match con.query("select * where timestamp > TIMESTAMPADD(HOUR, NOW(), -6)") {
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
        let timestamp: Timestamp = get_value![row_data[0]];
        let timestamp_number: i64 = timestamp.value;
        let active: bool = get_value![row_data[1]];
        let vol: f64 = get_value![row_data[2]];
        println!(
            "Time = {:?} Active = {:?} Voltage = {:.2}",
            timestamp_number, active, vol
        );
    }
}
