extern crate rust_client;

use rust_client::get_value;
use rust_client::griddb::StoreFactory::*;
use rust_client::griddb::Value::*;
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
    let con = match store.get_container("point01") {
        Ok(result) => result,
        Err(error) => panic!("Error store put_container() with error code: {:?}", error),
    };
    // container execute query
    let query = match con.query("select * from point01 where not active and voltage > 50") {
        Ok(result) => result,
        Err(error) => panic!("Error container query data with error code: {:?}", error),
    };
    let row_set = match query.fetch() {
        Ok(result) => result,
        Err(error) => panic!("Error query fetch() data with error code: {:?}", error),
    };
    let mut agg_query;
    let mut ts;
    while row_set.has_next() {
        let row = match row_set.next() {
            Ok(result) => result,
            Err(error) => panic!("Error row set next() row with error code: {}", error),
        };
        let timestamp: Timestamp = get_value![row[0]];
        ts = timestamp.value;
        let average_query = format!("select AVG(voltage) from point01 where timestamp > TIMESTAMPADD(MINUTE, TO_TIMESTAMP_MS({ts}), -10) AND timestamp < TIMESTAMPADD(MINUTE, TO_TIMESTAMP_MS({ts}), 10)");
        agg_query = match con.query(&average_query[..]) {
            Ok(result) => result,
            Err(error) => panic!(
                "Error container query aggregation data with error code: {}",
                error
            ),
        };
        let agg_result = match agg_query.fetch() {
            Ok(result) => result,
            Err(error) => panic!(
                "Error query fetch() aggregation data with error code: {}",
                error
            ),
        };
        let agg_data = match agg_result.next_aggregation() {
            Ok(result) => result,
            Err(error) => panic!(
                "Error row set next() aggregation row with error code: {}",
                error
            ),
        };
        println!(
            "[Timestamp = {:?}] Average voltage = {:.2}",
            ts,
            agg_data.get_as_f64().1
        );
    }
}
