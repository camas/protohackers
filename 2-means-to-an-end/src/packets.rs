use chrono::NaiveDateTime;

#[derive(Debug)]
pub enum ClientPacket {
    Insert {
        time: NaiveDateTime,
        price: i32,
    },
    Query {
        min_time: NaiveDateTime,
        max_time: NaiveDateTime,
    },
}

#[derive(Debug)]
pub enum ServerPacket {
    QueryResponse { mean_price: i32 },
}
