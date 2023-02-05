#[derive(Debug, Clone)]
pub enum TableColumnType {
    Text,
    SmallInt,
    BigInt,
    Boolean,
    Real,
    Double,
    Integer,
    Json,
    Timestamp,
}

impl TableColumnType {
    pub fn as_number(&self) -> i32 {
        match self {
            TableColumnType::Text => 0,
            TableColumnType::SmallInt => 1,
            TableColumnType::BigInt => 2,
            TableColumnType::Boolean => 3,
            TableColumnType::Real => 4,
            TableColumnType::Double => 5,
            TableColumnType::Integer => 6,
            TableColumnType::Json => 7,
            TableColumnType::Timestamp => 8,
        }
    }

    pub fn equals_to(&self, other_one: &TableColumnType) -> bool {
        let self_one = self.as_number();
        let other_one = other_one.as_number();
        self_one == other_one
    }

    pub fn from_db_string(src: &str) -> Option<Self> {
        if src.starts_with("timestamp") {
            return Some(TableColumnType::Timestamp);
        }

        if src.starts_with("double") {
            return Some(TableColumnType::Double);
        }

        match src {
            "text" => Some(TableColumnType::Text),
            "smallint" => Some(TableColumnType::SmallInt),
            "bigint" => Some(TableColumnType::BigInt),
            "boolean" => Some(TableColumnType::Boolean),
            "real" => Some(TableColumnType::Real),
            "integer" => Some(TableColumnType::Integer),
            "json" => Some(TableColumnType::Json),
            "timestamp" => Some(TableColumnType::Timestamp),
            _ => None,
        }
    }
}
