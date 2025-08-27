#[derive(Debug, Clone, Copy)]
pub enum TableColumnType {
    Text,
    SmallInt,
    BigInt,
    Boolean,
    Real,
    DoublePrecision,
    Integer,
    Json,
    Timestamp,
    Jsonb,
}

impl TableColumnType {
    pub fn is_string(&self) -> bool {
        match self {
            TableColumnType::Text => true,
            _ => false,
        }
    }

    pub fn is_json_or_jsonb(&self) -> bool {
        match self {
            TableColumnType::Json => true,
            TableColumnType::Jsonb => true,
            _ => false,
        }
    }
    pub fn as_number(&self) -> i32 {
        match self {
            TableColumnType::Text => 0,
            TableColumnType::SmallInt => 1,
            TableColumnType::BigInt => 2,
            TableColumnType::Boolean => 3,
            TableColumnType::Real => 4,
            TableColumnType::DoublePrecision => 5,
            TableColumnType::Integer => 6,
            TableColumnType::Json => 7,
            TableColumnType::Timestamp => 8,
            TableColumnType::Jsonb => 8,
        }
    }

    pub fn equals_to(&self, other_one: &TableColumnType) -> bool {
        let self_one = self.as_number();
        let other_one = other_one.as_number();
        self_one == other_one
    }

    pub fn from_db_string(src: &str) -> Self {
        match Self::try_from_db_string(src) {
            Some(v) => v,
            None => panic!("Unsupported db type string: {}", src),
        }
    }

    pub fn try_from_db_string(src: &str) -> Option<Self> {
        if src.starts_with("timestamp") {
            return Some(TableColumnType::Timestamp);
        }

        if src.starts_with("double") {
            return Some(TableColumnType::DoublePrecision);
        }

        match src {
            "text" => Some(TableColumnType::Text),
            "smallint" => Some(TableColumnType::SmallInt),
            "bigint" => Some(TableColumnType::BigInt),
            "boolean" => Some(TableColumnType::Boolean),
            "real" => Some(TableColumnType::Real),
            "integer" => Some(TableColumnType::Integer),
            "json" => Some(TableColumnType::Json),
            "jsonb" => Some(TableColumnType::Jsonb),
            "timestamp" => Some(TableColumnType::Timestamp),
            "character varying" => Some(TableColumnType::Text),
            _ => None,
        }
    }

    pub fn as_db_type_str(&self) -> &'static str {
        match self {
            TableColumnType::Text => "text",
            TableColumnType::SmallInt => "smallint",
            TableColumnType::BigInt => "bigint",
            TableColumnType::Boolean => "boolean",
            TableColumnType::Real => "real",
            TableColumnType::DoublePrecision => "double precision",
            TableColumnType::Integer => "integer",
            TableColumnType::Json => "json",
            TableColumnType::Jsonb => "jsonb",
            TableColumnType::Timestamp => "timestamp",
        }
    }
}
