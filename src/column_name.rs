use std::collections::HashSet;

use rust_extensions::StrOrString;

#[derive(Clone, Debug)]
pub struct ColumnName {
    pub name: StrOrString<'static>,
}

impl ColumnName {
    pub fn new(name: StrOrString<'static>) -> Self {
        Self { name }
    }
    pub fn push_name(&self, dest: &mut String) {
        let has_reserved = is_reserved(self.name.as_str());
        if has_reserved {
            dest.push('"');
        }

        dest.push_str(self.name.as_str());

        if has_reserved {
            dest.push('"');
        }
    }

    pub fn to_string(&self) -> String {
        let mut result = String::new();
        self.push_name(&mut result);
        result
    }
}

impl Into<ColumnName> for &'static str {
    fn into(self) -> ColumnName {
        ColumnName {
            name: StrOrString::create_as_str(self),
        }
    }
}

impl Into<ColumnName> for &'static String {
    fn into(self) -> ColumnName {
        ColumnName {
            name: StrOrString::create_as_str(self),
        }
    }
}

impl Into<ColumnName> for String {
    fn into(self) -> ColumnName {
        ColumnName {
            name: StrOrString::create_as_string(self),
        }
    }
}

pub fn is_reserved(name: &str) -> bool {
    RESERVED.contains(name.to_lowercase().as_str())
}

lazy_static::lazy_static! {
    pub static ref RESERVED: HashSet<&'static str> = {
        let mut result = HashSet::new();
        result.insert("namespace");
        result
    };
}
