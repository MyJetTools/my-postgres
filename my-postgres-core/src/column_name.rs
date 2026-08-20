use std::borrow::Cow;
use std::collections::HashSet;

#[derive(Clone, Debug)]
pub struct ColumnName {
    pub name: Cow<'static, str>,
}

impl ColumnName {
    pub fn new(name: Cow<'static, str>) -> Self {
        Self { name }
    }
    pub fn push_name(&self, dest: &mut String) {
        let has_reserved = is_reserved(self.name.as_ref());
        if has_reserved {
            dest.push('"');
        }

        dest.push_str(self.name.as_ref());

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
            name: Cow::Borrowed(self),
        }
    }
}

impl Into<ColumnName> for &'static String {
    fn into(self) -> ColumnName {
        ColumnName {
            name: Cow::Borrowed(self.as_str()),
        }
    }
}

impl Into<ColumnName> for String {
    fn into(self) -> ColumnName {
        ColumnName {
            name: Cow::Owned(self),
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
