use rust_extensions::slice_of_u8_utils::SliceOfU8Ext;

use crate::ColumnName;

#[derive(Clone, Debug)]
pub enum IndexOrder {
    Asc,
    Desc,
}

impl IndexOrder {
    pub fn from_str(src: Option<&str>) -> Self {
        if let Some(src) = src {
            if src.to_uppercase() == "DESC" {
                return Self::Desc;
            }
        }

        Self::Asc
    }

    pub fn is_the_same_to(&self, other: &Self) -> bool {
        match self {
            IndexOrder::Asc => match other {
                IndexOrder::Asc => true,
                IndexOrder::Desc => false,
            },
            IndexOrder::Desc => match other {
                IndexOrder::Asc => false,
                IndexOrder::Desc => true,
            },
        }
    }
}

#[derive(Clone, Debug)]
pub struct IndexField {
    pub name: ColumnName,
    pub order: IndexOrder,
}

impl IndexField {
    pub fn from_str(src: &str) -> Self {
        let src = src.trim();
        let mut first = None;
        let mut second = None;
        let mut i = 0;
        for v in src.split(' ') {
            if i == 0 {
                first = Some(v);
            } else if i == 1 {
                second = Some(v);
            }

            i += 1;
        }

        if first.is_none() {
            panic!("Invalid index field definition: {}", src);
        }

        Self {
            name: first.unwrap().to_string().into(),
            order: IndexOrder::from_str(second),
        }
    }
}

#[derive(Clone, Debug)]
pub struct IndexSchema {
    pub is_unique: bool,
    pub fields: Vec<IndexField>,
}

impl IndexSchema {
    pub fn new(is_unique: bool, fields: Vec<IndexField>) -> Self {
        Self { is_unique, fields }
    }

    pub fn from_index_def(index_def: &str) -> Self {
        let opening_parenthesis = find_opening_parenthesis(index_def);
        if opening_parenthesis.is_none() {
            panic!("Invalid index definition: {}", index_def);
        }

        let opening_parenthesis = opening_parenthesis.unwrap();

        let close_parenthesis = index_def
            .as_bytes()
            .find_byte_pos(')' as u8, opening_parenthesis);

        if close_parenthesis.is_none() {
            panic!("Invalid index definition: {}", index_def);
        }

        let params = &index_def[opening_parenthesis + 1..close_parenthesis.unwrap()].trim();

        Self {
            is_unique: find_is_unique(&index_def[..opening_parenthesis]),
            fields: parse_params(params),
        }
    }

    pub fn generate_create_index_sql(&self, table_name: &str, index_name: &str) -> String {
        let fields = self.generate_fields();
        if self.is_unique {
            format!("create unique index {index_name} on {table_name} ({fields})")
        } else {
            format!("create index {index_name} on {table_name} ({fields})")
        }
    }

    fn generate_fields(&self) -> String {
        let mut result = String::new();

        let mut i = 0;

        for field in &self.fields {
            if i > 0 {
                result.push(',');
            }

            field.name.push_name(&mut result);
            match field.order {
                IndexOrder::Asc => result.push_str(" ASC"),
                IndexOrder::Desc => result.push_str(" DESC"),
            }

            i += 1;
        }

        result
    }

    pub fn is_the_same_with(&self, other: &Self) -> bool {
        if self.is_unique != other.is_unique {
            return false;
        }

        if self.fields.len() != other.fields.len() {
            return false;
        }

        for i in 0..self.fields.len() {
            let field = self.fields.get(i).unwrap();
            let other_field = other.fields.get(i).unwrap();

            if field.name.name.as_str() != other_field.name.name.as_str() {
                return false;
            }

            if !field.order.is_the_same_to(&other_field.order) {
                return false;
            }
        }

        true
    }
}

fn find_opening_parenthesis(src: &str) -> Option<usize> {
    let mut no = 0;
    let mut found = None;
    for c in src.chars() {
        if c == '(' {
            found = Some(no);
        }
        no += 1;
    }
    found
}

fn parse_params(src: &str) -> Vec<IndexField> {
    let mut result = Vec::new();

    for c in src.split(',') {
        result.push(IndexField::from_str(c))
    }

    result
}

fn find_is_unique(src: &str) -> bool {
    let mut found_crate = false;
    for word in src.split(' ') {
        if found_crate {
            return word.to_uppercase() == "UNIQUE";
        }

        if word.to_uppercase() == "CREATE" {
            found_crate = true;
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use crate::table_schema::IndexOrder;

    use super::IndexSchema;

    #[test]
    fn test_single_fields_index_parse() {
        let index_def = "CREATE UNIQUE INDEX pd_pk ON public.pd USING btree (id)";

        let schema = IndexSchema::from_index_def(index_def);
        assert_eq!(schema.fields.len(), 1);
        assert_eq!(schema.is_unique, true);

        let item = schema.fields.get(0).unwrap();

        assert_eq!(item.name.name.as_str(), "id");
        assert_eq!(item.order.is_the_same_to(&IndexOrder::Asc), true);
    }

    #[test]
    fn test_single_fields_index_non_unique_parse() {
        let index_def = "CREATE INDEX pd_pk ON public.pd USING btree (id)";

        let schema = IndexSchema::from_index_def(index_def);
        assert_eq!(schema.fields.len(), 1);
        assert_eq!(schema.is_unique, false);

        let item = schema.fields.get(0).unwrap();

        assert_eq!(item.name.name.as_str(), "id");
        assert_eq!(item.order.is_the_same_to(&IndexOrder::Asc), true);
    }

    #[test]
    fn test_multiple_fields_non_unique_index_parse() {
        let index_def = "CREATE INDEX pd_test_index ON public.pd USING btree (email DESC, id)";

        let schema = IndexSchema::from_index_def(index_def);
        assert_eq!(schema.fields.len(), 2);
        assert_eq!(schema.is_unique, false);

        let item = schema.fields.get(0).unwrap();

        assert_eq!(item.name.name.as_str(), "email");
        assert_eq!(item.order.is_the_same_to(&IndexOrder::Desc), true);

        let item = schema.fields.get(1).unwrap();
        assert_eq!(item.name.name.as_str(), "id");
        assert_eq!(item.order.is_the_same_to(&IndexOrder::Asc), true);
    }
}
