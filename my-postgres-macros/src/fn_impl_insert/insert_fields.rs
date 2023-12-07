use types_reader::StructProperty;

use crate::{e_tag::GetETag, postgres_struct_schema::PostgresStructSchema};

pub struct InsertFields<'s> {
    items: Vec<&'s StructProperty<'s>>,
}

impl<'s> InsertFields<'s> {
    pub fn new(src: &'s impl PostgresStructSchema<'s>) -> Self {
        Self {
            items: src.get_fields(),
        }
    }

    pub fn get_fields_amount(&self) -> usize {
        self.items.len()
    }

    pub fn as_slice(&'s self) -> &'s [&'s StructProperty<'s>] {
        self.items.as_slice()
    }
}

impl<'s> GetETag<'s> for InsertFields<'s> {
    fn get_items(&'s self) -> Vec<&'s StructProperty<'s>> {
        let mut result = Vec::with_capacity(self.items.len());
        for field in &self.items {
            result.push(*field)
        }
        result
    }
}
