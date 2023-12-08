use types_reader::StructProperty;

use crate::{
    e_tag::GetETag, postgres_struct_ext::PostgresStructPropertyExt,
    postgres_struct_schema::PostgresStructSchema,
};

#[derive(Default)]
pub struct UpdateFields<'s> {
    pub update_fields: Vec<&'s StructProperty<'s>>,
    pub where_fields: Vec<&'s StructProperty<'s>>,
}

impl<'s> UpdateFields<'s> {
    pub fn new_from_update_model(items: &'s impl PostgresStructSchema<'s>) -> Self {
        let fields = items.get_fields();
        let mut update_fields = Vec::with_capacity(fields.len());
        let mut where_fields = Vec::with_capacity(fields.len());

        for field in fields {
            if field.is_primary_key() {
                where_fields.push(field)
            } else {
                update_fields.push(field)
            }
        }

        Self {
            update_fields,
            where_fields,
        }
    }

    pub fn get_update_fields(&'s self) -> &'s [&'s StructProperty<'s>] {
        &self.update_fields
    }

    pub fn get_where_fields(&'s self) -> &'s [&'s StructProperty<'s>] {
        self.where_fields.as_slice()
    }

    pub fn get_fields_amount(&self) -> usize {
        self.update_fields.len() + self.where_fields.len()
    }
}

impl<'s> GetETag<'s> for UpdateFields<'s> {
    fn get_items(&'s self) -> Vec<&'s StructProperty<'s>> {
        let mut result = Vec::with_capacity(self.update_fields.len() + self.where_fields.len());

        for field in &self.update_fields {
            result.push(*field)
        }

        for field in &self.where_fields {
            result.push(*field)
        }
        result
    }
}
