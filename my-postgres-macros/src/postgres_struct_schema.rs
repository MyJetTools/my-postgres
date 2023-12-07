use types_reader::{StructProperty, StructureSchema, TypeName};

use crate::postgres_struct_ext::PostgresStructPropertyExt;

pub trait PostgresStructSchema<'s> {
    fn get_fields(&'s self) -> Vec<&'s StructProperty>;

    fn get_name(&'s self) -> &'s TypeName;
}

impl<'s> PostgresStructSchema<'s> for StructureSchema<'s> {
    fn get_fields(&'s self) -> Vec<&'s StructProperty> {
        let all = self.get_all();
        let mut result = Vec::with_capacity(all.len());

        for field in all {
            if field.has_ignore_table_column() || field.has_ignore_attr() {
                continue;
            }

            result.push(field);
        }

        result
    }

    fn get_name(&'s self) -> &'s TypeName {
        &self.name
    }
}

/*
pub fn filter_fields(src: Vec<StructProperty>) -> Result<Vec<StructProperty>, syn::Error> {
    let mut result = Vec::with_capacity(src.len());

    for itm in src {
        if itm.has_ignore_attr() {
            continue;
        }

        result.push(itm);
    }

    return Ok(result);
}


 */
