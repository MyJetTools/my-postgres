use std::collections::BTreeMap;

use types_reader::{StructProperty, StructureSchema, TypeName};

use crate::{
    attributes::*, fn_impl_update::UpdateFields, postgres_struct_ext::PostgresStructPropertyExt,
};

pub struct GenerateAdditionalWhereStruct<'s> {
    pub prop: &'s StructProperty<'s>,
    pub attr: GenerateWhereModelAttribute,
}

#[derive(Default)]
pub struct DbIndexes<'s> {
    pub primary_keys: BTreeMap<u8, String>,
    pub indexes: BTreeMap<String, BTreeMap<u8, DbIndexField<'s>>>,
}

pub struct DbIndexField<'s> {
    pub prop: &'s StructProperty<'s>,
    pub attr: DbIndexAttribute<'s>,
}

pub trait PostgresStructSchema<'s> {
    fn get_fields(&'s self) -> Vec<&'s StructProperty>;

    fn get_name(&'s self) -> &'s TypeName;

    fn get_select_properties_to_generate(
        &'s self,
    ) -> Result<BTreeMap<String, Vec<&'s StructProperty>>, syn::Error> {
        let mut result = BTreeMap::new();

        for prop in self.get_fields() {
            let attrs: Option<Vec<GenerateSelectModelAttribute>> = prop.try_get_attributes()?;

            if attrs.is_none() {
                continue;
            }

            let attrs = attrs.unwrap();

            for attr in attrs {
                if !result.contains_key(&attr.name) {
                    result.insert(attr.name.clone(), Vec::new());
                }

                result.get_mut(&attr.name).unwrap().push(prop);
            }
        }

        Ok(result)
    }

    fn get_update_properties_to_generate(
        &'s self,
    ) -> Result<BTreeMap<String, UpdateFields<'s>>, syn::Error> {
        let mut result = BTreeMap::new();

        for prop in self.get_fields() {
            let attrs: Option<Vec<GenerateUpdateModelAttribute>> = prop.try_get_attributes()?;

            if attrs.is_none() {
                continue;
            }

            let attrs = attrs.unwrap();

            for attr in attrs {
                if !result.contains_key(&attr.name) {
                    result.insert(attr.name.clone(), UpdateFields::default());
                }

                match &attr.param_type {
                    GenerateType::Where => {
                        result.get_mut(&attr.name).unwrap().where_fields.push(prop);
                    }
                    GenerateType::Update => {
                        result.get_mut(&attr.name).unwrap().update_fields.push(prop);
                    }
                }
            }
        }

        Ok(result)
    }

    fn get_where_properties_to_generate(
        &'s self,
    ) -> Result<BTreeMap<String, Vec<GenerateAdditionalWhereStruct<'s>>>, syn::Error> {
        let mut result = BTreeMap::new();

        for prop in self.get_fields() {
            let attrs: Option<Vec<GenerateWhereModelAttribute>> = prop.try_get_attributes()?;

            if attrs.is_none() {
                continue;
            }

            let attrs = attrs.unwrap();

            for attr in attrs {
                if !result.contains_key(&attr.name) {
                    result.insert(attr.name.clone(), Vec::new());
                }

                result
                    .get_mut(&attr.name)
                    .unwrap()
                    .push(GenerateAdditionalWhereStruct { prop, attr });
            }
        }

        Ok(result)
    }

    fn get_db_index_fields(&'s self) -> Result<DbIndexes<'s>, syn::Error> {
        let mut result = DbIndexes::default();

        let mut last_primary_key_id = 0;

        for field in self.get_fields() {
            let db_column_name = field.get_db_column_name()?;

            let primary_key_attr: Option<PrimaryKeyAttribute> = field.try_get_attribute()?;

            if let Some(attr) = primary_key_attr {
                let value = attr.get_id(last_primary_key_id);
                if result.primary_keys.contains_key(&value) {
                    return Err(syn::Error::new_spanned(
                        field.field,
                        format!("Primary key order id {} is already used", value),
                    ));
                }
                result
                    .primary_keys
                    .insert(value, db_column_name.to_string());
                last_primary_key_id += 1;
            };

            let index_attrs: Option<Vec<DbIndexAttribute>> = field.try_get_attributes()?;

            if let Some(index_attrs) = index_attrs {
                for attr in index_attrs {
                    if !result.indexes.contains_key(attr.index_name) {
                        result
                            .indexes
                            .insert(attr.index_name.to_string(), BTreeMap::new());
                    }

                    let index_by_name = result.indexes.get_mut(attr.index_name).unwrap();

                    if index_by_name.contains_key(&attr.id) {
                        panic!(
                            "Duplicate index id {} for index {}",
                            attr.id, attr.index_name
                        );
                    }

                    index_by_name.insert(
                        attr.id,
                        DbIndexField {
                            prop: field,
                            attr: attr,
                        },
                    );
                }
            }
        }

        Ok(result)
    }
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
