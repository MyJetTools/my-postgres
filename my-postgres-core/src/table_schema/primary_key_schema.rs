use crate::{table_schema::DEFAULT_SCHEMA, ColumnName};

#[derive(Debug, Clone)]
pub struct PrimaryKeySchema(Option<Vec<ColumnName>>);

impl PrimaryKeySchema {
    pub fn from_vec(src: Vec<ColumnName>) -> Self {
        if src.len() == 0 {
            return Self(None);
        }

        let mut result = Vec::with_capacity(src.len());
        for itm in src {
            result.push(itm);
        }

        Self(Some(result))
    }

    pub fn is_same_with(&self, other: &PrimaryKeySchema) -> bool {
        if let Some(self_elements) = &self.0 {
            if let Some(other_elements) = &other.0 {
                if self_elements.len() != other_elements.len() {
                    return false;
                }

                for i in 0..self_elements.len() {
                    if self_elements.get(i).unwrap().name.as_str()
                        != other_elements.get(i).unwrap().name.as_str()
                    {
                        return false;
                    }
                }

                return true;
            } else {
                return false;
            }
        } else {
            if other.0.is_some() {
                return false;
            }

            return true;
        }
    }

    pub fn generate_update_primary_key_sql(
        &self,
        table_name: &str,
        primary_key_name: &str,
        db_primary_key: &Self,
    ) -> Option<Vec<String>> {
        match &db_primary_key.0 {
            Some(_) => {
                let schema = DEFAULT_SCHEMA;
                let mut result = Vec::with_capacity(2);
                result.push(format!(
                    "alter table {schema}.{table_name} drop constraint {primary_key_name};"
                ));

                if let Some(primary_key_columns) = self.generate_primary_key_sql_columns() {
                    result.push(format!(
                        "alter table {schema}.{table_name} add constraint {primary_key_name} primary key ({primary_key_columns});"
                    ));
                } else {
                    panic!(
                        "Your schema for primary key {} for table {} does not have columns which exists in DB. Please delete them manually",
                        primary_key_name, table_name
                    );
                }

                return Some(result);
            }
            None => {
                let schema = DEFAULT_SCHEMA;

                match self.generate_primary_key_sql_columns() {
                    Some(primary_key_columns) => {
                        return vec![format!(
                                "alter table {schema}.{table_name} add constraint {primary_key_name} primary key ({primary_key_columns});")
                            ].into();
                    }
                    None => return None,
                }
            }
        }
    }

    pub fn generate_primary_key_sql_columns(&self) -> Option<String> {
        if let Some(primary_key_columns) = &self.0 {
            let mut result = String::new();
            let mut no = 0;
            for column_name in primary_key_columns {
                if no > 0 {
                    result.push_str(", ");
                }
                result.push_str(column_name.name.as_str());
                no += 1;
            }

            return Some(result);
        }

        None
    }
}
