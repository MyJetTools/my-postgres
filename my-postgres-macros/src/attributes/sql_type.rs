use types_reader::macros::*;

#[derive(MacrosEnum)]
pub enum SqlType {
    #[value("bigint")]
    Bigint,
    #[value("timestamp")]
    Timestamp,
    #[value("jsonb")]
    JsonB,
}

#[attribute_name("sql_type")]
#[derive(MacrosParameters)]
pub struct SqlTypeAttribute {
    #[default]
    pub name: SqlType,
}

impl SqlTypeAttribute {
    pub fn generate_attribute(&self) -> proc_macro2::TokenStream {
        let name = self.name.as_str();
        quote::quote! {
            #[sql_type(#name)]
        }
    }

    /*
    pub fn generate_table_column_type(&self) -> proc_macro2::TokenStream {
        let result = match self.name {
            SqlType::Bigint => "my_postgres::table_schema::TableColumnType::BigInt",
            SqlType::Timestamp => "my_postgres::table_schema::TableColumnType::Timestamp",
            SqlType::JsonB => "my_postgres::table_schema::TableColumnType::Jsonb",
        };

        proc_macro2::TokenStream::from_str(result).unwrap()
    }
     */
}
