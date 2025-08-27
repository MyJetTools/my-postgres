use std::str::FromStr;

use proc_macro2::TokenStream;
use types_reader::{AnyValueAsStr, MacrosAttribute, PropertyType, StructProperty};

use crate::{attributes::*, e_tag::ETagData};

pub struct DbColumnName<'s> {
    pub attr: Option<DbColumnNameAttribute<'s>>,
    pub property_name: &'s str,
}

impl<'s> DbColumnName<'s> {
    pub fn to_column_name_token(&'s self, force_cast_db_type: bool) -> proc_macro2::TokenStream {
        let db_column_name = match self.attr.as_ref() {
            Some(attr) => attr.name,
            None => self.property_name,
        };

        let filed_name = self.property_name;
        quote::quote! {
            my_postgres::DbColumnName{
                field_name: #filed_name,
                db_column_name: #db_column_name,
                force_cast_db_type: #force_cast_db_type
            }
        }
    }
}

impl<'s> DbColumnName<'s> {
    pub fn as_str(&self) -> &str {
        if let Some(attr) = &self.attr {
            return attr.name;
        }

        self.property_name
    }

    pub fn to_string(&self) -> String {
        self.as_str().to_string()
    }

    pub fn get_overridden_column_name(&'s self) -> DbColumnNameAttribute<'s> {
        if let Some(attr) = &self.attr {
            return DbColumnNameAttribute { name: attr.name };
        }

        DbColumnNameAttribute {
            name: self.property_name,
        }
    }
}

pub enum DefaultValue {
    Inherit,
    Value(String),
}

pub trait PostgresStructPropertyExt<'s> {
    fn is_primary_key(&self) -> bool;

    fn get_db_column_name(&'s self) -> Result<DbColumnName<'s>, syn::Error>;

    fn has_ignore_attr(&self) -> bool;
    fn has_ignore_if_none_attr(&self) -> bool;

    fn is_line_no(&self) -> bool;

    fn get_field_metadata(&self) -> Result<proc_macro2::TokenStream, syn::Error>;

    fn has_ignore_table_column(&self) -> bool;

    fn has_inline_where_model_attr(&self) -> bool;

    fn get_e_tag(&'s self) -> Result<Option<ETagData<'s>>, syn::Error>;

    fn get_ty(&'s self) -> &'s PropertyType<'s>;

    fn get_field_name_ident(&self) -> &syn::Ident;

    fn get_default_value(&self) -> Result<Option<DefaultValue>, syn::Error>;

    fn must_not_have_sql_type_attr(&self) -> Result<(), syn::Error>;
    fn try_get_sql_type_attr_value(
        &self,
        expected: &[&'static str],
    ) -> Result<Option<&str>, syn::Error>;
    fn get_sql_type_attr_value(&self, expected: &[SqlType]) -> Result<SqlType, syn::Error>;

    fn get_sql_type_as_token_stream(&self) -> Result<proc_macro2::TokenStream, syn::Error>;

    fn inside_json(&self) -> Result<Option<&str>, syn::Error>;

    fn get_force_cast_db_type(&self) -> bool;

    fn fill_attributes(
        &self,
        fields: &mut Vec<TokenStream>,
        override_db_column_name: Option<DbColumnNameAttribute>,
    ) -> Result<(), syn::Error>;

    fn render_field_value(
        &'s self,
        is_update: bool,
    ) -> Result<proc_macro2::TokenStream, syn::Error> {
        match &self.get_ty() {
            types_reader::PropertyType::OptionOf(_) => return self.fill_option_of_value(is_update),
            types_reader::PropertyType::Struct(..) => return self.get_value(is_update),
            _ => return self.get_value(is_update),
        }
    }

    fn get_value(&self, is_update: bool) -> Result<proc_macro2::TokenStream, syn::Error> {
        let name = self.get_field_name_ident();

        let metadata = self.get_field_metadata()?;

        let ignore_if_none = self.has_ignore_if_none_attr();

        let result = if is_update {
            quote::quote! {
                my_postgres::sql_update::SqlUpdateModelValue{
                    value: Some(&self.#name),
                    ignore_if_none: #ignore_if_none,
                    metadata: #metadata
                }
            }
            .into()
        } else {
            quote::quote! {
                my_postgres::SqlWhereValueWrapper::Value {
                    value: &self.#name,
                    metadata: #metadata
                }
            }
            .into()
        };

        Ok(result)
    }

    fn fill_option_of_value(
        &self,
        is_update: bool,
    ) -> Result<proc_macro2::TokenStream, syn::Error> {
        let prop_name = self.get_field_name_ident();

        let metadata = self.get_field_metadata()?;

        let else_case: proc_macro2::TokenStream = if self.has_ignore_if_none_attr() {
            if is_update {
                quote::quote!(my_postgres::sql_update::SqlUpdateModelValue::Ignore).into()
            } else {
                quote::quote!(my_postgres::sql_update::SqlUpdateModelValue::Ignore).into()
            }
        } else {
            if is_update {
                quote::quote!(my_postgres::sql_update::SqlUpdateModelValue::Null).into()
            } else {
                quote::quote!(my_postgres::sql_update::SqlUpdateModelValue::Null).into()
            }
        };

        let result = if is_update {
            let ignore_if_none = self.has_ignore_if_none_attr();

            quote::quote! {
               if let Some(value) = &self.#prop_name{
                  my_postgres::sql_update::SqlUpdateModelValue {value: Some(value), ignore_if_none:#ignore_if_none, metadata: #metadata}
               }else{
                my_postgres::sql_update::SqlUpdateModelValue {value: None, ignore_if_none:#ignore_if_none, metadata: #metadata}
               }
            }
        } else {
            quote::quote! {
               if let Some(value) = &self.#prop_name{
                  my_postgres::SqlWhereValueWrapper::Value {value, metadata: #metadata}
               }else{
                    #else_case
               }
            }
        };

        Ok(result)
    }
}

impl<'s> PostgresStructPropertyExt<'s> for StructProperty<'s> {
    fn get_force_cast_db_type(&self) -> bool {
        self.attrs.try_get_attr("force_cast_db_type").is_some()
    }

    fn get_field_name_ident(&self) -> &syn::Ident {
        self.get_field_name_ident()
    }

    fn get_ty(&'s self) -> &'s PropertyType<'s> {
        &self.ty
    }

    fn inside_json(&self) -> Result<Option<&str>, syn::Error> {
        let json_attr: Option<InsideJsonAttribute> = self.try_get_attribute()?;
        if let Some(json_attr) = json_attr {
            return Ok(Some(json_attr.name));
        }

        Ok(None)
    }

    fn has_inline_where_model_attr(&self) -> bool {
        self.attrs.has_attr("inline_where_model")
    }

    fn is_primary_key(&self) -> bool {
        self.attrs.has_attr(PrimaryKeyAttribute::NAME)
    }

    fn get_db_column_name(&'s self) -> Result<DbColumnName<'s>, syn::Error> {
        let attr: Option<DbColumnNameAttribute> = self.try_get_attribute()?;

        let result = DbColumnName {
            attr,
            property_name: &self.name,
        };

        Ok(result)
    }

    fn has_ignore_attr(&self) -> bool {
        self.attrs.has_attr(IgnoreAttribute::NAME)
    }

    fn has_ignore_if_none_attr(&self) -> bool {
        self.attrs.has_attr(IgnoreIfNoneAttribute::NAME)
    }

    fn has_ignore_table_column(&self) -> bool {
        self.attrs.has_attr(IgnoreTableColumnAttribute::NAME)
    }

    /*
    fn has_json_attr(&self) -> bool {
        self.attrs.has_attr(JsonAttribute::NAME)
    }
     */

    fn is_line_no(&self) -> bool {
        self.attrs.has_attr(LineNoAttribute::NAME) || self.name == LineNoAttribute::NAME
    }

    fn get_default_value(&self) -> Result<Option<DefaultValue>, syn::Error> {
        let default_value_attr: Option<DefaultValueAttribute> = self.try_get_attribute()?;
        if let Some(attr) = default_value_attr {
            match attr.value {
                Some(value) => {
                    return Ok(Some(DefaultValue::Value(value.to_string())));
                }
                None => return Ok(Some(DefaultValue::Inherit)),
            }
        }

        return Ok(None);
    }

    fn get_field_metadata(&self) -> Result<proc_macro2::TokenStream, syn::Error> {
        let sql_type: Option<SqlTypeAttribute> = self.try_get_attribute()?;
        let operator: Option<WhereOperatorAttribute> = self.try_get_attribute()?;
        let wrap_column_name: Option<WrapColumnNameAttribute> = self.try_get_attribute()?;
        if sql_type.is_none() && operator.is_none() && wrap_column_name.is_none() {
            return Ok(quote::quote!(None));
        }

        let sql_type = self.get_sql_type_as_token_stream()?;

        let operator = if let Some(operator) = operator {
            let operator = operator.op.get_metadata_operator();
            quote::quote!(Some(#operator))
        } else {
            quote::quote!(None)
        };

        let wrap_column_name = if let Some(wrap_column_name) = wrap_column_name {
            let name = wrap_column_name.name;
            quote::quote!(Some(#name))
        } else {
            quote::quote!(None)
        };

        Ok(quote::quote! {
            Some(my_postgres::SqlValueMetadata{
                sql_type: #sql_type,
                operator: #operator,
                wrap_column_name: #wrap_column_name
            })
        })
    }

    fn get_e_tag(&'s self) -> Result<Option<ETagData<'s>>, syn::Error> {
        if !self.attrs.has_attr("e_tag") {
            return Ok(None);
        }

        let result = ETagData {
            field_name: self.get_field_name_ident(),
            column_name: self.get_db_column_name()?.to_string(),
        };

        Ok(Some(result))
    }

    fn fill_attributes(
        &self,
        fields: &mut Vec<TokenStream>,
        override_db_column_name: Option<DbColumnNameAttribute>,
    ) -> Result<(), syn::Error> {
        if let Some(override_db_column_name) = override_db_column_name {
            fields.push(override_db_column_name.generate_attribute());
        } else {
            if let Some(db_column_name) = self.get_db_column_name()?.attr {
                fields.push(db_column_name.generate_attribute());
            }
        }

        let sql_type_attribute: Option<SqlTypeAttribute> = self.try_get_attribute()?;

        if let Some(sql_type) = sql_type_attribute {
            fields.push(sql_type.generate_attribute());
        }

        Ok(())
    }

    fn must_not_have_sql_type_attr(&self) -> Result<(), syn::Error> {
        let sql_type = self.attrs.try_get_attr("sql_type");

        if let Some(sql_type) = sql_type {
            return Err(
                sql_type.throw_error_at_param_token("This field must not have sql_type attribute")
            );
        }

        Ok(())
    }

    fn get_sql_type_attr_value(&self, expected: &[SqlType]) -> Result<SqlType, syn::Error> {
        let sql_type: SqlTypeAttribute = self.get_attribute()?;

        for exp in expected {
            if exp.as_str() == sql_type.name.as_str() {
                return Ok(sql_type.name);
            }
        }

        let expected: Vec<&str> = expected.iter().map(|itm| itm.as_str()).collect();

        self.throw_error(&format!(
            "sql_type attribute should have one of the following values: {:?}",
            expected
        ))
    }

    fn try_get_sql_type_attr_value(
        &self,
        expected: &[&'static str],
    ) -> Result<Option<&str>, syn::Error> {
        let sql_type = self
            .attrs
            .try_get_single_or_named_param("sql_type", "name")?;

        let Some(sql_type) = sql_type else {
            return Ok(None);
        };

        let as_str = sql_type.as_str()?;

        if expected.is_empty() {
            return Ok(Some(as_str));
        }

        for exp in expected {
            if *exp == as_str {
                return Ok(Some(as_str));
            }
        }

        Err(sql_type.throw_error(&format!(
            "sql_type attribute should have one of the following values: {:?}",
            expected
        )))
    }

    fn get_sql_type_as_token_stream(&self) -> Result<proc_macro2::TokenStream, syn::Error> {
        let ty = if let PropertyType::OptionOf(ty) = &self.ty {
            ty.as_ref()
        } else {
            &self.ty
        };

        let result = match ty {
            PropertyType::U8 => {
                self.must_not_have_sql_type_attr()?;
                quote::quote!(my_postgres::table_schema::TableColumnType::SmallInt)
            }
            PropertyType::I8 => {
                self.must_not_have_sql_type_attr()?;
                quote::quote!(my_postgres::table_schema::TableColumnType::SmallInt)
            }
            PropertyType::U16 => {
                self.must_not_have_sql_type_attr()?;
                quote::quote!(my_postgres::table_schema::TableColumnType::SmallInt)
            }
            PropertyType::I16 => {
                self.must_not_have_sql_type_attr()?;
                quote::quote!(my_postgres::table_schema::TableColumnType::SmallInt)
            }
            PropertyType::U32 => {
                self.must_not_have_sql_type_attr()?;
                quote::quote!(my_postgres::table_schema::TableColumnType::Integer)
            }
            PropertyType::I32 => {
                self.must_not_have_sql_type_attr()?;
                quote::quote!(my_postgres::table_schema::TableColumnType::Integer)
            }
            PropertyType::U64 => {
                self.must_not_have_sql_type_attr()?;
                quote::quote!(my_postgres::table_schema::TableColumnType::BigInt)
            }
            PropertyType::I64 => {
                self.must_not_have_sql_type_attr()?;
                quote::quote!(my_postgres::table_schema::TableColumnType::BigInt)
            }
            PropertyType::F32 => {
                self.must_not_have_sql_type_attr()?;
                quote::quote!(my_postgres::table_schema::TableColumnType::Real)
            }
            PropertyType::F64 => {
                self.must_not_have_sql_type_attr()?;
                quote::quote!(my_postgres::table_schema::TableColumnType::DoublePrecision)
            }
            PropertyType::USize => {
                self.must_not_have_sql_type_attr()?;
                quote::quote!(my_postgres::table_schema::TableColumnType::BigInt)
            }
            PropertyType::ISize => {
                self.must_not_have_sql_type_attr()?;
                quote::quote!(my_postgres::table_schema::TableColumnType::BigInt)
            }
            PropertyType::String => {
                if let Some(sql_type) = self.try_get_sql_type_attr_value(&["json", "jsonb"])? {
                    match sql_type {
                        "json" => {
                            quote::quote!(my_postgres::table_schema::TableColumnType::Json)
                        }
                        "jsonb" => {
                            quote::quote!(my_postgres::table_schema::TableColumnType::Jsonb)
                        }
                        _ => {
                            panic!("Unsupported sql_type: {}", sql_type);
                        }
                    }
                } else {
                    quote::quote!(my_postgres::table_schema::TableColumnType::Text)
                }
            }
            PropertyType::Bool => {
                self.must_not_have_sql_type_attr()?;
                quote::quote!(my_postgres::table_schema::TableColumnType::Text)
            }
            PropertyType::DateTime => {
                match self.get_sql_type_attr_value(&[SqlType::Timestamp, SqlType::Bigint])? {
                    SqlType::Timestamp => {
                        quote::quote!(my_postgres::table_schema::TableColumnType::Timestamp)
                    }
                    SqlType::Bigint => {
                        quote::quote!(my_postgres::table_schema::TableColumnType::BigInt)
                    }
                    _ => {
                        panic!("DateTime type should have sql_type attribute set with 'timestamp' value or 'bigint'");
                    }
                }
            }
            PropertyType::OptionOf(_) => {
                panic!("OptionOf should be unwrapped before");
            }
            PropertyType::VecOf(_property_type) => {
                get_json_or_json_b(self.get_sql_type_attr_value(&[SqlType::Json, SqlType::JsonB])?)
            }
            PropertyType::Struct(name, _type_path) => {
                let tp_as_token = TokenStream::from_str(name).unwrap();
                quote::quote! (#tp_as_token::get_sql_type())
            }
            PropertyType::HashMap(_property_type, _property_type1) => {
                get_json_or_json_b(self.get_sql_type_attr_value(&[SqlType::Json, SqlType::JsonB])?)
            }
            PropertyType::RefTo { .. } => {
                panic!("RefTo is not supported for sql");
            }
        };

        //let ty_token = ty.get_token_stream_with_generics();

        //let meta_data = self.get_field_metadata()?;

        Ok(result)
    }
}

fn get_json_or_json_b(value: SqlType) -> proc_macro2::TokenStream {
    match value {
        SqlType::Json => {
            quote::quote!(my_postgres::table_schema::TableColumnType::Timestamp)
        }
        SqlType::JsonB => {
            quote::quote!(my_postgres::table_schema::TableColumnType::BigInt)
        }
        _ => {
            panic!("sql_type attribute must be 'timestamp' or 'bigint'");
        }
    }
}
