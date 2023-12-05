use proc_macro2::TokenStream;
use types_reader::{ObjectValue, PropertyType, StructProperty};

use crate::{
    e_tag::ETagData,
    table_schema::{GenerateAdditionalUpdateModelAttributeParams, GenerateType},
};

pub const ATTR_PRIMARY_KEY: &str = "primary_key";
pub const ATTR_DB_COLUMN_NAME: &str = "db_column_name";
//pub const ATTR_IGNORE_IF_NULL: &str = "ignore_if_null";

pub const ATTR_SQL_TYPE: &str = "sql_type";
pub const ATTR_JSON: &str = "json";

pub enum DefaultValue {
    Inherit,
    Value(String),
}

pub struct IndexAttr {
    pub id: u8,
    pub index_name: String,
    pub name: String,
    pub is_unique: bool,
    pub order: String,
}

pub struct GenerateAdditionalUpdateStruct {
    pub struct_name: String,
    pub field_name: String,
    pub field_ty: TokenStream,
    pub is_where: bool,
}

pub struct GenerateAdditionalWhereStruct {
    pub struct_name: String,
    pub field_name: String,
    pub field_ty: TokenStream,
    pub operator: Option<String>,
    pub operator_from: Option<String>,
    pub operator_to: Option<String>,
    pub generate_as_str: bool,
    pub generate_as_vec: bool,
    pub generate_as_opt: bool,
    pub ignore_if_none: bool,
    pub generate_limit_field: Option<String>,
}

pub struct GenerateAdditionalSelectStruct {
    pub struct_name: String,
    pub field_name: String,
    pub field_ty: TokenStream,
}

pub trait PostgresStructPropertyExt<'s> {
    fn is_primary_key(&self) -> bool;

    fn get_primary_key_id(&self, last_id: u8) -> Result<Option<u8>, syn::Error>;

    fn get_sql_type(&self) -> Result<&ObjectValue, syn::Error>;

    fn try_get_sql_type(&self) -> Option<&ObjectValue>;

    fn get_db_column_name_as_token(&self) -> Result<proc_macro2::TokenStream, syn::Error>;

    fn get_db_column_name_as_string(&self) -> Result<&str, syn::Error>;

    fn try_get_db_column_name_as_string(&self) -> Result<Option<&str>, syn::Error>;

    fn has_json_attr(&self) -> bool;

    fn has_ignore_attr(&self) -> bool;
    fn has_ignore_if_none_attr(&self) -> bool;

    fn is_line_no(&self) -> bool;

    fn sql_value_to_mask(&self) -> bool;

    fn get_field_metadata(&self) -> Result<proc_macro2::TokenStream, syn::Error>;

    fn has_ignore_table_column(&self) -> bool;

    fn get_index_attrs(&self) -> Result<Option<Vec<IndexAttr>>, syn::Error>;

    fn get_e_tag(&'s self) -> Result<Option<ETagData<'s>>, syn::Error>;

    fn get_ty(&self) -> &PropertyType;

    fn get_field_name_ident(&self) -> &syn::Ident;

    fn get_default_if_null_value(&self) -> Result<Option<&str>, syn::Error>;

    fn get_default_value(&self) -> Result<Option<DefaultValue>, syn::Error>;

    fn get_inside_json(&self) -> Result<Option<Vec<&str>>, syn::Error>;

    fn get_where_operator(&self) -> Result<Option<&str>, syn::Error>;

    fn get_generate_additional_update_models(
        &self,
    ) -> Result<Option<Vec<GenerateAdditionalUpdateStruct>>, syn::Error>;

    fn get_generate_additional_where_models(
        &self,
    ) -> Result<Option<Vec<GenerateAdditionalWhereStruct>>, syn::Error>;

    fn get_generate_additional_select_models(
        &self,
    ) -> Result<Option<Vec<GenerateAdditionalSelectStruct>>, syn::Error>;

    fn render_field_value(&self, is_update: bool) -> Result<proc_macro2::TokenStream, syn::Error> {
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
    fn get_field_name_ident(&self) -> &syn::Ident {
        self.get_field_name_ident()
    }

    fn get_ty(&self) -> &PropertyType {
        &self.ty
    }

    fn sql_value_to_mask(&self) -> bool {
        if self.ty.is_string() {
            return true;
        }

        if let PropertyType::OptionOf(sub_ty) = &self.ty {
            if sub_ty.is_string() {
                return true;
            }
        }

        false
    }

    fn is_primary_key(&self) -> bool {
        self.attrs.has_attr(ATTR_PRIMARY_KEY)
    }

    fn get_primary_key_id(&self, last_id: u8) -> Result<Option<u8>, syn::Error> {
        if let Some(value) = self.attrs.try_get_attr(ATTR_PRIMARY_KEY) {
            match value.try_get_single_param() {
                Some(value) => {
                    return Ok(Some(value.parse("must be value from 0..255".into())?));
                }
                None => {
                    return Ok(Some(last_id + 1));
                }
            }
        }

        Ok(None)
    }

    fn has_ignore_attr(&self) -> bool {
        self.attrs.has_attr("ignore")
    }

    fn get_sql_type(&self) -> Result<&ObjectValue, syn::Error> {
        self.attrs.get_single_or_named_param(ATTR_SQL_TYPE, "name")
    }

    fn try_get_sql_type(&self) -> Option<&ObjectValue> {
        self.attrs
            .try_get_single_or_named_param(ATTR_SQL_TYPE, "name")
    }

    fn has_ignore_if_none_attr(&self) -> bool {
        self.attrs.has_attr("ignore_if_none")
    }

    fn has_ignore_table_column(&self) -> bool {
        self.attrs.has_attr("ignore_table_column")
    }

    fn has_json_attr(&self) -> bool {
        self.attrs.has_attr(ATTR_JSON)
    }

    fn is_line_no(&self) -> bool {
        self.attrs.has_attr("line_no") || self.name == "line_no"
    }

    fn get_default_if_null_value(&self) -> Result<Option<&str>, syn::Error> {
        if let Some(attr) = self.attrs.try_get_attr("default_if_null") {
            let result = attr
                .get_from_single_or_named("value")?
                .any_value_as_str()
                .as_str();
            return Ok(Some(result));
        }

        return Ok(None);
    }

    fn get_default_value(&self) -> Result<Option<DefaultValue>, syn::Error> {
        if let Some(attr) = self.attrs.try_get_attr("default_value") {
            let result = attr.try_get_value_from_single_or_named("value");

            match result {
                Some(value) => {
                    let result = value.any_value_as_str().as_str();
                    return Ok(Some(DefaultValue::Value(result.to_string())));
                }
                None => return Ok(Some(DefaultValue::Inherit)),
            }
        }

        return Ok(None);
    }

    fn get_db_column_name_as_token(&self) -> Result<proc_macro2::TokenStream, syn::Error> {
        if let Ok(attr) = self.attrs.get_attr(ATTR_DB_COLUMN_NAME) {
            if let Ok(result) = attr.get_from_single_or_named("name") {
                let name = result.any_value_as_str().as_str();
                return Ok(quote::quote!(#name));
            }
        }

        let name = self.name.as_str();

        Ok(quote::quote!(#name))
    }

    fn get_db_column_name_as_string(&self) -> Result<&str, syn::Error> {
        if let Ok(attr) = self
            .attrs
            .get_single_or_named_param(ATTR_DB_COLUMN_NAME, "name")
        {
            return Ok(attr.as_string()?);
        }

        Ok(self.name.as_str())
    }

    fn try_get_db_column_name_as_string(&self) -> Result<Option<&str>, syn::Error> {
        if let Some(attr) = self
            .attrs
            .try_get_single_or_named_param(ATTR_DB_COLUMN_NAME, "name")
        {
            return Ok(Some(attr.as_string()?));
        }

        Ok(None)
    }

    fn get_index_attrs(&self) -> Result<Option<Vec<IndexAttr>>, syn::Error> {
        let attrs = self.attrs.try_get_attrs("db_index");

        if attrs.is_none() {
            return Ok(None);
        }

        let attrs = attrs.unwrap();

        let mut result = Vec::with_capacity(attrs.len());

        for attr in attrs {
            let id = attr
                .get_named_param("id")?
                .get_value()?
                .parse("id must be a number 0..255".into())?;

            let index_name = attr.get_named_param("index_name")?.try_into()?;
            let is_unique = attr.get_named_param("is_unique")?.try_into()?;

            let order: &str = attr.get_named_param("order")?.try_into()?;
            if order != "DESC" && order != "ASC" {
                return Err(attr.throw_error("order must be DESC or ASC"));
            }

            result.push(IndexAttr {
                id,
                index_name,
                is_unique,
                order: order.to_string(),
                name: self.get_db_column_name_as_string()?.to_string(),
            })
        }

        Ok(Some(result))
    }

    fn get_field_metadata(&self) -> Result<proc_macro2::TokenStream, syn::Error> {
        let sql_type = self.try_get_sql_type();
        let operator = self.get_where_operator()?;
        if sql_type.is_none() && operator.is_none() {
            return Ok(quote::quote!(None));
        }

        let sql_type = if let Some(sql_type) = sql_type {
            let sql_type = sql_type.as_string()?;
            quote::quote!(Some(#sql_type))
        } else {
            quote::quote!(None)
        };

        let operator = if let Some(operator) = operator {
            quote::quote!(Some(#operator))
        } else {
            quote::quote!(None)
        };

        Ok(quote::quote! {
            Some(my_postgres::SqlValueMetadata{
                sql_type: #sql_type,
                operator: #operator
            })
        })
    }

    fn get_e_tag(&'s self) -> Result<Option<ETagData<'s>>, syn::Error> {
        if !self.attrs.has_attr("e_tag") {
            return Ok(None);
        }

        let result = ETagData {
            field_name: self.get_field_name_ident(),
            column_name: self.get_db_column_name_as_string()?,
        };

        Ok(Some(result))
    }

    fn get_inside_json(&self) -> Result<Option<Vec<&str>>, syn::Error> {
        if let Some(attr) = self.attrs.try_get_attr("inside_json") {
            let value = attr.get_from_single_or_named("field")?;

            let value = value.as_string()?.as_str();
            return Ok(Some(value.split('.').collect()));
        }

        return Ok(None);
    }

    fn get_generate_additional_update_models(
        &self,
    ) -> Result<Option<Vec<GenerateAdditionalUpdateStruct>>, syn::Error> {
        let params = self.attrs.try_get_attrs("generate_update_model");

        if params.is_none() {
            return Ok(None);
        }

        let params = params.unwrap();

        let mut result = Vec::with_capacity(params.len());

        for param in params {
            let params: GenerateAdditionalUpdateModelAttributeParams =
                params.get(0).unwrap().try_into()?;

            let is_where = match params.param_type {
                GenerateType::Update => true,
                GenerateType::Where => false,
            };

            let itm = GenerateAdditionalUpdateStruct {
                struct_name: params.name,
                field_name: self.name.to_string(),
                field_ty: self.ty.get_token_stream(),
                is_where,
            };
        }

        Ok(Some(result))
    }

    fn get_generate_additional_where_models(
        &self,
    ) -> Result<Option<Vec<GenerateAdditionalWhereStruct>>, syn::Error> {
        let params = self.attrs.try_get_attrs("generate_where_model");

        if params.is_none() {
            return Ok(None);
        }

        let params = params.unwrap();

        let mut result = Vec::new();

        for param_list in params {
            let struct_name = param_list.get_from_single_or_named("name")?.try_into()?;

            let operator = param_list.try_get_named_param("operator");

            let operator = match operator {
                Some(value) => Some(value.try_into()?),
                None => None,
            };

            let operator_from = param_list.try_get_named_param("operator_from");

            let operator_from = match operator_from {
                Some(value) => Some(value.try_into()?),
                None => None,
            };

            let operator_to = param_list.try_get_named_param("operator_to");

            let operator_to = match operator_to {
                Some(value) => Some(value.try_into()?),
                None => None,
            };

            let limit_field_name = match param_list.try_get_named_param("limit") {
                Some(name) => Some(name.try_into()?),
                None => None,
            };

            let itm = GenerateAdditionalWhereStruct {
                struct_name,
                field_name: self.name.to_string(),
                field_ty: self.ty.get_token_stream(),
                operator,
                operator_from,
                operator_to,
                generate_as_str: param_list.has_param("as_str"),
                generate_as_vec: param_list.has_param("as_vec"),
                generate_as_opt: param_list.has_param("as_option"),
                ignore_if_none: param_list.has_param("ignore_if_none"),
                generate_limit_field: limit_field_name,
            };

            result.push(itm)
        }

        Ok(Some(result))
    }

    fn get_generate_additional_select_models(
        &self,
    ) -> Result<Option<Vec<GenerateAdditionalSelectStruct>>, syn::Error> {
        let params = self.attrs.try_get_attrs("generate_select_model");

        if params.is_none() {
            return Ok(None);
        }

        let params = params.unwrap();

        let mut result = Vec::new();

        for param_list in params {
            let struct_name = param_list.get_from_single_or_named("name")?.try_into()?;

            let itm = GenerateAdditionalSelectStruct {
                struct_name,
                field_name: self.name.to_string(),
                field_ty: self.ty.get_token_stream(),
            };

            result.push(itm)
        }

        Ok(Some(result))
    }

    fn get_where_operator(&self) -> Result<Option<&str>, syn::Error> {
        let op_value = self.attrs.try_get_single_or_named_param("operator", "op");
        if op_value.is_none() {
            return Ok(None);
        }

        let op_value = op_value.unwrap();

        let value = op_value.try_into()?;
        if value == "="
            || value == "!="
            || value == "<"
            || value == "<="
            || value == ">"
            || value == ">="
            || value == "<>"
        {
            return Ok(Some(value));
        }

        if value == "like" || value == "LIKE" {
            return Ok(Some(" like "));
        }

        return Err(syn::Error::new_spanned(
            self.field,
            format!("Invalid operator {}", value),
        ));
    }
}

pub fn filter_fields(src: Vec<StructProperty>) -> Result<Vec<StructProperty>, syn::Error> {
    let mut result = Vec::with_capacity(src.len());

    for itm in src {
        if itm.has_ignore_attr() {
            continue;
        }

        let is_date_time = match &itm.ty {
            PropertyType::DateTime => true,
            PropertyType::OptionOf(sub_ty) => sub_ty.is_date_time(),
            _ => false,
        };

        if is_date_time {
            let attr = itm.get_sql_type()?;

            let attr: &str = attr.try_into()?;
            if attr != "timestamp" && attr != "bigint" {
                let result = syn::Error::new_spanned(
                    itm.field,
                    format!("Sql type must be 'timestamp' or 'bigint'"),
                );

                return Err(result);
            }
        }

        result.push(itm);
    }

    return Ok(result);
}
