use std::str::FromStr;

use types_reader::{StructProperty, TypeName};

use crate::{postgres_struct_ext::{PostgresStructPropertyExt, DbColumnName}, postgres_struct_schema::PostgresStructSchema};

pub struct WhereFields<'s> {
    pub limit: Option<&'s StructProperty<'s>>,
    pub offset: Option<&'s StructProperty<'s>>,
    pub where_fields: Vec<&'s StructProperty<'s>>,
}

impl<'s> WhereFields<'s> {
    pub fn new(src_fields: &'s impl PostgresStructSchema<'s>) -> Self {
        let mut limit = None;
        let mut offset = None;
        let mut other_fields = Vec::new();
        for struct_prop in src_fields.get_fields() {
            if struct_prop.attrs.has_attr("limit") {
                limit = Some(struct_prop);
            } else if struct_prop.attrs.has_attr("offset") {
                offset = Some(struct_prop);
            } else {
                other_fields.push(struct_prop);
            }
        }

        Self {
            limit,
            offset,
            where_fields: other_fields,
        }
    }

    pub fn generate_implementation(
        &self,
        type_name: &TypeName,
    ) -> Result<proc_macro2::TokenStream, syn::Error> {
        let where_data = self.fn_fill_where_content()?;

        let result = crate::render_impl::impl_sql_where_model(
            &type_name,
            {
                quote::quote! {
                    use my_postgres::SqlWhereValueProvider;
                    #where_data
                }
            },
            self.generate_has_conditions_fn(),
            self.generate_limit_fn(),
            self.generate_offset_fn(),
        );

        Ok(result.into())
    }

    pub fn generate_limit_fn(&self) -> proc_macro2::TokenStream {
        if let Some(limit) = &self.limit {
            let name = limit.get_field_name_ident();
            quote::quote! {self.#name.into()}
        } else {
            quote::quote! {None}
        }
    }

    pub fn generate_offset_fn(&self) -> proc_macro2::TokenStream {
        if let Some(offset) = &self.offset {
            let name = offset.get_field_name_ident();
            quote::quote! {self.#name.into()}
        } else {
            quote::quote! {None}
        }
    }


    pub fn all_fields_are_optional_and_has_ignore_if_none(&self)->bool{
        for itm in &self.where_fields{
            if !itm.ty.is_option(){
                return false;
            }

            if !itm.has_ignore_if_none_attr(){
                return false; 
            }
        }

        true
    }


    pub fn generate_has_conditions_fn(&self) -> proc_macro2::TokenStream {

        if self.where_fields.len() == 0{
            return quote::quote!(false)    ;
        }


        if !self.all_fields_are_optional_and_has_ignore_if_none(){
            let has_fields = self.where_fields.len() > 0;
            return quote::quote! {#has_fields};
        }

        let mut result = Vec::new();


        for itm in &self.where_fields{
            let prop_name_ident = itm.get_field_name_ident();
            let ignore_if_none = itm.has_ignore_if_none_attr();

            if itm.ty.is_option(){
                if ignore_if_none{
                    result.push(quote::quote! {
                        if self.#prop_name_ident.is_some(){
                            return true;
                        }
                    });
                }
                else{
                    result.push(quote::quote! {
                        if self.#prop_name_ident.is_some(){
                            return true;
                        }
                    });
                }
            }
            else{
                result.push(quote::quote! {
                    return true;
                });
            }
        }

        
        quote::quote! {
            #(#result)*
            false
        }
    }

    pub fn fn_fill_where_content(&self) -> Result<proc_macro2::TokenStream, syn::Error> {
        let mut lines = Vec::new();

        lines.push(quote::quote! {
            let mut condition_no = 0;
        });

        for prop in &self.where_fields {
            let prop_name_ident = prop.get_field_name_ident();
            let db_column_name = prop.get_db_column_name()?;
            let metadata = prop.get_field_metadata()?;

            let ignore_if_none = prop.has_ignore_if_none_attr();

            let where_condition = render_full_where_condition(&db_column_name, None);

            if prop.ty.is_option() {
                if ignore_if_none {
                    lines.push(quote::quote! {
                        if let Some(value) = &self.#prop_name_ident{
                            if value.fill_where_value(#where_condition, sql, params, &#metadata){
                                condition_no+=1;
                            }

                        }
                    });
                } else {
                    let db_column_name = db_column_name.as_str();
                    lines.push(quote::quote! {
                        if let Some(value) = &self.#prop_name_ident{
                            if value.fill_where_value(#where_condition, sql, params, &#metadata){
                                condition_no+=1;
                            }
                        }
                        else{
                            if condition_no>0{
                                sql.push_str(" AND ");
                            }
                            sql.push_str(#db_column_name);
                            sql.push_str(" IS NULL");
                        }
                        condition_no+=1;
                    });
                }
            } else {
                lines.push(quote::quote! {
                    if self.#prop_name_ident.fill_where_value(#where_condition, sql, params, &#metadata){
                        condition_no+=1;
                    }
                    
                });
            }
        }

        let result = quote::quote! {
            #(#lines)*
        };

        Ok(result)
    }
}

pub fn render_full_where_condition(
    db_column_name: &DbColumnName,
    json_column_name: Option<&str>,
) -> proc_macro2::TokenStream {
    let json_column_name = if let Some(json_column_name) = json_column_name {
        let json_column_name = proc_macro2::TokenStream::from_str(json_column_name).unwrap();
        quote::quote!(Some(#json_column_name))
    } else {
        quote::quote!(None)
    };
    
    let db_column_name = db_column_name.as_str();
    quote::quote! {
        Some(my_postgres::RenderFullWhereCondition{
            column_name: #db_column_name,
            condition_no,
            json_prefix: #json_column_name
        })
    }
}
