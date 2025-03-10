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
            if struct_prop.attrs.has_attr("limit") || (struct_prop.name == "limit" && struct_prop.ty.is_usize()) {
                limit = Some(struct_prop);
            } else if struct_prop.attrs.has_attr("offset")|| (struct_prop.name == "offset" && struct_prop.ty.is_usize()) {
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

    pub fn has_at_least_one_non_optional_field(&self)->bool{
        for itm in &self.where_fields{

            if itm.ty.is_vec(){
                continue;
            }

            if itm.ty.is_option() && itm.has_ignore_if_none_attr(){
                continue;
            }

            return true;
        }

        false
    }


    pub fn get_fields_with_programmatically_understanding_has_condition(&self)->Vec<&StructProperty>{
        let mut result = Vec::new();
        for itm in &self.where_fields{
            if itm.ty.is_option() && itm.has_ignore_if_none_attr(){
                result.push(*itm);
            }

            if itm.ty.is_vec(){
                result.push(*itm);
            }

        }

        result
    }


    pub fn generate_has_conditions_fn(&self) -> proc_macro2::TokenStream {

        if self.where_fields.len() == 0{
            return quote::quote!(false)    ;
        }


        if self.has_at_least_one_non_optional_field(){
            return quote::quote!(true);
        }

        let fields_to_render = self.get_fields_with_programmatically_understanding_has_condition();

        if fields_to_render.len() == 0{
            return quote::quote! {true};
        }


        let mut result = Vec::new();

        for itm in fields_to_render{
            let prop_name_ident = itm.get_field_name_ident();
            let ignore_if_none = itm.has_ignore_if_none_attr();


            if itm.ty.is_vec(){
                result.push(quote::quote! {
                    if self.#prop_name_ident.len()>0{
                        return true;
                    }
                });
            }
            else
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


            let inside_json =  prop.inside_json()?;

            let where_condition = render_full_where_condition(&db_column_name, None, inside_json);


            if prop.has_inline_where_model_attr(){

                lines.push(quote::quote!{

                    if self.#prop_name_ident.has_conditions(){
                        if condition_no > 0 {
                            sql.push_str(" AND ");
                        }
    
                        sql.push('(');
                        self.#prop_name_ident.fill_where_component(sql, params);
                        sql.push(')');
                    }
                });
                continue;
            }


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
    inside_json: Option<&str>,
) -> proc_macro2::TokenStream {
    let json_column_name = if let Some(json_column_name) = json_column_name {
        let json_column_name = proc_macro2::TokenStream::from_str(json_column_name).unwrap();
        quote::quote!(#json_column_name.clone())
    } else {
        quote::quote!(vec![])
    };
    
    let db_column_name = if let Some(inside_json) = inside_json{
        let mut  result = String::new();

        for (n, part) in inside_json.split('.').enumerate(){

            if n ==0{
                result.push('"');
            }else{
                result.push('\'');
            }

            result.push_str(part);
            if n ==0{
                result.push('"');
            }
            else{
                result.push('\'');
            }

            result.push_str("->>");
        }

        result.push('\'');
        result.push_str(db_column_name.as_str());
        result.push('\'');
        result
    }else{
        db_column_name.as_str().to_string()
    };
    
    quote::quote! {
        Some(my_postgres::RenderFullWhereCondition{
            column_name: #db_column_name.into(),
            condition_no,
            json_prefix: #json_column_name
        })
    }
}
