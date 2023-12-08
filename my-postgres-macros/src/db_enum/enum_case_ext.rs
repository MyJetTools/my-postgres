use rust_extensions::StrOrString;
use types_reader::EnumCase;

use crate::attributes::EnumCaseAttribute;

pub struct EnumCaseValue<'s> {
    pub attr: Option<EnumCaseAttribute<'s>>,
    pub value: &'s EnumCase<'s>,
}

impl<'s> EnumCaseValue<'s> {
    pub fn get_value_as_str(&self) -> StrOrString<'s> {
        if let Some(attr) = &self.attr {
            if let Some(value) = attr.value {
                return value.into();
            }

            if let Some(value) = attr.id {
                return value.into();
            }
        }

        if let Some(attr) = &self.attr {
            if let Some(value) = attr.value {
                return value.into();
            }
        }

        self.value.get_name_ident().to_string().into()
    }

    pub fn as_number(&self) -> Result<Option<i64>, syn::Error> {
        if let Some(attr) = &self.attr {
            if let Some(value) = attr.value {
                match value.parse() {
                    Ok(value) => return Ok(Some(value)),
                    Err(err) => {
                        return Err(syn::Error::new_spanned(self.value.get_name_ident(), err))
                    }
                }
            }

            if let Some(value) = attr.id {
                match value.parse() {
                    Ok(value) => return Ok(Some(value)),
                    Err(err) => {
                        return Err(syn::Error::new_spanned(self.value.get_name_ident(), err))
                    }
                }
            }
        }

        Ok(None)
    }
}

pub trait EnumCaseExt<'s> {
    fn get_value(&'s self) -> Result<EnumCaseValue<'s>, syn::Error>;
}

impl<'s> EnumCaseExt<'s> for EnumCase<'s> {
    fn get_value(&'s self) -> Result<EnumCaseValue<'s>, syn::Error> {
        let attr: Option<EnumCaseAttribute> = self.try_get_attribute()?;

        let result = EnumCaseValue { attr, value: self };
        Ok(result)
    }
}
