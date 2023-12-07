use types_reader::macros::*;

#[attribute_name("primary_key")]
#[derive(MacrosParameters)]
pub struct PrimaryKeyAttribute {
    #[default]
    pub id: Option<u8>,
}

impl PrimaryKeyAttribute {
    pub fn get_id(&self, prev_id: u8) -> u8 {
        if let Some(id) = self.id {
            id
        } else {
            prev_id + 1
        }
    }
}
