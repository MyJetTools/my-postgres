use super::SqlWhereModel;

pub struct NoneWhereModel;

impl NoneWhereModel {
    pub fn new() -> Option<&'static Self> {
        None
    }
}

impl SqlWhereModel for NoneWhereModel {
    fn get_where_field_name_data<'s>(&'s self, _no: usize) -> Option<super::WhereFieldData<'s>> {
        None
    }

    fn get_limit(&self) -> Option<usize> {
        None
    }

    fn get_offset(&self) -> Option<usize> {
        None
    }
}
