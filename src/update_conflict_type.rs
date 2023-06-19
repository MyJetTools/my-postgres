use rust_extensions::StrOrString;

pub enum UpdateConflictType<'s> {
    OnPrimaryKeyConstraint(StrOrString<'s>),
    OnUniqueColumnNameConstraint(StrOrString<'s>),
}

impl<'s> UpdateConflictType<'s> {
    pub fn generate_sql(&self, dest: &mut String) {
        match self {
            UpdateConflictType::OnPrimaryKeyConstraint(column_name) => {
                dest.push_str(" ON CONFLICT ON CONSTRAINT ");
                dest.push_str(column_name.as_str());
            }
            UpdateConflictType::OnUniqueColumnNameConstraint(column_name) => {
                dest.push_str(" ON CONFLICT (");
                dest.push_str(column_name.as_str());
                dest.push_str(")");
            }
        }
    }
}

pub trait IntoUpdateConflictType<'s> {
    fn to_primary_string_constrain(self) -> UpdateConflictType<'s>;
    fn to_unique_column_name_constraint(self) -> UpdateConflictType<'s>;
}

impl<'s> IntoUpdateConflictType<'s> for &'s str {
    fn to_primary_string_constrain(self) -> UpdateConflictType<'s> {
        UpdateConflictType::OnPrimaryKeyConstraint(StrOrString::create_as_str(self))
    }

    fn to_unique_column_name_constraint(self) -> UpdateConflictType<'s> {
        UpdateConflictType::OnUniqueColumnNameConstraint(StrOrString::create_as_str(self))
    }
}

impl<'s> IntoUpdateConflictType<'s> for &'s String {
    fn to_primary_string_constrain(self) -> UpdateConflictType<'s> {
        UpdateConflictType::OnPrimaryKeyConstraint(StrOrString::create_as_str(self))
    }

    fn to_unique_column_name_constraint(self) -> UpdateConflictType<'s> {
        UpdateConflictType::OnUniqueColumnNameConstraint(StrOrString::create_as_str(self))
    }
}

impl<'s> IntoUpdateConflictType<'s> for String {
    fn to_primary_string_constrain(self) -> UpdateConflictType<'s> {
        UpdateConflictType::OnPrimaryKeyConstraint(StrOrString::create_as_string(self))
    }

    fn to_unique_column_name_constraint(self) -> UpdateConflictType<'s> {
        UpdateConflictType::OnUniqueColumnNameConstraint(StrOrString::create_as_string(self))
    }
}
