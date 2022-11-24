use std::ops::{Deref, DerefMut};

use tokio::sync::{RwLockReadGuard, RwLockWriteGuard};

use crate::*;

pub struct RentedConnection<'s> {
    connection: RwLockReadGuard<'s, Option<PostgresConnection>>,
}

impl<'s> RentedConnection<'s> {
    pub fn new(
        src: RwLockReadGuard<'s, Option<PostgresConnection>>,
    ) -> Result<RentedConnection, MyPostgressError> {
        let result = Self { connection: src };

        if result.connection.is_none() {
            return Err(MyPostgressError::NoConnection);
        }

        Ok(result)
    }
}

impl std::ops::Deref for RentedConnection<'_> {
    type Target = PostgresConnection;

    fn deref(&self) -> &Self::Target {
        self.connection.as_ref().unwrap()
    }
}

/// Mut

pub struct RentedConnectionMut<'s> {
    connection: RwLockWriteGuard<'s, Option<PostgresConnection>>,
}

impl<'s> RentedConnectionMut<'s> {
    pub fn new(
        src: RwLockWriteGuard<'s, Option<PostgresConnection>>,
    ) -> Result<RentedConnectionMut, MyPostgressError> {
        let result = Self { connection: src };

        if result.connection.is_none() {
            return Err(MyPostgressError::NoConnection);
        }
        Ok(result)
    }
}

impl Deref for RentedConnectionMut<'_> {
    type Target = PostgresConnection;

    fn deref(&self) -> &Self::Target {
        self.connection.as_ref().unwrap()
    }
}

impl DerefMut for RentedConnectionMut<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.connection.as_mut().unwrap()
    }
}
