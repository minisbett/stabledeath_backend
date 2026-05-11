mod impls;
pub mod models;

use color_eyre::{Result, eyre::Context};
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use sqlx::sqlite::{SqliteStatement, SqliteTypeInfo};
use sqlx::{Describe, Error as SqlxError, Execute, SqlitePool};
use sqlx::{
    Either, Executor, Sqlite, Transaction,
    pool::PoolConnection,
    sqlite::{SqlitePoolOptions, SqliteQueryResult, SqliteRow},
};

#[derive(Debug)]
pub struct Database {
    pool: sqlx::sqlite::SqlitePool,
}

impl Database {
    pub async fn new(uri: &str) -> Result<Self> {
        let pool = SqlitePoolOptions::new().connect(uri).await?;

        Ok(Self { pool })
    }

    pub async fn acquire(&self) -> Result<PoolConnection<Sqlite>> {
        self.pool
            .acquire()
            .await
            .wrap_err("Failed to acquire an SQLite connection")
    }

    pub async fn begin(&self) -> Result<Transaction<'static, Sqlite>> {
        self.pool
            .begin()
            .await
            .wrap_err("Failed to start an SQLite transaction")
    }
}

impl<'d, 'p> Executor<'p> for &'d Database {
    type Database = Sqlite;

    #[inline]
    fn fetch_many<'e, 'q: 'e, E: 'q>(
        self,
        query: E,
    ) -> BoxStream<'e, std::result::Result<Either<SqliteQueryResult, SqliteRow>, SqlxError>>
    where
        'p: 'e,
        E: Execute<'q, Self::Database>,
    {
        <&SqlitePool as Executor<'p>>::fetch_many(&self.pool, query)
    }

    #[inline]
    fn fetch_optional<'e, 'q: 'e, E: 'q>(
        self,
        query: E,
    ) -> BoxFuture<'e, Result<Option<SqliteRow>, SqlxError>>
    where
        'p: 'e,
        E: Execute<'q, Self::Database>,
    {
        <&SqlitePool as Executor<'p>>::fetch_optional(&self.pool, query)
    }

    #[inline]
    fn prepare_with<'e, 'q: 'e>(
        self,
        sql: &'q str,
        parameters: &'e [SqliteTypeInfo],
    ) -> BoxFuture<'e, Result<SqliteStatement<'q>, SqlxError>>
    where
        'p: 'e,
    {
        <&SqlitePool as Executor<'p>>::prepare_with(&self.pool, sql, parameters)
    }

    #[inline]
    fn describe<'e, 'q: 'e>(
        self,
        sql: &'q str,
    ) -> BoxFuture<'e, Result<Describe<Self::Database>, SqlxError>>
    where
        'p: 'e,
    {
        <&SqlitePool as Executor<'p>>::describe(&self.pool, sql)
    }
}
