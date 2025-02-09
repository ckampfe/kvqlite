use sqlx::sqlite::SqliteQueryResult;
use sqlx::{Executor, SqliteConnection};
use std::future::Future;
use std::ops::{Deref, DerefMut};

pub(crate) trait SqliteConnectionExt {
    fn begin_immediate(&mut self) -> impl Future<Output = sqlx::Result<Transaction>>;
}

impl SqliteConnectionExt for SqliteConnection {
    async fn begin_immediate(&mut self) -> sqlx::Result<Transaction> {
        let conn = &mut *self;

        conn.execute("BEGIN IMMEDIATE;").await?;

        Ok(Transaction {
            conn,
            is_open: true,
        })
    }
}

pub(crate) struct Transaction<'c> {
    conn: &'c mut SqliteConnection,
    /// is the transaction open?
    is_open: bool,
}

impl Transaction<'_> {
    pub(crate) async fn commit(mut self) -> sqlx::Result<SqliteQueryResult> {
        let res = self.conn.execute("COMMIT;").await;

        if res.is_ok() {
            self.is_open = false;
        }

        res
    }
}

impl Drop for Transaction<'_> {
    fn drop(&mut self) {
        if self.is_open {
            // this is probably expensive and not-ideal but
            // I'm not sure how else to hook into sqlx's runtime
            std::thread::scope(|s| {
                s.spawn(|| {
                    let rt = tokio::runtime::Builder::new_current_thread()
                        .build()
                        .unwrap();
                    rt.block_on(async {
                        let _ = self.execute("ROLLBACK;").await;
                    });
                });
            });
        }
    }
}

impl Deref for Transaction<'_> {
    type Target = SqliteConnection;

    fn deref(&self) -> &Self::Target {
        self.conn
    }
}

impl DerefMut for Transaction<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.conn
    }
}
