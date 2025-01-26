use sqlx::sqlite::SqliteConnectOptions;
use sqlx::{Connection, SqliteConnection};
use std::path::Path;
use std::{env, fs};
use tokio::runtime::Runtime;

fn main() -> anyhow::Result<()> {
    // Compile protos
    let out_dir = Path::new(env::var("OUT_DIR")?.as_str()).join("protos");
    if out_dir.exists() {
        fs::remove_dir_all(&out_dir)?;
    }
    fs::create_dir_all(&out_dir)?;
    let protoc = protoc_bin_vendored::protoc_bin_path()?;
    let mut prost_config = prost_build::Config::new();
    prost_config.protoc_executable(protoc);
    prost_config.out_dir(out_dir);
    prost_config.compile_protos(&["protos/common.proto"], &[""])?;

    prost_config.extern_path(".common", "crate::protos");
    prost_config.compile_protos(&["protos/wal.proto"], &[""])?;
    println!("cargo:rerun-if-changed=protos");

    // Set up a temporary sqlite database, so SQLx's compile time
    // syntax check has something to connect to
    let db_file = Path::new(env::var("OUT_DIR").unwrap().as_str()).join("_sqlx_sqlite_db.tmp");
    if db_file.exists() {
        fs::remove_file(&db_file).unwrap();
    }

    {
        // Create a new db and run migrations to create the schema
        let db_file = db_file.clone();
        let rt = Runtime::new().unwrap();
        rt.block_on(async move {
            let mut conn = SqliteConnection::connect_with(
                &SqliteConnectOptions::new()
                    .create_if_missing(true)
                    .filename(db_file),
            )
            .await
            .unwrap();
            sqlx::migrate!("./migrations").run(&mut conn).await.unwrap();
        })
    }

    // pass the path to the newly created sqlite db to sqlx via the `DATABASE_URL` env variable
    println!(
        "cargo:rustc-env=DATABASE_URL=sqlite:{}",
        db_file.clone().into_os_string().into_string().unwrap()
    );
    println!("cargo:rerun-if-changed=migrations");

    Ok(())
}
