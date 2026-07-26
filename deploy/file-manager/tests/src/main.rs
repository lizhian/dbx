use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{bail, Result};
use opendal::services;
use opendal::{ErrorKind, Operator};

const FIXTURE: &[u8] = include_bytes!("../fixtures/source.txt");

#[tokio::main]
async fn main() -> Result<()> {
    let sftp_key = format!("{}/../runtime/sftp/id_ed25519", env!("CARGO_MANIFEST_DIR"));

    let operators = vec![
        ("FTP", ftp("ftp://127.0.0.1:2121")?),
        ("SFTP", sftp(&sftp_key)?),
        ("S3", s3()?),
        ("WebDAV", webdav()?),
        ("WebHDFS", webhdfs()?),
        ("HDFS Native", hdfs_native()?),
    ];

    let mut failures = Vec::new();
    for (name, operator) in operators {
        if let Err(error) = test_operator(name, operator).await {
            eprintln!("{name:<14} 失败  {error:#}");
            failures.push(name);
        }
    }

    if !failures.is_empty() {
        bail!("未通过的协议：{}", failures.join("、"));
    }
    println!("\n全部 6 个 OpenDAL 协议连接及文件操作验证通过。");
    Ok(())
}

fn ftp(endpoint: &str) -> Result<Operator> {
    let builder = services::Ftp::default().endpoint(endpoint).root("/ftp/dbx/").user("dbx").password("dbx-password");
    Ok(Operator::new(builder)?.finish())
}

fn sftp(private_key: &str) -> Result<Operator> {
    let builder = services::Sftp::default()
        .endpoint("ssh://127.0.0.1:2222")
        .root("/config")
        .user("dbx")
        .key(private_key)
        .known_hosts_strategy("Accept");
    Ok(Operator::new(builder)?.finish())
}

fn s3() -> Result<Operator> {
    let builder = services::S3::default()
        .endpoint("http://127.0.0.1:9000")
        .region("us-east-1")
        .bucket("dbx")
        .root("/root/")
        .access_key_id("dbx-access-key")
        .secret_access_key("dbx-secret-key")
        .disable_config_load()
        .disable_ec2_metadata();
    Ok(Operator::new(builder)?.finish())
}

fn webdav() -> Result<Operator> {
    let builder = services::Webdav::default()
        .endpoint("http://127.0.0.1:8080")
        .root("/")
        .username("dbx")
        .password("dbx-password");
    Ok(Operator::new(builder)?.finish())
}

fn webhdfs() -> Result<Operator> {
    let builder = services::Webhdfs::default().endpoint("http://127.0.0.1:9870").root("/").user_name("dbx");
    Ok(Operator::new(builder)?.finish())
}

fn hdfs_native() -> Result<Operator> {
    let options = HashMap::from([("dfs.client.use.datanode.hostname".to_string(), "true".to_string())]);
    let builder = services::HdfsNative::default().name_node("hdfs://127.0.0.1:19000").root("/").options(options);
    Ok(Operator::new(builder)?.finish())
}

async fn test_operator(name: &str, operator: Operator) -> Result<()> {
    let suffix = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let directory = format!("dbx-opendal-test-{suffix}/");
    let source = format!("{directory}source.txt");
    let copied = format!("{directory}copied.txt");
    let renamed = format!("{directory}renamed.txt");

    operator.create_dir(&directory).await?;
    operator.write(&source, FIXTURE.to_vec()).await?;

    let metadata = operator.stat(&source).await?;
    if !metadata.is_file() || metadata.content_length() != FIXTURE.len() as u64 {
        bail!("stat 返回的文件类型或长度不正确");
    }

    assert_content(&operator, &source).await?;

    let entries = operator.list(&directory).await?;
    if !entries.iter().any(|entry| entry.path() == source) {
        bail!("list 未返回刚写入的文件");
    }

    let copy_mode = copy_with_fallback(&operator, &source, &copied).await?;
    assert_content(&operator, &copied).await?;

    let rename_mode = rename_with_fallback(&operator, &copied, &renamed).await?;
    assert_content(&operator, &renamed).await?;
    if operator.exists(&copied).await? {
        bail!("rename 后源文件仍然存在");
    }

    operator.delete(&source).await?;
    operator.delete(&renamed).await?;
    operator.delete(&directory).await?;

    if operator.exists(&source).await? || operator.exists(&renamed).await? {
        bail!("delete 后文件仍然存在");
    }

    println!("{name:<14} 通过  write/read/stat/list/delete；copy={copy_mode}；rename={rename_mode}");
    Ok(())
}

async fn assert_content(operator: &Operator, path: &str) -> Result<()> {
    let actual = operator.read(path).await?.to_vec();
    if actual != FIXTURE {
        bail!("read 返回内容与测试文件不一致");
    }
    Ok(())
}

async fn copy_with_fallback(operator: &Operator, from: &str, to: &str) -> Result<&'static str> {
    match operator.copy(from, to).await {
        Ok(_) => Ok("原生"),
        Err(error) if error.kind() == ErrorKind::Unsupported => {
            let content = operator.read(from).await?;
            operator.write(to, content).await?;
            Ok("OpenDAL read+write 回退")
        }
        Err(error) => Err(error.into()),
    }
}

async fn rename_with_fallback(operator: &Operator, from: &str, to: &str) -> Result<&'static str> {
    match operator.rename(from, to).await {
        Ok(()) => Ok("原生"),
        Err(error) if error.kind() == ErrorKind::Unsupported => {
            let copy_mode = copy_with_fallback(operator, from, to).await?;
            operator.delete(from).await?;
            if copy_mode == "原生" {
                Ok("OpenDAL copy+delete 回退")
            } else {
                Ok("OpenDAL read+write+delete 回退")
            }
        }
        Err(error) => Err(error.into()),
    }
}
