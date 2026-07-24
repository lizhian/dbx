use anyhow::{bail, Context, Result};
#[path = "../webhdfs_gate.rs"]
mod webhdfs_gate;
use webhdfs_gate::{
    candidate_a_abort, candidate_a_copy, candidate_a_write, candidate_b_copy, candidate_b_write, delete_path,
    GateConfig, OPEN_DAL_VERSION,
};

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let config = GateConfig::from_env()?;
    let command = args.first().map(String::as_str).unwrap_or("help");
    match command {
        "info" => {
            println!(
                "{}",
                serde_json::json!({
                    "opendal_version": OPEN_DAL_VERSION,
                    "endpoint": config.endpoint.as_str(),
                    "root": config.root,
                    "atomic_write_dir": config.atomic_write_dir,
                    "chunk_bytes": config.chunk_bytes,
                })
            );
        }
        "write-a" => {
            let (path, size) = path_and_size(&args)?;
            println!("{}", serde_json::to_string(&candidate_a_write(&config, path, size).await?)?);
        }
        "copy-a" => {
            let (source, destination) = two_paths(&args)?;
            println!("{}", serde_json::to_string(&candidate_a_copy(&config, source, destination).await?)?);
        }
        "abort-a" => {
            let path = args.get(1).context("usage: webhdfs_gate abort-a <path>")?;
            candidate_a_abort(&config, path).await?;
            println!("{}", serde_json::json!({"candidate":"atomic-concat","operation":"abort","cleanup":"passed"}));
        }
        "write-b" => {
            let (path, size) = path_and_size(&args)?;
            println!("{}", serde_json::to_string(&candidate_b_write(&config, path, size).await?)?);
        }
        "copy-b" => {
            let (source, destination) = two_paths(&args)?;
            println!("{}", serde_json::to_string(&candidate_b_copy(&config, source, destination).await?)?);
        }
        "delete" => {
            let path = args.get(1).context("usage: webhdfs_gate delete <path>")?;
            println!("{}", serde_json::json!({"deleted": delete_path(&config, path).await?}));
        }
        "help" | "--help" | "-h" => print_help(),
        other => bail!("unknown command {other}; run webhdfs_gate help"),
    }
    Ok(())
}

fn path_and_size(args: &[String]) -> Result<(&str, u64)> {
    let path = args.get(1).context("missing remote path")?;
    let size = args.get(2).context("missing byte count")?.parse::<u64>().context("byte count must be an integer")?;
    Ok((path, size))
}

fn two_paths(args: &[String]) -> Result<(&str, &str)> {
    Ok((args.get(1).context("missing source path")?, args.get(2).context("missing destination path")?))
}

fn print_help() {
    println!(
        "WebHDFS Gate (OpenDAL {OPEN_DAL_VERSION})

Commands:
  info
  write-a <path> <bytes>       OpenDAL atomic_write_dir + CONCAT
  copy-a <source> <dest>       bounded OpenDAL fallback copy
  abort-a <path>               verify temporary-block cleanup
  write-b <path> <bytes>       narrow CREATE -> 307 -> streaming PUT
  copy-b <source> <dest>       narrow bounded OPEN/CREATE relay
  delete <path>

Required environment:
  WEBHDFS_GATE_ENDPOINT

See docs/testing/webhdfs-gate.md for the complete environment and release Gate."
    );
}
