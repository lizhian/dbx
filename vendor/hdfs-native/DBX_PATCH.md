# DBX hdfs-native patch

This directory vendors `hdfs-native` 0.13.5 from crates.io. The original
package metadata and `.cargo_vcs_info.json` are retained. `LICENSE` contains
the Apache License 2.0 declared by the upstream package.

DBX changes `Pipeline` task handles to cancellation-safe owned handles and
aborts any unfinished acknowledgement listener, packet sender, and heartbeat
task from `Drop`. This covers both an unclosed writer and cancellation while
`Pipeline::shutdown` is awaiting a DataNode task. Without this patch Tokio
detaches dropped `JoinHandle`s, which can retain a DataNode socket indefinitely.

DBX also aborts an unfinished replicated block-read listener when its stream is
dropped or moves to another replica, and terminates the listener after its first
read failure. This bounds DataNode sockets when downloads or relay copies are
cancelled.

Finally, DBX owns and aborts both background tasks of `RpcConnection` and
fails outstanding call waiters when the connection is dropped. This prevents
NameNode listener or sender sockets from surviving cache eviction and
connection-test timeouts.

The process-global DataNode connection cache is disabled because its entries
are not scoped by DBX connection or identity and its expiry is only evaluated
by a later read. Completed reads therefore close their sockets immediately.
Socket connection futures are also awaited directly so cancellation cannot
detach an in-progress OS connect.

DataNode address selection now honors Hadoop's
`dfs.client.use.datanode.hostname` option, while retaining IP-address behavior
by default.

The lease-renewal task now holds a `Weak<NamenodeProtocol>` and releases each
temporary strong reference before sleeping. This removes the
protocol-to-task-to-protocol ownership cycle created after the first write.

Remove this patch after DBX upgrades to an upstream release with equivalent
`Pipeline` drop semantics and cancellation regression coverage.
