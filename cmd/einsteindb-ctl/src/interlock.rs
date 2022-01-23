use encryption_export::data_key_manager_from_config;
use embedded_engine_rocks::raw_util::{db_exist, new_embedded_engine_opt};
use embedded_engine_rocks::Rocksembedded_engine;
use embedded_engine_traits::{
    embedded_engines, Error as embedded_engineError, VioletaBFTembedded_engine, ALL_CFS, CF_DEFAULT, CF_LOCK, CF_WRITE, DATA_CFS,
};
use futures::{executor::block_on, future, stream, Stream, StreamExt, TryStreamExt};
use grpcio::{ChannelBuilder, Environment};
use ekvproto::debugpb::{Db as DBType, *};
use ekvproto::kvrpcpb::MvccInfo;
use ekvproto::metapb::{Peer, Brane};
use ekvproto::violetabft_cmdpb::VioletaBFTCmdRequest;
use ekvproto::violetabft_serverpb::PeerState;
use fidel_client::{Config as fidelConfig, fidelClient, RpcClient};
use protobuf::Message;
use violetabft::evioletabftpb::{ConfChange, ConfChangeV2, Entry, EntryType};
use violetabft_log_embedded_engine::VioletaBFTLogembedded_engine;
use violetabftstore::store::INIT_EPOCH_CONF_VER;
use security::SecurityManager;
use serde_json::json;
use std::borrow::ToOwned;
use std::cmp::Ordering;
use std::path::PathBuf;
use std::pin::Pin;
use std::string::ToString;
use std::sync::Arc;
use std::time::Duration;
use std::{str, u64};
use einsteindb::config::{ConfigController, TiKvConfig};
use einsteindb::server::debug::{BottommostLevelCompaction, Debugger, BraneInfo};
use einsteindb_util::escape;

use crate::util::*;

pub const METRICS_PROMETHEUS: &str = "prometheus";
pub const METRICS_ROCKSDB_KV: &str = "rocksdb_kv";
pub const METRICS_ROCKSDB_RAFT: &str = "rocksdb_violetabft";
pub const METRICS_JEMALLOC: &str = "jemalloc";
pub const LOCK_FILE_ERROR: &str = "IO error: While lock file";

type MvccInfoStream = Pin<Box<dyn Stream<Item = Result<(Vec<u8>, MvccInfo), String>>>>;

pub fn new_debug_executor(
    cfg: &TiKvConfig,
    data_dir: Option<&str>,
    skip_paranoid_checks: bool,
    host: Option<&str>,
    mgr: Arc<SecurityManager>,
) -> Box<dyn DebugExecutor> {
    if let Some(remote) = host {
        return Box::new(new_debug_client(remote, mgr)) as Box<dyn DebugExecutor>;
    }

    // TODO: perhaps we should allow user skip specifying data path.
    let data_dir = data_dir.unwrap();
    let kv_path = cfg.infer_kv_embedded_engine_path(Some(data_dir)).unwrap();

    let key_manager = data_key_manager_from_config(&cfg.security.encryption, &cfg.storage.data_dir)
        .unwrap()
        .map(Arc::new);

    let cache = cfg.storage.block_cache.build_shared_cache();
    let shared_block_cache = cache.is_some();
    let env = cfg
        .build_shared_rocks_env(key_manager.clone(), None /*io_rate_limiter*/)
        .unwrap();

    let mut kv_db_opts = cfg.rocksdb.build_opt();
    kv_db_opts.set_env(env.clone());
    kv_db_opts.set_paranoid_checks(!skip_paranoid_checks);
    let kv_cfs_opts = cfg
        .rocksdb
        .build_cf_opts(&cache, None, cfg.storage.api_version());
    let kv_path = PathBuf::from(kv_path).canonicalize().unwrap();
    let kv_path = kv_path.to_str().unwrap();
    let kv_db = match new_embedded_engine_opt(kv_path, kv_db_opts, kv_cfs_opts) {
        Ok(db) => db,
        Err(e) => handle_embedded_engine_error(e),
    };
    let mut kv_db = Rocksembedded_engine::from_db(Arc::new(kv_db));
    kv_db.set_shared_block_cache(shared_block_cache);

    let cfg_controller = ConfigController::default();
    if !cfg.violetabft_embedded_engine.enable {
        let mut violetabft_db_opts = cfg.violetabftdb.build_opt();
        violetabft_db_opts.set_env(env);
        let violetabft_db_cf_opts = cfg.violetabftdb.build_cf_opts(&cache);
        let violetabft_path = cfg.infer_violetabft_db_path(Some(data_dir)).unwrap();
        if !db_exist(&violetabft_path) {
            error!("violetabft db not exists: {}", violetabft_path);
            einsteindb_util::logger::exit_process_gracefully(-1);
        }
        let violetabft_db = match new_embedded_engine_opt(&violetabft_path, violetabft_db_opts, violetabft_db_cf_opts) {
            Ok(db) => db,
            Err(e) => handle_embedded_engine_error(e),
        };
        let mut violetabft_db = Rocksembedded_engine::from_db(Arc::new(violetabft_db));
        violetabft_db.set_shared_block_cache(shared_block_cache);
        let debugger = Debugger::new(embedded_engines::new(kv_db, violetabft_db), cfg_controller);
        Box::new(debugger) as Box<dyn DebugExecutor>
    } else {
        let mut config = cfg.violetabft_embedded_engine.config();
        config.dir = cfg.infer_violetabft_embedded_engine_path(Some(data_dir)).unwrap();
        if !VioletaBFTLogembedded_engine::exists(&config.dir) {
            error!("violetabft embedded_engine not exists: {}", config.dir);
            einsteindb_util::logger::exit_process_gracefully(-1);
        }
        let violetabft_db = VioletaBFTLogembedded_engine::new(config, key_manager, None /*io_rate_limiter*/).unwrap();
        let debugger = Debugger::new(embedded_engines::new(kv_db, violetabft_db), cfg_controller);
        Box::new(debugger) as Box<dyn DebugExecutor>
    }
}

pub fn new_debug_client(host: &str, mgr: Arc<SecurityManager>) -> DebugClient {
    let env = Arc::new(Environment::new(1));
    let cb = ChannelBuilder::new(env)
        .max_receive_message_len(1 << 30) // 1G.
        .max_send_message_len(1 << 30)
        .keepalive_time(Duration::from_secs(10))
        .keepalive_timeout(Duration::from_secs(3));

    let channel = mgr.connect(cb, host);
    DebugClient::new(channel)
}

pub trait DebugExecutor {
    fn dump_value(&self, cf: &str, key: Vec<u8>) {
        let value = self.get_value_by_key(cf, key);
        println!("value: {}", escape(&value));
    }

    fn dump_brane_size(&self, brane: u64, cfs: Vec<&str>) -> usize {
        let sizes = self.get_brane_size(brane, cfs);
        let mut total_size = 0;
        println!("brane id: {}", brane);
        for (cf, size) in sizes {
            println!("cf {} brane size: {}", cf, convert_gbmb(size as u64));
            total_size += size;
        }
        total_size
    }

    fn dump_all_brane_size(&self, cfs: Vec<&str>) {
        let branes = self.get_all_branes_in_store();
        let branes_number = branes.len();
        let mut total_size = 0;
        for brane in branes {
            total_size += self.dump_brane_size(brane, cfs.clone());
        }
        println!("total brane number: {}", branes_number);
        println!("total brane size: {}", convert_gbmb(total_size as u64));
    }

    fn dump_brane_info(&self, brane_ids: Option<Vec<u64>>, skip_tombstone: bool) {
        let brane_ids = brane_ids.unwrap_or_else(|| self.get_all_branes_in_store());
        let mut brane_objects = serde_json::map::Map::new();
        for brane_id in brane_ids {
            let r = self.get_brane_info(brane_id);
            if skip_tombstone {
                let brane_state = r.brane_local_state.as_ref();
                if brane_state.map_or(false, |s| s.get_state() == PeerState::Tombstone) {
                    return;
                }
            }
            let brane_object = json!({
                "brane_id": brane_id,
                "brane_local_state": r.brane_local_state.map(|s| {
                    let r = s.get_brane();
                    let brane_epoch = r.get_brane_epoch();
                    let peers = r.get_peers();
                    json!({
                        "brane": json!({
                            "id": r.get_id(),
                            "start_key": hex::encode_upper(r.get_start_key()),
                            "end_key": hex::encode_upper(r.get_end_key()),
                            "brane_epoch": json!({
                                "conf_ver": brane_epoch.get_conf_ver(),
                                "version": brane_epoch.get_version()
                            }),
                            "peers": peers.iter().map(|p| json!({
                                "id": p.get_id(),
                                "store_id": p.get_store_id(),
                                "role": format!("{:?}", p.get_role()),
                            })).collect::<Vec<_>>(),
                        }),
                    })
                }),
                "violetabft_local_state": r.violetabft_local_state.map(|s| {
                    let hard_state = s.get_hard_state();
                    json!({
                        "hard_state": json!({
                            "term": hard_state.get_term(),
                            "vote": hard_state.get_vote(),
                            "commit": hard_state.get_commit(),
                        }),
                        "last_index": s.get_last_index(),
                    })
                }),
                "violetabft_apply_state": r.violetabft_apply_state.map(|s| {
                    let truncated_state = s.get_truncated_state();
                    json!({
                        "applied_index": s.get_applied_index(),
                        "commit_index": s.get_commit_index(),
                        "commit_term": s.get_commit_term(),
                        "truncated_state": json!({
                            "index": truncated_state.get_index(),
                            "term": truncated_state.get_term(),
                        })
                    })
                })
            });
            brane_objects.insert(brane_id.to_string(), brane_object);
        }

        println!(
            "{}",
            serde_json::to_string_pretty(&json!({ "brane_infos": brane_objects })).unwrap()
        );
    }

    fn dump_violetabft_log(&self, brane: u64, index: u64) {
        let idx_key = keys::violetabft_log_key(brane, index);
        println!("idx_key: {}", escape(&idx_key));
        println!("brane: {}", brane);
        println!("log index: {}", index);

        let mut entry = self.get_violetabft_log(brane, index);
        let data = entry.take_data();
        println!("entry {:?}", entry);
        println!("msg len: {}", data.len());

        if data.is_empty() {
            return;
        }

        match entry.get_entry_type() {
            EntryType::EntryNormal => {
                let mut msg = VioletaBFTCmdRequest::default();
                msg.merge_from_bytes(&data).unwrap();
                println!("Normal: {:#?}", msg);
            }
            EntryType::EntryConfChange => {
                let mut msg = ConfChange::new();
                msg.merge_from_bytes(&data).unwrap();
                let ctx = msg.take_context();
                println!("ConfChange: {:?}", msg);
                let mut cmd = VioletaBFTCmdRequest::default();
                cmd.merge_from_bytes(&ctx).unwrap();
                println!("ConfChange.VioletaBFTCmdRequest: {:#?}", cmd);
            }
            EntryType::EntryConfChangeV2 => {
                let mut msg = ConfChangeV2::new();
                msg.merge_from_bytes(&data).unwrap();
                let ctx = msg.take_context();
                println!("ConfChangeV2: {:?}", msg);
                let mut cmd = VioletaBFTCmdRequest::default();
                cmd.merge_from_bytes(&ctx).unwrap();
                println!("ConfChangeV2.VioletaBFTCmdRequest: {:#?}", cmd);
            }
        }
    }

    /// Dump mvcc infos for a given key range. The given `from` and `to` must
    /// be raw key with `z` prefix. Both `to` and `limit` are empty value means
    /// what we want is point query instead of range scan.
    fn dump_mvccs_infos(
        &self,
        from: Vec<u8>,
        to: Vec<u8>,
        mut limit: u64,
        cfs: Vec<&str>,
        start_ts: Option<u64>,
        commit_ts: Option<u64>,
    ) {
        if !from.starts_with(b"z") || (!to.is_empty() && !to.starts_with(b"z")) {
            println!("from and to should start with \"z\"");
            einsteindb_util::logger::exit_process_gracefully(-1);
        }
        if !to.is_empty() && to < from {
            println!("\"to\" must be greater than \"from\"");
            einsteindb_util::logger::exit_process_gracefully(-1);
        }

        cfs.iter().for_each(|cf| {
            if !DATA_CFS.contains(cf) {
                eprintln!("CF \"{}\" doesn't exist.", cf);
                einsteindb_util::logger::exit_process_gracefully(-1);
            }
        });

        let point_query = to.is_empty() && limit == 0;
        if point_query {
            limit = 1;
        }

        let scan_future =
            self.get_mvcc_infos(from.clone(), to, limit)
                .try_for_each(move |(key, mvcc)| {
                    if point_query && key != from {
                        println!("no mvcc infos for {}", escape(&from));
                        return future::err::<(), String>("no mvcc infos".to_owned());
                    }

                    println!("key: {}", escape(&key));
                    if cfs.contains(&CF_LOCK) && mvcc.has_lock() {
                        let lock_info = mvcc.get_lock();
                        if start_ts.map_or(true, |ts| lock_info.get_start_ts() == ts) {
                            println!("\tlock cf value: {:?}", lock_info);
                        }
                    }
                    if cfs.contains(&CF_DEFAULT) {
                        for value_info in mvcc.get_values() {
                            if commit_ts.map_or(true, |ts| value_info.get_start_ts() == ts) {
                                println!("\tdefault cf value: {:?}", value_info);
                            }
                        }
                    }
                    if cfs.contains(&CF_WRITE) {
                        for write_info in mvcc.get_writes() {
                            if start_ts.map_or(true, |ts| write_info.get_start_ts() == ts)
                                && commit_ts.map_or(true, |ts| write_info.get_commit_ts() == ts)
                            {
                                println!("\t write cf value: {:?}", write_info);
                            }
                        }
                    }
                    println!();
                    future::ok::<(), String>(())
                });
        if let Err(e) = block_on(scan_future) {
            println!("{}", e);
            einsteindb_util::logger::exit_process_gracefully(-1);
        }
    }

    fn raw_scan(&self, from_key: &[u8], to_key: &[u8], limit: usize, cf: &str) {
        if !ALL_CFS.contains(&cf) {
            eprintln!("CF \"{}\" doesn't exist.", cf);
            einsteindb_util::logger::exit_process_gracefully(-1);
        }
        if !to_key.is_empty() && from_key >= to_key {
            eprintln!(
                "to_key should be greater than from_key, but got from_key: \"{}\", to_key: \"{}\"",
                escape(from_key),
                escape(to_key)
            );
            einsteindb_util::logger::exit_process_gracefully(-1);
        }
        if limit == 0 {
            eprintln!("limit should be greater than 0");
            einsteindb_util::logger::exit_process_gracefully(-1);
        }

        self.raw_scan_impl(from_key, to_key, limit, cf);
    }

    fn diff_brane(
        &self,
        brane: u64,
        to_host: Option<&str>,
        to_data_dir: Option<&str>,
        to_config: &TiKvConfig,
        mgr: Arc<SecurityManager>,
    ) {
        let rhs_debug_executor = new_debug_executor(to_config, to_data_dir, false, to_host, mgr);

        let r1 = self.get_brane_info(brane);
        let r2 = rhs_debug_executor.get_brane_info(brane);
        println!("brane id: {}", brane);
        println!("db1 brane state: {:?}", r1.brane_local_state);
        println!("db2 brane state: {:?}", r2.brane_local_state);
        println!("db1 apply state: {:?}", r1.violetabft_apply_state);
        println!("db2 apply state: {:?}", r2.violetabft_apply_state);

        match (r1.brane_local_state, r2.brane_local_state) {
            (None, None) => {}
            (Some(_), None) | (None, Some(_)) => {
                println!("db1 and db2 don't have same brane local_state");
            }
            (Some(brane_local_1), Some(brane_local_2)) => {
                let brane1 = brane_local_1.get_brane();
                let brane2 = brane_local_2.get_brane();
                if brane1 != brane2 {
                    println!("db1 and db2 have different brane:");
                    println!("db1 brane: {:?}", brane1);
                    println!("db2 brane: {:?}", brane2);
                    return;
                }
                let start_key = keys::data_key(brane1.get_start_key());
                let end_key = keys::data_key(brane1.get_end_key());
                let mut mvcc_infos_1 = self.get_mvcc_infos(start_key.clone(), end_key.clone(), 0);
                let mut mvcc_infos_2 = rhs_debug_executor.get_mvcc_infos(start_key, end_key, 0);

                let mut has_diff = false;
                let mut key_counts = [0; 2];

                let mut take_item = |i: usize| -> Option<(Vec<u8>, MvccInfo)> {
                    let wait = match i {
                        1 => block_on(future::poll_fn(|cx| mvcc_infos_1.poll_next_unpin(cx))),
                        _ => block_on(future::poll_fn(|cx| mvcc_infos_2.poll_next_unpin(cx))),
                    };
                    match wait? {
                        Err(e) => {
                            println!("db{} scan data in brane {} fail: {}", i, brane, e);
                            einsteindb_util::logger::exit_process_gracefully(-1);
                        }
                        Ok(s) => Some(s),
                    }
                };

                let show_only = |i: usize, k: &[u8]| {
                    println!("only db{} has: {}", i, escape(k));
                };

                let (mut item1, mut item2) = (take_item(1), take_item(2));
                while item1.is_some() && item2.is_some() {
                    key_counts[0] += 1;
                    key_counts[1] += 1;
                    let t1 = item1.take().unwrap();
                    let t2 = item2.take().unwrap();
                    match t1.0.cmp(&t2.0) {
                        Ordering::Less => {
                            show_only(1, &t1.0);
                            has_diff = true;
                            item1 = take_item(1);
                            item2 = Some(t2);
                            key_counts[1] -= 1;
                        }
                        Ordering::Greater => {
                            show_only(2, &t2.0);
                            has_diff = true;
                            item1 = Some(t1);
                            item2 = take_item(2);
                            key_counts[0] -= 1;
                        }
                        _ => {
                            if t1.1 != t2.1 {
                                println!("diff mvcc on key: {}", escape(&t1.0));
                                has_diff = true;
                            }
                            item1 = take_item(1);
                            item2 = take_item(2);
                        }
                    }
                }
                let mut item = item1.map(|t| (1, t)).or_else(|| item2.map(|t| (2, t)));
                while let Some((i, (key, _))) = item.take() {
                    key_counts[i - 1] += 1;
                    show_only(i, &key);
                    has_diff = true;
                    item = take_item(i).map(|t| (i, t));
                }
                if !has_diff {
                    println!("db1 and db2 have same data in brane: {}", brane);
                }
                println!(
                    "db1 has {} keys, db2 has {} keys",
                    key_counts[0], key_counts[1]
                );
            }
        }
    }

    fn compact(
        &self,
        address: Option<&str>,
        db: DBType,
        cf: &str,
        from: Option<Vec<u8>>,
        to: Option<Vec<u8>>,
        threads: u32,
        bottommost: BottommostLevelCompaction,
    ) {
        let from = from.unwrap_or_default();
        let to = to.unwrap_or_default();
        self.do_compaction(db, cf, &from, &to, threads, bottommost);
        println!(
            "store:{:?} compact db:{:?} cf:{} range:[{:?}, {:?}) success!",
            address.unwrap_or("local"),
            db,
            cf,
            from,
            to
        );
    }

    fn compact_brane(
        &self,
        address: Option<&str>,
        db: DBType,
        cf: &str,
        brane_id: u64,
        threads: u32,
        bottommost: BottommostLevelCompaction,
    ) {
        let brane_local = self.get_brane_info(brane_id).brane_local_state.unwrap();
        let r = brane_local.get_brane();
        let from = keys::data_key(r.get_start_key());
        let to = keys::data_end_key(r.get_end_key());
        self.do_compaction(db, cf, &from, &to, threads, bottommost);
        println!(
            "store:{:?} compact_brane db:{:?} cf:{} range:[{:?}, {:?}) success!",
            address.unwrap_or("local"),
            db,
            cf,
            from,
            to
        );
    }

    fn print_bad_branes(&self);

    fn set_brane_tombstone_after_remove_peer(
        &self,
        mgr: Arc<SecurityManager>,
        cfg: &fidelConfig,
        brane_ids: Vec<u64>,
    ) {
        self.check_local_mode();
        let rpc_client =
            RpcClient::new(cfg, None, mgr).unwrap_or_else(|e| perror_and_exit("RpcClient::new", e));

        let branes = brane_ids
            .into_iter()
            .map(|brane_id| {
                if let Some(brane) = block_on(rpc_client.get_brane_by_id(brane_id))
                    .unwrap_or_else(|e| perror_and_exit("Get brane id from fidel", e))
                {
                    return brane;
                }
                println!("no such brane in fidel: {}", brane_id);
                einsteindb_util::logger::exit_process_gracefully(-1);
            })
            .collect();
        self.set_brane_tombstone(branes);
    }

    fn set_brane_tombstone_force(&self, brane_ids: Vec<u64>) {
        self.check_local_mode();
        self.set_brane_tombstone_by_id(brane_ids);
    }

    /// Recover the cluster when given `store_ids` are failed.
    fn remove_fail_stores(
        &self,
        store_ids: Vec<u64>,
        brane_ids: Option<Vec<u64>>,
        promote_learner: bool,
    );

    fn drop_unapplied_violetabftlog(&self, brane_ids: Option<Vec<u64>>);

    /// Recreate the brane with metadata from fidel, but alloc new id for it.
    fn recreate_brane(&self, sec_mgr: Arc<SecurityManager>, fidel_cfg: &fidelConfig, brane_id: u64);

    fn check_brane_consistency(&self, _: u64);

    fn check_local_mode(&self);

    fn recover_branes_mvcc(
        &self,
        mgr: Arc<SecurityManager>,
        cfg: &fidelConfig,
        brane_ids: Vec<u64>,
        read_only: bool,
    ) {
        self.check_local_mode();
        let rpc_client =
            RpcClient::new(cfg, None, mgr).unwrap_or_else(|e| perror_and_exit("RpcClient::new", e));

        let branes = brane_ids
            .into_iter()
            .map(|brane_id| {
                if let Some(brane) = block_on(rpc_client.get_brane_by_id(brane_id))
                    .unwrap_or_else(|e| perror_and_exit("Get brane id from fidel", e))
                {
                    return brane;
                }
                println!("no such brane in fidel: {}", brane_id);
                einsteindb_util::logger::exit_process_gracefully(-1);
            })
            .collect();
        self.recover_branes(branes, read_only);
    }

    fn recover_mvcc_all(&self, threads: usize, read_only: bool) {
        self.check_local_mode();
        self.recover_all(threads, read_only);
    }

    fn get_all_branes_in_store(&self) -> Vec<u64>;

    fn get_value_by_key(&self, cf: &str, key: Vec<u8>) -> Vec<u8>;

    fn get_brane_size(&self, brane: u64, cfs: Vec<&str>) -> Vec<(String, usize)>;

    fn get_brane_info(&self, brane: u64) -> BraneInfo;

    fn get_violetabft_log(&self, brane: u64, index: u64) -> Entry;

    fn get_mvcc_infos(&self, from: Vec<u8>, to: Vec<u8>, limit: u64) -> MvccInfoStream;

    fn raw_scan_impl(&self, from_key: &[u8], end_key: &[u8], limit: usize, cf: &str);

    fn do_compaction(
        &self,
        db: DBType,
        cf: &str,
        from: &[u8],
        to: &[u8],
        threads: u32,
        bottommost: BottommostLevelCompaction,
    );

    fn set_brane_tombstone(&self, branes: Vec<Brane>);

    fn set_brane_tombstone_by_id(&self, branes: Vec<u64>);

    fn recover_branes(&self, branes: Vec<Brane>, read_only: bool);

    fn recover_all(&self, threads: usize, read_only: bool);

    fn modify_einsteindb_config(&self, config_name: &str, config_value: &str);

    fn dump_metrics(&self, tags: Vec<&str>);

    fn dump_brane_properties(&self, brane_id: u64);

    fn dump_range_properties(&self, start: Vec<u8>, end: Vec<u8>);

    fn dump_store_info(&self);

    fn dump_cluster_info(&self);
}

impl DebugExecutor for DebugClient {
    fn check_local_mode(&self) {
        println!("This command is only for local mode");
        einsteindb_util::logger::exit_process_gracefully(-1);
    }

    fn get_all_branes_in_store(&self) -> Vec<u64> {
        DebugClient::get_all_branes_in_store(self, &GetAllBranesInStoreRequest::default())
            .unwrap_or_else(|e| perror_and_exit("DebugClient::get_all_branes_in_store", e))
            .take_branes()
    }

    fn get_value_by_key(&self, cf: &str, key: Vec<u8>) -> Vec<u8> {
        let mut req = GetRequest::default();
        req.set_db(DBType::Kv);
        req.set_cf(cf.to_owned());
        req.set_key(key);
        self.get(&req)
            .unwrap_or_else(|e| perror_and_exit("DebugClient::get", e))
            .take_value()
    }

    fn get_brane_size(&self, brane: u64, cfs: Vec<&str>) -> Vec<(String, usize)> {
        let cfs = cfs.into_iter().map(ToOwned::to_owned).collect::<Vec<_>>();
        let mut req = BraneSizeRequest::default();
        req.set_cfs(cfs.into());
        req.set_brane_id(brane);
        self.brane_size(&req)
            .unwrap_or_else(|e| perror_and_exit("DebugClient::brane_size", e))
            .take_entries()
            .into_iter()
            .map(|mut entry| (entry.take_cf(), entry.get_size() as usize))
            .collect()
    }

    fn get_brane_info(&self, brane: u64) -> BraneInfo {
        let mut req = BraneInfoRequest::default();
        req.set_brane_id(brane);
        let mut resp = self
            .brane_info(&req)
            .unwrap_or_else(|e| perror_and_exit("DebugClient::brane_info", e));

        let mut brane_info = BraneInfo::default();
        if resp.has_violetabft_local_state() {
            brane_info.violetabft_local_state = Some(resp.take_violetabft_local_state());
        }
        if resp.has_violetabft_apply_state() {
            brane_info.violetabft_apply_state = Some(resp.take_violetabft_apply_state());
        }
        if resp.has_brane_local_state() {
            brane_info.brane_local_state = Some(resp.take_brane_local_state());
        }
        brane_info
    }

    fn get_violetabft_log(&self, brane: u64, index: u64) -> Entry {
        let mut req = VioletaBFTLogRequest::default();
        req.set_brane_id(brane);
        req.set_log_index(index);
        self.violetabft_log(&req)
            .unwrap_or_else(|e| perror_and_exit("DebugClient::violetabft_log", e))
            .take_entry()
    }

    fn get_mvcc_infos(&self, from: Vec<u8>, to: Vec<u8>, limit: u64) -> MvccInfoStream {
        let mut req = ScanMvccRequest::default();
        req.set_from_key(from);
        req.set_to_key(to);
        req.set_limit(limit);
        Box::pin(
            self.scan_mvcc(&req)
                .unwrap()
                .map_err(|e| e.to_string())
                .map_ok(|mut resp| (resp.take_key(), resp.take_info())),
        )
    }

    fn raw_scan_impl(&self, _: &[u8], _: &[u8], _: usize, _: &str) {
        unimplemented!();
    }

    fn do_compaction(
        &self,
        db: DBType,
        cf: &str,
        from: &[u8],
        to: &[u8],
        threads: u32,
        bottommost: BottommostLevelCompaction,
    ) {
        let mut req = CompactRequest::default();
        req.set_db(db);
        req.set_cf(cf.to_owned());
        req.set_from_key(from.to_owned());
        req.set_to_key(to.to_owned());
        req.set_threads(threads);
        req.set_bottommost_level_compaction(bottommost.into());
        self.compact(&req)
            .unwrap_or_else(|e| perror_and_exit("DebugClient::compact", e));
    }

    fn dump_metrics(&self, tags: Vec<&str>) {
        let mut req = GetMetricsRequest::default();
        req.set_all(true);
        if tags.len() == 1 && tags[0] == METRICS_PROMETHEUS {
            req.set_all(false);
        }
        let mut resp = self
            .get_metrics(&req)
            .unwrap_or_else(|e| perror_and_exit("DebugClient::metrics", e));
        for tag in tags {
            println!("tag:{}", tag);
            let metrics = match tag {
                METRICS_ROCKSDB_KV => resp.take_rocksdb_kv(),
                METRICS_ROCKSDB_RAFT => resp.take_rocksdb_violetabft(),
                METRICS_JEMALLOC => resp.take_jemalloc(),
                METRICS_PROMETHEUS => resp.take_prometheus(),
                _ => String::from(
                    "unsupported tag, should be one of prometheus/jemalloc/rocksdb_violetabft/rocksdb_kv",
                ),
            };
            println!("{}", metrics);
        }
    }

    fn set_brane_tombstone(&self, _: Vec<Brane>) {
        unimplemented!("only available for local mode");
    }

    fn set_brane_tombstone_by_id(&self, _: Vec<u64>) {
        unimplemented!("only available for local mode");
    }

    fn recover_branes(&self, _: Vec<Brane>, _: bool) {
        unimplemented!("only available for local mode");
    }

    fn recover_all(&self, _: usize, _: bool) {
        unimplemented!("only available for local mode");
    }

    fn print_bad_branes(&self) {
        unimplemented!("only available for local mode");
    }

    fn remove_fail_stores(&self, _: Vec<u64>, _: Option<Vec<u64>>, _: bool) {
        unimplemented!("only available for local mode");
    }

    fn drop_unapplied_violetabftlog(&self, _: Option<Vec<u64>>) {
        unimplemented!("only available for local mode");
    }

    fn recreate_brane(&self, _: Arc<SecurityManager>, _: &fidelConfig, _: u64) {
        unimplemented!("only available for local mode");
    }

    fn check_brane_consistency(&self, brane_id: u64) {
        let mut req = BraneConsistencyCheckRequest::default();
        req.set_brane_id(brane_id);
        self.check_brane_consistency(&req)
            .unwrap_or_else(|e| perror_and_exit("DebugClient::check_brane_consistency", e));
        println!("success!");
    }

    fn modify_einsteindb_config(&self, config_name: &str, config_value: &str) {
        let mut req = ModifyTikvConfigRequest::default();
        req.set_config_name(config_name.to_owned());
        req.set_config_value(config_value.to_owned());
        self.modify_einsteindb_config(&req)
            .unwrap_or_else(|e| perror_and_exit("DebugClient::modify_einsteindb_config", e));
        println!("success");
    }

    fn dump_brane_properties(&self, brane_id: u64) {
        let mut req = GetBranePropertiesRequest::default();
        req.set_brane_id(brane_id);
        let resp = self
            .get_brane_properties(&req)
            .unwrap_or_else(|e| perror_and_exit("DebugClient::get_brane_properties", e));
        for prop in resp.get_props() {
            println!("{}: {}", prop.get_name(), prop.get_value());
        }
    }

    fn dump_range_properties(&self, _: Vec<u8>, _: Vec<u8>) {
        unimplemented!("only available for local mode");
    }

    fn dump_store_info(&self) {
        let req = GetStoreInfoRequest::default();
        let resp = self
            .get_store_info(&req)
            .unwrap_or_else(|e| perror_and_exit("DebugClient::get_store_info", e));
        println!("store id: {}", resp.get_store_id());
        println!("api version: {:?}", resp.get_api_version())
    }

    fn dump_cluster_info(&self) {
        let req = GetClusterInfoRequest::default();
        let resp = self
            .get_cluster_info(&req)
            .unwrap_or_else(|e| perror_and_exit("DebugClient::get_cluster_info", e));
        println!("{}", resp.get_cluster_id())
    }
}

impl<ER: VioletaBFTembedded_engine> DebugExecutor for Debugger<ER> {
    fn check_local_mode(&self) {}

    fn get_all_branes_in_store(&self) -> Vec<u64> {
        self.get_all_branes_in_store()
            .unwrap_or_else(|e| perror_and_exit("Debugger::get_all_branes_in_store", e))
    }

    fn get_value_by_key(&self, cf: &str, key: Vec<u8>) -> Vec<u8> {
        self.get(DBType::Kv, cf, &key)
            .unwrap_or_else(|e| perror_and_exit("Debugger::get", e))
    }

    fn get_brane_size(&self, brane: u64, cfs: Vec<&str>) -> Vec<(String, usize)> {
        self.brane_size(brane, cfs)
            .unwrap_or_else(|e| perror_and_exit("Debugger::brane_size", e))
            .into_iter()
            .map(|(cf, size)| (cf.to_owned(), size as usize))
            .collect()
    }

    fn get_brane_info(&self, brane: u64) -> BraneInfo {
        self.brane_info(brane)
            .unwrap_or_else(|e| perror_and_exit("Debugger::brane_info", e))
    }

    fn get_violetabft_log(&self, brane: u64, index: u64) -> Entry {
        self.violetabft_log(brane, index)
            .unwrap_or_else(|e| perror_and_exit("Debugger::violetabft_log", e))
    }

    fn get_mvcc_infos(&self, from: Vec<u8>, to: Vec<u8>, limit: u64) -> MvccInfoStream {
        let iter = self
            .scan_mvcc(&from, &to, limit)
            .unwrap_or_else(|e| perror_and_exit("Debugger::scan_mvcc", e));
        let stream = stream::iter(iter).map_err(|e| e.to_string());
        Box::pin(stream)
    }

    fn raw_scan_impl(&self, from_key: &[u8], end_key: &[u8], limit: usize, cf: &str) {
        let res = self
            .raw_scan(from_key, end_key, limit, cf)
            .unwrap_or_else(|e| perror_and_exit("Debugger::raw_scan_impl", e));

        for (k, v) in &res {
            println!("key: \"{}\", value: \"{}\"", escape(k), escape(v));
        }
        println!();
        println!("Total scanned keys: {}", res.len());
    }

    fn do_compaction(
        &self,
        db: DBType,
        cf: &str,
        from: &[u8],
        to: &[u8],
        threads: u32,
        bottommost: BottommostLevelCompaction,
    ) {
        self.compact(db, cf, from, to, threads, bottommost)
            .unwrap_or_else(|e| perror_and_exit("Debugger::compact", e));
    }

    fn set_brane_tombstone(&self, branes: Vec<Brane>) {
        let ret = self
            .set_brane_tombstone(branes)
            .unwrap_or_else(|e| perror_and_exit("Debugger::set_brane_tombstone", e));
        if ret.is_empty() {
            println!("success!");
            return;
        }
        for (brane_id, error) in ret {
            println!("brane: {}, error: {}", brane_id, error);
        }
    }

    fn set_brane_tombstone_by_id(&self, brane_ids: Vec<u64>) {
        let ret = self
            .set_brane_tombstone_by_id(brane_ids)
            .unwrap_or_else(|e| perror_and_exit("Debugger::set_brane_tombstone_by_id", e));
        if ret.is_empty() {
            println!("success!");
            return;
        }
        for (brane_id, error) in ret {
            println!("brane: {}, error: {}", brane_id, error);
        }
    }

    fn recover_branes(&self, branes: Vec<Brane>, read_only: bool) {
        let ret = self
            .recover_branes(branes, read_only)
            .unwrap_or_else(|e| perror_and_exit("Debugger::recover branes", e));
        if ret.is_empty() {
            println!("success!");
            return;
        }
        for (brane_id, error) in ret {
            println!("brane: {}, error: {}", brane_id, error);
        }
    }

    fn recover_all(&self, threads: usize, read_only: bool) {
        Debugger::recover_all(self, threads, read_only)
            .unwrap_or_else(|e| perror_and_exit("Debugger::recover all", e));
    }

    fn print_bad_branes(&self) {
        let bad_branes = self
            .bad_branes()
            .unwrap_or_else(|e| perror_and_exit("Debugger::bad_branes", e));
        if !bad_branes.is_empty() {
            for (brane_id, error) in bad_branes {
                println!("{}: {}", brane_id, error);
            }
            return;
        }
        println!("all branes are healthy")
    }

    fn remove_fail_stores(
        &self,
        store_ids: Vec<u64>,
        brane_ids: Option<Vec<u64>>,
        promote_learner: bool,
    ) {
        println!("removing stores {:?} from configurations...", store_ids);
        self.remove_failed_stores(store_ids, brane_ids, promote_learner)
            .unwrap_or_else(|e| perror_and_exit("Debugger::remove_fail_stores", e));
        println!("success");
    }

    fn drop_unapplied_violetabftlog(&self, brane_ids: Option<Vec<u64>>) {
        println!("removing unapplied violetabftlog on brane {:?} ...", brane_ids);
        self.drop_unapplied_violetabftlog(brane_ids)
            .unwrap_or_else(|e| perror_and_exit("Debugger::remove_fail_stores", e));
        println!("success");
    }

    fn recreate_brane(&self, mgr: Arc<SecurityManager>, fidel_cfg: &fidelConfig, brane_id: u64) {
        let rpc_client = RpcClient::new(fidel_cfg, None, mgr)
            .unwrap_or_else(|e| perror_and_exit("RpcClient::new", e));

        let mut brane = match block_on(rpc_client.get_brane_by_id(brane_id)) {
            Ok(Some(brane)) => brane,
            Ok(None) => {
                println!("no such brane {} on fidel", brane_id);
                einsteindb_util::logger::exit_process_gracefully(-1);
            }
            Err(e) => perror_and_exit("RpcClient::get_brane_by_id", e),
        };

        let new_brane_id = rpc_client
            .alloc_id()
            .unwrap_or_else(|e| perror_and_exit("RpcClient::alloc_id", e));
        let new_peer_id = rpc_client
            .alloc_id()
            .unwrap_or_else(|e| perror_and_exit("RpcClient::alloc_id", e));

        let store_id = self.get_store_ident().expect("get store id").store_id;

        brane.set_id(new_brane_id);
        let old_version = brane.get_brane_epoch().get_version();
        brane.mut_brane_epoch().set_version(old_version + 1);
        brane.mut_brane_epoch().set_conf_ver(INIT_EPOCH_CONF_VER);

        brane.peers.clear();
        let mut peer = Peer::default();
        peer.set_id(new_peer_id);
        peer.set_store_id(store_id);
        brane.mut_peers().push(peer);

        println!(
            "initing empty brane {} with peer_id {}...",
            new_brane_id, new_peer_id
        );
        self.recreate_brane(brane)
            .unwrap_or_else(|e| perror_and_exit("Debugger::recreate_brane", e));
        println!("success");
    }

    fn dump_metrics(&self, _tags: Vec<&str>) {
        unimplemented!("only available for online mode");
    }

    fn check_brane_consistency(&self, _: u64) {
        println!("only support remote mode");
        einsteindb_util::logger::exit_process_gracefully(-1);
    }

    fn modify_einsteindb_config(&self, _: &str, _: &str) {
        println!("only support remote mode");
        einsteindb_util::logger::exit_process_gracefully(-1);
    }

    fn dump_brane_properties(&self, brane_id: u64) {
        let props = self
            .get_brane_properties(brane_id)
            .unwrap_or_else(|e| perror_and_exit("Debugger::get_brane_properties", e));
        for (name, value) in props {
            println!("{}: {}", name, value);
        }
    }

    fn dump_range_properties(&self, start: Vec<u8>, end: Vec<u8>) {
        let props = self
            .get_range_properties(&start, &end)
            .unwrap_or_else(|e| perror_and_exit("Debugger::get_range_properties", e));
        for (name, value) in props {
            println!("{}: {}", name, value);
        }
    }

    fn dump_store_info(&self) {
        let store_ident_info = self.get_store_ident();
        if let Ok(ident) = store_ident_info {
            println!("store id: {}", ident.get_store_id());
            println!("api version: {:?}", ident.get_api_version());
        }
    }

    fn dump_cluster_info(&self) {
        let store_ident_info = self.get_store_ident();
        if let Ok(ident) = store_ident_info {
            println!("cluster id: {}", ident.get_cluster_id());
        }
    }
}

fn handle_embedded_engine_error(err: embedded_engineError) -> ! {
    error!("error while open kvdb: {}", err);
    if let embedded_engineError::embedded_engine(msg) = err {
        if msg.starts_with(LOCK_FILE_ERROR) {
            error!(
                "LOCK file conflict indicates TiKV process is running. \
                Do NOT delete the LOCK file and force the command to run. \
                Doing so could cause data corruption."
            );
        }
    }

    einsteindb_util::logger::exit_process_gracefully(-1);
}