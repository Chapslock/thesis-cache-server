use rocksdb::{DB, Options, DBCompactionStyle};
use std::sync::Arc;

pub trait DBOperations {
    fn init(file_path: &str) -> Self;
    fn save(&self, k: &str, v: &str) -> bool;
    fn find(&self, k: &str) -> Option<String>;
    fn delete(&self, k: &str) -> bool;
}

#[derive(Clone)]
pub struct Database {
    inst: Arc<DB>,
}

impl DBOperations for Database {
    fn init(file_path: &str) -> Self {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.set_max_open_files(1000);
        opts.increase_parallelism(3);
        opts.set_use_fsync(false);
        opts.set_bytes_per_sync(8388608);
        opts.optimize_for_point_lookup(1024);
        opts.set_table_cache_num_shard_bits(6);
        opts.set_max_write_buffer_number(32);
        opts.set_write_buffer_size(536870912);
        opts.set_target_file_size_base(1073741824);
        opts.set_min_write_buffer_number_to_merge(4);
        opts.set_level_zero_stop_writes_trigger(2000);
        opts.set_level_zero_slowdown_writes_trigger(0);
        opts.set_compaction_style(DBCompactionStyle::Universal);
        opts.set_disable_auto_compactions(true);
        return Database {         
            inst: Arc::new(DB::open(&opts, file_path).unwrap()),
        };
    }

    fn save(&self, k: &str, v: &str) -> bool {
        return self.inst.put(k.as_bytes(), v.as_bytes()).is_ok();
    }

    fn find(&self, k: &str) -> Option<String> {
        match self.inst.get(k.as_bytes()) {
            Ok(Some(v)) => {
                let result = String::from_utf8(v).unwrap();
                println!("Cache hit for key : {} \nReturning value!", k);
                return Some(result);
            }
            Ok(None) => {
                println!("Cache miss for key : {} \nReturning None", k);
                return None;
            }
            Err(e) => {
                println!(
                    "Error occured while retrieving value with key: {} \n Error: {}",
                    k, e
                );
                return None;
            }
        }
    }

    fn delete(&self, k: &str) -> bool {
        return self.inst.delete(k.as_bytes()).is_ok();
    }
}
