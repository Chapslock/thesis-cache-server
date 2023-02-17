use rocksdb::{DB, Options};
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
        opts.set_use_direct_reads(true); 
        opts.set_use_direct_io_for_flush_and_compaction(true);
        opts.set_compaction_readahead_size(2 * 1024 * 1024); // 2MB
        opts.set_writable_file_max_buffer_size(1024 * 1024); // 1MB
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
                println!("Cache hit for key : {}", k);
                return Some(result);
            }
            Ok(None) => {
                println!("Cache miss for key : {}", k);
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
