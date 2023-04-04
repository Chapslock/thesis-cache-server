use crate::conf::{PATH_TO_TEST_FILES, TEST_FILES};
use std::{fs::File, io::Read, sync::Arc};

use rocksdb::DB;

pub fn populate_database_with_dummy_data(db: Arc<DB>) -> std::io::Result<()> {
    for file_name in &TEST_FILES {
        let mut file_path: String = PATH_TO_TEST_FILES.to_owned();
        file_path.push_str(&file_name);
        println!("{}", &file_path);
        let mut file = File::open(file_path)?;
        let mut file_contents = String::new();
        file.read_to_string(&mut file_contents)
            .expect("Failed to read from file!");
        db.put(file_name, &file_contents)
            .expect("Failed to add value to database!");
    }
    return Ok(());
}
