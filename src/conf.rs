pub static NOTFOUND: &[u8] = b"Not Found";
pub static CACHE_DATABASE_PATH: &str = "./database";
pub static CHUNK_SIZE: u64 = 10 * 1024 * 1024; // 10MB
pub static LOGGING_ENABLED: bool = false;

//*
pub static SERVER_IP: [u8; 4] = [0, 0, 0, 0];
pub static SERVER_PORT: u16 = 80;
pub static PATH_TO_TEST_FILES: &str = "/var/www/html/";
pub static TEST_FILES: [&str; 4] = ["500KB.html", "1MB.html", "10MB.html", "100MB.html"];
// */
/*
pub static SERVER_IP: [u8; 4] = [127, 0, 0, 1];
pub static SERVER_PORT: u16 = 3000;
pub static PATH_TO_TEST_FILES: &str = "C:\\Users\\Charl.Kivioja\\Desktop\\http-test-server\\testFiles\\";
pub static TEST_FILES: [&str; 2] = ["500KB.html", "100MB.html"];
// */