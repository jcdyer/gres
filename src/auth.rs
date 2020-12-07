use crypto::digest::Digest;
use crypto::md5::Md5;

pub fn build_md5_hash(user: &str, password: &str, salt: &[u8]) -> String {
    let mut userpasshasher = Md5::new();
    let mut final_hash = String::with_capacity(35);
    final_hash.push_str("md5");

    userpasshasher.input_str(password);
    userpasshasher.input_str(user);
    let hash = userpasshasher.result_str();
    let mut saltedhasher = Md5::new();
    saltedhasher.input_str(&hash);
    saltedhasher.input(salt);
    final_hash.push_str(&saltedhasher.result_str());
    final_hash
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_md5_hash() {
        assert_eq!(build_md5_hash("", "", b"abcd"), "md5743b08b8561cc75c4f899c35d6c3c3eb");
    }
}
