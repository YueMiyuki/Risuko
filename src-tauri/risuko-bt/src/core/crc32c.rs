const POLYNOMIAL: u32 = 0x82F6_3B78;

const TABLE: [u32; 256] = build_table();

const fn build_table() -> [u32; 256] {
    let mut table = [0u32; 256];
    let mut index = 0;
    while index < 256 {
        let mut crc = index as u32;
        let mut bit = 0;
        while bit < 8 {
            crc = if crc & 1 == 1 {
                (crc >> 1) ^ POLYNOMIAL
            } else {
                crc >> 1
            };
            bit += 1;
        }
        table[index] = crc;
        index += 1;
    }
    table
}

pub(crate) fn crc32c(data: &[u8]) -> u32 {
    !data.iter().fold(u32::MAX, |crc, &byte| {
        (crc >> 8) ^ TABLE[usize::from((crc as u8) ^ byte)]
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn check_value_matches_the_castagnoli_reference() {
        assert_eq!(crc32c(b"123456789"), 0xE306_9283);
    }

    #[test]
    fn empty_input_hashes_to_zero() {
        assert_eq!(crc32c(b""), 0);
    }

    #[test]
    fn rfc_3720_iscsi_vectors_match() {
        assert_eq!(crc32c(&[0u8; 32]), 0x8A91_36AA);
        assert_eq!(crc32c(&[0xFFu8; 32]), 0x62A8_AB43);
        let ascending: Vec<u8> = (0..32).collect();
        assert_eq!(crc32c(&ascending), 0x46DD_794E);
        let descending: Vec<u8> = (0..32).rev().collect();
        assert_eq!(crc32c(&descending), 0x113F_DB5C);
    }
}
