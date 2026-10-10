pub struct Rc4 {
    s: [u8; 256],
    i: u8,
    j: u8,
}

impl Rc4 {
    pub fn new(key: &[u8]) -> Self {
        let mut s = [0u8; 256];
        for (idx, b) in s.iter_mut().enumerate() {
            *b = idx as u8;
        }
        if !key.is_empty() {
            let mut j = 0u8;
            for i in 0..256 {
                j = j.wrapping_add(s[i]).wrapping_add(key[i % key.len()]);
                s.swap(i, j as usize);
            }
        }
        Self { s, i: 0, j: 0 }
    }

    pub fn apply_keystream(&mut self, data: &mut [u8]) {
        for byte in data.iter_mut() {
            self.i = self.i.wrapping_add(1);
            self.j = self.j.wrapping_add(self.s[self.i as usize]);
            self.s.swap(self.i as usize, self.j as usize);
            let k = self.s[self.s[self.i as usize].wrapping_add(self.s[self.j as usize]) as usize];
            *byte ^= k;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Rc4;

    #[test]
    fn known_answer_vector() {
        let mut c = Rc4::new(b"Key");
        let mut buf = *b"Plaintext";
        c.apply_keystream(&mut buf);
        assert_eq!(buf, [0xBB, 0xF3, 0x16, 0xE8, 0xD9, 0x40, 0xAF, 0x0A, 0xD3]);
    }

    #[test]
    fn keystream_is_contiguous_across_calls() {
        let mut whole = Rc4::new(b"secret");
        let mut a = [1u8; 8];
        whole.apply_keystream(&mut a);

        let mut split = Rc4::new(b"secret");
        let mut b = [1u8; 8];
        split.apply_keystream(&mut b[..3]);
        split.apply_keystream(&mut b[3..]);
        assert_eq!(a, b);
    }

    #[test]
    fn roundtrips() {
        let key = b"another-key";
        let mut enc = Rc4::new(key);
        let mut dec = Rc4::new(key);
        let mut buf = *b"the quick brown fox";
        enc.apply_keystream(&mut buf);
        assert_ne!(&buf, b"the quick brown fox");
        dec.apply_keystream(&mut buf);
        assert_eq!(&buf, b"the quick brown fox");
    }
}
