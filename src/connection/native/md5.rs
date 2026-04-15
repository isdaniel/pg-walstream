//! Minimal MD5 hash implementation for PostgreSQL password authentication.
//!
//! Based on [RFC 1321](https://www.ietf.org/rfc/rfc1321.txt).
//! Only used for the PostgreSQL MD5 authentication handshake.

/// Compute the MD5 digest of the given data, returning the 16-byte hash.
pub(crate) fn compute(data: &[u8]) -> [u8; 16] {
    let mut ctx = Context::new();
    ctx.consume(data);
    ctx.finalize()
}

pub(crate) struct Context {
    buffer: [u8; 64],
    count: u64,
    state: [u32; 4],
}

#[rustfmt::skip]
const PADDING: [u8; 64] = [
    0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
];

impl Context {
    pub(crate) fn new() -> Self {
        Self {
            buffer: [0; 64],
            count: 0,
            state: [0x67452301, 0xefcdab89, 0x98badcfe, 0x10325476],
        }
    }

    pub(crate) fn consume(&mut self, data: &[u8]) {
        let mut input = [0u32; 16];
        let mut k = ((self.count >> 3) & 0x3f) as usize;
        self.count = self.count.wrapping_add((data.len() as u64) << 3);
        for &value in data {
            self.buffer[k] = value;
            k += 1;
            if k == 0x40 {
                for (item, chunk) in input.iter_mut().zip(self.buffer.chunks_exact(4)) {
                    *item = u32::from_le_bytes(chunk.try_into().unwrap());
                }
                transform(&mut self.state, &input);
                k = 0;
            }
        }
    }

    pub(crate) fn finalize(mut self) -> [u8; 16] {
        let mut input = [0u32; 16];
        let k = ((self.count >> 3) & 0x3f) as usize;
        input[14] = self.count as u32;
        input[15] = (self.count >> 32) as u32;
        self.consume(&PADDING[..(if k < 56 { 56 - k } else { 120 - k })]);
        for (item, chunk) in input.iter_mut().take(14).zip(self.buffer.chunks_exact(4)) {
            *item = u32::from_le_bytes(chunk.try_into().unwrap());
        }
        transform(&mut self.state, &input);
        let mut digest = [0u8; 16];
        let mut j = 0;
        for i in 0..4 {
            digest[j] = (self.state[i] & 0xff) as u8;
            digest[j + 1] = ((self.state[i] >> 8) & 0xff) as u8;
            digest[j + 2] = ((self.state[i] >> 16) & 0xff) as u8;
            digest[j + 3] = ((self.state[i] >> 24) & 0xff) as u8;
            j += 4;
        }
        digest
    }
}

#[rustfmt::skip]
fn transform(state: &mut [u32; 4], input: &[u32; 16]) {
    let (mut a, mut b, mut c, mut d) = (state[0], state[1], state[2], state[3]);
    macro_rules! add(
        ($a:expr, $b:expr) => ($a.wrapping_add($b));
    );
    macro_rules! rotate(
        ($x:expr, $n:expr) => ($x.rotate_left($n));
    );
    {
        macro_rules! F(
            ($x:expr, $y:expr, $z:expr) => (($x & $y) | (!$x & $z));
        );
        macro_rules! T(
            ($a:expr, $b:expr, $c:expr, $d:expr, $x:expr, $s:expr, $ac:expr) => ({
                $a = add!(add!(add!($a, F!($b, $c, $d)), $x), $ac);
                $a = rotate!($a, $s);
                $a = add!($a, $b);
            });
        );
        const S1: u32 =  7;
        const S2: u32 = 12;
        const S3: u32 = 17;
        const S4: u32 = 22;
        T!(a, b, c, d, input[ 0], S1, 3614090360);
        T!(d, a, b, c, input[ 1], S2, 3905402710);
        T!(c, d, a, b, input[ 2], S3,  606105819);
        T!(b, c, d, a, input[ 3], S4, 3250441966);
        T!(a, b, c, d, input[ 4], S1, 4118548399);
        T!(d, a, b, c, input[ 5], S2, 1200080426);
        T!(c, d, a, b, input[ 6], S3, 2821735955);
        T!(b, c, d, a, input[ 7], S4, 4249261313);
        T!(a, b, c, d, input[ 8], S1, 1770035416);
        T!(d, a, b, c, input[ 9], S2, 2336552879);
        T!(c, d, a, b, input[10], S3, 4294925233);
        T!(b, c, d, a, input[11], S4, 2304563134);
        T!(a, b, c, d, input[12], S1, 1804603682);
        T!(d, a, b, c, input[13], S2, 4254626195);
        T!(c, d, a, b, input[14], S3, 2792965006);
        T!(b, c, d, a, input[15], S4, 1236535329);
    }
    {
        macro_rules! F(
            ($x:expr, $y:expr, $z:expr) => (($x & $z) | ($y & !$z));
        );
        macro_rules! T(
            ($a:expr, $b:expr, $c:expr, $d:expr, $x:expr, $s:expr, $ac:expr) => ({
                $a = add!(add!(add!($a, F!($b, $c, $d)), $x), $ac);
                $a = rotate!($a, $s);
                $a = add!($a, $b);
            });
        );
        const S1: u32 =  5;
        const S2: u32 =  9;
        const S3: u32 = 14;
        const S4: u32 = 20;
        T!(a, b, c, d, input[ 1], S1, 4129170786);
        T!(d, a, b, c, input[ 6], S2, 3225465664);
        T!(c, d, a, b, input[11], S3,  643717713);
        T!(b, c, d, a, input[ 0], S4, 3921069994);
        T!(a, b, c, d, input[ 5], S1, 3593408605);
        T!(d, a, b, c, input[10], S2,   38016083);
        T!(c, d, a, b, input[15], S3, 3634488961);
        T!(b, c, d, a, input[ 4], S4, 3889429448);
        T!(a, b, c, d, input[ 9], S1,  568446438);
        T!(d, a, b, c, input[14], S2, 3275163606);
        T!(c, d, a, b, input[ 3], S3, 4107603335);
        T!(b, c, d, a, input[ 8], S4, 1163531501);
        T!(a, b, c, d, input[13], S1, 2850285829);
        T!(d, a, b, c, input[ 2], S2, 4243563512);
        T!(c, d, a, b, input[ 7], S3, 1735328473);
        T!(b, c, d, a, input[12], S4, 2368359562);
    }
    {
        macro_rules! F(
            ($x:expr, $y:expr, $z:expr) => ($x ^ $y ^ $z);
        );
        macro_rules! T(
            ($a:expr, $b:expr, $c:expr, $d:expr, $x:expr, $s:expr, $ac:expr) => ({
                $a = add!(add!(add!($a, F!($b, $c, $d)), $x), $ac);
                $a = rotate!($a, $s);
                $a = add!($a, $b);
            });
        );
        const S1: u32 =  4;
        const S2: u32 = 11;
        const S3: u32 = 16;
        const S4: u32 = 23;
        T!(a, b, c, d, input[ 5], S1, 4294588738);
        T!(d, a, b, c, input[ 8], S2, 2272392833);
        T!(c, d, a, b, input[11], S3, 1839030562);
        T!(b, c, d, a, input[14], S4, 4259657740);
        T!(a, b, c, d, input[ 1], S1, 2763975236);
        T!(d, a, b, c, input[ 4], S2, 1272893353);
        T!(c, d, a, b, input[ 7], S3, 4139469664);
        T!(b, c, d, a, input[10], S4, 3200236656);
        T!(a, b, c, d, input[13], S1,  681279174);
        T!(d, a, b, c, input[ 0], S2, 3936430074);
        T!(c, d, a, b, input[ 3], S3, 3572445317);
        T!(b, c, d, a, input[ 6], S4,   76029189);
        T!(a, b, c, d, input[ 9], S1, 3654602809);
        T!(d, a, b, c, input[12], S2, 3873151461);
        T!(c, d, a, b, input[15], S3,  530742520);
        T!(b, c, d, a, input[ 2], S4, 3299628645);
    }
    {
        macro_rules! F(
            ($x:expr, $y:expr, $z:expr) => ($y ^ ($x | !$z));
        );
        macro_rules! T(
            ($a:expr, $b:expr, $c:expr, $d:expr, $x:expr, $s:expr, $ac:expr) => ({
                $a = add!(add!(add!($a, F!($b, $c, $d)), $x), $ac);
                $a = rotate!($a, $s);
                $a = add!($a, $b);
            });
        );
        const S1: u32 =  6;
        const S2: u32 = 10;
        const S3: u32 = 15;
        const S4: u32 = 21;
        T!(a, b, c, d, input[ 0], S1, 4096336452);
        T!(d, a, b, c, input[ 7], S2, 1126891415);
        T!(c, d, a, b, input[14], S3, 2878612391);
        T!(b, c, d, a, input[ 5], S4, 4237533241);
        T!(a, b, c, d, input[12], S1, 1700485571);
        T!(d, a, b, c, input[ 3], S2, 2399980690);
        T!(c, d, a, b, input[10], S3, 4293915773);
        T!(b, c, d, a, input[ 1], S4, 2240044497);
        T!(a, b, c, d, input[ 8], S1, 1873313359);
        T!(d, a, b, c, input[15], S2, 4264355552);
        T!(c, d, a, b, input[ 6], S3, 2734768916);
        T!(b, c, d, a, input[13], S4, 1309151649);
        T!(a, b, c, d, input[ 4], S1, 4149444226);
        T!(d, a, b, c, input[11], S2, 3174756917);
        T!(c, d, a, b, input[ 2], S3,  718787259);
        T!(b, c, d, a, input[ 9], S4, 3951481745);
    }
    state[0] = add!(state[0], a);
    state[1] = add!(state[1], b);
    state[2] = add!(state[2], c);
    state[3] = add!(state[3], d);
}

/// Helper to format a 16-byte digest as a lowercase hex string.
pub(crate) fn hex_digest(digest: &[u8; 16]) -> String {
    use std::fmt::Write;
    let mut s = String::with_capacity(32);
    for b in digest {
        let _ = write!(s, "{b:02x}");
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    /// RFC 1321 test vectors.
    #[test]
    fn test_rfc1321_vectors() {
        let cases: &[(&[u8], &str)] = &[
            (b"", "d41d8cd98f00b204e9800998ecf8427e"),
            (b"a", "0cc175b9c0f1b6a831c399e269772661"),
            (b"abc", "900150983cd24fb0d6963f7d28e17f72"),
            (b"message digest", "f96b697d7cb7938d525a2f31aaf161d0"),
            (
                b"abcdefghijklmnopqrstuvwxyz",
                "c3fcd3d76192e4007dfb496cca67e13b",
            ),
            (
                b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789",
                "d174ab98d277d9f5a5611c2c9f419d9f",
            ),
            (
                b"0123456789012345678901234567890123456789012345678901234567890123",
                "7f7bfd348709deeaace19e3f535f8c54",
            ),
            (
                b"12345678901234567890123456789012345678901234567890123456789012345678901234567890",
                "57edf4a22be3c955ac49da2e2107b67a",
            ),
        ];
        for (input, expected) in cases {
            let digest = compute(input);
            assert_eq!(hex_digest(&digest), *expected, "input: {:?}", input);
        }
    }

    /// Incremental consume produces the same result as one-shot compute.
    #[test]
    fn test_incremental_consume() {
        let mut ctx = Context::new();
        ctx.consume(b"abc");
        let digest = ctx.finalize();
        assert_eq!(hex_digest(&digest), "900150983cd24fb0d6963f7d28e17f72");

        // Multi-part consume
        let mut ctx = Context::new();
        ctx.consume(b"message ");
        ctx.consume(b"digest");
        let digest = ctx.finalize();
        assert_eq!(hex_digest(&digest), "f96b697d7cb7938d525a2f31aaf161d0");
    }

    /// Verify that data spanning multiple 64-byte blocks works correctly.
    #[test]
    fn test_multi_block_input() {
        // 80 bytes = more than one 64-byte block
        let digest = compute(
            b"12345678901234567890123456789012345678901234567890123456789012345678901234567890",
        );
        assert_eq!(hex_digest(&digest), "57edf4a22be3c955ac49da2e2107b67a");
    }

    /// Verify exact 64-byte block boundary.
    #[test]
    fn test_exact_block_boundary() {
        let digest = compute(b"0123456789012345678901234567890123456789012345678901234567890123");
        assert_eq!(hex_digest(&digest), "7f7bfd348709deeaace19e3f535f8c54");
    }

    /// Verify 55-byte input (padding boundary edge case: 55 + 1 + 8 = 64).
    #[test]
    fn test_55_byte_padding_boundary() {
        let input = b"1234567890123456789012345678901234567890123456789012345";
        assert_eq!(input.len(), 55);
        let digest = compute(input);
        // Verified against reference implementation
        assert_eq!(hex_digest(&digest), "c9ccf168f46840a4adea67ca940b06c9");
    }

    /// Verify 56-byte input (padding boundary edge case: needs extra block).
    #[test]
    fn test_56_byte_padding_boundary() {
        let input = b"12345678901234567890123456789012345678901234567890123456";
        assert_eq!(input.len(), 56);
        let digest = compute(input);
        // Verified against reference implementation
        assert_eq!(hex_digest(&digest), "a2b28d37eaa2801d5d78ecbd47cb50d2");
    }

    #[test]
    fn test_hex_digest() {
        let bytes: [u8; 16] = [
            0xd4, 0x1d, 0x8c, 0xd9, 0x8f, 0x00, 0xb2, 0x04, 0xe9, 0x80, 0x09, 0x98, 0xec, 0xf8,
            0x42, 0x7e,
        ];
        assert_eq!(hex_digest(&bytes), "d41d8cd98f00b204e9800998ecf8427e");
    }

    /// Large data test: hash 1 MB of zeros.
    #[test]
    fn test_large_data() {
        let data = vec![0u8; 1024 * 1024];
        let digest = compute(&data);
        assert_eq!(hex_digest(&digest), "b6d81b360a5672d80c27430f39571b33");
    }

    /// PostgreSQL MD5 password hash simulation.
    #[test]
    fn test_pg_md5_password_pattern() {
        // Simulate md5(password + user) using incremental API
        let mut ctx = Context::new();
        ctx.consume(b"test");
        ctx.consume(b"test");
        let inner = ctx.finalize();
        let inner_hex = hex_digest(&inner);

        // Simulate md5(inner_hex + salt) using incremental API
        let salt = [0u8; 4];
        let mut ctx = Context::new();
        ctx.consume(inner_hex.as_bytes());
        ctx.consume(&salt);
        let outer = ctx.finalize();
        let result = format!("md5{}", hex_digest(&outer));

        assert!(result.starts_with("md5"));
        assert_eq!(result.len(), 35);
    }
}
