const MIN_LATITUDE: f64 = -85.05112878;
const MAX_LATITUDE: f64 = 85.05112878;
const MIN_LONGITUDE: f64 = -180.00;
const MAX_LONGITUDE: f64 = 180.00;

const LATITUDE_RANGE: f64 = MAX_LATITUDE - MIN_LATITUDE;
const LONGITUDE_RANGE: f64 = MAX_LONGITUDE - MIN_LONGITUDE;

pub fn encode_geo(long: f64, lat: f64) -> u64 {
    let long_bits = normalized_long(long);
    let lat_bits: u32 = normalized_latitude(lat);

    let mut result = 0u64;
    for i in 0..26 {
        result |= ((lat_bits >> i & 1) as u64) << (2 * i);
        result |= ((long_bits >> i & 1) as u64) << (2 * i + 1);
    }
    result
}

fn decode_geo(value: u64) -> (f64, f64) {
    let mut long_bits = 0u32;
    let mut lat_bits = 0u32;
    for i in 0..26 {
        lat_bits |= ((value >> (2 * i) & 1) as u32) << i;
        long_bits |= ((value >> (2 * i + 1) & 1) as u32) << i;
    }

    (
        denormalize_longitude(long_bits),
        denormalize_latitude(lat_bits),
    )
}

fn normalized_latitude(lat: f64) -> u32 {
    let normal = 2.0_f64.powi(26) * (lat - MIN_LATITUDE) / LATITUDE_RANGE;
    normal as u32
}
fn normalized_long(long: f64) -> u32 {
    let normal = 2.0_f64.powi(26) * (long - MIN_LONGITUDE) / LONGITUDE_RANGE;
    normal as u32
}

fn denormalize_latitude(normalized: u32) -> f64 {
    MIN_LATITUDE + (normalized as f64 / 2.0_f64.powi(26)) * LATITUDE_RANGE
}

fn denormalize_longitude(normalized: u32) -> f64 {
    MIN_LONGITUDE + (normalized as f64 / 2.0_f64.powi(26)) * LONGITUDE_RANGE
}
