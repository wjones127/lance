//! Test hash performance on row addresses.

use lance_core::format::RowAddress;
use criterion::{criterion_group, criterion_main, Criterion};
use std::collections::HashSet;
use xxhash_rust::xxh3::Xxh3Builder;
use nohash_hasher::{BuildNoHashHasher, IntSet};

fn generate_row_ids(num_fragments: u32, rows_per_fragment: u32) -> impl Iterator<Item = RowAddress> {
    (0..num_fragments).flat_map(move |frag_id| {
        (0..rows_per_fragment).map(move |row_id| RowAddress::new_from_parts(frag_id, row_id))
    })
}

// TODO: make a perfect hasher for known number of fragments and sizes
struct RowAddressHashBuilder {
    offsets: Vec<(usize, usize)>,
}

impl RowAddressHashBuilder {
    fn new(num_fragments: u32, rows_per_fragment: u32) -> Self {
        let mut offsets = Vec::with_capacity(num_fragments as usize);
        let mut offset = 0;
        for _ in 0..num_fragments {
            offsets.push((offset, offset + rows_per_fragment as usize));
            offset += rows_per_fragment as usize;
        }
        Self { offsets }
    }
}

struct RowAddressHash<'a> {
    offsets: &'a [(usize, usize)],
}

impl Hasher for RowAddressHash<'_> {
}



fn bench_row_address_hash(c: &mut Criterion) {
    let num_fragments: u32 = 100;
    let rows_per_fragment: u32 = 10_000;
    let capacity: usize = (num_fragments * rows_per_fragment) as usize;

    let mut group = c.benchmark_group("row_address_hash");
    group.bench_function("hash_default", |b| {
        b.iter(|| {
            let mut set = HashSet::with_capacity(capacity);
            for row_id in generate_row_ids(num_fragments, rows_per_fragment) {
                set.insert(row_id);
            }
        })
    });
    group.bench_function("hash_nohash", |b| {
        b.iter(|| {
            let hasher: BuildNoHashHasher<u64> = BuildNoHashHasher::default();
            let mut set: IntSet<u64> = HashSet::with_capacity_and_hasher(capacity, hasher);
            for row_id in generate_row_ids(num_fragments, rows_per_fragment) {
                set.insert(row_id.into());
            }
        })
    });
    group.bench_function("hash_xxh3", |b| {
        
        b.iter(|| {
            let hasher = Xxh3Builder::default();
            let mut set = HashSet::with_capacity_and_hasher(capacity, hasher);
            for row_id in generate_row_ids(num_fragments, rows_per_fragment) {
                set.insert(row_id);
            }
        })
    });
    group.finish();
}

criterion_group!(
    name=benches;
    config = Criterion::default().significance_level(0.1).sample_size(10);
    targets = bench_row_address_hash);
criterion_main!(benches);