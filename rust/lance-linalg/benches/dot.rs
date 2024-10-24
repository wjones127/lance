// Copyright 2023 Lance Developers.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::iter::{repeat_with, Sum};

use arrow_array::{
    types::{Float16Type, Float32Type, Float64Type},
    Float32Array,
};
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use half::bf16;
use lance_arrow::{ArrowFloatType, FloatArray};
use num_traits::Float;

#[cfg(target_os = "linux")]
use pprof::criterion::{Output, PProfProfiler};

use lance_linalg::distance::dot::{dot, dot_distance, Dot};
use lance_testing::datagen::generate_random_array_with_seed;
use rand::Rng;

#[inline]
fn dot_scalar<T: Float + Sum>(x: &[T], y: &[T]) -> T {
    x.iter().zip(y.iter()).map(|(&xi, &yi)| xi * yi).sum::<T>()
}

fn run_bench<T: ArrowFloatType + Dot>(c: &mut Criterion) {
    const DIMENSION: usize = 1024;
    const TOTAL: usize = 1024 * 1024; // 1M vectors

    let key = generate_random_array_with_seed::<T>(DIMENSION, [0; 32]);
    // 1M of 1024 D vectors
    let target = generate_random_array_with_seed::<T>(TOTAL * DIMENSION, [42; 32]);

    let type_name = std::any::type_name::<T::Native>();
    c.bench_function(format!("Dot({type_name}, arrow_artiy)").as_str(), |b| {
        b.iter(|| {
            T::ArrayType::from(
                target
                    .as_slice()
                    .chunks(DIMENSION)
                    .map(|arr| dot_scalar(key.as_slice(), arr))
                    .collect::<Vec<_>>(),
            )
        });
    });

    c.bench_function(
        format!("Dot({type_name}, auto-vectorization)").as_str(),
        |b| {
            let x = key.as_slice();
            b.iter(|| {
                Float32Array::from(
                    target
                        .as_slice()
                        .chunks(DIMENSION)
                        .map(|y| black_box(dot(x, y)))
                        .collect::<Vec<_>>(),
                )
            });
        },
    );

    // TODO: SIMD needs generic specialization
}

fn bench_distance(c: &mut Criterion) {
    const DIMENSION: usize = 1024;
    const TOTAL: usize = 1024 * 1024; // 1M vectors

    run_bench::<Float16Type>(c);
    c.bench_function("Dot(f16, SIMD)", |b| {
        let key = generate_random_array_with_seed::<Float16Type>(DIMENSION, [0; 32]);
        // 1M of 1024 D vectors
        let target = generate_random_array_with_seed::<Float16Type>(TOTAL * DIMENSION, [42; 32]);
        b.iter(|| unsafe {
            let x = key.values().as_ref();
            Float32Array::from_trusted_len_iter((0..target.len() / DIMENSION).map(|idx| {
                let y = target.values()[idx * DIMENSION..(idx + 1) * DIMENSION].as_ref();
                Some(dot_distance(x, y))
            }))
        });
    });

    let mut rng = rand::thread_rng();
    let key = repeat_with(|| rng.gen::<u16>())
        .map(bf16::from_bits)
        .take(DIMENSION)
        .collect::<Vec<_>>();
    let target = repeat_with(|| rng.gen::<u16>())
        .map(bf16::from_bits)
        .take(TOTAL * DIMENSION)
        .collect::<Vec<_>>();
    c.bench_function("Dot(bf16, auto-vectorization)", |b| {
        b.iter(|| {
            let x = key.as_slice();
            black_box(
                target
                    .chunks(DIMENSION)
                    .map(|y| dot_distance(x, y))
                    .collect::<Vec<_>>(),
            )
        });
    });

    run_bench::<Float32Type>(c);
    c.bench_function("Dot(f32, SIMD)", |b| {
        let key = generate_random_array_with_seed::<Float32Type>(DIMENSION, [0; 32]);
        // 1M of 1024 D vectors
        let target = generate_random_array_with_seed::<Float32Type>(TOTAL * DIMENSION, [42; 32]);
        b.iter(|| unsafe {
            let x = key.values().as_ref();
            Float32Array::from_trusted_len_iter((0..target.len() / DIMENSION).map(|idx| {
                let y = target.values()[idx * DIMENSION..(idx + 1) * DIMENSION].as_ref();
                Some(Float32Type::dot(x, y))
            }))
        });
    });

    run_bench::<Float64Type>(c);
}

#[cfg(target_os = "linux")]
criterion_group!(
    name=benches;
    config = Criterion::default().significance_level(0.1).sample_size(10)
        .with_profiler(PProfProfiler::new(100, Output::Flamegraph(None)));
    targets = bench_distance);

// Non-linux version does not support pprof.
#[cfg(not(target_os = "linux"))]
criterion_group!(
    name=benches;
    config = Criterion::default().significance_level(0.1).sample_size(10);
    targets = bench_distance);

criterion_main!(benches);
