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

use std::iter::Sum;

use half::f16;
#[cfg(feature = "fp16kernels")]
use lance_core::utils::cpu::SimdSupport;
#[allow(unused_imports)]
use lance_core::utils::cpu::FP16_SIMD_SUPPORT;
use num_traits::{AsPrimitive, Float};

use crate::simd::{
    f32::{f32x16, f32x8},
    SIMD,
};

use super::L2;

/// L2 normalization
pub trait L2Norm: L2 {
    /// L2 Normalization over a Vector.
    fn norm_l2(vector: &[Self::Native]) -> f32;
}

// Default implementation
#[inline]
pub fn norm_l2_impl<T: Float + Sum + AsPrimitive<f32>, const LANES: usize>(vector: &[T]) -> f32 {
    let chunks = vector.chunks_exact(LANES);
    let sum = if chunks.remainder().is_empty() {
        T::zero()
    } else {
        chunks.remainder().iter().map(|&v| v.powi(2)).sum::<T>()
    };
    let mut sums = [T::zero(); LANES];
    for chunk in chunks {
        for i in 0..LANES {
            sums[i] = sums[i].add(chunk[i].powi(2));
        }
    }
    (sum + sums.iter().copied().sum::<T>()).sqrt().as_()
}

#[cfg(feature = "fp16kernels")]
mod kernel {
    use super::*;

    // These are the `norm_l2_f16` function in f16.c. Our build.rs script compiles
    // a version of this file for each SIMD level with different suffixes.
    extern "C" {
        #[cfg(target_arch = "aarch64")]
        pub fn norm_l2_f16_neon(ptr: *const f16, len: u32) -> f32;
        #[cfg(all(kernel_suppport = "avx512", target_arch = "x86_64"))]
        pub fn norm_l2_f16_avx512(ptr: *const f16, len: u32) -> f32;
        #[cfg(target_arch = "x86_64")]
        pub fn norm_l2_f16_avx2(ptr: *const f16, len: u32) -> f32;
    }
}

#[inline]
pub fn norm_l2_f16(vector: &[f16]) -> f32 {
    match *FP16_SIMD_SUPPORT {
        #[cfg(all(feature = "fp16kernels", target_arch = "aarch64"))]
        SimdSupport::Neon => unsafe {
            kernel::norm_l2_f16_neon(vector.as_ptr(), vector.len() as u32)
        },
        #[cfg(all(
            feature = "fp16kernels",
            kernel_suppport = "avx512",
            target_arch = "x86_64"
        ))]
        SimdSupport::Avx512 => unsafe {
            kernel::norm_l2_f16_avx512(vector.as_ptr(), vector.len() as u32)
        },
        #[cfg(all(feature = "fp16kernels", target_arch = "x86_64"))]
        SimdSupport::Avx2 => unsafe {
            kernel::norm_l2_f16_avx2(vector.as_ptr(), vector.len() as u32)
        },
        _ => norm_l2_f16_impl(vector),
    }
}

#[inline]
fn norm_l2_f16_impl(arr: &[f16]) -> f32 {
    // Please run `cargo bench --bench norm_l2" on Apple Silicon when
    // change the following code.
    const LANES: usize = 16;
    let chunks = arr.chunks_exact(LANES);
    let sum = if chunks.remainder().is_empty() {
        0.0
    } else {
        chunks
            .remainder()
            .iter()
            .map(|v| v.to_f32().powi(2))
            .sum::<f32>()
    };

    let mut sums: [f32; LANES] = [0_f32; LANES];
    for chk in chunks {
        // Convert to f32
        let mut f32_vals: [f32; LANES] = [0_f32; LANES];
        for i in 0..LANES {
            f32_vals[i] = chk[i].to_f32();
        }
        // Vectorized multiply
        for i in 0..LANES {
            sums[i] += f32_vals[i].powi(2);
        }
    }
    (sums.iter().copied().sum::<f32>() + sum).sqrt()
}

#[inline]
pub fn norm_l2_f32(vector: &[f32]) -> f32 {
    let dim = vector.len();
    if dim % 16 == 0 {
        let mut sum = f32x16::zeros();
        for i in (0..dim).step_by(16) {
            let x = unsafe { f32x16::load_unaligned(vector.as_ptr().add(i)) };
            sum += x * x;
        }
        sum.reduce_sum().sqrt()
    } else if dim % 8 == 0 {
        let mut sum = f32x8::zeros();
        for i in (0..dim).step_by(8) {
            let x = unsafe { f32x8::load_unaligned(vector.as_ptr().add(i)) };
            sum += x * x;
        }
        sum.reduce_sum().sqrt()
    } else {
        // Fallback to scalar
        norm_l2_impl::<f32, 16>(vector)
    }
}
