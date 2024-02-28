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
use half::f16;
use proptest::prelude::*;

/// Arbitrary finite f16 value.
pub fn arbitrary_f16() -> impl Strategy<Value = f16> {
    any::<u16>().prop_map(|bits| {
        // Convert arbitrary u16 to f16
        let val = f16::from_bits(bits);
        // Convert Inf -> Max, -Inf -> Min, NaN -> 0
        if val.is_infinite() && val.is_sign_positive() {
            f16::MAX
        } else if val.is_infinite() && val.is_sign_negative() {
            f16::MIN
        } else if val.is_nan() {
            f16::from_f32(0.0)
        } else {
            val
        }
    })
}

/// Arbitrary finite f16 vector.
pub fn artibrary_f16_vector(dim_range: std::ops::Range<usize>) -> impl Strategy<Value = Vec<f16>> {
    prop::collection::vec(arbitrary_f16(), dim_range)
}

/// Two arbitrary finite f16 vectors of matching dimension.
pub fn arbitrary_f16_vectors(
    dim_range: std::ops::Range<usize>,
) -> impl Strategy<Value = (Vec<f16>, Vec<f16>)> {
    dim_range.prop_flat_map(|dim| {
        let x = prop::collection::vec(arbitrary_f16(), dim);
        let y = prop::collection::vec(arbitrary_f16(), dim);
        (x, y)
    })
}
