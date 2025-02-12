use arrow2::array::Array;
use common_error::DaftResult;
use itertools::izip;

use super::{as_arrow::AsArrow, DaftSumAggable};
use crate::{array::ops::GroupIndices, datatypes::*};
macro_rules! impl_daft_numeric_agg {
    ($T:ident, $AggType: ident) => {
        impl DaftSumAggable for &DataArray<$T> {
            type Output = DaftResult<DataArray<$T>>;

            fn sum(&self) -> Self::Output {
                let primitive_arr = self.as_arrow();
                let sum_value = arrow2::compute::aggregate::sum_primitive(primitive_arr);
                Ok(DataArray::<$T>::from_iter(
                    self.field.clone(),
                    std::iter::once(sum_value),
                ))
            }

            fn grouped_sum(&self, groups: &GroupIndices) -> Self::Output {
                let arrow_array = self.as_arrow();
                let sum_per_group = if arrow_array.null_count() > 0 {
                    DataArray::<$T>::from_iter(
                        self.field.clone(),
                        groups.iter().map(|g| {
                            g.iter().fold(None, |acc, index| {
                                let idx = *index as usize;
                                match (acc, arrow_array.is_null(idx)) {
                                    (acc, true) => acc,
                                    (None, false) => Some(arrow_array.value(idx)),
                                    (Some(acc), false) => Some(acc + arrow_array.value(idx)),
                                }
                            })
                        }),
                    )
                } else {
                    DataArray::<$T>::from_values_iter(
                        self.field.clone(),
                        groups.iter().map(|g| {
                            g.iter().fold(0 as $AggType, |acc, index| {
                                let idx = *index as usize;
                                acc + unsafe { arrow_array.value_unchecked(idx) }
                            })
                        }),
                    )
                };

                Ok(sum_per_group)
            }

            fn grouped_cum_sum(&self, groups: &GroupIndices) -> Self::Output {
                println!("self {:?}", self);
                let arrow_array = self.as_arrow();

                let cum_sum_group = if arrow_array.null_count() > 0 {
                    let mut res = vec![];
                    for g in groups {
                        let mut group_sum = None;
                        for idx in g {
                            let is_null = arrow_array.is_null(*idx as usize);
                            if is_null {
                                res.push(group_sum);
                            } else if !is_null && group_sum.is_none() {
                                group_sum = Some(arrow_array.value(*idx as usize));
                                res.push(group_sum);
                            } else {
                                let cum_sum = group_sum.unwrap();
                                group_sum = Some(cum_sum + arrow_array.value(*idx as usize));
                                res.push(group_sum)
                            }
                        }
                    }

                    DataArray::<$T>::from_iter(self.field.clone(), res.into_iter())

                    // let new_arrow_arry = Box::new(arrow2::array::PrimitiveArray::from_vec(res));
                    // DataArray::from((self.field.name.as_ref(), new_arrow_arry))
                } else {
                    let mut res = vec![];
                    for g in groups {
                        let mut group_sum = 0 as $AggType;
                        for idx in g {
                            group_sum += unsafe { arrow_array.value_unchecked(*idx as usize) };
                            res.push(group_sum as $AggType)
                        }
                    }

                    DataArray::<$T>::from_values_iter(self.field.clone(), res.into_iter())

                    // let new_arrow_arry = Box::new(arrow2::array::PrimitiveArray::from_vec(res));
                    // DataArray::from((self.field.name.as_ref(), new_arrow_arry))
                };

                Ok(cum_sum_group)
            }

            fn grouped_row_sum(
                &self,
                groups: &GroupIndices,
                left: Option<u64>,
                right: Option<u64>,
            ) -> Self::Output {
                let arrow_array = self.as_arrow();

                let cum_sum_group = if arrow_array.null_count() > 0 {
                    let mut res = vec![];
                    for g in groups {
                        let mut group_sum = None;
                        let mut left_idx = 0;
                        let mut right_idx = 0;
                        let mut curr_idx = 0;

                        while right_idx < g.len() {
                            if !arrow_array.is_null(g[right_idx] as usize) {
                                if group_sum.is_none() {
                                    group_sum = Some(arrow_array.value(g[right_idx] as usize));
                                } else {
                                    group_sum = Some(
                                        group_sum.unwrap()
                                            + arrow_array.value(g[right_idx] as usize),
                                    );
                                }
                            }
                            match (left, right) {
                                (Some(l), Some(r)) => {
                                    let dist = r + l;
                                    if right_idx - left_idx > dist as usize {
                                        if !arrow_array.is_null(g[left_idx] as usize) {
                                            group_sum = Some(
                                                group_sum.unwrap()
                                                    - arrow_array.value(g[left_idx] as usize),
                                            );
                                        }
                                        left_idx += 1;
                                    }
                                }
                                _ => (),
                            }

                            match right {
                                Some(r) => {
                                    if right_idx - curr_idx == r as usize {
                                        res.push(group_sum);
                                        curr_idx += 1;
                                    }
                                }
                                _ => (),
                            }

                            right_idx += 1;
                        }

                        while curr_idx < g.len() {
                            match left {
                                Some(l) => {
                                    if curr_idx - left_idx > l as usize {
                                        if !arrow_array.is_null(g[left_idx] as usize) {
                                            group_sum = Some(
                                                group_sum.unwrap()
                                                    - arrow_array.value(g[left_idx] as usize),
                                            );
                                        }
                                        left_idx += 1;
                                    }
                                }
                                _ => (),
                            }

                            res.push(group_sum);
                            curr_idx += 1;
                        }
                    }

                    DataArray::<$T>::from_iter(self.field.clone(), res.into_iter())
                } else {
                    let mut res = vec![];
                    for g in groups {
                        let mut group_sum = 0 as $AggType;
                        let mut left_idx = 0;
                        let mut right_idx = 0;
                        let mut curr_idx = 0;

                        while right_idx < g.len() {
                            group_sum +=
                                unsafe { arrow_array.value_unchecked(g[right_idx] as usize) };
                            match (left, right) {
                                (Some(l), Some(r)) => {
                                    let dist = r + l;
                                    if right_idx - left_idx > dist as usize {
                                        group_sum -= unsafe {
                                            arrow_array.value_unchecked(g[left_idx] as usize)
                                        };
                                        left_idx += 1;
                                    }
                                }
                                _ => (),
                            }

                            match right {
                                Some(r) => {
                                    if right_idx - curr_idx == r as usize {
                                        res.push(group_sum as $AggType);
                                        curr_idx += 1;
                                    }
                                }
                                _ => (),
                            }

                            right_idx += 1;
                        }

                        while curr_idx < g.len() {
                            match left {
                                Some(l) => {
                                    if curr_idx - left_idx > l as usize {
                                        group_sum -= unsafe {
                                            arrow_array.value_unchecked(g[left_idx] as usize)
                                        };
                                        left_idx += 1;
                                    }
                                }
                                _ => (),
                            }

                            res.push(group_sum as $AggType);
                            curr_idx += 1;
                        }
                    }

                    DataArray::<$T>::from_values_iter(self.field.clone(), res.into_iter())
                };

                Ok(cum_sum_group)
            }

            fn grouped_range_sum(
                &self,
                groups: &GroupIndices,
                left_ranges: &[Vec<u64>],
                right_ranges: &[Vec<u64>],
            ) -> Self::Output {
                let arrow_array = self.as_arrow();

                let cum_sum_group = {
                    let mut res = vec![];
                    for (group_left, group_right, group_idx) in
                        izip!(left_ranges, right_ranges, groups)
                    {
                        let mut left_idx = 0;
                        let mut right_idx = 0;
                        let mut sum = 0 as $AggType;
                        for (left, right) in group_left.iter().zip(group_right.iter()) {
                            while right_idx < *right as usize {
                                if arrow_array.is_null(group_idx[right_idx] as usize) {
                                    right_idx += 1;
                                    continue;
                                }
                                sum += unsafe {
                                    arrow_array.value_unchecked(group_idx[right_idx] as usize)
                                };
                                right_idx += 1;
                            }

                            while left_idx < *left as usize {
                                if arrow_array.is_null(group_idx[left_idx] as usize) {
                                    left_idx += 1;
                                    continue;
                                }
                                sum -= unsafe {
                                    arrow_array.value_unchecked(group_idx[left_idx] as usize)
                                };
                                left_idx += 1;
                            }

                            res.push(sum as $AggType);
                        }
                    }

                    DataArray::<$T>::from_values_iter(self.field.clone(), res.into_iter())
                };

                Ok(cum_sum_group)
            }
        }
    };
}

impl_daft_numeric_agg!(Int64Type, i64);
impl_daft_numeric_agg!(UInt64Type, u64);
impl_daft_numeric_agg!(Float32Type, f32);
impl_daft_numeric_agg!(Float64Type, f64);
impl_daft_numeric_agg!(Decimal128Type, i128);
