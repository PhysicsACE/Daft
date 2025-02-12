use std::ops::{Add, Sub};

use common_error::{DaftError, DaftResult};
use daft_core::{array::ops::IntoGroups, datatypes::UInt64Array, prelude::*};
use daft_dsl::{
    Expr, ExprRef,
    LiteralValue::{Date, Duration, Float64, Int32, Int64, Timestamp, UInt32, UInt64},
    WindowSpecRef,
};

use crate::Table;

impl Table {
    pub fn partitioned_window(
        &self,
        window_spec: WindowSpecRef,
        window_fns: &[ExprRef],
        _window_names: Vec<String>,
    ) -> DaftResult<Self> {
        if window_spec.partition_by.is_empty() {
            return Err(DaftError::ValueError(
                "Non partitioned window functions not yet implemented".to_string(),
            ));
        }

        let framed = window_spec.has_frame();
        if !framed {
            return self.cumulative_window(window_spec, window_fns);
        }

        if !window_spec.is_range() {
            self.row_window(window_spec, window_fns)
        } else {
            self.range_window(window_spec, window_fns)
        }
    }

    pub fn range_window(
        &self,
        window_spec: WindowSpecRef,
        window_fns: &[ExprRef],
    ) -> DaftResult<Self> {
        let window_exprs = window_fns
            .iter()
            .map(|e| match e.as_ref() {
                Expr::WindowExpression(e) => Ok(e),
                _ => Err(DaftError::ValueError(format!(
                    "Trying to run non-window function in Partitioned Window! {e}",
                ))),
            })
            .collect::<DaftResult<Vec<_>>>()?;

        let ordered_table = if window_spec.order_by.is_empty() {
            self
        } else {
            &self.sort(
                window_spec.order_by.as_slice(),
                window_spec.descending.as_slice(),
                std::iter::repeat(false)
                    .take(window_spec.order_by.len())
                    .collect::<Vec<_>>()
                    .as_slice(),
            )?
        };

        let groupby_table =
            ordered_table.eval_expression_list(window_spec.partition_by.as_slice())?;
        let (_, groupvals_indices) = groupby_table.make_groups()?;

        let group_idx_input = Some(&groupvals_indices);

        let left_frame = match &window_spec.left_bound {
            None => None,
            Some(e) => match e.as_ref() {
                Expr::Literal(Int32(_))
                | Expr::Literal(Int64(_))
                | Expr::Literal(Float64(_))
                | Expr::Literal(UInt32(_))
                | Expr::Literal(UInt64(_))
                | Expr::Literal(Date(_))
                | Expr::Literal(Timestamp(..))
                | Expr::Literal(Duration(..)) => Some(e),
                _ => {
                    return Err(DaftError::ValueError(
                        "Range between bounds must be integer, float or temporal types".to_string(),
                    ))
                }
            },
        };

        let right_frame = match &window_spec.right_bound {
            None => None,
            Some(e) => match e.as_ref() {
                Expr::Literal(Int32(_))
                | Expr::Literal(Int64(_))
                | Expr::Literal(Float64(_))
                | Expr::Literal(UInt32(_))
                | Expr::Literal(UInt64(_))
                | Expr::Literal(Date(_))
                | Expr::Literal(Timestamp(..))
                | Expr::Literal(Duration(..)) => Some(e),
                _ => {
                    return Err(DaftError::ValueError(
                        "Range between bounds must be integer, float or temporal types".to_string(),
                    ))
                }
            },
        };

        let orderby_col = ordered_table.eval_expression(window_spec.order_by[0].as_ref())?;
        if !orderby_col.data_type().is_numeric() && !orderby_col.data_type().is_temporal() {
            return Err(DaftError::ValueError(
                "Orderby column for range between clause must be numeric or temporal".to_string(),
            ));
        }

        let mut left_bounds: Vec<Vec<u64>> = vec![];
        let mut right_bounds: Vec<Vec<u64>> = vec![];
        for group in &groupvals_indices {
            let group_indices: Vec<u64> = group.clone();
            let group_series = UInt64Array::from(("", group_indices)).into_series();
            let orderby_group = orderby_col.take(&group_series)?;

            let left_bound = match left_frame {
                None => vec![0_u64, group.len() as u64],
                Some(ref e) => {
                    let lit_series = ordered_table.eval_expression(e.as_ref())?;
                    let diff = orderby_group.clone().sub(lit_series)?;
                    let offset =
                        orderby_group.search_sorted(&diff, window_spec.descending[0], true)?;
                    let to_vector: Vec<u64> = offset.as_arrow().values_iter().copied().collect();
                    to_vector
                }
            };

            let right_bound = match right_frame {
                None => vec![0_u64, group.len() as u64],
                Some(ref e) => {
                    let lit_series = ordered_table.eval_expression(e.as_ref())?;
                    let diff = orderby_group.clone().add(lit_series)?;
                    let offset =
                        orderby_group.search_sorted(&diff, window_spec.descending[0], false)?;
                    let to_vector: Vec<u64> = offset.as_arrow().values_iter().copied().collect();
                    to_vector
                }
            };

            left_bounds.push(left_bound);
            right_bounds.push(right_bound);
        }

        let windowed_cols = window_exprs
            .iter()
            .map(|e| {
                ordered_table.eval_window_expression(
                    e,
                    group_idx_input,
                    false,
                    None,
                    None,
                    Some(&left_bounds),
                    Some(&right_bounds),
                    true,
                )
            })
            .collect::<DaftResult<Vec<_>>>()?;

        let mut ordered_take = vec![];
        for group in groupvals_indices {
            ordered_take.extend(group);
        }

        let ordered_take_series = UInt64Array::from(("", ordered_take)).into_series();
        let ordered_cols = ordered_table.take(&ordered_take_series)?;

        Self::from_nonempty_columns([&ordered_cols.columns[..], &windowed_cols].concat())
    }

    pub fn row_window(
        &self,
        window_spec: WindowSpecRef,
        window_fns: &[ExprRef],
    ) -> DaftResult<Self> {
        let window_exprs = window_fns
            .iter()
            .map(|e| match e.as_ref() {
                Expr::WindowExpression(e) => Ok(e),
                _ => Err(DaftError::ValueError(format!(
                    "Trying to run non-window function in Partitioned Window! {e}",
                ))),
            })
            .collect::<DaftResult<Vec<_>>>()?;

        let ordered_table = if window_spec.order_by.is_empty() {
            self
        } else {
            &self.sort(
                window_spec.order_by.as_slice(),
                window_spec.descending.as_slice(),
                std::iter::repeat(false)
                    .take(window_spec.order_by.len())
                    .collect::<Vec<_>>()
                    .as_slice(),
            )?
        };

        let groupby_table =
            ordered_table.eval_expression_list(window_spec.partition_by.as_slice())?;
        let (_, groupvals_indices) = groupby_table.make_groups()?;

        let group_idx_input = Some(&groupvals_indices);

        let left_frame = match &window_spec.left_bound {
            None => None,
            Some(e) => match e.as_ref() {
                Expr::Literal(lit_value) => match lit_value {
                    Int32(v) => Some(*v as u64),
                    Int64(v) => Some(*v as u64),
                    _ => {
                        return Err(DaftError::ValueError(
                            "WindowSpec row between values must be integers".to_string(),
                        ))
                    }
                },
                _ => {
                    return Err(DaftError::ValueError(
                        "WindowSpec row between values must be integers".to_string(),
                    ))
                }
            },
        };

        let right_frame = match &window_spec.right_bound {
            None => None,
            Some(e) => match e.as_ref() {
                Expr::Literal(lit_value) => match lit_value {
                    Int32(v) => Some(*v as u64),
                    Int64(v) => Some(*v as u64),
                    _ => {
                        return Err(DaftError::ValueError(
                            "WindowSpec row between values must be integers".to_string(),
                        ))
                    }
                },
                _ => {
                    return Err(DaftError::ValueError(
                        "WindowSpec row between values must be integers".to_string(),
                    ))
                }
            },
        };

        let windowed_cols = window_exprs
            .iter()
            .map(|e| {
                ordered_table.eval_window_expression(
                    e,
                    group_idx_input,
                    false,
                    left_frame,
                    right_frame,
                    None,
                    None,
                    false,
                )
            })
            .collect::<DaftResult<Vec<_>>>()?;

        let mut ordered_take = vec![];
        for group in groupvals_indices {
            ordered_take.extend(group);
        }

        let ordered_take_series = UInt64Array::from(("", ordered_take)).into_series();
        let ordered_cols = ordered_table.take(&ordered_take_series)?;

        Self::from_nonempty_columns([&ordered_cols.columns[..], &windowed_cols].concat())
    }

    pub fn cumulative_window(
        &self,
        window_spec: WindowSpecRef,
        window_fns: &[ExprRef],
    ) -> DaftResult<Self> {
        let window_exprs = window_fns
            .iter()
            .map(|e| match e.as_ref() {
                Expr::WindowExpression(e) => Ok(e),
                _ => Err(DaftError::ValueError(format!(
                    "Trying to run non-window function in Partitioned Window! {e}",
                ))),
            })
            .collect::<DaftResult<Vec<_>>>()?;

        let ordered_table = if window_spec.order_by.is_empty() {
            self
        } else {
            &self.sort(
                window_spec.order_by.as_slice(),
                window_spec.descending.as_slice(),
                std::iter::repeat(false)
                    .take(window_spec.order_by.len())
                    .collect::<Vec<_>>()
                    .as_slice(),
            )?
        };

        let groupby_table =
            ordered_table.eval_expression_list(window_spec.partition_by.as_slice())?;
        let (_, groupvals_indices) = groupby_table.make_groups()?;

        let group_idx_input = Some(&groupvals_indices);

        let windowed_cols = window_exprs
            .iter()
            .map(|e| {
                ordered_table.eval_window_expression(
                    e,
                    group_idx_input,
                    true,
                    None,
                    None,
                    None,
                    None,
                    false,
                )
            })
            .collect::<DaftResult<Vec<_>>>()?;

        let mut ordered_take = vec![];
        for group in groupvals_indices {
            ordered_take.extend(group);
        }

        let ordered_take_series = UInt64Array::from(("", ordered_take)).into_series();
        let ordered_cols = ordered_table.take(&ordered_take_series)?;

        Self::from_nonempty_columns([&ordered_cols.columns[..], &windowed_cols].concat())
    }
}
