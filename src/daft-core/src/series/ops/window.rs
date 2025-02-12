use common_error::{DaftError, DaftResult};

use crate::{
    array::ops::{arrow2::comparison::build_is_equal, DaftSumAggable, GroupIndices},
    datatypes::*,
    series::{IntoSeries, Series},
};

impl Series {
    pub fn windowed_sum(&self, groups: Option<&GroupIndices>) -> DaftResult<Self> {
        match self.data_type() {
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
                let casted = self.cast(&DataType::Int64)?;
                match groups {
                    Some(groups) => {
                        Ok(DaftSumAggable::grouped_cum_sum(&casted.i64()?, groups)?.into_series())
                    }
                    _ => Err(DaftError::ValueError(
                        "Window nodes must have partition by clause".to_string(),
                    )),
                }
            }
            DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
                let casted = self.cast(&DataType::UInt64)?;
                match groups {
                    Some(groups) => {
                        Ok(DaftSumAggable::grouped_cum_sum(&casted.u64()?, groups)?.into_series())
                    }
                    _ => Err(DaftError::ValueError(
                        "Window nodes must have partition by clause".to_string(),
                    )),
                }
            }
            DataType::Float32 => match groups {
                Some(groups) => Ok(DaftSumAggable::grouped_cum_sum(
                    &self.downcast::<Float32Array>()?,
                    groups,
                )?
                .into_series()),
                _ => Err(DaftError::ValueError(
                    "Window nodes must have partition by clause".to_string(),
                )),
            },
            DataType::Float64 => match groups {
                Some(groups) => Ok(DaftSumAggable::grouped_cum_sum(
                    &self.downcast::<Float64Array>()?,
                    groups,
                )?
                .into_series()),
                _ => Err(DaftError::ValueError(
                    "Window nodes must have partition by clause".to_string(),
                )),
            },
            DataType::Decimal128(_, _) => {
                let casted = self.cast(&try_sum_supertype(self.data_type())?)?;

                match groups {
                    Some(groups) => Ok(DaftSumAggable::grouped_cum_sum(
                        &casted.downcast::<Decimal128Array>()?,
                        groups,
                    )?
                    .into_series()),
                    _ => Err(DaftError::ValueError(
                        "Window nodes must have partition by clause".to_string(),
                    )),
                }
            }
            other => Err(DaftError::TypeError(format!(
                "Numeric sum is not implemented for type {}",
                other
            ))),
        }
    }

    pub fn windowed_row_sum(
        &self,
        groups: Option<&GroupIndices>,
        left_row: Option<u64>,
        right_row: Option<u64>,
    ) -> DaftResult<Self> {
        match self.data_type() {
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
                let casted = self.cast(&DataType::Int64)?;
                match groups {
                    Some(groups) => Ok(DaftSumAggable::grouped_row_sum(
                        &casted.i64()?,
                        groups,
                        left_row,
                        right_row,
                    )?
                    .into_series()),
                    _ => Err(DaftError::ValueError(
                        "Window nodes must have partition by clause".to_string(),
                    )),
                }
            }
            DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
                let casted = self.cast(&DataType::UInt64)?;
                match groups {
                    Some(groups) => Ok(DaftSumAggable::grouped_row_sum(
                        &casted.u64()?,
                        groups,
                        left_row,
                        right_row,
                    )?
                    .into_series()),
                    _ => Err(DaftError::ValueError(
                        "Window nodes must have partition by clause".to_string(),
                    )),
                }
            }
            DataType::Float32 => match groups {
                Some(groups) => Ok(DaftSumAggable::grouped_row_sum(
                    &self.downcast::<Float32Array>()?,
                    groups,
                    left_row,
                    right_row,
                )?
                .into_series()),
                _ => Err(DaftError::ValueError(
                    "Window nodes must have partition by clause".to_string(),
                )),
            },
            DataType::Float64 => match groups {
                Some(groups) => Ok(DaftSumAggable::grouped_row_sum(
                    &self.downcast::<Float64Array>()?,
                    groups,
                    left_row,
                    right_row,
                )?
                .into_series()),
                _ => Err(DaftError::ValueError(
                    "Window nodes must have partition by clause".to_string(),
                )),
            },
            DataType::Decimal128(_, _) => {
                let casted = self.cast(&try_sum_supertype(self.data_type())?)?;

                match groups {
                    Some(groups) => Ok(DaftSumAggable::grouped_row_sum(
                        &casted.downcast::<Decimal128Array>()?,
                        groups,
                        left_row,
                        right_row,
                    )?
                    .into_series()),
                    _ => Err(DaftError::ValueError(
                        "Window nodes must have partition by clause".to_string(),
                    )),
                }
            }
            other => Err(DaftError::TypeError(format!(
                "Numeric sum is not implemented for type {}",
                other
            ))),
        }
    }

    pub fn windowed_range_sum(
        &self,
        groups: Option<&GroupIndices>,
        left_ranges: &[Vec<u64>],
        right_ranges: &[Vec<u64>],
    ) -> DaftResult<Self> {
        match self.data_type() {
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
                let casted = self.cast(&DataType::Int64)?;
                match groups {
                    Some(groups) => Ok(DaftSumAggable::grouped_range_sum(
                        &casted.i64()?,
                        groups,
                        left_ranges,
                        right_ranges,
                    )?
                    .into_series()),
                    _ => Err(DaftError::ValueError(
                        "Window nodes must have partition by clause".to_string(),
                    )),
                }
            }
            DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
                let casted = self.cast(&DataType::UInt64)?;
                match groups {
                    Some(groups) => Ok(DaftSumAggable::grouped_range_sum(
                        &casted.u64()?,
                        groups,
                        left_ranges,
                        right_ranges,
                    )?
                    .into_series()),
                    _ => Err(DaftError::ValueError(
                        "Window nodes must have partition by clause".to_string(),
                    )),
                }
            }
            DataType::Float32 => match groups {
                Some(groups) => Ok(DaftSumAggable::grouped_range_sum(
                    &self.downcast::<Float32Array>()?,
                    groups,
                    left_ranges,
                    right_ranges,
                )?
                .into_series()),
                _ => Err(DaftError::ValueError(
                    "Window nodes must have partition by clause".to_string(),
                )),
            },
            DataType::Float64 => match groups {
                Some(groups) => Ok(DaftSumAggable::grouped_range_sum(
                    &self.downcast::<Float64Array>()?,
                    groups,
                    left_ranges,
                    right_ranges,
                )?
                .into_series()),
                _ => Err(DaftError::ValueError(
                    "Window nodes must have partition by clause".to_string(),
                )),
            },
            DataType::Decimal128(_, _) => {
                let casted = self.cast(&try_sum_supertype(self.data_type())?)?;

                match groups {
                    Some(groups) => Ok(DaftSumAggable::grouped_range_sum(
                        &casted.downcast::<Decimal128Array>()?,
                        groups,
                        left_ranges,
                        right_ranges,
                    )?
                    .into_series()),
                    _ => Err(DaftError::ValueError(
                        "Window nodes must have partition by clause".to_string(),
                    )),
                }
            }
            other => Err(DaftError::TypeError(format!(
                "Numeric sum is not implemented for type {}",
                other
            ))),
        }
    }

    pub fn rank(&self, groups: Option<&GroupIndices>) -> DaftResult<Self> {
        let arrow_array = self.to_arrow();
        let is_equal = build_is_equal(&*arrow_array, &*arrow_array, true, true)?;

        if groups.is_none() {
            let mut rank = 1;
            let mut starting = 0;
            let mut res = vec![];

            while starting < arrow_array.len() {
                let mut cursor = starting;
                while cursor < arrow_array.len() && is_equal(starting, cursor) {
                    res.push(rank as u64);
                    cursor += 1;
                }

                let contiguous = cursor - starting;
                rank += contiguous;
                starting = cursor;
            }

            let rank_series = UInt64Array::from((self.field().name.as_ref(), res.as_slice()));
            Ok(rank_series.into_series())
        } else {
            let mut res = vec![];
            for group in groups.unwrap() {
                let mut rank = 1;
                let mut starting = 0;

                while starting < group.len() {
                    let mut cursor = starting;
                    while cursor < group.len()
                        && is_equal(group[starting] as usize, group[cursor] as usize)
                    {
                        res.push(rank as u64);
                        cursor += 1;
                    }

                    let contiguous = cursor - starting;
                    rank += contiguous;
                    starting = cursor;
                }
            }

            let rank_series = UInt64Array::from((self.field().name.as_ref(), res.as_slice()));
            Ok(rank_series.into_series())
        }
    }

    pub fn lead(
        &self,
        offset: u64,
        default: &Self,
        groups: Option<&GroupIndices>,
    ) -> DaftResult<Self> {
        let self_with_default = vec![self, default];
        let default_series = Self::concat(self_with_default.as_slice())?;
        let true_len: u64 = (default_series.len() - 1) as u64;
        let mut res = vec![];
        if groups.is_none() {
            let mut i: u64 = 0;
            while i < true_len {
                if i + offset >= true_len {
                    res.push(true_len);
                    i += 1;
                    continue;
                }
                res.push(i + offset);
                i += 1;
            }
        } else {
            for group in groups.unwrap() {
                let mut i: u64 = 0;
                while i < group.len() as u64 {
                    if i + offset >= group.len() as u64 {
                        res.push(true_len);
                        i += 1;
                        continue;
                    }
                    res.push(group[(i + offset) as usize]);
                    i += 1;
                }
            }
        }

        let take_series = UInt64Array::from(("", res.as_slice())).into_series();
        default_series.take(&take_series)
    }
}
