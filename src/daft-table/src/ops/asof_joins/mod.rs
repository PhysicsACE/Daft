use std::collections::{HashMap, HashSet};

use daft_core::{schema::Schema, utils::supertype::try_get_supertype};

use crate::Table;
use common_error::{DaftError, DaftResult};
use core::cmp::Ordering;
use daft_core::array::ops::arrow2::comparison::build_multi_array_is_equal;
use daft_core::array::ops::as_arrow::AsArrow;
use daft_core::datatypes::UInt64Array;
use daft_core::kernels::search_sorted::build_partial_compare_with_nulls;
use daft_core::IntoSeries;
use daft_dsl::Expr;

fn match_types_for_tables(left: &Table, right: &Table) -> DaftResult<(Table, Table)> {
    let mut lseries = vec![];
    let mut rseries = vec![];

    for (ls, rs) in left.columns.iter().zip(right.columns.iter()) {
        let st = try_get_supertype(ls.data_type(), rs.data_type());
        if let Ok(st) = st {
            lseries.push(ls.cast(&st)?);
            rseries.push(rs.cast(&st)?);
        } else {
            return Err(DaftError::SchemaMismatch(format!(
                "Can not perform join between due to mismatch of types of left: {} vs right: {}",
                ls.field(),
                rs.field()
            )));
        }
    }
    Ok((Table::from_columns(lseries)?, Table::from_columns(rseries)?))
}

fn asof_search(
    arr: &Vec<u64>,
    l_idx: usize,
    comparator: &dyn Fn(usize, usize) -> Option<Ordering>,
    allow_exact_matches: bool,
) -> u64 {
    let mut l = 0;
    let mut r = arr.len();

    (while l < r {
        let mid = l + (r - l) / 2;
        match comparator(l_idx, mid) {
            Some(Ordering::Less) => l = mid + 1,
            Some(Ordering::Equal) => {
                if allow_exact_matches {
                    l = mid + 1
                } else {
                    r = mid
                }
            }
            Some(Ordering::Greater) => r = mid,
            None => todo!(),
        }
    });

    arr[l]
}

// enum MergeJoinState {
//     // Previous pair of rows were not equal.
//     Mismatch,
//     // Previous pair of rows were incomparable, i.e. for one or more join keys, they both had null values.
//     BothNull,
//     // Currently on a left-side equality run, where the left-side run started at the stored index and is relative to
//     // a fixed right-side row.
//     LRun(usize),
//     // Currently on a right-side equality run, where the right-side run started at the stored index and is relative to
//     // a fixed left-side row.
//     RRun(usize),
//     EqRun,
//     // A staged left-side equality run starting at the stored index and ending at the current left pointer; this run
//     // may be equal to one or more future right-side rows.
//     StagedEq,
// }

pub fn infer_asof_schema(
    left: &Schema,
    right: &Schema,
    left_on: &[Expr],
    right_on: &[Expr],
    left_by: &[Expr],
    right_by: &[Expr],
) -> DaftResult<Schema> {
    if left_on.len() != right_on.len() {
        return Err(DaftError::ValueError(format!(
            "Length of left_on does not match length of right_on for Join {} vs {}",
            left_on.len(),
            right_on.len()
        )));
    }

    if left_by.len() != right_by.len() {
        return Err(DaftError::ValueError(format!(
            "Length of left_by does not match length of right_by for Join {} vs {}",
            left_by.len(),
            right_by.len()
        )));
    }

    let lfields = left_on
        .iter()
        .map(|e| e.to_field(left))
        .chain(left_by.iter().map(|e| e.to_field(left)))
        .collect::<DaftResult<Vec<_>>>()?;
    let rfields = right_on
        .iter()
        .map(|e| e.to_field(right))
        .chain(right_by.iter().map(|e| e.to_field(right)))
        .collect::<DaftResult<Vec<_>>>()?;

    let mut join_fields = lfields
        .iter()
        .map(|f| left.get_field(&f.name).cloned())
        .collect::<DaftResult<Vec<_>>>()?;
    let left_names = lfields.iter().map(|e| e.name.as_str());
    let right_names = rfields.iter().map(|e| e.name.as_str());
    let mut names_so_far = HashSet::new();
    join_fields.iter().for_each(|f| {
        names_so_far.insert(f.name.clone());
    });
    for field in left.fields.values() {
        if names_so_far.contains(&field.name) {
            continue;
        } else {
            join_fields.push(field.clone());
            names_so_far.insert(field.name.clone());
        }
    }
    let zipped_names: Vec<_> = left_names.zip(right_names).map(|(l, r)| (l, r)).collect();
    let right_to_left_keys: HashMap<&str, &str> = HashMap::from_iter(zipped_names.iter().copied());
    for field in right.fields.values() {
        // Skip fields if they were used in the join and have the same name as the corresponding left field
        match right_to_left_keys.get(field.name.as_str()) {
            Some(val) if val.eq(&field.name.as_str()) => {
                continue;
            }
            _ => (),
        }

        let mut curr_name = field.name.clone();
        while names_so_far.contains(&curr_name) {
            curr_name = "right.".to_string() + curr_name.as_str();
        }
        join_fields.push(field.rename(curr_name.clone()));
        names_so_far.insert(curr_name.clone());
    }

    Schema::new(join_fields)
}

impl Table {
    pub fn hash_asof_join(
        &self,
        right: &Self,
        left_on: &[Expr],
        right_on: &[Expr],
        left_by: &[Expr],
        right_by: &[Expr],
        allow_exact_matches: bool,
    ) -> DaftResult<Self> {
        if left_on.len() != right_on.len() {
            return Err(DaftError::ValueError(format!(
                "Mismatch on join on clauses: left: {:?}, right: {:?}",
                left_on.len(),
                right_on.len(),
            )));
        }

        let join_schema = infer_asof_schema(
            &self.schema,
            &right.schema,
            left_on,
            right_on,
            left_by,
            right_by,
        )?;

        if self.is_empty() || right.is_empty() {
            return Self::empty(Some(join_schema.into()));
        }

        let left_col = self.eval_expression(&left_on[0])?;
        let right_col = right.eval_expression(&right_on[0])?;

        let comparator = build_partial_compare_with_nulls(
            left_col.to_arrow().as_ref(),
            right_col.to_arrow().as_ref(),
            false,
        )?;

        let left_index = self.eval_expression_list(left_by)?;
        let right_index = self.eval_expression_list(right_by)?;

        let (left_index, right_index) = match_types_for_tables(&left_index, &right_index)?;

        let is_equal = build_multi_array_is_equal(
            left_index.columns.as_slice(),
            right_index.columns.as_slice(),
            false,
            false,
        )?;

        let probe_table = right_index.to_probe_hash_table()?;
        let l_hashes = left_index.hash_rows()?;

        let mut left_idx = vec![];
        let mut right_idx = vec![];

        for (l_idx, h) in l_hashes.as_arrow().values_iter().enumerate() {
            if let Some((_, indices)) = probe_table.raw_entry().from_hash(*h, |other| {
                *h == other.hash && {
                    let j = other.idx;
                    is_equal(l_idx, j as usize)
                }
            }) {
                let idx = asof_search(indices, l_idx, &comparator, allow_exact_matches);
                left_idx.push(l_idx as u64);
                right_idx.push(idx);
            }
        }

        let left_series = UInt64Array::from(("left_indices", left_idx));
        let right_series = UInt64Array::from(("right_indices", right_idx));
        let (left_idx, right_idx) = (left_series.into_series(), right_series.into_series());

        let mut join_fields = left_index
            .column_names()
            .iter()
            .map(|s| self.schema.get_field(s).cloned())
            .collect::<DaftResult<Vec<_>>>()?;

        let mut join_series = self
            .get_columns(left_index.column_names().as_slice())?
            .take(&left_idx)?
            .columns;
        drop(left_index);
        drop(right_index);

        let mut names_so_far = HashSet::new();
        join_fields.iter().for_each(|f| {
            names_so_far.insert(f.name.clone());
        });
        for field in self.schema.fields.values() {
            if names_so_far.contains(&field.name) {
                continue;
            } else {
                join_fields.push(field.clone());
                join_series.push(self.get_column(&field.name)?.take(&left_idx)?);
                names_so_far.insert(field.name.clone());
            }
        }

        drop(left_idx);

        let left_names = left_on
            .iter()
            .map(|e| e.name())
            .chain(left_by.iter().map(|e| e.name()));
        let right_names = right_on
            .iter()
            .map(|e| e.name())
            .chain(right_by.iter().map(|e| e.name()));
        let zipped_names: DaftResult<_> = left_names
            .zip(right_names)
            .map(|(l, r)| Ok((l?, r?)))
            .collect();
        let zipped_names: Vec<(&str, &str)> = zipped_names?;
        let right_to_left_keys: HashMap<&str, &str> =
            HashMap::from_iter(zipped_names.iter().copied());

        for field in right.schema.fields.values() {
            // Skip fields if they were used in the join and have the same name as the corresponding left field
            match right_to_left_keys.get(field.name.as_str()) {
                Some(val) if val.eq(&field.name.as_str()) => {
                    continue;
                }
                _ => (),
            }

            let mut curr_name = field.name.clone();
            while names_so_far.contains(&curr_name) {
                curr_name = "right.".to_string() + curr_name.as_str();
            }
            join_fields.push(field.rename(curr_name.clone()));
            join_series.push(
                right
                    .get_column(&field.name)?
                    .rename(curr_name.clone())
                    .take(&right_idx)?,
            );
            names_so_far.insert(curr_name.clone());
        }

        drop(right_idx);
        Table::new(join_schema, join_series)
    }

    // pub fn range_asof_join(
    //     &self,
    //     right: &Self,
    //     left_on: &[Expr],
    //     right_on: &[Expr],
    //     allow_exact_matches: bool,
    // ) -> DaftResult<Self> {
    //     if left.num_columns() != right.num_columns() {
    //         return Err(DaftError::ValueError(format!(
    //             "Mismatch of join on clauses: left: {:?} vs right: {:?}",
    //             left.num_columns(),
    //             right.num_columns()
    //         )));
    //     }

    //     if left.num_columns() == 0 {
    //         return Err(DaftError::ValueError(
    //             "No columns were passed in to join on".to_string(),
    //         ));
    //     }

    //     let has_null_type = left.columns.iter().any(|s| s.data_type().is_null())
    //         || right.columns.iter().any(|s| s.data_type().is_null());
    //     if has_null_type {
    //         return Ok((
    //             UInt64Array::empty("left_indices", &DataType::UInt64).into_series(),
    //             UInt64Array::empty("right_indices", &DataType::UInt64).into_series(),
    //         ));
    //     }
    //     let types_not_match = left
    //         .columns
    //         .iter()
    //         .zip(right.columns.iter())
    //         .any(|(l, r)| l.data_type() != r.data_type());
    //     if types_not_match {
    //         return Err(DaftError::SchemaMismatch(
    //             "Types between left and right do not match".to_string(),
    //         ));
    //     }

    //     if left.is_empty() || right.is_empty() {
    //         return Ok((
    //             UInt64Array::empty("left_indices", &DataType::UInt64).into_series(),
    //             UInt64Array::empty("right_indices", &DataType::UInt64).into_series(),
    //         ));
    //     }

    //     let left_col = self.eval_expression(left_on)?;
    //     let right_col = right.eval_expression(right_on)?;

    //     let comparator = build_partial_compare_with_nulls(
    //         left_col.to_arrow().as_ref(),
    //         right_col.to_arrow().as_ref(),
    //         false,
    //     )?;

    //     let mut left_indices = vec![];
    //     let mut right_indices = vec![];
    //     let mut left_idx = 0;
    //     let mut right_idx = 0;

    //     let mut state = MergeJoinState::Mismatch;
    //     while left_idx < left.len() && right_idx < right.len() {
    //         match comparator(left_idx, right_idx) {
    //             Some(Ordering::Less) => {
    //                 state = match state {
    //                     MergeJoinState::Mismatch => {
    //                         left_indices.push(left_idx);
    //                         right_indices.push(right_idx - 1);
    //                         MergeJoinState::RRun(right_idx)
    //                     }
    //                     MergeJoinState::RRun(start_right_idx) => {
    //                         left.indices.push(left_idx);
    //                         right.indices.push(start_right_idx - 1);
    //                         MergeJoinState::RRun(start_right_idx)
    //                     }
    //                     MergeJoinState::LRun => {
    //                         left_indices.push(left_idx);
    //                         right_indices.push(right_idx - 1);
    //                         MergeJoinState::RRun(right_idx)
    //                     }
    //                     MergeJoinState::EqRun(eq_start_idx) => {
    //                         right_idx -= 1;
    //                         MergeJoinState::StagedEq(eq_start_idx);
    //                     }
    //                     _ => MergeJoinState::RRun(right_idx),
    //                 };
    //                 match state {
    //                     MergeJoinState::StagedEq => {
    //                         continue;
    //                     }
    //                     _ => left_idx += 1,
    //                 }
    //             }
    //             Some(Ordering::Greater) => {
    //                 state = MergeJoinState::LRun;
    //                 right_idx += 1;
    //             }
    //             Some(Ordering::Equal) => {
    //                 match state {
    //                     MergeJoinState::LRun => {
    //                         if !allow_exact_matches {
    //                             left_indices.push(left_idx - 1);
    //                             right_indices.push(right_idx - 1);
    //                         }
    //                     }
    //                     MergeJoinState::StagedEq => {
    //                         left_indices.push(left_idx);
    //                         right_indices.push(right_idx);
    //                     }
    //                     _ => {}
    //                 }
    //                 state = match state {
    //                     MergeJoinState::StagedEq => stage,
    //                     _ => {
    //                         if allow_exact_matches {
    //                             MergeJoinState::EqRun
    //                         } else {
    //                             MergeJoinState::RRun(right_idx)
    //                         }
    //                     }
    //                 };
    //                 match state {
    //                     MergeJoinState::StagedEq => {
    //                         left_idx += 1;
    //                     }
    //                     MergeJoinState::EqRun(eq_start_idx) => {
    //                         right_idx += 1;
    //                     }
    //                     _ => {
    //                         left_idx += 1;
    //                     }
    //                 }
    //             }
    //             None => {
    //                 state = MergeJoinState::BothNull;
    //                 left_idx += 1;
    //                 right_idx += 1;
    //             }
    //         }
    //     }

    //     todo!()
    // }
}
