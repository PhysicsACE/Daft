use common_error::DaftResult;
use daft_core::array::ops::DaftCompare;
use daft_dsl::Expr;
use daft_io::IOStatsContext;
use daft_table::infer_asof_schema;
use daft_table::infer_join_schema;

use crate::micropartition::MicroPartition;

use daft_stats::TruthValue;

impl MicroPartition {
    pub fn hash_join(&self, right: &Self, left_on: &[Expr], right_on: &[Expr]) -> DaftResult<Self> {
        let io_stats = IOStatsContext::new("MicroPartition::hash_join");
        let join_schema = infer_join_schema(&self.schema, &right.schema, left_on, right_on)?;

        if self.len() == 0 || right.len() == 0 {
            return Ok(Self::empty(Some(join_schema.into())));
        }

        let tv = match (&self.statistics, &right.statistics) {
            (_, None) => TruthValue::Maybe,
            (None, _) => TruthValue::Maybe,
            (Some(l), Some(r)) => {
                let l_eval_stats = l.eval_expression_list(left_on, &self.schema)?;
                let r_eval_stats = r.eval_expression_list(right_on, &right.schema)?;
                let mut curr_tv = TruthValue::Maybe;
                for (lc, rc) in l_eval_stats
                    .columns
                    .values()
                    .zip(r_eval_stats.columns.values())
                {
                    if let TruthValue::False = lc.equal(rc)?.to_truth_value() {
                        curr_tv = TruthValue::False;
                        break;
                    }
                }
                curr_tv
            }
        };
        if let TruthValue::False = tv {
            return Ok(Self::empty(Some(join_schema.into())));
        }

        // TODO(Clark): Elide concatenations where possible by doing a chunk-aware local table join.
        let lt = self.concat_or_get(io_stats.clone())?;
        let rt = right.concat_or_get(io_stats)?;

        match (lt.as_slice(), rt.as_slice()) {
            ([], _) | (_, []) => Ok(Self::empty(Some(join_schema.into()))),
            ([lt], [rt]) => {
                let joined_table = lt.hash_join(rt, left_on, right_on)?;
                Ok(MicroPartition::new_loaded(
                    join_schema.into(),
                    vec![joined_table].into(),
                    None,
                ))
            }
            _ => unreachable!(),
        }
    }

    pub fn sort_merge_join(
        &self,
        right: &Self,
        left_on: &[Expr],
        right_on: &[Expr],
        is_sorted: bool,
    ) -> DaftResult<Self> {
        let io_stats = IOStatsContext::new("MicroPartition::sort_merge_join");
        let join_schema = infer_join_schema(&self.schema, &right.schema, left_on, right_on)?;

        if self.len() == 0 || right.len() == 0 {
            return Ok(Self::empty(Some(join_schema.into())));
        }

        let tv = match (&self.statistics, &right.statistics) {
            (_, None) => TruthValue::Maybe,
            (None, _) => TruthValue::Maybe,
            (Some(l), Some(r)) => {
                let l_eval_stats = l.eval_expression_list(left_on, &self.schema)?;
                let r_eval_stats = r.eval_expression_list(right_on, &right.schema)?;
                let mut curr_tv = TruthValue::Maybe;
                for (lc, rc) in l_eval_stats
                    .columns
                    .values()
                    .zip(r_eval_stats.columns.values())
                {
                    if let TruthValue::False = lc.equal(rc)?.to_truth_value() {
                        curr_tv = TruthValue::False;
                        break;
                    }
                }
                curr_tv
            }
        };
        if let TruthValue::False = tv {
            return Ok(Self::empty(Some(join_schema.into())));
        }

        // TODO(Clark): Elide concatenations where possible by doing a chunk-aware local table join.
        let lt = self.concat_or_get(io_stats.clone())?;
        let rt = right.concat_or_get(io_stats)?;

        match (lt.as_slice(), rt.as_slice()) {
            ([], _) | (_, []) => Ok(Self::empty(Some(join_schema.into()))),
            ([lt], [rt]) => {
                let joined_table = lt.sort_merge_join(rt, left_on, right_on, is_sorted)?;
                Ok(MicroPartition::new_loaded(
                    join_schema.into(),
                    vec![joined_table].into(),
                    None,
                ))
            }
            _ => unreachable!(),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn hash_asof_join(
        &self,
        right: &Self,
        left_on: &[Expr],
        right_on: &[Expr],
        left_by: &[Expr],
        right_by: &[Expr],
        allow_exact_matches: bool,
    ) -> DaftResult<Self> {
        let io_stats = IOStatsContext::new("MicroPartition::hash_asof_join");
        let join_schema = infer_asof_schema(
            &self.schema,
            &right.schema,
            left_on,
            right_on,
            left_by,
            right_by,
        )?;

        if self.len() == 0 || right.len() == 0 {
            return Ok(Self::empty(Some(join_schema.into())));
        }

        let tv = match (&self.statistics, &right.statistics) {
            (_, None) => TruthValue::Maybe,
            (None, _) => TruthValue::Maybe,
            (Some(l), Some(r)) => {
                let l_eval_stats = l.eval_expression_list(left_on, &self.schema)?;
                let r_eval_stats = r.eval_expression_list(right_on, &right.schema)?;
                let mut curr_tv = TruthValue::Maybe;
                for (lc, rc) in l_eval_stats
                    .columns
                    .values()
                    .zip(r_eval_stats.columns.values())
                {
                    if let TruthValue::False = lc.equal(rc)?.to_truth_value() {
                        curr_tv = TruthValue::False;
                        break;
                    }
                }
                curr_tv
            }
        };
        if let TruthValue::False = tv {
            return Ok(Self::empty(Some(join_schema.into())));
        }

        let lt = self.concat_or_get(io_stats.clone())?;
        let rt = right.concat_or_get(io_stats)?;

        match (lt.as_slice(), rt.as_slice()) {
            ([], _) | (_, []) => Ok(Self::empty(Some(join_schema.into()))),
            ([lt], [rt]) => {
                let joined_table = lt.hash_asof_join(
                    rt,
                    left_on,
                    right_on,
                    left_by,
                    right_by,
                    allow_exact_matches,
                )?;
                Ok(MicroPartition::new_loaded(
                    join_schema.into(),
                    vec![joined_table].into(),
                    None,
                ))
            }
            _ => unreachable!(),
        }
    }

    pub fn range_asof_join(
        &self,
        _right: &Self,
        _left_on: &[Expr],
        _right_on: &[Expr],
        _allow_exact_matches: bool,
    ) -> DaftResult<Self> {
        todo!()
    }
}
