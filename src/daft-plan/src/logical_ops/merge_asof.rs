use std::{collections::HashSet, sync::Arc};

use common_error::{DaftError, DaftResult};
use daft_core::{
    schema::{hash_index_map, Schema, SchemaRef},
    DataType,
};
use daft_dsl::Expr;
use itertools::Itertools;
use snafu::ResultExt;

use crate::{
    logical_plan::{self, CreationSnafu},
    JoinDirection, LogicalPlan,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MergeAsOf {
    // Upstream nodes.
    pub left: Arc<LogicalPlan>,
    pub right: Arc<LogicalPlan>,

    pub left_on: Vec<Expr>,
    pub right_on: Vec<Expr>,
    pub left_by: Vec<Expr>,
    pub right_by: Vec<Expr>,
    pub join_direction: JoinDirection,
    pub allow_exact_matches: bool,
    pub output_schema: SchemaRef,

    // Joins may rename columns from the right input; this struct tracks those renames.
    // Output name -> Original name
    pub right_input_mapping: indexmap::IndexMap<String, String>,
}

impl std::hash::Hash for MergeAsOf {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        std::hash::Hash::hash(&self.left, state);
        std::hash::Hash::hash(&self.right, state);
        std::hash::Hash::hash(&self.left_on, state);
        std::hash::Hash::hash(&self.right_on, state);
        std::hash::Hash::hash(&self.left_by, state);
        std::hash::Hash::hash(&self.right_by, state);
        std::hash::Hash::hash(&self.join_direction, state);
        std::hash::Hash::hash(&self.allow_exact_matches, state);
        std::hash::Hash::hash(&self.output_schema, state);
        state.write_u64(hash_index_map(&self.right_input_mapping))
    }
}

impl MergeAsOf {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn try_new(
        left: Arc<LogicalPlan>,
        right: Arc<LogicalPlan>,
        left_on: Vec<Expr>,
        right_on: Vec<Expr>,
        left_by: Vec<Expr>,
        right_by: Vec<Expr>,
        join_direction: JoinDirection,
        allow_exact_matches: bool,
    ) -> logical_plan::Result<Self> {
        for (on_exprs, schema) in [(&left_on, left.schema()), (&right_on, right.schema())] {
            let on_fields = on_exprs
                .iter()
                .map(|e| e.to_field(schema.as_ref()))
                .collect::<DaftResult<Vec<_>>>()
                .context(CreationSnafu)?;
            let on_schema = Schema::new(on_fields).context(CreationSnafu)?;
            for (field, expr) in on_schema.fields.values().zip(on_exprs.iter()) {
                if matches!(field.dtype, DataType::Null) {
                    return Err(DaftError::ValueError(format!(
                        "Can't join on null type expressions: {expr}"
                    )))
                    .context(CreationSnafu);
                }
            }
        }
        for (by_exprs, schema) in [(&left_by, left.schema()), (&right_by, right.schema())] {
            let by_fields = by_exprs
                .iter()
                .map(|e| e.to_field(schema.as_ref()))
                .collect::<DaftResult<Vec<_>>>()
                .context(CreationSnafu)?;
            let by_schema = Schema::new(by_fields).context(CreationSnafu)?;
            for (field, expr) in by_schema.fields.values().zip(by_exprs.iter()) {
                if field.dtype.is_null()
                    || (!field.dtype.is_numeric() && !field.dtype.is_temporal())
                {
                    return Err(DaftError::ValueError(format!(
                        "AsOf join can only join on numeric or temporal types: {expr}"
                    )))
                    .context(CreationSnafu);
                }
            }
        }
        let mut right_input_mapping = indexmap::IndexMap::new();
        // Schema inference ported from existing behaviour for parity,
        // but contains bug https://github.com/Eventual-Inc/Daft/issues/1294
        let output_schema = {
            let left_join_keys = left_on
                .iter()
                .chain(left_by.iter())
                .map(|e| e.name())
                .collect::<common_error::DaftResult<HashSet<_>>>()
                .context(CreationSnafu)?;
            let left_schema = &left.schema().fields;
            let fields = left_schema
                .iter()
                .map(|(_, field)| field)
                .cloned()
                .chain(right.schema().fields.iter().filter_map(|(rname, rfield)| {
                    if left_join_keys.contains(rname.as_str()) {
                        right_input_mapping.insert(rname.clone(), rname.clone());
                        None
                    } else if left_schema.contains_key(rname) {
                        let new_name = format!("right.{}", rname);
                        right_input_mapping.insert(new_name.clone(), rname.clone());
                        Some(rfield.rename(new_name))
                    } else {
                        right_input_mapping.insert(rname.clone(), rname.clone());
                        Some(rfield.clone())
                    }
                }))
                .collect::<Vec<_>>();
            Schema::new(fields).context(CreationSnafu)?.into()
        };
        Ok(Self {
            left,
            right,
            left_on,
            right_on,
            left_by,
            right_by,
            join_direction,
            allow_exact_matches,
            output_schema,
            right_input_mapping,
        })
    }

    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        res.push(format!("Join: Direction = {}", self.join_direction));
        if !self.left_on.is_empty() && !self.right_on.is_empty() && self.left_on == self.right_on {
            res.push(format!(
                "On = {}",
                self.left_on.iter().map(|e| e.to_string()).join(", ")
            ));
        } else {
            if !self.left_on.is_empty() {
                res.push(format!(
                    "Left on = {}",
                    self.left_on.iter().map(|e| e.to_string()).join(", ")
                ));
            }
            if !self.right_on.is_empty() {
                res.push(format!(
                    "Right on = {}",
                    self.right_on.iter().map(|e| e.to_string()).join(", ")
                ));
            }
        }
        if !self.left_by.is_empty() && !self.right_by.is_empty() && self.left_by == self.right_by {
            res.push(format!(
                "By = {}",
                self.left_by.iter().map(|e| e.to_string()).join(", ")
            ));
        } else {
            if !self.left_by.is_empty() {
                res.push(format!(
                    "Left by = {}",
                    self.left_by.iter().map(|e| e.to_string()).join(", ")
                ));
            }
            if !self.right_by.is_empty() {
                res.push(format!(
                    "Right by = {}",
                    self.right_by.iter().map(|e| e.to_string()).join(", ")
                ));
            }
        }
        res.push(format!(
            "Output schema = {}",
            self.output_schema.short_string()
        ));
        res
    }
}
