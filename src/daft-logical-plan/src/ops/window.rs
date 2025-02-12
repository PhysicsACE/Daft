use std::sync::Arc;

use common_error::DaftResult;
use daft_dsl::{ExprRef, WindowSpecRef};
use daft_schema::schema::{Schema, SchemaRef};
use itertools::Itertools;

use crate::{
    logical_plan::{self},
    stats::StatsState,
    LogicalPlan,
};

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Window {
    pub input: Arc<LogicalPlan>,
    pub window_spec: WindowSpecRef,
    pub window_fns: Vec<ExprRef>,
    pub window_names: Vec<String>,
    pub output_schema: SchemaRef,
    pub stats_state: StatsState,
}

impl Window {
    pub(crate) fn try_new(
        input: Arc<LogicalPlan>,
        window_spec: WindowSpecRef,
        window_fns: Vec<ExprRef>,
        window_names: Vec<String>,
    ) -> logical_plan::Result<Self> {
        let output_schema = {
            let upstream_schema = input.schema();
            let fields = window_fns
                .iter()
                .map(|e| e.to_field(&upstream_schema))
                .collect::<DaftResult<Vec<_>>>()?;

            let mut new_schema = upstream_schema.fields.clone();
            fields
                .into_iter()
                .zip(window_names.iter())
                .for_each(|(field, name)| {
                    new_schema.insert(name.clone(), field.rename(name.clone()));
                });
            Arc::new(Schema { fields: new_schema })
        };

        Ok(Self {
            input,
            window_spec,
            window_fns,
            window_names,
            output_schema,
            stats_state: StatsState::NotMaterialized,
        })
    }

    pub(crate) fn with_materialized_stats(mut self) -> Self {
        let input_stats = self.input.materialized_stats();
        self.stats_state = StatsState::Materialized(input_stats.clone().into());
        self
    }

    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec!["Window".to_string()];
        res.push(format!(
            "Functions: {}",
            self.window_fns.iter().map(|e| e.to_string()).join(", ")
        ));
        res.push(format!(
            "Partition By: {}",
            self.window_spec
                .partition_by
                .iter()
                .map(|e| e.to_string())
                .join(", ")
        ));
        res.push(format!(
            "Order By: {}",
            self.window_spec
                .order_by
                .iter()
                .map(|e| e.to_string())
                .join(", ")
        ));
        res.push(format!(
            "Output Schema: {}",
            self.output_schema.short_string()
        ));
        if let StatsState::Materialized(stats) = &self.stats_state {
            res.push(format!("Stats: {}", stats));
        }
        res
    }
}
