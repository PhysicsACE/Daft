use daft_dsl::{ExprRef, WindowSpecRef};
use itertools::Itertools;
use serde::{Deserialize, Serialize};

use crate::{impl_default_tree_display, PhysicalPlanRef};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PartitionedWindow {
    pub input: PhysicalPlanRef,
    pub window_spec: WindowSpecRef,
    pub window_fns: Vec<ExprRef>,
    pub window_names: Vec<String>,
}

impl PartitionedWindow {
    pub(crate) fn new(
        input: PhysicalPlanRef,
        window_spec: WindowSpecRef,
        window_fns: Vec<ExprRef>,
        window_names: Vec<String>,
    ) -> Self {
        Self {
            input,
            window_spec,
            window_fns,
            window_names,
        }
    }

    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec!["PartitionedWindow".to_string()];
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
        res
    }
}

impl_default_tree_display!(PartitionedWindow);
