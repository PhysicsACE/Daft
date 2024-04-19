use daft_dsl::Expr;
use itertools::Itertools;

use crate::{physical_plan::PhysicalPlanRef, JoinDirection};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RangeAsOfJoin {
    // Upstream node.
    pub left: PhysicalPlanRef,
    pub right: PhysicalPlanRef,
    pub left_on: Vec<Expr>,
    pub right_on: Vec<Expr>,
    pub join_direction: JoinDirection,
    pub allow_exact_matches: bool,
    pub num_partitions: usize,
}

impl RangeAsOfJoin {
    pub(crate) fn new(
        left: PhysicalPlanRef,
        right: PhysicalPlanRef,
        left_on: Vec<Expr>,
        right_on: Vec<Expr>,
        join_direction: JoinDirection,
        allow_exact_matches: bool,
        num_partitions: usize,
    ) -> Self {
        Self {
            left,
            right,
            left_on,
            right_on,
            join_direction,
            allow_exact_matches,
            num_partitions,
        }
    }

    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        res.push(format!("AsOfJoin: Direction = {}", self.join_direction));
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
        res
    }
}
