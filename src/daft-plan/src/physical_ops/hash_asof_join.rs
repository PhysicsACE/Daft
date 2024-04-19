use daft_dsl::Expr;
use itertools::Itertools;

use crate::{physical_plan::PhysicalPlanRef, JoinDirection};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct HashAsOfJoin {
    // Upstream node.
    pub left: PhysicalPlanRef,
    pub right: PhysicalPlanRef,
    pub left_on: Vec<Expr>,
    pub right_on: Vec<Expr>,
    pub left_by: Vec<Expr>,
    pub right_by: Vec<Expr>,
    pub join_direction: JoinDirection,
    pub allow_exact_matches: bool,
}

impl HashAsOfJoin {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        left: PhysicalPlanRef,
        right: PhysicalPlanRef,
        left_on: Vec<Expr>,
        right_on: Vec<Expr>,
        left_by: Vec<Expr>,
        right_by: Vec<Expr>,
        join_direction: JoinDirection,
        allow_exact_matches: bool,
    ) -> Self {
        Self {
            left,
            right,
            left_on,
            right_on,
            left_by,
            right_by,
            join_direction,
            allow_exact_matches,
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
        res
    }
}
