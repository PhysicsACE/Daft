use common_error::DaftResult;
use daft_dsl::{ExprRef, WindowSpecRef};
use daft_io::IOStatsContext;
use daft_table::Table;

use crate::micropartition::MicroPartition;

impl MicroPartition {
    pub fn partitioned_window(
        &self,
        window_spec: WindowSpecRef,
        window_fns: &[ExprRef],
        window_names: Vec<String>,
    ) -> DaftResult<Self> {
        let io_stats = IOStatsContext::new("MicroPartition::window");

        let tables = self.concat_or_get(io_stats)?;

        match tables.as_slice() {
            [] => {
                let empty_table = Table::empty(Some(self.schema.clone()))?;
                let windowed =
                    empty_table.partitioned_window(window_spec, window_fns, window_names)?;
                Ok(Self::new_loaded(
                    windowed.schema.clone(),
                    vec![windowed].into(),
                    None,
                ))
            }
            [t] => {
                let windowed = t.partitioned_window(window_spec, window_fns, window_names)?;
                Ok(Self::new_loaded(
                    windowed.schema.clone(),
                    vec![windowed].into(),
                    None,
                ))
            }
            _ => unreachable!(),
        }
    }
}
