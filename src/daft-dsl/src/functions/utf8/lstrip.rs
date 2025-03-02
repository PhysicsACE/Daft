use daft_core::{
    datatypes::{DataType, Field},
    schema::Schema,
    series::Series,
};

use crate::functions::FunctionExpr;
use crate::Expr;
use common_error::{DaftError, DaftResult};

use super::super::FunctionEvaluator;

pub(super) struct LstripEvaluator {}

impl FunctionEvaluator for LstripEvaluator {
    fn fn_name(&self) -> &'static str {
        "lstrip"
    }

    fn to_field(&self, inputs: &[Expr], schema: &Schema, _: &FunctionExpr) -> DaftResult<Field> {
        match inputs {
            [data] => match data.to_field(schema) {
                Ok(data_field) => match &data_field.dtype {
                    DataType::Utf8 => Ok(Field::new(data_field.name, DataType::Utf8)),
                    _ => Err(DaftError::TypeError(format!(
                        "Expects input to lstrip to be utf8, but received {data_field}",
                    ))),
                },
                Err(e) => Err(e),
            },
            _ => Err(DaftError::SchemaMismatch(format!(
                "Expected 1 input args, got {}",
                inputs.len()
            ))),
        }
    }

    fn evaluate(&self, inputs: &[Series], _: &FunctionExpr) -> DaftResult<Series> {
        match inputs {
            [data] => data.utf8_lstrip(),
            _ => Err(DaftError::ValueError(format!(
                "Expected 1 input args, got {}",
                inputs.len()
            ))),
        }
    }
}
