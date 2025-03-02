use daft_core::{
    datatypes::{DataType, Field},
    schema::Schema,
    series::Series,
};

use crate::Expr;

use crate::functions::FunctionExpr;
use common_error::{DaftError, DaftResult};

use super::{super::FunctionEvaluator, Utf8Expr};

pub(super) struct ExtractAllEvaluator {}

impl FunctionEvaluator for ExtractAllEvaluator {
    fn fn_name(&self) -> &'static str {
        "extractall"
    }

    fn to_field(&self, inputs: &[Expr], schema: &Schema, _: &FunctionExpr) -> DaftResult<Field> {
        match inputs {
            [data, pattern] => match (data.to_field(schema), pattern.to_field(schema)) {
                (Ok(data_field), Ok(pattern_field)) => {
                    match (&data_field.dtype, &pattern_field.dtype) {
                        (DataType::Utf8, DataType::Utf8) => {
                            Ok(Field::new(data_field.name, DataType::List(Box::new(DataType::Utf8))))
                        }
                        _ => Err(DaftError::TypeError(format!(
                            "Expects inputs to extractAll to be utf8, but received {data_field} and {pattern_field}",
                        ))),
                    }
                }
                (Err(e), _) | (_, Err(e)) => Err(e),
            },
            _ => Err(DaftError::SchemaMismatch(format!(
                "Expected 2 input args, got {}",
                inputs.len()
            ))),
        }
    }

    fn evaluate(&self, inputs: &[Series], expr: &FunctionExpr) -> DaftResult<Series> {
        match inputs {
            [data, pattern] => {
                let index = match expr {
                    FunctionExpr::Utf8(Utf8Expr::ExtractAll(index)) => index,
                    _ => panic!("Expected Utf8 ExtractAll Expr, got {expr}"),
                };
                data.utf8_extract_all(pattern, *index)
            }
            _ => Err(DaftError::ValueError(format!(
                "Expected 2 input args, got {}",
                inputs.len()
            ))),
        }
    }
}
