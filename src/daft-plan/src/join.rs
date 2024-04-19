use std::{
    fmt::{Display, Formatter, Result},
    str::FromStr,
};

use common_error::{DaftError, DaftResult};
use daft_core::impl_bincode_py_state_serialization;
#[cfg(feature = "python")]
use pyo3::{
    exceptions::PyValueError, pyclass, pymethods, types::PyBytes, PyObject, PyResult, PyTypeInfo,
    Python, ToPyObject,
};

use serde::{Deserialize, Serialize};

/// Type of a join operation.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "python", pyclass(module = "daft.daft"))]
pub enum JoinType {
    Inner,
    Left,
    Right,
}

#[cfg(feature = "python")]
#[pymethods]
impl JoinType {
    /// Create a JoinType from its string representation.
    ///
    /// Args:
    ///     join_type: String representation of the join type, e.g. "inner", "left", or "right".
    #[staticmethod]
    pub fn from_join_type_str(join_type: &str) -> PyResult<Self> {
        Self::from_str(join_type).map_err(|e| PyValueError::new_err(e.to_string()))
    }

    pub fn __str__(&self) -> PyResult<String> {
        Ok(self.to_string())
    }
}

impl_bincode_py_state_serialization!(JoinType);

impl JoinType {
    pub fn iterator() -> std::slice::Iter<'static, JoinType> {
        use JoinType::*;

        static JOIN_TYPES: [JoinType; 3] = [Inner, Left, Right];
        JOIN_TYPES.iter()
    }
}

impl FromStr for JoinType {
    type Err = DaftError;

    fn from_str(join_type: &str) -> DaftResult<Self> {
        use JoinType::*;

        match join_type {
            "inner" => Ok(Inner),
            "left" => Ok(Left),
            "right" => Ok(Right),
            _ => Err(DaftError::TypeError(format!(
                "Join type {} is not supported; only the following types are supported: {:?}",
                join_type,
                JoinType::iterator().as_slice()
            ))),
        }
    }
}

impl Display for JoinType {
    fn fmt(&self, f: &mut Formatter) -> Result {
        // Leverage Debug trait implementation, which will already return the enum variant as a string.
        write!(f, "{:?}", self)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "python", pyclass(module = "daft.daft"))]
pub enum JoinStrategy {
    Hash,
    SortMerge,
    Broadcast,
}

#[cfg(feature = "python")]
#[pymethods]
impl JoinStrategy {
    /// Create a JoinStrategy from its string representation.
    ///
    /// Args:
    ///     join_strategy: String representation of the join strategy, e.g. "hash", "sort_merge", or "broadcast".
    #[staticmethod]
    pub fn from_join_strategy_str(join_strategy: &str) -> PyResult<Self> {
        Self::from_str(join_strategy).map_err(|e| PyValueError::new_err(e.to_string()))
    }

    pub fn __str__(&self) -> PyResult<String> {
        Ok(self.to_string())
    }
}

impl_bincode_py_state_serialization!(JoinStrategy);

impl JoinStrategy {
    pub fn iterator() -> std::slice::Iter<'static, JoinStrategy> {
        use JoinStrategy::*;

        static JOIN_STRATEGIES: [JoinStrategy; 3] = [Hash, SortMerge, Broadcast];
        JOIN_STRATEGIES.iter()
    }
}

impl FromStr for JoinStrategy {
    type Err = DaftError;

    fn from_str(join_strategy: &str) -> DaftResult<Self> {
        use JoinStrategy::*;

        match join_strategy {
            "hash" => Ok(Hash),
            "sort_merge" => Ok(SortMerge),
            "broadcast" => Ok(Broadcast),
            _ => Err(DaftError::TypeError(format!(
                "Join strategy {} is not supported; only the following strategies are supported: {:?}",
                join_strategy,
                JoinStrategy::iterator().as_slice()
            ))),
        }
    }
}

impl Display for JoinStrategy {
    fn fmt(&self, f: &mut Formatter) -> Result {
        // Leverage Debug trait implementation, which will already return the enum variant as a string.
        write!(f, "{:?}", self)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "python", pyclass(module = "daft.daft"))]
pub enum JoinDirection {
    Backward,
    Forward,
    Nearest,
}

#[cfg(feature = "python")]
#[pymethods]
impl JoinDirection {
    /// Create a JoinDirection from its string representation.
    ///
    /// Args:
    ///     join_strategy: String representation of the join strategy, e.g. "backward", "forward", or "nearest".
    #[staticmethod]
    pub fn from_dir_type_str(join_direction: &str) -> PyResult<Self> {
        Self::from_str(join_direction).map_err(|e| PyValueError::new_err(e.to_string()))
    }

    pub fn __str__(&self) -> PyResult<String> {
        Ok(self.to_string())
    }
}

impl_bincode_py_state_serialization!(JoinDirection);

impl JoinDirection {
    pub fn iterator() -> std::slice::Iter<'static, JoinDirection> {
        use JoinDirection::*;

        static JOIN_DIRECTIONS: [JoinDirection; 3] = [Backward, Forward, Nearest];
        JOIN_DIRECTIONS.iter()
    }
}

impl FromStr for JoinDirection {
    type Err = DaftError;

    fn from_str(join_direction: &str) -> DaftResult<Self> {
        use JoinDirection::*;

        match join_direction {
            "backward" => Ok(Backward),
            "forward" => Ok(Forward),
            "nearest" => Ok(Nearest),
            _ => Err(DaftError::TypeError(format!(
                "Join direction {} is not supported; only the following strategies are supported: {:?}",
                join_direction,
                JoinDirection::iterator().as_slice()
            ))),
        }
    }
}

impl Display for JoinDirection {
    fn fmt(&self, f: &mut Formatter) -> Result {
        // Leverage Debug trait implementation, which will already return the enum variant as a string.
        write!(f, "{:?}", self)
    }
}
