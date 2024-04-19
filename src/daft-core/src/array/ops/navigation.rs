// use arrow2;

// use super::DaftNavigationWindow;
// use super::as_arrow::AsArrow;
// use crate::{array::DataArray, datatypes::*};
// use common_error::DaftResult;
// use arrow2::array::Array;

// macro_rules! impl_daft_navigation_window {
//     ($T:ident) => {
//         impl DaftNavigationWindow for &DataArray<$T> {
//             type Output = DaftResult<DataArray<$T>>;

//             fn lead(&self, n: u64, default: Option<&Self>) -> Self::Output {
//                 let primitive_arr = self.as_arrow();
//                 let default_val = None;
//                 if default.is_some() {
//                     default_val = Some(default.unwrap().as_arrow().value(0));
//                 }

//                 let length = primitive_arr.len();
//                 let mut res: Vec<Option<$T>> = Vec::with_capacity(length);
//                 let mut idx = 0;
//                 while idx < length {
//                     if idx + n as usize > length {
//                         res.push(default_val);
//                         continue;
//                     }
//                     res.push(Some(primitive_arr.value(idx + n as usize)));
//                     idx += 1;
//                 }

//                 let new_primitive = Box::new(PrimitiveArray::from(res));
//                 Ok(DataArray::from((self.field.name.as_ref(), new_primitive)))
//             }
//         }
//     }
// }
