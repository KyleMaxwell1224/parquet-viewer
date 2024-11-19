use datafusion::prelude::*;
use datafusion::error::DataFusionError;
use std::error::Error;

#[derive(Clone)]
pub struct RowData {
    pub headers: Vec<String>,
    pub values: Vec<String>,
}

pub async fn execute_query(
    file_path: &str,
    sql_query: &str,
) -> Result<Vec<RowData>, Box<dyn Error + Send + Sync>> {
    let mut ctx = SessionContext::new();
    ctx.register_parquet("parquet_table", file_path, ParquetReadOptions::default())
        .await?;

    let df = ctx.sql(sql_query).await?;
    let results = df.collect().await?;

    let mut rows = Vec::new();

    for batch in results {
        let headers = batch
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect::<Vec<_>>();

        let columns = batch.columns();

        for row_index in 0..batch.num_rows() {
            let mut values = Vec::new();

            for col in columns {
                let value = arrow_value_to_string(col, row_index);
                values.push(value);
            }

            rows.push(RowData {
                headers: headers.clone(),
                values,
            });
        }
    }

    Ok(rows)
}

fn arrow_value_to_string(array: &datafusion::arrow::array::ArrayRef, index: usize) -> String {
    if array.is_null(index) {
        return "NULL".to_string();
    }

    use datafusion::arrow::array::*;
    use datafusion::arrow::datatypes::DataType;

    match array.data_type() {
        DataType::Boolean => {
            let array = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            array.value(index).to_string()
        }
        DataType::Int8 => {
            let array = array.as_any().downcast_ref::<Int8Array>().unwrap();
            array.value(index).to_string()
        }
        // ... handle other primitive types ...

        DataType::Utf8 => {
            let array = array.as_any().downcast_ref::<StringArray>().unwrap();
            array.value(index).to_string()
        }
        DataType::LargeUtf8 => {
            let array = array.as_any().downcast_ref::<LargeStringArray>().unwrap();
            array.value(index).to_string()
        }
        DataType::Struct(fields) => {
            let struct_array = array.as_any().downcast_ref::<StructArray>().unwrap();
            let mut field_values = Vec::new();

            for (i, field) in fields.iter().enumerate() {
                let child_array = struct_array.column(i);
                let value = arrow_value_to_string(child_array, index);
                field_values.push(format!("\"{}\": {}", field.name(), value));
            }

            format!("{{{}}}", field_values.join(", "))
        }
        DataType::List(_) => {
            let list_array = array.as_any().downcast_ref::<ListArray>().unwrap();
            let value_offsets = list_array.value_offsets();
            let start = value_offsets[index] as usize;
            let end = value_offsets[index + 1] as usize;

            let values = list_array.values();
            let mut element_values = Vec::new();

            for i in start..end {
                let element_value = arrow_value_to_string(&values, i);
                element_values.push(element_value);
            }

            format!("[{}]", element_values.join(", "))
        }
        DataType::LargeList(_) => {
            let list_array = array.as_any().downcast_ref::<LargeListArray>().unwrap();
            let value_offsets = list_array.value_offsets();
            let start = value_offsets[index] as usize;
            let end = value_offsets[index + 1] as usize;

            let values = list_array.values();
            let mut element_values = Vec::new();

            for i in start..end {
                let element_value = arrow_value_to_string(&values, i);
                element_values.push(element_value);
            }

            format!("[{}]", element_values.join(", "))
        }
        // Optionally handle Map, Union, etc.

        _ => "[Unsupported Type]".to_string(),
    }
}