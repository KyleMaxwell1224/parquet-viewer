use std::fs::File;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::record::{Field, Row};

#[derive(Clone)]
pub struct RowData {
    pub headers: Vec<String>,
    pub values: Vec<String>,
}

pub fn read_parquet_file(file_path: &str) -> Result<Vec<RowData>, Box<dyn std::error::Error>> {
    let file = File::open(file_path)?;
    let reader = SerializedFileReader::new(file)?;
    let iter = reader.get_row_iter(None)?;

    let mut rows = Vec::new();

    for record in iter {
        let mut headers = Vec::new();
        let mut values = Vec::new();

        for (name, field) in record.get_column_iter() {
            headers.push(name.clone());
            values.push(format_field(field));
        }

        rows.push(RowData { headers, values });
    }

    Ok(rows)
}

pub fn query_data(data: &[RowData], query: &str) -> Vec<RowData> {
    // Simple query parsing: column_name=value
    let parts: Vec<&str> = query.split('=').collect();
    if parts.len() != 2 {
        return data.to_vec();
    }
    let column_name = parts[0].trim();
    let value = parts[1].trim();

    data.iter()
        .filter(|row| {
            row.headers
                .iter()
                .zip(&row.values)
                .any(|(header, val)| header == column_name && val == value)
        })
        .cloned()
        .collect()
}

fn format_field(field: &Field) -> String {
    match field {
        Field::Null => "NULL".to_string(),
        Field::Bool(b) => b.to_string(),
        Field::Byte(b) => b.to_string(),
        Field::UByte(b) => b.to_string(),
        Field::Short(s) => s.to_string(),
        Field::UShort(s) => s.to_string(),
        Field::Int(i) => i.to_string(),
        Field::UInt(u) => u.to_string(),
        Field::Long(l) => l.to_string(),
        Field::ULong(u) => u.to_string(),
        Field::Float(f) => f.to_string(),
        Field::Double(d) => d.to_string(),
        Field::Str(s) => s.clone(),
        Field::Bytes(b) => String::from_utf8_lossy(b.data()).to_string(),
        Field::Group(_g) => "[Group]".to_string(),
        _ => "[Unsupported]".to_string(),
    }
}