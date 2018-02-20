use value::Type;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Column {
    pub name: String,
    pub type_: Type,
}

impl Column {
    pub fn new(name: String, type_: Type) -> Column {
        Column { name, type_ }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Schema {
    pub columns: Vec<Column>,
}

impl Schema {
    pub fn new(columns: &[(&str, Type)]) -> Schema {
        Schema {
            columns: columns
                .into_iter()
                .map(|&(name, ref type_)| {
                    Column::new(name.to_string(), type_.clone())
                })
                .collect(),
        }
    }

    pub fn keys(&self) -> Vec<&String> {
        self.columns.iter().map(|column| &column.name).collect()
    }

    pub fn iter(&self) -> ::std::slice::Iter<Column> {
        self.columns.iter()
    }

    pub fn type_(&self, name: &str) -> Option<Type> {
        self.columns
            .iter()
            .find(|column| column.name == name)
            .map(|column| column.type_.clone())
            .clone()
    }

    pub fn select(&self, column_names: &[&str]) -> Schema {
        Schema {
            columns: self.columns
                .iter()
                .filter(|&column| column_names.contains(&column.name.as_str()))
                .cloned()
                .collect(),
        }
    }
}

impl IntoIterator for Schema {
    type Item = Column;
    type IntoIter = ::std::vec::IntoIter<Column>;

    fn into_iter(self) -> Self::IntoIter {
        self.columns.into_iter()
    }
}