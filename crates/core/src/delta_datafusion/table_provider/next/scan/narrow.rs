//! Narrowing a Delta scan to the struct fields a projection reads.
//!
//! DataFusion's physical projection pushdown offers a [`ProjectionExec`]
//! placed above a scan to the scan through
//! `ExecutionPlan::try_swapping_with_projection`. A projection that reads a
//! struct column through `get_field` names the fields it needs; a scan that
//! knows them can hand the parquet reader a read schema whose struct is
//! narrower than the file's, and the reader then decodes those leaves alone
//! (`nested_schema_pruning` in the parquet data source: the expression
//! adapter rewrites the column into a cast to the narrower struct, and the
//! cast is resolved to a leaf mask). The kernel's per-file transform is a
//! sparse struct patch, which passes a field it does not mention through
//! unmodified, so a narrowed struct crosses it unchanged.
//!
//! This module collects what a projection reads and derives the narrowed
//! schemas: the Arrow schemas the parquet read and the scan contract carry,
//! and the kernel schemas the transform is evaluated between. Top-level
//! columns keep their names, order and positions — only the fields inside a
//! struct column are pruned — so the projection above the narrowed scan is
//! the original one, unchanged.
//!
//! [`ProjectionExec`]: datafusion::physical_plan::projection::ProjectionExec

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_schema::{DataType, Field, FieldRef, Fields, Schema};
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::datasource::physical_plan::ParquetSource;
use datafusion::physical_expr::ScalarFunctionExpr;
use datafusion::physical_expr::expressions::{Column, Literal};
use datafusion::physical_plan::projection::ProjectionExpr;
use datafusion::physical_plan::{ExecutionPlan, PhysicalExpr};
use datafusion_datasource::TableSchema;
use datafusion_datasource::file::FileSource;
use datafusion_datasource::file_scan_config::FileScanConfigBuilder;
use datafusion_datasource::source::DataSourceExec;
use delta_kernel::schema::{DataType as KernelDataType, StructField, StructType};

use super::expr_adapter::relax_schema_nested_nullability;
use super::plan::KernelScanPlan;

/// DataFusion's struct field access.
const GET_FIELD: &str = "get_field";

/// How a projection reads one column, or one field of a struct: whole, or
/// through named fields of it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum FieldAccess {
    Whole,
    Fields(BTreeMap<String, FieldAccess>),
}

impl FieldAccess {
    /// Record one read along `path`: an empty path reads the whole value,
    /// and a whole read absorbs every narrower one.
    fn record(&mut self, path: &[&str]) {
        match path.split_first() {
            None => *self = Self::Whole,
            Some((head, rest)) => {
                if let Self::Fields(children) = self {
                    children
                        .entry((*head).to_owned())
                        .or_insert_with(|| Self::Fields(BTreeMap::new()))
                        .record(rest);
                }
            }
        }
    }
}

/// The columns a projection reads, by name, with the fields it reads of each.
pub(crate) type ColumnAccess = BTreeMap<String, FieldAccess>;

/// What `expressions` read of `input`'s columns, or `None` when every read is
/// of a whole column, so nothing narrows.
pub(crate) fn access_of(expressions: &[ProjectionExpr], input: &Schema) -> Option<ColumnAccess> {
    let mut access = ColumnAccess::new();
    for projection in expressions {
        collect(&projection.expr, input, &mut access);
    }
    access
        .values()
        .any(|column| matches!(column, FieldAccess::Fields(_)))
        .then_some(access)
}

fn collect(expr: &Arc<dyn PhysicalExpr>, input: &Schema, access: &mut ColumnAccess) {
    if let Some(column) = expr.downcast_ref::<Column>() {
        record(access, input, column, &[]);
        return;
    }
    let mut path: Vec<&str> = Vec::new();
    if let Some(column) = field_chain(expr, &mut path) {
        // Collected outermost field first; the read goes root outwards.
        path.reverse();
        record(access, input, column, &path);
        return;
    }
    for child in expr.children() {
        collect(child, input, access);
    }
}

fn record(access: &mut ColumnAccess, input: &Schema, column: &Column, path: &[&str]) {
    let Some(field) = input.fields().get(column.index()) else {
        return;
    };
    access
        .entry(field.name().clone())
        .or_insert_with(|| FieldAccess::Fields(BTreeMap::new()))
        .record(path);
}

/// The column at the root of a chain of field accesses, the field names
/// pushed outermost first; `None` where `expr` is not such a chain.
fn field_chain<'e>(expr: &'e Arc<dyn PhysicalExpr>, path: &mut Vec<&'e str>) -> Option<&'e Column> {
    let function = expr.downcast_ref::<ScalarFunctionExpr>()?;
    if function.name() != GET_FIELD {
        return None;
    }
    let [inner, name] = function.args() else {
        return None;
    };
    let name = match name.downcast_ref::<Literal>()?.value() {
        ScalarValue::Utf8(Some(name))
        | ScalarValue::LargeUtf8(Some(name))
        | ScalarValue::Utf8View(Some(name)) => name.as_str(),
        _ => return None,
    };
    path.push(name);
    if let Some(column) = inner.downcast_ref::<Column>() {
        return Some(column);
    }
    field_chain(inner, path)
}

/// `schema` with every struct column the access names through its fields
/// pruned to those fields, recursively. A column the access does not name,
/// reads whole, or reads through fields the struct does not have keeps its
/// type; names, order, positions and metadata are preserved throughout.
pub(crate) fn narrow_arrow_schema(schema: &Schema, access: &ColumnAccess) -> Schema {
    let fields: Vec<FieldRef> = schema
        .fields()
        .iter()
        .map(|field| match access.get(field.name()) {
            Some(column) => narrow_arrow_field(field, column)
                .map(Arc::new)
                .unwrap_or_else(|| Arc::clone(field)),
            None => Arc::clone(field),
        })
        .collect();
    Schema::new_with_metadata(fields, schema.metadata().clone())
}

fn narrow_arrow_field(field: &Field, access: &FieldAccess) -> Option<Field> {
    let FieldAccess::Fields(children) = access else {
        return None;
    };
    let DataType::Struct(fields) = field.data_type() else {
        return None;
    };
    let kept: Vec<FieldRef> = fields
        .iter()
        .filter_map(|inner| {
            let access = children.get(inner.name().as_str())?;
            Some(
                narrow_arrow_field(inner, access)
                    .map(Arc::new)
                    .unwrap_or_else(|| Arc::clone(inner)),
            )
        })
        .collect();
    // A struct read through fields it does not have stays whole: the leaf
    // clipper needs one surviving leaf per level, and an empty struct has
    // none.
    if kept.is_empty() {
        return None;
    }
    let narrowed = DataType::Struct(Fields::from(kept));
    (&narrowed != field.data_type()).then(|| field.clone().with_data_type(narrowed))
}

/// The kernel schema `schema` narrowed exactly as [`narrow_arrow_schema`]
/// narrows the Arrow one: what the per-file transform is evaluated between.
pub(crate) fn narrow_kernel_schema(
    schema: &StructType,
    access: &ColumnAccess,
) -> Result<StructType> {
    let fields = schema
        .fields()
        .map(|field| match access.get(field.name().as_str()) {
            Some(column) => narrow_kernel_field(field, column)
                .map(|narrowed| narrowed.unwrap_or_else(|| field.clone())),
            None => Ok(field.clone()),
        })
        .collect::<Result<Vec<StructField>>>()?;
    StructType::try_new(fields).map_err(kernel_error)
}

fn narrow_kernel_field(field: &StructField, access: &FieldAccess) -> Result<Option<StructField>> {
    let FieldAccess::Fields(children) = access else {
        return Ok(None);
    };
    let KernelDataType::Struct(inner) = field.data_type() else {
        return Ok(None);
    };
    let kept = inner
        .fields()
        .filter_map(|nested| {
            let access = children.get(nested.name().as_str())?;
            Some(
                narrow_kernel_field(nested, access)
                    .map(|narrowed| narrowed.unwrap_or_else(|| nested.clone())),
            )
        })
        .collect::<Result<Vec<StructField>>>()?;
    if kept.is_empty() {
        return Ok(None);
    }
    let narrowed = StructType::try_new(kept).map_err(kernel_error)?;
    Ok(Some(
        StructField::new(
            field.name().clone(),
            KernelDataType::Struct(Box::new(narrowed)),
            field.is_nullable(),
        )
        .with_metadata(
            field
                .metadata
                .iter()
                .map(|(key, value)| (key.clone(), value.clone())),
        ),
    ))
}

fn kernel_error(error: delta_kernel::Error) -> DataFusionError {
    DataFusionError::External(Box::new(error))
}

/// The parquet read of `input` rebuilt for the narrowed `plan`: the same
/// files, groups, statistics, predicate and reader factory, under a table
/// schema whose structs are the narrowed ones — which is what makes the
/// expression adapter cast each file's wider struct down, and the reader
/// decode only the leaves the cast keeps. `None` where `input` is not one
/// parquet data source, which is the shape the scan builds for one store.
pub(crate) fn narrowed_parquet_input(
    input: &Arc<dyn ExecutionPlan>,
    plan: &KernelScanPlan,
) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    let Some(source_exec) = input.downcast_ref::<DataSourceExec>() else {
        return Ok(None);
    };
    let Some((config, parquet)) = source_exec.downcast_to_file_source::<ParquetSource>() else {
        return Ok(None);
    };
    // Nested nullability relaxed exactly as the original read's schema was
    // (`get_read_plan`): Delta's non-null nested fields are a write-time
    // invariant the files may not carry.
    let read_schema = Arc::new(relax_schema_nested_nullability(&plan.parquet_read_schema));
    let table_schema = TableSchema::builder(read_schema)
        .with_table_partition_cols(vec![plan.contract.file_id_field.clone()])
        .build();
    let mut source = ParquetSource::new(table_schema)
        .with_table_parquet_options(parquet.table_parquet_options().clone());
    if let Some(factory) = parquet.parquet_file_reader_factory() {
        source = source.with_parquet_file_reader_factory(Arc::clone(factory));
    }
    if let Some(predicate) = FileSource::filter(parquet) {
        source = source.with_predicate(predicate).with_pushdown_filters(true);
    }
    let config = FileScanConfigBuilder::from(config.clone())
        .with_source(Arc::new(source))
        .build();
    Ok(Some(DataSourceExec::from_data_source(config)))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn person() -> Schema {
        Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new(
                "properties",
                DataType::Struct(Fields::from(vec![
                    Field::new("name", DataType::Utf8, true),
                    Field::new("age", DataType::Int64, true),
                    Field::new(
                        "address",
                        DataType::Struct(Fields::from(vec![
                            Field::new("city", DataType::Utf8, true),
                            Field::new("zip", DataType::Utf8, true),
                        ])),
                        true,
                    ),
                ])),
                false,
            ),
        ])
    }

    fn access(paths: &[&[&str]]) -> ColumnAccess {
        let mut access = ColumnAccess::new();
        for path in paths {
            access
                .entry("properties".to_owned())
                .or_insert_with(|| FieldAccess::Fields(BTreeMap::new()))
                .record(path);
        }
        access
    }

    #[test]
    fn a_struct_is_pruned_to_the_fields_read() {
        let narrowed = narrow_arrow_schema(&person(), &access(&[&["name"], &["address", "city"]]));
        let DataType::Struct(fields) = narrowed.field(1).data_type() else {
            panic!("properties stays a struct");
        };
        let names: Vec<&str> = fields.iter().map(|field| field.name().as_str()).collect();
        assert_eq!(names, vec!["name", "address"]);
        let DataType::Struct(address) = fields[1].data_type() else {
            panic!("address stays a struct");
        };
        assert_eq!(address.len(), 1);
        assert_eq!(address[0].name(), "city");
        assert_eq!(
            narrowed.field(0),
            person().field(0),
            "an unnamed column is untouched"
        );
    }

    #[test]
    fn a_whole_read_and_an_unknown_field_keep_the_struct() {
        let mut whole = ColumnAccess::new();
        whole.insert("properties".to_owned(), FieldAccess::Whole);
        assert_eq!(narrow_arrow_schema(&person(), &whole), person());
        assert_eq!(
            narrow_arrow_schema(&person(), &access(&[&["missing"]])),
            person()
        );
    }

    #[test]
    fn a_whole_read_absorbs_a_field_read() {
        let mut field = FieldAccess::Fields(BTreeMap::new());
        field.record(&["name"]);
        field.record(&[]);
        assert_eq!(field, FieldAccess::Whole);
        field.record(&["age"]);
        assert_eq!(field, FieldAccess::Whole);
    }
}
