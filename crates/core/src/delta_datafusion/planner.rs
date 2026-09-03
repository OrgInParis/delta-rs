//! DataFusion query planners for Delta operations.
//!
//! [`DeltaPlanner`] always installs the extension planners required by
//! delta-rs. Applications with their own logical extension nodes can add
//! [`ExtensionPlanner`] trait objects through
//! [`DeltaPlanner::with_extension_planners`] without replacing Delta's
//! planners.
use std::fmt;
use std::sync::{Arc, LazyLock};

use async_trait::async_trait;
use datafusion::logical_expr::physical_planning_context::PhysicalPlanningContext;
use datafusion::logical_expr::{LogicalPlan, UserDefinedLogicalNode};
use datafusion::physical_planner::PhysicalPlanner;
use datafusion::{
    catalog::Session,
    execution::context::QueryPlanner,
    physical_plan::ExecutionPlan,
    physical_planner::{DefaultPhysicalPlanner, ExtensionPlanner},
};

use crate::delta_datafusion::DataFusionResult;
use crate::delta_datafusion::data_validation::DataValidationExtensionPlanner;
use crate::operations::delete::DeleteMetricExtensionPlanner;
use crate::operations::merge::MergeMetricExtensionPlanner;
use crate::operations::update::UpdateMetricExtensionPlanner;
use crate::operations::write::metrics::WriteMetricExtensionPlanner;

static DELTA_EXTENSION_PLANNERS: LazyLock<Vec<Arc<dyn ExtensionPlanner + Send + Sync>>> =
    LazyLock::new(|| {
        vec![
            MergeMetricExtensionPlanner::new(),
            WriteMetricExtensionPlanner::new(),
            DeleteMetricExtensionPlanner::new(),
            UpdateMetricExtensionPlanner::new(),
            DataValidationExtensionPlanner::new(),
        ]
    });

static DELTA_PLANNER: LazyLock<Arc<DeltaPlanner>> = LazyLock::new(|| Arc::new(DeltaPlanner));

/// Deltaplanner
#[derive(Debug)]
pub struct DeltaPlanner;

impl DeltaPlanner {
    /// Return the shared, lazily-initialized [`DeltaPlanner`] instance.
    ///
    /// The planner is stateless, so a single cached instance is reused rather than
    /// allocating a new one per query.
    pub fn new() -> Arc<Self> {
        DELTA_PLANNER.clone()
    }

    /// Compose delta-rs's planners with application extension planners.
    ///
    /// Delta's planner always has first opportunity to lower the logical nodes
    /// introduced by Delta operations. Application planners are consulted, in
    /// the order supplied, only when Delta does not recognize a node.
    pub fn with_extension_planners(
        extension_planners: Vec<Arc<dyn ExtensionPlanner + Send + Sync>>,
    ) -> Arc<dyn QueryPlanner + Send + Sync> {
        Arc::new(ComposedDeltaPlanner { extension_planners })
    }
}

#[async_trait]
impl QueryPlanner for DeltaPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session: &dyn Session,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let planner = Arc::new(Box::new(DefaultPhysicalPlanner::with_extension_planners(
            vec![DeltaExtensionPlanner::new()],
        )));
        planner.create_physical_plan(logical_plan, session).await
    }
}

struct ComposedDeltaPlanner {
    extension_planners: Vec<Arc<dyn ExtensionPlanner + Send + Sync>>,
}

impl fmt::Debug for ComposedDeltaPlanner {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ComposedDeltaPlanner")
            .field("application_planner_count", &self.extension_planners.len())
            .finish()
    }
}

#[async_trait]
impl QueryPlanner for ComposedDeltaPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session: &dyn Session,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let mut extension_planners: Vec<Arc<dyn ExtensionPlanner + Send + Sync>> =
            Vec::with_capacity(1 + self.extension_planners.len());
        extension_planners.push(DeltaExtensionPlanner::new());
        extension_planners.extend(self.extension_planners.iter().cloned());
        DefaultPhysicalPlanner::with_extension_planners(extension_planners)
            .create_physical_plan(logical_plan, session)
            .await
    }
}

/// Extension [`PhysicalPlanner`](datafusion::physical_planner::PhysicalPlanner) that knows
/// how to lower delta-rs custom logical nodes into executable physical plans.
pub struct DeltaExtensionPlanner;

impl DeltaExtensionPlanner {
    /// Construct a new extension planner.
    pub fn new() -> Arc<Self> {
        Arc::new(Self {})
    }
}

#[async_trait]
impl ExtensionPlanner for DeltaExtensionPlanner {
    async fn plan_extension(
        &self,
        planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        session_state: &dyn Session,
        planning_ctx: &PhysicalPlanningContext,
    ) -> DataFusionResult<Option<Arc<dyn ExecutionPlan>>> {
        for ext_planner in DELTA_EXTENSION_PLANNERS.iter() {
            if let Some(plan) = ext_planner
                .plan_extension(
                    planner,
                    node,
                    logical_inputs,
                    physical_inputs,
                    session_state,
                    planning_ctx,
                )
                .await?
            {
                return Ok(Some(plan));
            }
        }
        Ok(None)
    }
}
