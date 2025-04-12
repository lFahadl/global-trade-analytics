from dagster import Definitions

from .views import v_global_yearly_metrics_view
from .assets import calculate_product_year_metrics, calculate_bilateral_flows, calculate_export_specialization, calculate_complexity_dynamics
from .resources import bigquery_resource

# Load specific assets from the views module
analytics_assets = [
    v_global_yearly_metrics_view
]

# Define the Dagster definitions for the analytics module
defs = Definitions(
    assets=analytics_assets,
    resources={
        "bq_resource": bigquery_resource,
    },
)
