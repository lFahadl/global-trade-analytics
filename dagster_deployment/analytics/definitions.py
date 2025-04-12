from dagster import Definitions

from .views import v_global_yearly_metrics_view, create_eci_balance_trends_view, create_export_portfolio_view
from .views import create_diversification_complexity_view, create_coi_predictive_power_view, create_portfolio_evolution_view
from .assets import calculate_product_year_metrics, calculate_bilateral_flows, calculate_export_specialization, calculate_complexity_dynamics
from .assets2 import calculate_product_complexity, calculate_export_portfolio, calculate_partner_diversification, calculate_coi_growth_analysis
from .resources import bigquery_resource

# Load specific assets from the views module and assets modules
analytics_assets = [
    # Views
    v_global_yearly_metrics_view,
    create_eci_balance_trends_view,
    create_export_portfolio_view,
    create_diversification_complexity_view,
    create_coi_predictive_power_view,
    create_portfolio_evolution_view,
    
    # Assets from assets.py
    calculate_product_year_metrics,
    calculate_bilateral_flows,
    calculate_export_specialization,
    calculate_complexity_dynamics,
    
    # Assets from assets2.py
    calculate_product_complexity,
    calculate_export_portfolio,
    calculate_partner_diversification,
    calculate_coi_growth_analysis
]

# Define the Dagster definitions for the analytics module
defs = Definitions(
    assets=analytics_assets,
    resources={
        "bq_resource": bigquery_resource,
    },
)
