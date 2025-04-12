#!/usr/bin/env python

import os
from typing import override
import streamlit as st

# IMPORTANT: Set page configuration must be the first Streamlit command
st.set_page_config(
    page_title="Global Trade Analytics Dashboard",
    page_icon="🌐",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Now import the rest of the libraries
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from google.cloud import bigquery
from google.oauth2 import service_account
from dotenv import load_dotenv

# Load environment variables
load_dotenv(override=True)

# Get environment variables
GCP_PROJECT_ID = os.environ.get('GCP_PROJECT_ID')
GOOGLE_APPLICATION_CREDENTIALS = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')
RAW_DATASET = os.environ.get('RAW_DATASET')
PROCESSED_DATASET = os.environ.get('PROCESSED_DATASET')
ANALYTICS_DATASET = os.environ.get('ANALYTICS_DATASET')

# Create credentials and BigQuery client with caching
@st.cache_resource(ttl=3600)  # Cache for 1 hour
def get_bigquery_client():
    """Create and return a cached BigQuery client"""
    credentials = service_account.Credentials.from_service_account_file(
        GOOGLE_APPLICATION_CREDENTIALS,
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    return bigquery.Client(credentials=credentials, project=GCP_PROJECT_ID)

# Create BigQuery client
client = get_bigquery_client()

# Get dataset location with caching
@st.cache_data(ttl=86400)  # Cache for 24 hours
def get_dataset_location():
    try:
        dataset = client.get_dataset(f"{GCP_PROJECT_ID}.{RAW_DATASET}")
        return dataset.location
    except Exception as e:
        # Use st.warning instead of st.error to avoid early Streamlit commands
        # This will be displayed only when the function is actually called
        return "US"  # Default to US if we can't determine the location

# Dataset location
DATASET_LOCATION = get_dataset_location()

# Dashboard title
st.title("Global Trade Analytics Dashboard")

# Sidebar for navigation
st.sidebar.title("Navigation")
page = st.sidebar.radio(
    "Select a page",
    ["Overview", "Economic Complexity vs. Trade Balance", "Export Complexity Portfolio", 
     "Partner Diversification", "Complexity Outlook Index", "Export Portfolio Evolution"]
)
# page = "Overview"  # Only show Overview page for now

# Function to load data from BigQuery with caching
@st.cache_data(ttl=3600)  # Cache for 1 hour
def load_data(query, query_id=None):
    """Load data from BigQuery with caching.
    
    Args:
        query: SQL query to execute
        query_id: Optional identifier for the query for better cache management
        
    Returns:
        DataFrame with query results
    """
    try:
        df = client.query(query, location=DATASET_LOCATION).to_dataframe()
        return df
    except Exception as e:
        # Log the error but don't use st.error here to avoid early Streamlit commands
        # We'll check for empty dataframes later and show errors then
        print(f"Error loading data: {e}")
        return pd.DataFrame()

# Function to get available years with caching
@st.cache_data(ttl=86400)  # Cache for 24 hours
def get_available_years():
    query = f"""
    SELECT DISTINCT year 
    FROM `{GCP_PROJECT_ID}.{PROCESSED_DATASET}.combined_trade_data`
    ORDER BY year
    """
    years_df = load_data(query, "available_years")
    if not years_df.empty:
        return years_df['year'].tolist()
    return []

# Function to get available countries with caching
@st.cache_data(ttl=86400)  # Cache for 24 hours
def get_available_countries():
    query = f"""
    SELECT DISTINCT country_id 
    FROM `{GCP_PROJECT_ID}.{PROCESSED_DATASET}.combined_trade_data`
    ORDER BY country_id
    """
    countries_df = load_data(query, "available_countries")
    if not countries_df.empty:
        return countries_df['country_id'].tolist()
    return []

# Load global metrics data with caching (this loads all years at once)
@st.cache_data(ttl=3600)  # Cache for 1 hour
def load_global_metrics():
    query = f"""
    SELECT *
    FROM `{GCP_PROJECT_ID}.{ANALYTICS_DATASET}.v_global_yearly_metrics`
    ORDER BY year
    """
    return load_data(query, "global_metrics")

# Load top countries by trade volume with caching (for a specific year)
@st.cache_data(ttl=3600)  # Cache for 1 hour
def load_top_countries(year):
    query = f"""
    SELECT
      country_id,
      total_exports + total_imports as trade_volume,
      total_exports,
      total_imports,
      eci
    FROM `{GCP_PROJECT_ID}.{PROCESSED_DATASET}.country_year_metrics`
    WHERE year = {year}
    ORDER BY trade_volume DESC
    LIMIT 15
    """
    return load_data(query, f"top_countries_{year}")

# Load ECI vs. Trade Balance data with caching
@st.cache_data(ttl=3600)  # Cache for 1 hour
def load_eci_balance_trends(year):
    query = f"""
    SELECT * 
    FROM `{GCP_PROJECT_ID}.{ANALYTICS_DATASET}.v_eci_balance_trends`
    WHERE year = {year}
    """
    return load_data(query, f"eci_balance_{year}")

# Load Export Complexity Portfolio data with caching
@st.cache_data(ttl=3600)  # Cache for 1 hour
def load_export_portfolio(year):
    query = f"""
    SELECT *
    FROM `{GCP_PROJECT_ID}.{ANALYTICS_DATASET}.v_export_complexity_portfolio`
    WHERE year = {year}
    ORDER BY eci DESC
    """
    return load_data(query, f"export_portfolio_{year}")

# Load Partner Diversification data with caching
@st.cache_data(ttl=3600)  # Cache for 1 hour
def load_partner_diversification(year):
    query = f"""
    SELECT *
    FROM `{GCP_PROJECT_ID}.{ANALYTICS_DATASET}.v_diversification_complexity`
    WHERE year = {year}
    ORDER BY partner_hhi
    """
    return load_data(query, f"partner_diversification_{year}")

# Load Complexity Outlook Index data with caching
@st.cache_data(ttl=3600)  # Cache for 1 hour
def load_coi_predictive_power():
    query = f"""
    SELECT *
    FROM `{GCP_PROJECT_ID}.{ANALYTICS_DATASET}.v_coi_predictive_power`
    ORDER BY base_year, coi_quartile
    """
    return load_data(query, "coi_predictive_power")

# Add a cache clearing button in the sidebar
if st.sidebar.button("Clear Cache"):
    # Clear all st.cache_data caches
    st.cache_data.clear()
    st.success("Cache cleared successfully!")

# Cache available years and countries
available_years = get_available_years()
latest_year = max(available_years) if available_years else 2022
available_countries = get_available_countries()

# Sidebar filters
st.sidebar.title("Filters")
available_years = sorted(available_years)
selected_year = st.sidebar.selectbox("Select Year", available_years, index=len(available_years)-1)

# Overview page
if page == "Overview":
    st.header("Global Trade Overview")
    
    # Show loading spinner while data is being loaded
    with st.spinner("Loading global metrics..."):
        # Load global metrics (all years at once, cached)
        global_metrics_df = load_global_metrics()
    
    if not global_metrics_df.empty:
        # Create metrics for the selected year
        selected_year_metrics = global_metrics_df[global_metrics_df['year'] == selected_year].iloc[0] if selected_year in global_metrics_df['year'].values else None
        
        if selected_year_metrics is not None:
            # Calculate year-over-year changes
            prev_year_metrics = global_metrics_df[global_metrics_df['year'] == selected_year - 1].iloc[0] if selected_year - 1 in global_metrics_df['year'].values else None
            
            col1, col2, col3 = st.columns(3)
            
            # Global Trade Volume Trend
            st.subheader("Global Trade Volume Trend")
            fig = px.line(
                global_metrics_df,
                x="year",
                y="global_trade_volume",  # Plot only global exports (representing volume)
                labels={"value": "Global Trade Volume (USD)", "year": "Year"}, # Updated label
                title="Global Trade Volume Over Time"
            )
            fig.update_layout(showlegend=False) # Legend is not needed for a single line
            st.plotly_chart(fig, use_container_width=True)
            
            # Average ECI Trend
            st.subheader("Economic Complexity Index Trend")
            fig = px.line(
                global_metrics_df,
                x="year",
                y="avg_eci",
                labels={"avg_eci": "Average ECI", "year": "Year"},
                title="Global Average Economic Complexity Index Over Time"
            )
            st.plotly_chart(fig, use_container_width=True)
            
            # Load and display top countries (specific to selected year, cached per year)
            st.subheader("Top Countries by Trade Volume")
            with st.spinner(f"Loading top countries for {selected_year}..."):
                top_countries_df = load_top_countries(selected_year)
            
            if not top_countries_df.empty:
                fig = px.bar(
                    top_countries_df,
                    x="country_id",
                    y="trade_volume",
                    color="eci",
                    labels={"country_id": "Country", "trade_volume": "Trade Volume (USD)", "eci": "ECI"},
                    title=f"Top 15 Countries by Trade Volume ({selected_year})",
                    color_continuous_scale=px.colors.sequential.Viridis
                )
                fig.update_layout(xaxis_tickangle=-45)
                st.plotly_chart(fig, use_container_width=True)
        else:
            st.warning(f"No data available for the selected year: {selected_year}")
    else:
        st.error("Failed to load global metrics data. Please check your BigQuery connection.")

# Economic Complexity vs. Trade Balance page
elif page == "Economic Complexity vs. Trade Balance":
    st.header("Economic Complexity vs. Trade Balance Trends")
    
    # Load ECI vs. Trade Balance data
    eci_balance_df = load_eci_balance_trends(selected_year)
    
    if not eci_balance_df.empty:
        # Add information about data availability
        if 'No change data' in eci_balance_df['trend_category'].values:
            st.info("📊 Note: Some countries only have data for a single year, so year-over-year changes cannot be calculated. These countries are shown with a 'No change data' category.")
        
        # Create quadrant labels
        eci_balance_df['quadrant'] = eci_balance_df['trend_category']
        
        # Create color map for quadrants including the fallback category
        color_map = {
            'Both improving': '#2ca02c',      # Green
            'ECI improving only': '#1f77b4',  # Blue
            'Balance improving only': '#ff7f0e', # Orange
            'Both worsening': '#d62728',      # Red
            'No change data': '#9467bd'       # Purple
        }
        
        # Create two separate dataframes - one for countries with change data and one for countries without
        change_data_df = eci_balance_df[eci_balance_df['trend_category'] != 'No change data']
        no_change_data_df = eci_balance_df[eci_balance_df['trend_category'] == 'No change data']
        
        # Only create the quadrant chart if we have countries with change data
        if not change_data_df.empty:
            # Create quadrant scatter plot
            st.subheader("ECI Change vs. Trade Balance Change Quadrant Chart")
            
            fig = px.scatter(
                change_data_df,
                x="eci_change",
                y="balance_change",
                color="quadrant",
                hover_name="country_id",
                labels={
                    "eci_change": "ECI Change (Year-over-Year)",
                    "balance_change": "Trade Balance Change (Year-over-Year)",
                    "quadrant": "Trend Category"
                },
                title=f"Economic Complexity vs. Trade Balance Trends ({selected_year})",
                color_discrete_map=color_map,
                size_max=15,
                opacity=0.7
            )
            
            # Add quadrant lines
            fig.add_shape(
                type="line", line=dict(dash="dash", width=1, color="gray"),
                x0=0, y0=fig.data[0].y.min(), x1=0, y1=fig.data[0].y.max()
            )
            fig.add_shape(
                type="line", line=dict(dash="dash", width=1, color="gray"),
                x0=fig.data[0].x.min(), y0=0, x1=fig.data[0].x.max(), y1=0
            )
            
            # Add quadrant annotations
            fig.add_annotation(x=0.75*fig.data[0].x.max(), y=0.75*fig.data[0].y.max(), 
                            text="Both improving", showarrow=False, font=dict(size=14, color="#2ca02c"))
            fig.add_annotation(x=0.75*fig.data[0].x.max(), y=0.75*fig.data[0].y.min(), 
                            text="ECI improving only", showarrow=False, font=dict(size=14, color="#1f77b4"))
            fig.add_annotation(x=0.75*fig.data[0].x.min(), y=0.75*fig.data[0].y.max(), 
                            text="Balance improving only", showarrow=False, font=dict(size=14, color="#ff7f0e"))
            fig.add_annotation(x=0.75*fig.data[0].x.min(), y=0.75*fig.data[0].y.min(), 
                            text="Both worsening", showarrow=False, font=dict(size=14, color="#d62728"))
            
            # Add trend line if we have enough data points
            if len(change_data_df) > 1:
                fig.update_layout(
                    shapes=[
                        dict(
                            type='line',
                            x0=fig.data[0].x.min(),
                            y0=np.polyval(np.polyfit(change_data_df['eci_change'], change_data_df['balance_change'], 1), fig.data[0].x.min()),
                            x1=fig.data[0].x.max(),
                            y1=np.polyval(np.polyfit(change_data_df['eci_change'], change_data_df['balance_change'], 1), fig.data[0].x.max()),
                            line=dict(color='rgba(50, 50, 50, 0.5)', width=2, dash='dot')
                        )
                    ]
                )
            
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.warning("No countries with year-over-year change data available for the selected year.")
        
        # Country distribution by trend category
        st.subheader("Country Distribution by Trend Category")
        trend_counts = eci_balance_df['trend_category'].value_counts().reset_index()
        trend_counts.columns = ['Trend Category', 'Count']
        
        fig = px.pie(
            trend_counts,
            values='Count',
            names='Trend Category',
            title=f"Distribution of Countries by Trend Category ({selected_year})",
            color='Trend Category',
            color_discrete_map=color_map
        )
        fig.update_traces(textposition='inside', textinfo='percent+label')
        st.plotly_chart(fig, use_container_width=True)
        
        # If we have countries with no change data, show them in a table
        if not no_change_data_df.empty:
            st.subheader("Countries with Single-Year Data")
            st.write("These countries only have data for one year, so year-over-year changes cannot be calculated:")
            
            # Create a simple table of countries with no change data
            display_df = no_change_data_df[['country_id', 'eci']].copy()
            display_df.columns = ['Country', 'Economic Complexity Index (ECI)']
            
            st.dataframe(
                display_df,
                use_container_width=True,
                hide_index=True
            )
    else:
        st.warning(f"No ECI vs. Trade Balance data available for the selected year: {selected_year}")

# Export Complexity Portfolio page
elif page == "Export Complexity Portfolio":
    st.header("Export Complexity Portfolio Analysis")
    
    # Add country selection for this page
    selected_countries = st.multiselect(
        "Select Countries (max 10)",
        options=available_countries,
        default=available_countries[:5] if len(available_countries) >= 5 else available_countries,
        max_selections=10
    )
    
    if not selected_countries:
        st.warning("Please select at least one country to view the export complexity portfolio.")
    else:
        # Load export complexity portfolio data
        export_portfolio_query = f"""
        SELECT * 
        FROM `{GCP_PROJECT_ID}.{ANALYTICS_DATASET}.v_export_complexity_portfolio`
        WHERE year = {selected_year} AND country_id IN UNNEST({selected_countries})
        ORDER BY eci DESC
        """
        export_portfolio_df = load_data(export_portfolio_query)
        
        if not export_portfolio_df.empty:
            # Create stacked bar chart
            st.subheader("Export Composition by Complexity Tier")
            
            # Prepare data for stacked bar chart
            portfolio_melted = pd.melt(
                export_portfolio_df,
                id_vars=['country_id', 'eci'],
                value_vars=['high_complexity_share', 'medium_high_share', 'medium_low_share', 'low_complexity_share'],
                var_name='complexity_tier',
                value_name='share'
            )
            
            # Map complexity tier names
            tier_map = {
                'high_complexity_share': 'High Complexity',
                'medium_high_share': 'Medium-High Complexity',
                'medium_low_share': 'Medium-Low Complexity',
                'low_complexity_share': 'Low Complexity'
            }
            portfolio_melted['complexity_tier'] = portfolio_melted['complexity_tier'].map(tier_map)
            
            # Create stacked bar chart
            fig = px.bar(
                portfolio_melted,
                x="country_id",
                y="share",
                color="complexity_tier",
                labels={"country_id": "Country", "share": "Share of Exports (%)", "complexity_tier": "Complexity Tier"},
                title=f"Export Composition by Complexity Tier ({selected_year})",
                color_discrete_sequence=px.colors.sequential.Viridis_r,
                height=600
            )
            
            # Add ECI line on secondary y-axis
            fig.add_trace(
                go.Scatter(
                    x=export_portfolio_df['country_id'],
                    y=export_portfolio_df['eci'],
                    mode='lines+markers',
                    name='ECI',
                    line=dict(color='red', width=2),
                    marker=dict(size=8),
                    yaxis="y2"
                )
            )
            
            # Update layout for secondary y-axis
            fig.update_layout(
                yaxis=dict(title="Share of Exports (%)", tickformat=",.0%", range=[0, 1]),
                yaxis2=dict(title="Economic Complexity Index", overlaying="y", side="right"),
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                barmode="stack",
                hovermode="x unified"
            )
            
            st.plotly_chart(fig, use_container_width=True)
            
            # Create table with detailed metrics
            st.subheader("Detailed Export Portfolio Metrics")
            
            # Format percentages for display
            display_df = export_portfolio_df.copy()
            for col in ['high_complexity_share', 'medium_high_share', 'medium_low_share', 'low_complexity_share']:
                display_df[col] = display_df[col].apply(lambda x: f"{x:.1%}")
            
            # Rename columns for display
            display_df = display_df.rename(columns={
                'country_id': 'Country',
                'eci': 'ECI',
                'high_complexity_share': 'High Complexity',
                'medium_high_share': 'Medium-High',
                'medium_low_share': 'Medium-Low',
                'low_complexity_share': 'Low Complexity',
                'weighted_complexity': 'Weighted Complexity'
            })
            
            st.dataframe(
                display_df[['Country', 'ECI', 'High Complexity', 'Medium-High', 'Medium-Low', 'Low Complexity', 'Weighted Complexity']],
                use_container_width=True,
                hide_index=True
            )
        else:
            st.warning(f"No export portfolio data available for the selected countries in {selected_year}.")

# Partner Diversification page
elif page == "Partner Diversification":
    st.header("Partner Diversification vs. Economic Complexity")
    
    # Load partner diversification data
    partner_diversification_df = load_partner_diversification(selected_year)
    
    if not partner_diversification_df.empty:
        # Create heat map
        st.subheader("Partner Diversification vs. Economic Complexity Heat Map")
        
        # Calculate combined metric for heat map
        partner_diversification_df['diversification_score'] = 1 - partner_diversification_df['partner_hhi']  # Higher is better
        partner_diversification_df['combined_score'] = (partner_diversification_df['diversification_score'] + 
                                               (partner_diversification_df['eci'] - partner_diversification_df['eci'].min()) / 
                                               (partner_diversification_df['eci'].max() - partner_diversification_df['eci'].min())) / 2
        
        # Sort countries by combined score
        top_countries = partner_diversification_df.sort_values('combined_score', ascending=False).head(20)['country_id'].tolist()
        filtered_df = partner_diversification_df[partner_diversification_df['country_id'].isin(top_countries)]
        
        # Create heat map
        fig = px.imshow(
            filtered_df.pivot_table(
                index='country_id',
                values='combined_score',
                aggfunc='mean'
            ).sort_values('combined_score', ascending=False),
            color_continuous_scale='RdBu_r',
            labels=dict(x="", y="Country", color="Combined Score"),
            title=f"Partner Diversification vs. Economic Complexity Heat Map ({selected_year})"
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
        # Create scatter plot
        st.subheader("Partner Diversification vs. Economic Complexity Scatter Plot")
        
        # Create color map for concentration categories
        color_map = {
            'Highly Diversified': '#1a9641',
            'Moderately Diversified': '#a6d96a',
            'Moderately Concentrated': '#fdae61',
            'Highly Concentrated': '#d7191c'
        }
        
        fig = px.scatter(
            partner_diversification_df,
            x="eci",
            y="partner_hhi",
            color="concentration_category",
            hover_name="country_id",
            size="partner_count",
            labels={
                "eci": "Economic Complexity Index",
                "partner_hhi": "Partner Concentration (HHI)",
                "concentration_category": "Concentration Category",
                "partner_count": "Number of Partners"
            },
            title=f"Partner Diversification vs. Economic Complexity ({selected_year})",
            color_discrete_map=color_map,
            size_max=20,
            opacity=0.7
        )
        
        # Update y-axis (lower HHI is better)
        fig.update_layout(
            yaxis=dict(title="Partner Concentration (HHI)", autorange="reversed")
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
        # Create distribution by concentration category
        st.subheader("Country Distribution by Concentration Category")
        concentration_counts = partner_diversification_df['concentration_category'].value_counts().reset_index()
        concentration_counts.columns = ['Concentration Category', 'Count']
        
        fig = px.pie(
            concentration_counts,
            values='Count',
            names='Concentration Category',
            title=f"Distribution of Countries by Concentration Category ({selected_year})",
            color='Concentration Category',
            color_discrete_map=color_map
        )
        fig.update_traces(textposition='inside', textinfo='percent+label')
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning(f"No partner diversification data available for the selected year: {selected_year}")

# Complexity Outlook Index page
elif page == "Complexity Outlook Index":
    st.header("Complexity Outlook Index Predictive Power")
    
    # Load COI predictive power data
    coi_df = load_coi_predictive_power()
    
    if not coi_df.empty:
        # Filter for base years
        available_base_years = sorted(coi_df['base_year'].unique().tolist())
        selected_base_year = st.selectbox(
            "Select Base Year",
            options=available_base_years,
            index=0 if available_base_years else 0
        )
        
        # Filter data for selected base year
        filtered_coi_df = coi_df[coi_df['base_year'] == selected_base_year]
        
        # Create grouped column chart
        st.subheader("Export Growth by COI Quartile")
        
        # Prepare data for grouped bar chart
        coi_melted = pd.melt(
            filtered_coi_df,
            id_vars=['coi_quartile', 'base_year'],
            value_vars=['avg_high_complexity_growth', 'avg_medium_high_growth'],
            var_name='growth_type',
            value_name='growth_rate'
        )
        
        # Map growth type names
        growth_map = {
            'avg_high_complexity_growth': 'High-complexity export growth',
            'avg_medium_high_growth': 'Medium-high complexity export growth'
        }
        coi_melted['growth_type'] = coi_melted['growth_type'].map(growth_map)
        
        # Convert quartile to string for better display
        coi_melted['coi_quartile'] = coi_melted['coi_quartile'].apply(lambda x: f"Q{int(x)}")
        
        # Create grouped bar chart
        fig = px.bar(
            coi_melted,
            x="coi_quartile",
            y="growth_rate",
            color="growth_type",
            barmode="group",
            labels={
                "coi_quartile": "COI Quartile (Q1-lowest to Q4-highest)",
                "growth_rate": "3-Year Export Growth Rate (%)",
                "growth_type": "Export Type"
            },
            title=f"3-Year Export Growth by COI Quartile (Base Year: {selected_base_year})",
            color_discrete_sequence=["#1f77b4", "#ff7f0e"],
            height=500
        )
        
        # Format y-axis as percentage
        fig.update_layout(
            yaxis=dict(tickformat=",.0%")
        )
        
        # Add error bars
        for i, growth_type in enumerate(['avg_high_complexity_growth', 'avg_medium_high_growth']):
            std_col = growth_type.replace('avg_', 'std_')
            fig.add_trace(
                go.Scatter(
                    x=filtered_coi_df['coi_quartile'].apply(lambda x: f"Q{int(x)}"),
                    y=filtered_coi_df[growth_type],
                    error_y=dict(
                        type='data',
                        array=filtered_coi_df[std_col],
                        visible=True
                    ),
                    mode='markers',
                    marker=dict(color='rgba(0,0,0,0)'),
                    showlegend=False
                )
            )
        
        # Add trend line
        x_numeric = [1, 2, 3, 4]  # Q1, Q2, Q3, Q4
        y_high = filtered_coi_df['avg_high_complexity_growth'].tolist()
        
        if len(y_high) == 4:  # Ensure we have all quartiles
            slope, intercept = np.polyfit(x_numeric, y_high, 1)
            fig.add_trace(
                go.Scatter(
                    x=[f"Q{i}" for i in x_numeric],
                    y=[slope * x + intercept for x in x_numeric],
                    mode='lines',
                    line=dict(color='rgba(31, 119, 180, 0.7)', width=2, dash='dot'),
                    name='Trend (High Complexity)'
                )
            )
        
        st.plotly_chart(fig, use_container_width=True)
        
        # Add statistical significance annotation
        st.markdown("""**Statistical Significance:**
        - Error bars represent standard deviation within each quartile
        - The trend line shows the relationship between COI quartile and high-complexity export growth
        - Countries in higher COI quartiles tend to experience stronger export growth in complex products
        """)
        
        # Create table with detailed metrics
        st.subheader("Detailed COI Predictive Power Metrics")
        
        # Format percentages for display
        display_df = filtered_coi_df.copy()
        display_df['coi_quartile'] = display_df['coi_quartile'].apply(lambda x: f"Q{int(x)}")
        for col in ['avg_high_complexity_growth', 'avg_medium_high_growth']:
            display_df[col] = display_df[col].apply(lambda x: f"{x:.1%}")
        
        # Rename columns for display
        display_df = display_df.rename(columns={
            'coi_quartile': 'COI Quartile',
            'base_year': 'Base Year',
            'avg_high_complexity_growth': 'High Complexity Growth',
            'avg_medium_high_growth': 'Medium-High Growth',
            'country_count': 'Number of Countries'
        })
        
        st.dataframe(
            display_df[['COI Quartile', 'Base Year', 'High Complexity Growth', 'Medium-High Growth', 'Number of Countries']],
            use_container_width=True,
            hide_index=True
        )
    else:
        st.warning("No Complexity Outlook Index data available.")

# Export Portfolio Evolution page
elif page == "Export Portfolio Evolution":
    st.header("Export Portfolio Evolution Timeline")
    
    # Add country selection for this page
    top_countries_query = f"""
    SELECT DISTINCT country_id, AVG(eci_rank) as avg_rank
    FROM `{GCP_PROJECT_ID}.{ANALYTICS_DATASET}.v_portfolio_evolution`
    GROUP BY country_id
    ORDER BY avg_rank
    LIMIT 16
    """
    top_countries_df = load_data(top_countries_query)
    top_countries = top_countries_df['country_id'].tolist() if not top_countries_df.empty else []
    
    selected_countries = st.multiselect(
        "Select Countries (max 6)",
        options=available_countries,
        default=top_countries[:6] if len(top_countries) >= 6 else top_countries,
        max_selections=6
    )
    
    if not selected_countries:
        st.warning("Please select at least one country to view the export portfolio evolution.")
    else:
        # Load portfolio evolution data
        portfolio_query = f"""
        SELECT * 
        FROM `{GCP_PROJECT_ID}.{ANALYTICS_DATASET}.v_portfolio_evolution`
        WHERE country_id IN UNNEST({selected_countries})
        ORDER BY year, country_id
        """
        portfolio_df = load_data(portfolio_query)
        
        if not portfolio_df.empty:
            # Create timeline visualization
            st.subheader("Export Complexity Evolution Over Time")
            
            # Create faceted area charts
            for country in selected_countries:
                country_data = portfolio_df[portfolio_df['country_id'] == country]
                
                if not country_data.empty:
                    # Prepare data for area chart
                    country_melted = pd.melt(
                        country_data,
                        id_vars=['country_id', 'year', 'eci', 'eci_rank'],
                        value_vars=['high_complexity_share', 'medium_high_share', 'medium_low_share', 'low_complexity_share'],
                        var_name='complexity_tier',
                        value_name='share'
                    )
                    
                    # Map complexity tier names
                    tier_map = {
                        'high_complexity_share': 'High Complexity',
                        'medium_high_share': 'Medium-High Complexity',
                        'medium_low_share': 'Medium-Low Complexity',
                        'low_complexity_share': 'Low Complexity'
                    }
                    country_melted['complexity_tier'] = country_melted['complexity_tier'].map(tier_map)
                    
                    # Create area chart
                    fig = px.area(
                        country_melted,
                        x="year",
                        y="share",
                        color="complexity_tier",
                        labels={
                            "year": "Year",
                            "share": "Share of Exports (%)",
                            "complexity_tier": "Complexity Tier"
                        },
                        title=f"{country} Export Portfolio Evolution (ECI Rank: {int(country_data['eci_rank'].iloc[-1])})",
                        color_discrete_sequence=["#08519c", "#3182bd", "#6baed6", "#bdd7e7"],
                        height=400
                    )
                    
                    # Format y-axis as percentage
                    fig.update_layout(
                        yaxis=dict(tickformat=",.0%", range=[0, 1]),
                        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
                    )
                    
                    # Add ECI indicator
                    fig.add_trace(
                        go.Scatter(
                            x=country_data['year'],
                            y=[0.05] * len(country_data),  # Position at bottom
                            mode='markers+text',
                            marker=dict(
                                symbol='triangle-up',
                                size=10,
                                color=country_data['eci'],
                                colorscale='Viridis',
                                colorbar=dict(title="ECI"),
                                showscale=False
                            ),
                            text=country_data['eci'].apply(lambda x: f"{x:.2f}"),
                            textposition="bottom center",
                            name="ECI",
                            hovertemplate="Year: %{x}<br>ECI: %{text}",
                            showlegend=False
                        )
                    )
                    
                    st.plotly_chart(fig, use_container_width=True)
            
            # Add animation controls
            st.subheader("Export Portfolio Animation")
            
            # Prepare data for animated chart
            animation_melted = pd.melt(
                portfolio_df,
                id_vars=['country_id', 'year', 'eci'],
                value_vars=['high_complexity_share', 'medium_high_share', 'medium_low_share', 'low_complexity_share'],
                var_name='complexity_tier',
                value_name='share'
            )
            
            # Map complexity tier names
            tier_map = {
                'high_complexity_share': 'High Complexity',
                'medium_high_share': 'Medium-High Complexity',
                'medium_low_share': 'Medium-Low Complexity',
                'low_complexity_share': 'Low Complexity'
            }
            animation_melted['complexity_tier'] = animation_melted['complexity_tier'].map(tier_map)
            
            # Create animated chart
            fig = px.area(
                animation_melted,
                x="year",
                y="share",
                color="complexity_tier",
                facet_col="country_id",
                facet_col_wrap=3,
                labels={
                    "year": "Year",
                    "share": "Share of Exports (%)",
                    "complexity_tier": "Complexity Tier",
                    "country_id": "Country"
                },
                title="Export Portfolio Evolution Across Countries",
                color_discrete_sequence=["#08519c", "#3182bd", "#6baed6", "#bdd7e7"],
                height=600
            )
            
            # Format y-axis as percentage
            fig.update_layout(
                yaxis=dict(tickformat=",.0%", range=[0, 1]),
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
            )
            
            # Update facet titles to show ECI
            for i, country in enumerate(selected_countries):
                if i < len(fig.layout.annotations):
                    latest_eci = portfolio_df[(portfolio_df['country_id'] == country) & 
                                             (portfolio_df['year'] == portfolio_df['year'].max())]['eci'].values
                    if len(latest_eci) > 0:
                        fig.layout.annotations[i].text = f"{country} (ECI: {latest_eci[0]:.2f})"
            
            st.plotly_chart(fig, use_container_width=True)
            
            # Add insights
            st.markdown("""**Key Insights:**
            - The charts show how countries' export portfolios have evolved over time in terms of product complexity
            - Countries with improving economic complexity typically show an increasing share of high and medium-high complexity exports
            - The ECI indicator at the bottom of each country chart shows the Economic Complexity Index value for each year
            - Countries with similar starting points can diverge significantly in their development trajectories
            """)
        else:
            st.warning(f"No portfolio evolution data available for the selected countries.")
