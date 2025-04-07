from dagster import asset, AssetExecutionContext
import pandas as pd

@asset
def trade_data_sample(context: AssetExecutionContext):
    """Generate a sample dataframe with trade data."""
    # Create a sample dataframe with trade data
    data = {
        'country_id': ['USA', 'CHN', 'DEU', 'JPN', 'GBR'],
        'year': [2022, 2022, 2022, 2022, 2022],
        'exports': [1500, 2800, 1900, 800, 700],
        'imports': [2500, 1800, 1600, 900, 900],
        'trade_balance': [-1000, 1000, 300, -100, -200]
    }
    
    df = pd.DataFrame(data)
    
    # Log basic info first
    context.log.info(f"Generated trade data sample with {len(df)} rows")
    context.log.info(f"Data:\n{df.to_string()}")
    context.log.info("Local development with auto-reload is working perfectly!")
    
    return df

@asset
def trade_metrics(context: AssetExecutionContext, trade_data_sample):
    """Calculate trade metrics based on the sample data."""
    context.log.info(f"Processing trade data with shape: {trade_data_sample.shape}")
    
    # Calculate total trade volume
    trade_data_sample['trade_volume'] = trade_data_sample['exports'] + trade_data_sample['imports']
    
    # Calculate trade openness (trade volume as % of a mock GDP value)
    mock_gdp = {
        'USA': 25000, 'CHN': 18000, 'DEU': 4500, 'JPN': 5000, 'GBR': 3200
    }
    
    trade_data_sample['gdp'] = trade_data_sample['country_id'].map(mock_gdp)
    trade_data_sample['trade_openness'] = (trade_data_sample['trade_volume'] / trade_data_sample['gdp']) * 100
    
    context.log.info(f"Generated trade metrics successfully")
    context.log.info(f"Metrics:\n{trade_data_sample.to_string()}")
    
    return trade_data_sample
