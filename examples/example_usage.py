from omni_python_sdk import OmniAPI
import pandas as pd
import matplotlib.pyplot as plt
from statsmodels.tsa.statespace.sarimax import SARIMAX

def plot_and_forecast(df: pd.DataFrame):
    # Ensure the date column is a datetime type and set frequency to weekly
    df['order_items.created_at[date]'] = pd.to_datetime(df['order_items.created_at[date]'])
    # Set the date column as the index
    df.set_index('order_items.created_at[date]', inplace=True)
    df.index = df.index.to_period('D').to_timestamp()
    df = df.asfreq('W')  # Set the frequency to weekly

    # Plot the original data
    plt.figure(figsize=(10, 6))
    plt.plot(df['order_items.sale_price_sum'], label='Original Data')
    plt.title('Sales Price Sum Over Time')
    plt.xlabel('Date')
    plt.ylabel('Sales Price Sum')
    plt.legend()
    plt.show()

    # Fit SARIMAX model with weekly seasonality and trend
    model = SARIMAX(df['order_items.sale_price_sum'], order=(1, 1, 1), seasonal_order=(1, 1, 1, 52), trend='t')
    model_fit = model.fit(disp=False, maxiter=200)

    # Forecast
    forecast_steps = 52  # Forecast the next 52 weeks (1 year)
    forecast = model_fit.get_forecast(steps=forecast_steps)
    forecast_index = pd.date_range(start=df.index[-1], periods=forecast_steps + 1, freq='W')[1:]
    forecast_series = pd.Series(forecast.predicted_mean, index=forecast_index)

    # Plot forecast
    plt.figure(figsize=(10, 6))
    plt.plot(df['order_items.sale_price_sum'], label='Original Data')
    plt.plot(forecast_series, label='Forecast', color='red')
    plt.title('Sales Price Sum Forecast')
    plt.xlabel('Date')
    plt.ylabel('Sales Price Sum')
    plt.legend()
    plt.show()

if __name__ == "__main__":
    api_key = "your_api_key"
    query = {
        "sorts": [
            {
                "column_name": "order_items.created_at[date]",
                "sort_descending": False
            }
        ],
        "table": "order_items",
        "fields": [
            "order_items.created_at[date]",
            "order_items.sale_price_sum"
        ],
        "modelId": "55d8bd00-67ab-4519-853c-282cafd7e085",
        "join_paths_from_topic_name": "order_items"
    }

    api = OmniAPI(api_key)
    table = api.run_query_blocking(query)
    df = table.to_pandas()
    plot_and_forecast(df)
