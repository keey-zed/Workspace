import pandas as pd
from sqlalchemy import create_engine
import time

# Define the Snowflake connection parameters
SNOWFLAKE_ACCOUNT = 'rz83141.ap-northeast-2.aws'
SNOWFLAKE_USER = 'KZ'
SNOWFLAKE_PASSWORD = 'Islam.1234567890'
SNOWFLAKE_DATABASE = 'SECONDYEARINTERNSHIP'
SNOWFLAKE_SCHEMA = "DATA"
SNOWFLAKE_WAREHOUSE = 'COMPUTE_WH'

# Create a Snowflake connection
engine = create_engine(
    f'snowflake://{SNOWFLAKE_USER}:{SNOWFLAKE_PASSWORD}@{SNOWFLAKE_ACCOUNT}/{SNOWFLAKE_DATABASE}/Data',
    echo=False  # Set to True for debug output
)

try:
    while True:
        # Define a query to retrieve the latest sensor data
        latest_sensor_data_query = f"""
            SELECT *
            FROM {SNOWFLAKE_SCHEMA}.SENSOR_DATA_TABLE
            ORDER BY eventtimestamp DESC
            LIMIT 100
        """

        # Execute the query and retrieve the data into a Pandas DataFrame
        sensor_data_df = pd.read_sql(latest_sensor_data_query, engine)

        # Process the retrieved sensor data
        print("Latest Sensor Data:")
        print(sensor_data_df)

        # Define a query to retrieve the latest maintenance logs
        latest_maintenance_logs_query = f"""
            SELECT *
            FROM {SNOWFLAKE_SCHEMA}.MAINTENANCE_LOGS_TABLE
            ORDER BY eventtimestamp DESC
            LIMIT 100
        """

        # Execute the query and retrieve the data into a Pandas DataFrame
        maintenance_logs_df = pd.read_sql(latest_maintenance_logs_query, engine)

        # Process the retrieved maintenance logs
        print("Latest Maintenance Logs:")
        print(maintenance_logs_df)

        # Define a query to retrieve the latest historical failure data
        latest_failure_data_query = f"""
            SELECT *
            FROM {SNOWFLAKE_SCHEMA}.HISTORICAL_FAILURE_DATA_TABLE
            ORDER BY eventtimestamp DESC
            LIMIT 100
        """

        # Execute the query and retrieve the data into a Pandas DataFrame
        failure_data_df = pd.read_sql(latest_failure_data_query, engine)

        # Process the retrieved historical failure data
        print("Latest Historical Failure Data:")
        print(failure_data_df)

        # Sleep for a specified interval (e.g., 5 seconds) before querying again
        time.sleep(5)

except Exception as e:
    print(f"An error occurred: {e}")

finally:
    # Close the SQLAlchemy engine
    engine.dispose()