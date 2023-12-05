import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
import tensorflow as tf
from tensorflow import keras
from keras.layers import Input, Dense
from keras.models import Model
from sqlalchemy import create_engine

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

# Define a query to retrieve the latest sensor data
latest_sensor_data_query = f"""
    SELECT *
    FROM {SNOWFLAKE_SCHEMA}.SENSOR_DATA_TABLE
    ORDER BY eventtimestamp DESC
 """
# Define a query to retrieve the latest maintenance logs
latest_maintenance_logs_query = f"""
    SELECT *
    FROM {SNOWFLAKE_SCHEMA}.MAINTENANCE_LOGS_TABLE
    ORDER BY eventtimestamp DESC
"""
# Define a query to retrieve the latest historical failure data
latest_failure_data_query = f"""
    SELECT *
    FROM {SNOWFLAKE_SCHEMA}.HISTORICAL_FAILURE_DATA_TABLE
    ORDER BY eventtimestamp DESC
"""

# Execute the query and retrieve the data into a Pandas DataFrame
sensor_data_df = pd.read_sql(latest_sensor_data_query, engine)

# Execute the query and retrieve the data into a Pandas DataFrame
maintenance_logs_df = pd.read_sql(latest_maintenance_logs_query, engine)

# Execute the query and retrieve the data into a Pandas DataFrame
failure_data_df = pd.read_sql(latest_failure_data_query, engine)

# Load and preprocess sensor data
sensor_data = sensor_data_df
sensor_data = sensor_data.dropna()
sensor_data_features = sensor_data.drop(["eventtimestamp", "machineryequipment"], axis=1)
scaler = StandardScaler()
scaled_sensor_data = scaler.fit_transform(sensor_data_features)

# Split data into train and test sets
X_train, X_test = train_test_split(scaled_sensor_data, test_size=0.2, random_state=42)

# Build the Autoencoder model
input_dim = X_train.shape[1]
encoding_dim = 10  # Number of neurons in the hidden layer

input_layer = Input(shape=(input_dim,))
encoder_layer = Dense(encoding_dim, activation="relu")(input_layer)
decoder_layer = Dense(input_dim, activation="sigmoid")(encoder_layer)

autoencoder = Model(inputs=input_layer, outputs=decoder_layer)

# Compile the model
autoencoder.compile(optimizer="adam", loss="mean_squared_error")

# Train the Autoencoder
autoencoder.fit(X_train, X_train, epochs=50, batch_size=32, shuffle=True, validation_data=(X_test, X_test))

# Use the trained Autoencoder for anomaly detection
reconstructed_X = autoencoder.predict(scaled_sensor_data)
mse = np.mean(np.power(scaled_sensor_data - reconstructed_X, 2), axis=1)
threshold = np.percentile(mse, 95)  # Adjust the threshold as needed

# Identify anomalies
anomalies = sensor_data[mse > threshold]

print("Anomalies:")
print(anomalies)