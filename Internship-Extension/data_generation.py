import pandas as pd
from faker import Faker
import random
from sqlalchemy import create_engine
from datetime import datetime, timedelta

# Create an instance of the Faker class to use for generating synthetic data
fake = Faker()

# Define the Snowflake connection parameters
SNOWFLAKE_ACCOUNT = 'rz83141.ap-northeast-2.aws'
SNOWFLAKE_USER = 'KZ'
SNOWFLAKE_PASSWORD = 'Islam.1234567890'
SNOWFLAKE_DATABASE = 'SECONDYEARINTERNSHIP'
SNOWFLAKE_SCHEMA = "DATA"
SNOWFLAKE_WAREHOUSE = 'COMPUTE_WH'

# Define the machinery and their corresponding attributes
machinery = {
    'Excavator': ['Engine_temperature', 'Hydraulic_system_temperature', 'Bearing_temperature', 'Vibration'],
    'Loader': ['Engine_temperature', 'Hydraulic_system_temperature', 'Transmission_temperature', 'Proximity'],
    'Haul Truck': ['Engine_temperature', 'Transmission_temperature', 'Braking_system_temperature', 'Speed'],
    'Conveyor': ['Motor_temperature', 'Bearing_temperature', 'Gearbox_temperature', 'Proximity', 'Speed'],
    'Crusher': ['Motor_temperature', 'Bearing_temperature', 'Hydraulic_system_temperature', 'Vibration', 'Current_Reading'],
    'Mill': ['Motor_temperature', 'Bearing_temperature', 'Lubrication_system_temperature', 'Vibration'],
    'Separator': ['Motor_temperature', 'Bearing_temperature', 'Fluid_temperature', 'Pressure', 'Flow'],
    'Pump': ['Fluid_system_pressure', 'Pump_discharge_pressure', 'Flow'],
    'Compressor': ['Compressor_system_pressure', 'Vibration']
}

# Define a dictionary with default values for all attributes
default_readings = {
    'Engine_temperature': None,
    'Hydraulic_system_temperature': None,
    'Bearing_temperature': None,
    'Vibration': None,
    'Transmission_temperature': None,
    'Braking_system_temperature': None,
    'Motor_temperature': None,
    'Gearbox_temperature': None,
    'Lubrication_system_temperature': None,
    'Fluid_temperature': None,
    'Pressure': None,
    'Proximity': None,
    'Speed': None,
    'Current_Reading': None,
    'Flow': None,
    'Fluid_system_pressure': None,
    'Pump_discharge_pressure': None,
    'Compressor_system_pressure': None,
}

# Function to generate synthetic sensor data with time-series patterns and anomalies
def generate_sensor_data():
    data = []
    start_date = datetime(2022, 1, 1)
    end_date = datetime(2023, 3, 31)

    for machine, attributes in machinery.items():
        for _ in range(100):
            timestamp = fake.date_time_between(start_date=start_date, end_date=end_date)
            equipment = fake.random_element(elements=(f'{machine} {i}' for i in range(1, 6)))

            readings = default_readings.copy()

            for attribute in attributes:
                readings[attribute] = generate_reading(attribute, timestamp)

            entry = {
                'eventtimestamp': timestamp.strftime('%Y-%m-%d %H:%M:%S'),
                'machineryequipment': equipment,
                **readings
            }
            data.append(entry)

    return data

# Function to generate synthetic readings for each sensor attribute with anomalies and dependencies
def generate_reading(attribute, timestamp):
    if attribute == 'Engine_temperature':
        temperature = round(random.uniform(80, 100), 2)
        # Introduce occasional temperature spikes
        if timestamp.hour in [10, 14, 18] and random.random() < 0.1:
            temperature += random.uniform(5, 10)
        return temperature
    elif attribute == 'Hydraulic_system_temperature':
        temperature = round(random.uniform(50, 70), 2)
        # Introduce dependencies from engine temperature
        temperature += random.uniform(0, 5) * 0.3  # Adjust the coefficient
        return temperature
    elif attribute == 'Bearing_temperature':
        temperature = round(random.uniform(40, 60), 2)
        # Introduce anomalies for bearing temperature
        if random.random() < 0.05:
            temperature += random.uniform(20, 40)
        return temperature
    elif attribute == 'Vibration':
        vibration = round(random.uniform(0, 5), 2)
        # Introduce occasional spikes in vibration
        if timestamp.hour in [9, 15] and random.random() < 0.1:
            vibration += random.uniform(1, 2)
        return vibration
    elif attribute == 'Transmission_temperature':
        temperature = round(random.uniform(70, 90), 2)
        # Introduce anomalies for transmission temperature
        if random.random() < 0.05:
            temperature += random.uniform(10, 20)
        return temperature
    elif attribute == 'Braking_system_temperature':
        temperature = round(random.uniform(80, 100), 2)
        # Introduce anomalies for braking system temperature
        if random.random() < 0.05:
            temperature += random.uniform(15, 30)
        return temperature
    elif attribute == 'Motor_temperature':
        temperature = round(random.uniform(60, 80), 2)
        # Introduce anomalies for motor temperature
        if random.random() < 0.05:
            temperature += random.uniform(10, 25)
        return temperature
    elif attribute == 'Gearbox_temperature':
        temperature = round(random.uniform(50, 70), 2)
        # Introduce anomalies for gearbox temperature
        if random.random() < 0.05:
            temperature += random.uniform(15, 30)
        return temperature
    elif attribute == 'Lubrication_system_temperature':
        temperature = round(random.uniform(70, 90), 2)
        # Introduce occasional temperature spikes
        if timestamp.hour in [10, 14, 18] and random.random() < 0.1:
            temperature += random.uniform(5, 10)
        return temperature
    elif attribute == 'Fluid_temperature':
        temperature = round(random.uniform(30, 50), 2)
        # Introduce anomalies for fluid temperature
        if random.random() < 0.05:
            temperature += random.uniform(10, 20)
        return temperature
    elif attribute == 'Pressure':
        pressure = round(random.uniform(100, 200), 2)
        # Introduce occasional pressure drops
        if timestamp.hour in [9, 15] and random.random() < 0.1:
            pressure -= random.uniform(10, 20)
        return pressure
    elif attribute == 'Proximity':
        proximity = round(random.uniform(0, 1), 2)
        # Introduce anomalies for proximity readings
        if random.random() < 0.05:
            proximity += random.uniform(0.2, 0.5)
        return proximity
    elif attribute == 'Speed':
        speed = round(random.uniform(5, 20), 2)
        # Introduce occasional speed fluctuations
        if timestamp.hour in [8, 12, 16] and random.random() < 0.1:
            speed += random.uniform(1, 3)
        return speed
    elif attribute == 'Current_Reading':
        current = round(random.uniform(30, 100), 2)
        # Introduce anomalies for current readings
        if random.random() < 0.05:
            current += random.uniform(10, 20)
        return current
    elif attribute == 'Flow':
        flow = round(random.uniform(50, 1000), 2)
        # Introduce occasional flow variations
        if timestamp.hour in [9, 13, 17] and random.random() < 0.1:
            flow += random.uniform(10, 50)
        return flow
    elif attribute == 'Fluid_system_pressure':
        pressure = round(random.uniform(100, 300), 2)
        # Introduce anomalies for fluid system pressure
        if random.random() < 0.05:
            pressure += random.uniform(20, 40)
        return pressure
    elif attribute == 'Pump_discharge_pressure':
        pressure = round(random.uniform(50, 150), 2)
        # Introduce occasional pressure spikes
        if timestamp.hour in [9, 15] and random.random() < 0.1:
            pressure += random.uniform(10, 20)
        return pressure
    elif attribute == 'Compressor_system_pressure':
        pressure = round(random.uniform(80, 120), 2)
        # Introduce anomalies for compressor system pressure
        if random.random() < 0.05:
            pressure += random.uniform(15, 30)
        return pressure
    else:
        return None

# Function to generate synthetic maintenance logs
def generate_maintenance_logs():
    data = []
    start_date = datetime(2022, 1, 1)
    end_date = datetime(2023, 3, 31)

    for machine, _ in machinery.items():
        # Generate a random number of maintenance logs based on machinery type and a range
        min_logs = 1  # Minimum number of logs
        max_logs = 30  # Maximum number of logs
        num_logs = random.randint(min_logs, max_logs)

        for _ in range(num_logs):
            timestamp = fake.date_time_between(start_date=start_date, end_date=end_date)
            equipment = fake.random_element(elements=(f'{machine} {i}' for i in range(1, 6)))
            
            # Adjust probabilities to reflect Inspection > Repair > Replacement order
            maintenance_action = fake.random_element(elements=(
                'Inspection', 'Inspection', 'Inspection', 'Inspection', 'Repair', 'Repair', 'Replacement'
            ))
            
            description = fake.paragraph()

            entry = {
                'eventtimestamp': timestamp.strftime('%Y-%m-%d %H:%M:%S'),
                'machineryequipment': equipment,
                'Maintenance_Action': maintenance_action,
                'Description': description
            }
            data.append(entry)

    return data

# Function to generate synthetic historical failure data
def generate_historical_failure_data(maintenance_logs):
    data = []
    
    for log in maintenance_logs:
        machine = log['machineryequipment']
        timestamp = datetime.strptime(log['eventtimestamp'], '%Y-%m-%d %H:%M:%S')
        
        # Calculate the number of failures based on maintenance action
        maintenance_action = log['Maintenance_Action']
        if maintenance_action == 'Repair':
            num_failures = random.randint(0, 1)
        elif maintenance_action == 'Replacement':
            num_failures = 0  # No failures expected after replacement
        elif maintenance_action == 'Inspection':
            num_failures = 0  # No failures expected after inspection
        else:
            num_failures = random.randint(0, 2)
        
        for _ in range(num_failures):
            timestamp_failure = timestamp - timedelta(hours=random.randint(1, 72))
            description = fake.paragraph()

            entry = {
                'eventtimestamp': timestamp_failure.strftime('%Y-%m-%d %H:%M:%S'),
                'machineryequipment': machine,
                'Description': description
            }
            data.append(entry)

    return data

# Function to insert data into Snowflake table
def insert_data_into_snowflake(engine, data, table_name):
    # Create a DataFrame from the data
    df = pd.DataFrame(data)

    # Convert column names to uppercase to match Snowflake's default behavior
    df.columns = [col.upper() for col in df.columns]

    # Write the DataFrame to Snowflake using SQLAlchemy
    df.to_sql(
        table_name,
        con=engine,
        schema=SNOWFLAKE_SCHEMA,
        if_exists="append",
        index=False
    )

# Connect to Snowflake
engine = create_engine(
    f'snowflake://{SNOWFLAKE_USER}:{SNOWFLAKE_PASSWORD}@{SNOWFLAKE_ACCOUNT}/{SNOWFLAKE_DATABASE}/Data',
    echo=True
)

try:
    # Generate synthetic data
    sensor_data = generate_sensor_data()
    maintenance_logs = generate_maintenance_logs()
    historical_failure_data = generate_historical_failure_data(maintenance_logs)

    # Call the function for each table
    insert_data_into_snowflake(engine, sensor_data, "SENSOR_DATA_TABLE")
    insert_data_into_snowflake(engine, maintenance_logs, "MAINTENANCE_LOGS_TABLE")
    insert_data_into_snowflake(engine, historical_failure_data, "HISTORICAL_FAILURE_DATA_TABLE")

except Exception as e:
    print(f"An error occurred: {e}")

finally:
    # Close the SQLAlchemy engine
    engine.dispose()