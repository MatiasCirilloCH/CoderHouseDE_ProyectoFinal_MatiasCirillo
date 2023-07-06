import os
import redshift_connector
import pandas as pd

def db_connection():

    #Connect to the cluster
    conn = redshift_connector.connect(
        host = os.getenv('REDSHIFT_HOST'),
        database = os.getenv('REDSHIFT_DATABASE'),
        port = int(os.getenv('REDSHIFT_PORT')),
        user = os.getenv('REDSHIFT_USER'),
        password = os.getenv('REDSHIFT_PASSWORD')
    )

    # Create a Cursor object
    cursor = conn.cursor()
    return conn, cursor

def make_load_query(df: pd.DataFrame):
    # Create query to load data into table
    query = f"""
    INSERT INTO weather (country, location_name, temperature, wind_speed, wind_direction, pressure, humidity, cloud, feels_like, visibility, last_updated)
    VALUES
    """

    for index, row in df.iterrows():
        if index == 0:
            query += f"""('{row['country']}', '{row['location_name']}', {row['temperature']}, {row['wind_speed']}, '{row['wind_direction']}', {row['pressure']}, {row['humidity']}, {row['cloud']}, {row['feels_like']}, {row['visibility']}, '{row['last_updated']}')
            """
        else:
            query += f""", ('{row['country']}', '{row['location_name']}', {row['temperature']}, {row['wind_speed']}, '{row['wind_direction']}', {row['pressure']}, {row['humidity']}, {row['cloud']}, {row['feels_like']}, {row['visibility']}, '{row['last_updated']}')
            """

    return query

def run_query(conn, cursor, query):
    try:
        # Query a table using the Cursor
        cursor.execute(query)
        conn.commit()
    except Exception as e:
        print(e)


    #Retrieve the query result set
    try:
        result = cursor.fetchall()
        # >> (['One Hundred Years of Solitude', 'Gabriel García Márquez'], ['A Brief History of Time', 'Stephen Hawking'])

        return result
    except:
        return 'Nothing to fetch'
    

def validate_duplicate_data(df: pd.DataFrame, conn, cursor):
    # Create connection to redshift
    

    # Make query to get all data from table
    query = 'SELECT * FROM weather'
    result = run_query(conn, cursor, query)

    # Create dataframe from query result
    columns = ['country', 'location_name', 'temperature', 'wind_speed', 'wind_direction', 'pressure', 'humidity', 'cloud', 'feels_like', 'visibility', 'last_updated']
    df_from_query = pd.DataFrame(result, columns=columns)

    sub_df = df[['country', 'last_updated']]
    sub_df_from_query = df_from_query[['country', 'last_updated']]

    concat_tables = pd.concat([sub_df_from_query, sub_df], ignore_index=True)

    duplicated = concat_tables.duplicated(subset=['country', 'last_updated'], keep= False).any()

    if duplicated:
        # print('Data duplicated')
        return True
    else:
        # print('Data not duplicated')
        return False