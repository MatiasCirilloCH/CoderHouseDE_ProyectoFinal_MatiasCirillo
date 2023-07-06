from ETL.Extraction import extraction
from ETL.Transform import transform
from ETL.Load import load

def main(subregion = "South America"):
    """This function will run the ETL process for the weather data
    
    Args:
        subregion (str, optional): Subregion to extract countries from. Defaults to "South America".
        
    Returns:
        None
            
    """

    # ----------------- EXTRACT ----------------- #
    # Get countries from API

    # Take subregion to extract countries and return a list of countries within this
    countries_list = extraction.country_extract(subregion)
    print(f'Countries into subregion {subregion}:\n', countries_list, '\n')

    # Create data to send to the API to get the weather of the subregion countries
    data = {'locations': []}
    for country in countries_list:
        data['locations'].append({'q': country})
    print('Data to send to API:\n', data, '\n')
    
    # Get weather from API
    weather = extraction.weather_extract(data)


    # ----------------- TRANSFORM ----------------- #

    # Clean data and return it with a dataframe, returned only the relevant columns:
    #   country
    #   location name
    #   temperature 
    #   wind speed
    #   wind direction 
    #   pressure 
    #   humidity 
    #   cloud 
    #   feels like 
    #   visibility
    #   last updated

    weather_df = transform.weather_json_to_df(weather)
    print(weather_df.info())
    print('Dataframe columns: \n', weather_df.columns, '\n')


    # ----------------- LOAD ----------------- #
    try:

        # Create connection to redshift
        conn, cursor = load.db_connection()

        
        # Load 'create_weather_table' from database_scripts folder and run it
        create_table_query = open('database_scripts/create_weather_table.sql', 'r').read()
        load.run_query(conn, cursor, create_table_query)
        print('Table created successfully\n')

        # Make query with weather_df to load into weather table
        query = load.make_load_query(weather_df)
        print('Query to load into Redshift:\n', query, '\n')

        print('Loading data into Redshift...')
        print('Checking for duplicated data...')
        if load.validate_duplicate_data(weather_df, conn, cursor):
            print('Data duplicated, no data loaded')
        else:
            print('No duplicated data, loading data...')
            try:
                load.run_query(conn, cursor, query)
                print('Data loaded successfully\n')
            except Exception as e:
                print('Error loading data to redshift')
                raise e

        # # View data in table # #
        # query = 'SELECT * FROM weather'
        # print(load.run_query(conn, cursor, query))

    except Exception as e:
        print('Error loading data to redshift')
        raise e
    
    finally:
        conn.close()

if __name__ == '__main__':
    main()