import os
import requests
import redshift_connector
import pandas as pd
from dotenv import load_dotenv
load_dotenv()

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

def get_pokemons():
    url = 'https://pokeapi.co/api/v2/pokemon?limit=151'
    response = requests.get(url)
    pokemons = response.json()['results']
    return pokemons


### MAIN ###


create_table_query = """
CREATE TABLE IF NOT EXISTS pokemons (
    id INT NOT NULL, 
    name VARCHAR(50) NOT NULL distkey, 
    PRIMARY KEY (id)
) sortkey(id);
"""

select_query = """
SELECT * FROM pokemons;
"""

# Get pokemons from API and create query to populate table
pokemons = get_pokemons()
populate_table_query = 'INSERT INTO pokemons (id, name) VALUES '
for pokemon in pokemons:
    id = pokemons.index(pokemon) + 1
    name = pokemon['name']
    populate_table_query += f"({id}, '{name}'),"

populate_table_query = populate_table_query[:-1] + ';'

conn, cursor = db_connection()
try:
    print(run_query(conn, cursor, create_table_query))
    print(run_query(conn, cursor, populate_table_query))
    print(pd.DataFrame(run_query(conn, cursor, select_query), columns=['id', 'name']))
except Exception as e:
    print(e)
conn.close()