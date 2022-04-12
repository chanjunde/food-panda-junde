from google.cloud import bigquery
import os

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join(os.getcwd(), "credentials.json")
client = bigquery.Client()
project_id = 'food-panda-junde'
target_data_set = 'food_panda_engineering_test'
public_data_set = f'{project_id}.geo_international_ports.world_port_index'


def run_query(q, table_to_drop):
    try:
        client.query(f"""DROP TABLE IF EXISTS {table_to_drop} PURGE""")
        client.query(q)
        print(f"\n---- Result for {table_to_drop.split('.')[-1]} ----")
        results = client.query(f"""SELECT * FROM {table_to_drop}""")
        for row in results:
            print(row)
        print(f"-------------------------------")
    except Exception as e:
        print(f"Query failed: {e}")
        raise


if __name__ == "__main__":

    # run query for question 1
    target_table = f"{project_id}.{target_data_set}.question_1"
    query = f"""
        CREATE TABLE {target_table} AS
        SELECT
        a.port_name,
        ST_DISTANCE(a.port_geom, b.port_geom_jurong_island) as distance_in_meters
        FROM (
            SELECT
            '1' as key,
            port_name,
            port_geom
            FROM {public_data_set}
            WHERE port_name != 'JURONG ISLAND'
        ) as a
        LEFT JOIN (
            SELECT
            '1' as key,
            port_geom as port_geom_jurong_island
            FROM {public_data_set}
            WHERE port_name = 'JURONG ISLAND'
            AND country = 'SG'
        ) as b
        ON a.key = b.key
        ORDER BY distance_in_meters ASC
        LIMIT 5
    """
    run_query(query, target_table)
    print(f"Question 1 completed: Wrote output to {target_table}")

    # run query for question 2
    target_table = f"{project_id}.{target_data_set}.question_2"
    query = f"""
        CREATE TABLE {target_table} AS
        SELECT 
        country,
        count(*) as port_count
        FROM {public_data_set}
        WHERE cargo_wharf
        GROUP BY country
        order by port_count DESC
        LIMIT 1
        """
    run_query(query, target_table)
    print(f"Question 2 completed: Wrote output to {target_table}")

    # run query for question 3
    target_table = f"{project_id}.{target_data_set}.question_3"
    distress_lon = -38.706256
    distress_lat = 32.610982
    query = f"""
            CREATE TABLE {target_table} AS
            SELECT 
            country,
            port_name,
            port_latitude,
            port_longitude
            FROM {public_data_set}
            WHERE provisions 
            AND water 
            AND fuel_oil 
            AND diesel
            ORDER BY ST_DISTANCE(port_geom, ST_GEOGPOINT({distress_lon} ,{distress_lat})) ASC
            LIMIT 1
            """
    run_query(query, target_table)
    print(f"Question 3 completed: Wrote output to {target_table}")



