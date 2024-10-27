import pyarrow.parquet as pq
import pandas as pd
from neo4j import GraphDatabase
import time

def load_transform_file(self, file_path):
    """
    Load the parquet file and transform it into a csv file.
    Then load the csv file into Neo4j.
    
    Args:
        file_path (str): Path to the parquet file to be loaded
    """

    # Read the parquet file
    trips = pq.read_table(file_path)
    trips = trips.to_pandas()

    # Filter and clean the data
    trips = trips[['tpep_pickup_datetime', 'tpep_dropoff_datetime', 'PULocationID', 'DOLocationID', 'trip_distance', 'fare_amount']]
    bronx = [3, 18, 20, 31, 32, 46, 47, 51, 58, 59, 60, 69, 78, 81, 94, 119, 126, 136, 147, 159, 167, 168, 169, 174, 182, 183, 184, 185, 199, 200, 208, 212, 213, 220, 235, 240, 241, 242, 247, 248, 250, 254, 259]
    trips = trips[trips['PULocationID'].isin(bronx) & trips['DOLocationID'].isin(bronx)]
    trips = trips[trips['trip_distance'] > 0.1]
    trips = trips[trips['fare_amount'] > 2.5]

    # Convert date-time columns to supported format
    trips['tpep_pickup_datetime'] = pd.to_datetime(trips['tpep_pickup_datetime'], format='%Y-%m-%d %H:%M:%S')
    trips['tpep_dropoff_datetime'] = pd.to_datetime(trips['tpep_dropoff_datetime'], format='%Y-%m-%d %H:%M:%S')

    # For each trip, create nodes and relationships
    with self.driver.session() as session:
        for _, row in trips.iterrows():
            session.run(
                """
                MERGE (loc1:Location {name: $pickup_id})
                MERGE (loc2:Location {name: $dropoff_id})
                MERGE (loc1)-[:TRIP {
                    distance: $distance, fare: $fare, 
                    pickup_dt: $pickup_dt, dropoff_dt: $dropoff_dt
                }]->(loc2)
                """,
                pickup_id=int(row['PULocationID']),
                dropoff_id=int(row['DOLocationID']),
                distance=float(row['trip_distance']),
                fare=float(row['fare_amount']),
                pickup_dt=row['tpep_pickup_datetime'],
                dropoff_dt=row['tpep_dropoff_datetime']
            )
