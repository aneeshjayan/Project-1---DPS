import pyarrow.parquet as pq
import pandas as pd
from neo4j import GraphDatabase
import time


class DataLoader:

    def __init__(self, uri, user, password):
        """
        Connect to the Neo4j database and other init steps
        
        Args:
            uri (str): URI of the Neo4j database
            user (str): Username of the Neo4j database
            password (str): Password of the Neo4j database
        """
        self.driver = GraphDatabase.driver(uri, auth=(user, password), encrypted=False)
        self.driver.verify_connectivity()

    def close(self):
        """
        Close the connection to the Neo4j database
        """
        self.driver.close()

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
                session.write_transaction(self.create_location_node, row['PULocationID'])
                session.write_transaction(self.create_location_node, row['DOLocationID'])
                session.write_transaction(
                    self.create_trip_relationship,
                    row['PULocationID'], row['DOLocationID'], row['trip_distance'],
                    row['fare_amount'], row['tpep_pickup_datetime'], row['tpep_dropoff_datetime']
                )

    def create_location_node(self, tx, location_id):
        """
        Create or find a Location node.
        
        Args:
            tx (Transaction): Neo4j transaction
            location_id (int): Location ID for the node
        """
        query = """
        MERGE (loc:Location {name: $location_id})
        """
        tx.run(query, location_id=int(location_id))

    def create_trip_relationship(self, tx, pickup_id, dropoff_id, distance, fare, pickup_dt, dropoff_dt):
        """
        Create a TRIP relationship between two Location nodes.
        
        Args:
            tx (Transaction): Neo4j transaction
            pickup_id (int): Pickup location ID
            dropoff_id (int): Dropoff location ID
            distance (float): Distance of the trip
            fare (float): Fare of the trip
            pickup_dt (datetime): Pickup date and time
            dropoff_dt (datetime): Dropoff date and time
        """
        query = """
        MATCH (p:Location {name: $pickup_id}), (d:Location {name: $dropoff_id})
        MERGE (p)-[:TRIP {
            distance: $distance, fare: $fare, 
            pickup_dt: $pickup_dt, dropoff_dt: $dropoff_dt
        }]->(d)
        """
        tx.run(query, pickup_id=int(pickup_id), dropoff_id=int(dropoff_id), distance=float(distance), 
               fare=float(fare), pickup_dt=pickup_dt, dropoff_dt=dropoff_dt)


def main():

    total_attempts = 10
    attempt = 0

    # The database takes some time to start up, try connecting multiple times
    while attempt < total_attempts:
        try:
            data_loader = DataLoader("neo4j://localhost:7687", "neo4j", "project1phase1")
            data_loader.load_transform_file("yellow_tripdata_2022-03.parquet")
            data_loader.close()
            
            attempt = total_attempts

        except Exception as e:
            print(f"(Attempt {attempt+1}/{total_attempts}) Error: ", e)
            attempt += 1
            time.sleep(10)


if __name__ == "__main__":
    main()
