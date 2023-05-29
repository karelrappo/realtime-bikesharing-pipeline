import requests
import psycopg2
from psycopg2 import Error
import configparser
import logging


# Creat connection to AWS Postgre database.
def create_connection(host, database, user, password, port):
    conn = None
    try:
        conn = psycopg2.connect(
            host=host, database=database, user=user, password=password, port=port
        )
    except psycopg2.Error as e:
        logging.error(f"Error connecting to the database: {e}")
    return conn


# Execute queries for creating or dropping the tables
def execute_sql(conn, query):
    try:
        c = conn.cursor()
        c.execute(query)
    except Error as e:
        print(e)


# Insert the data to tables
def insert_data(conn, table_name, data):
    if table_name == "networks":
        sql = """INSERT INTO networks(id, name, city, country, latitude, longitude)
                 VALUES(%s, %s, %s, %s, %s, %s) """
    elif table_name == "stations":
        sql = """INSERT INTO stations(id, name, latitude, longitude, free_bikes, empty_slots, timestamp, network_id)
                 VALUES(%s, %s, %s, %s, %s, %s, %s, %s) """
    else:
        raise ValueError("Invalid table name.")

    cur = conn.cursor()
    cur.executemany(sql, data)
    conn.commit()


# Fetch the data via API & insert into tables.
def fetch_citybikes_data(conn):
    networks_api_url = "http://api.citybik.es/v2/networks"

    response = requests.get(networks_api_url)

    if response.status_code == 200:
        data = response.json()
        networks = data["networks"]

        network_tuples = []
        for network in networks:
            network_tuple = (
                network.get("id", ""),
                network.get("name", ""),
                network["location"].get("city", ""),
                network["location"].get("country", ""),
                network["location"].get("latitude", 0.0),
                network["location"].get("longitude", 0.0),
            )
            network_tuples.append(network_tuple)

        insert_data(conn, "networks", network_tuples)

        for network in networks:
            network_api_url = f"http://api.citybik.es/v2/networks/{network['id']}"
            network_response = requests.get(network_api_url)

            if network_response.status_code == 200:
                network_data = network_response.json()

                station_tuples = []
                for station in network_data["network"]["stations"]:
                    station_tuple = (
                        station.get("id", ""),
                        station.get("name", ""),
                        station.get("latitude", 0.0),
                        station.get("longitude", 0.0),
                        station.get("free_bikes", 0),
                        station["empty_slots"]
                        if station["empty_slots"] is not None
                        else 0,
                        station.get("timestamp", ""),
                        network.get("id", ""),
                    )
                    station_tuples.append(station_tuple)

                insert_data(conn, "stations", station_tuples)
            else:
                print(
                    f"Error fetching data for network {network['id']}: {network_response.status_code}"
                )
    else:
        print(f"Error fetching data: {response.status_code}")


def main():
    config = configparser.ConfigParser()
    config.read("config.ini")

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )

    conn = create_connection(
        config.get("database", "host"),
        config.get("database", "database"),
        config.get("database", "user"),
        config.get("database", "password"),
        config.get("database", "port"),
    )

    if conn is not None:
        drop_tables = "DROP TABLE IF EXISTS networks, stations"

        create_networks_table = """CREATE TABLE networks (
                                    id text PRIMARY KEY,
                                    name text NOT NULL,
                                    city text NOT NULL,
                                    country text NOT NULL,
                                    latitude double precision NOT NULL,
                                    longitude double precision NOT NULL
                                    );"""
        create_stations_table = """CREATE TABLE stations (
                                    id text PRIMARY KEY,
                                    name text NOT NULL,
                                    latitude double precision NOT NULL,
                                    longitude double precision NOT NULL,
                                    free_bikes integer NOT NULL DEFAULT 0,
                                    empty_slots integer NOT NULL DEFAULT 0,
                                    timestamp text NOT NULL,
                                    network_id text NOT NULL,
                                    FOREIGN KEY (network_id) REFERENCES networks (id)
                                    );"""

        if config.getboolean("options", "overwrite"):
            logging.info("Dropping existing tables...")
            execute_sql(conn, drop_tables)

        logging.info("Creating new tables...")
        execute_sql(conn, create_networks_table)
        execute_sql(conn, create_stations_table)
        logging.info("Fetching and inserting data...")
        fetch_citybikes_data(conn)
        conn.close()


if __name__ == "__main__":
    main()
