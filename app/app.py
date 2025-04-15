#!/usr/bin/python
from cassandra.cluster import Cluster

import os
import glob
import sys


def initialize_cassandra():
    """Initialize Cassandra schema for search engine"""
    print("Initializing Cassandra schema...")

    # Connect to the Cassandra server
    cluster = Cluster(['cassandra-server'])
    session = cluster.connect()

    try:
        # Create keyspace if not exists
        session.execute("""
            CREATE KEYSPACE IF NOT EXISTS bigdata
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}
        """)

        # Set keyspace for subsequent operations
        session.set_keyspace('bigdata')

        # Create tables for the search engine
        session.execute("""
            CREATE TABLE IF NOT EXISTS inverted_index (
                term text,
                doc_id text,
                tf int,
                PRIMARY KEY (term, doc_id)
            )
        """)

        session.execute("""
            CREATE TABLE IF NOT EXISTS vocabulary (
                term text PRIMARY KEY,
                doc_freq int
            )
        """)

        session.execute("""
            CREATE TABLE IF NOT EXISTS doc_stats (
                doc_id text PRIMARY KEY,
                doc_length int
            )
        """)

        print("Cassandra schema initialized successfully")

        # available keyspaces for verification
        rows = session.execute('DESC keyspaces')
        print("Available keyspaces:")
        for row in rows:
            print(row)

        return cluster, session

    except Exception as e:
        print(f"Error initializing Cassandra schema: {e}")
        cluster.shutdown()
        return None, None


def load_data_to_cassandra(session, pipeline1_output, pipeline2_output):
    """Load MapReduce output into Cassandra tables"""
    print(f"Loading data from MapReduce outputs into Cassandra...")

    try:
        # temporary directories for downloaded HDFS files
        os.system("mkdir -p ./pipeline1_output ./pipeline2_output")

        # download files from HDFS
        os.system(f"hdfs dfs -get {pipeline1_output}/part-* ./pipeline1_output/ 2>/dev/null")
        os.system(f"hdfs dfs -get {pipeline2_output}/part-* ./pipeline2_output/ 2>/dev/null")

        # processing Pipeline 1 output
        doc_stats_count = 0
        inverted_index_count = 0

        for filename in glob.glob('./pipeline1_output/part-*'):
            with open(filename, 'r') as f:
                for line in f:
                    parts = line.strip().split('\t')
                    if len(parts) == 2 and parts[0].startswith('DOCLEN_'):
                        # process document length
                        doc_id = parts[0].split('DOCLEN_')[1]
                        doc_length = int(parts[1])
                        session.execute(
                            "INSERT INTO doc_stats (doc_id, doc_length) VALUES (%s, %s)",
                            (doc_id, doc_length)
                        )
                        doc_stats_count += 1
                    elif len(parts) == 3:
                        # process term frequency
                        term, doc_id, tf = parts
                        session.execute(
                            "INSERT INTO inverted_index (term, doc_id, tf) VALUES (%s, %s, %s)",
                            (term, doc_id, int(tf))
                        )
                        inverted_index_count += 1

        # process Pipeline 2 output
        vocab_count = 0
        for filename in glob.glob('./pipeline2_output/part-*'):
            with open(filename, 'r') as f:
                for line in f:
                    parts = line.strip().split('\t')
                    if len(parts) == 2:
                        term, doc_freq = parts
                        session.execute(
                            "INSERT INTO vocabulary (term, doc_freq) VALUES (%s, %s)",
                            (term, int(doc_freq))
                        )
                        vocab_count += 1

        print(f"Data successfully loaded into Cassandra:")
        print(f"- {doc_stats_count} document statistics entries")
        print(f"- {inverted_index_count} inverted index entries")
        print(f"- {vocab_count} vocabulary entries")

    except Exception as e:
        print(f"Error loading data into Cassandra: {e}")
    finally:
        # clean up
        os.system("rm -rf ./pipeline1_output ./pipeline2_output")


def verify_cassandra_tables(session):
    """Verifies that Cassandra tables were successfully created"""
    print("Checking Cassandra tables...")
    try:
        tables = session.execute("SELECT table_name FROM system_schema.tables WHERE keyspace_name='bigdata'")
        print("Existing tables in keyspace 'bigdata':")
        table_names = []
        for table in tables:
            table_names.append(table.table_name)
            print(f"- {table.table_name}")

        # check that all required tables are created
        required_tables = ["inverted_index", "vocabulary", "doc_stats"]
        missing_tables = [t for t in required_tables if t not in table_names]

        if missing_tables:
            print(f"WARNING: Missing tables: {', '.join(missing_tables)}")
            return False
        else:
            print("All required tables successfully created!")
            return True
    except Exception as e:
        print(f"Error checking Cassandra tables: {e}")
        return False


def main():
    print("hello app")

    if len(sys.argv) > 1 and sys.argv[1] == "load":
        # Called to load data after MapReduce completion
        pipeline1_output = sys.argv[2] if len(sys.argv) > 2 else "/tmp/mapreduce_pipeline1_output"
        pipeline2_output = sys.argv[3] if len(sys.argv) > 3 else "/tmp/mapreduce_pipeline2_output"

        cluster, session = initialize_cassandra()
        if session:
            try:
                verify_cassandra_tables(session)
                load_data_to_cassandra(session, pipeline1_output, pipeline2_output)
            finally:
                cluster.shutdown()
    else:
        cluster, session = initialize_cassandra()
        if session:
            verify_cassandra_tables(session)
            cluster.shutdown()


if __name__ == "__main__":
    main()