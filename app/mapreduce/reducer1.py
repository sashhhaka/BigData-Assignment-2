print("this is reducer 1")

#!/usr/bin/env python3
import sys
from cassandra.cluster import Cluster

# (Optional) Initialize Cassandra connection if you want to perform inserts directly.
# Note that when using Cassandra from Hadoop reducers, you must ensure the driver is available on the node.
def init_cassandra():
    cluster = Cluster(['localhost'])  # Adjust host if your Cassandra cluster differs.
    session = cluster.connect()
    # Create keyspace and tables if not already created.
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS bigdata 
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}
    """)
    session.set_keyspace('bigdata')
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
    return session

def main():
    current_key = None
    current_count = 0
    cassandra_session = None
    # Uncomment the following line if you wish to write to Cassandra from reducer.
    # cassandra_session = init_cassandra()

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        key, count = line.split("\t")
        count = int(count)
        if current_key == key:
            current_count += count
        else:
            if current_key is not None:
                # Process the previous key/value pair
                if current_key.startswith("DOCLEN_"):
                    # This is the document length record.
                    doc_id = current_key.split("DOCLEN_")[1]
                    # Optionally, insert doc_length into Cassandra.
                    # cassandra_session.execute("INSERT INTO doc_stats (doc_id, doc_length) VALUES (%s, %s)", (doc_id, current_count))
                    # Also, for debugging, output the doc_length.
                    print(f'DOCLEN_{doc_id}\t{current_count}')
                else:
                    # Composite key "term::doc_id"
                    try:
                        term, doc_id = current_key.split("::")
                    except ValueError:
                        continue
                    # Optionally, insert token frequency into Cassandra.
                    # cassandra_session.execute("INSERT INTO inverted_index (term, doc_id, tf) VALUES (%s, %s, %s)", (term, doc_id, current_count))
                    # Output the aggregated token count.
                    print(f'{term}\t{doc_id}\t{current_count}')
            current_key = key
            current_count = count

    # Don't forget to output the final key/value
    if current_key:
        if current_key.startswith("DOCLEN_"):
            doc_id = current_key.split("DOCLEN_")[1]
            print(f'DOCLEN_{doc_id}\t{current_count}')
        else:
            term, doc_id = current_key.split("::")
            print(f'{term}\t{doc_id}\t{current_count}')

    # Close Cassandra session if opened
    if cassandra_session:
        cassandra_session.shutdown()

if __name__ == '__main__':
    main()
