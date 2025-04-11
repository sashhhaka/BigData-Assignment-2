#!/usr/bin/env python3
import sys
from cassandra.cluster import Cluster

def init_cassandra():
    cluster = Cluster(['localhost'])
    session = cluster.connect()
    session.set_keyspace('bigdata')
    session.execute("""
        CREATE TABLE IF NOT EXISTS vocabulary (
            term text PRIMARY KEY,
            doc_freq int
        )
    """)
    return session

def main():
    current_term = None
    current_df = 0
    # Optionally, initialize Cassandra session for inserting vocabulary data.
    cassandra_session = None
    # Uncomment to enable Cassandra insertion.
    # cassandra_session = init_cassandra()

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        term, count = line.split("\t")
        count = int(count)
        if current_term == term:
            current_df += count
        else:
            if current_term is not None:
                # Optionally insert into Cassandra:
                # cassandra_session.execute("INSERT INTO vocabulary (term, doc_freq) VALUES (%s, %s)", (current_term, current_df))
                print(f'{current_term}\t{current_df}')
            current_term = term
            current_df = count

    if current_term:
        print(f'{current_term}\t{current_df}')

    if cassandra_session:
        cassandra_session.shutdown()

if __name__ == '__main__':
    main()
