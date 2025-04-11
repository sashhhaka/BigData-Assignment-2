#!/usr/bin/env python3
import sys

def main():
    for line in sys.stdin:
        parts = line.strip().split("\t")
        # We assume that lines from the previous reducer have exactly three fields (skip DOCLEN records)
        if len(parts) != 3:
            continue
        term, doc_id, tf = parts
        # Emit each term with a count of 1 for its occurrence in one document.
        print(f'{term}\t1')

if __name__ == '__main__':
    main()
