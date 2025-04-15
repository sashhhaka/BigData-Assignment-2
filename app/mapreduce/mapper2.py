#!/usr/bin/python
import sys

def main():
    for line in sys.stdin:
        parts = line.strip().split("\t")
        # exactly three fields (skip DOCLEN records)
        if len(parts) != 3:
            continue
        term, doc_id, tf = parts
        print(f'{term}\t1')

if __name__ == '__main__':
    main()
