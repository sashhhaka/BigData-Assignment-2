#!/usr/bin/python
import sys


def main():
    current_term = None
    current_df = 0

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
                print(f'{current_term}\t{current_df}')
            current_term = term
            current_df = count

    if current_term:
        print(f'{current_term}\t{current_df}')


if __name__ == '__main__':
    main()
