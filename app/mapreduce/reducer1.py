#!/usr/bin/python
import sys


def main():
    current_key = None
    current_count = 0

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
                # process the previous key/value pair
                if current_key.startswith("DOCLEN_"):
                    # document length record
                    doc_id = current_key.split("DOCLEN_")[1]
                    print(f'DOCLEN_{doc_id}\t{current_count}')
                else:
                    # Composite key "term::doc_id"
                    try:
                        term, doc_id = current_key.split("::")
                    except ValueError:
                        continue
                    print(f'{term}\t{doc_id}\t{current_count}')
            current_key = key
            current_count = count

    # output the final key/value
    if current_key:
        if current_key.startswith("DOCLEN_"):
            doc_id = current_key.split("DOCLEN_")[1]
            print(f'DOCLEN_{doc_id}\t{current_count}')
        else:
            try:
                term, doc_id = current_key.split("::")
                print(f'{term}\t{doc_id}\t{current_count}')
            except ValueError:
                pass


if __name__ == '__main__':
    main()
