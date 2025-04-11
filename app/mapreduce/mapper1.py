print("this is mapper 1")

# !/usr/bin/env python3
import sys
import re


def tokenize(text):
    # Simple tokenization: lower case and split by non-word characters.
    return re.findall(r'\w+', text.lower())


def main():
    for line in sys.stdin:
        # Split the input line
        parts = line.strip().split("\t")
        if len(parts) < 3:
            continue
        doc_id, doc_title, doc_text = parts[0], parts[1], parts[2]
        tokens = tokenize(doc_text)
        doc_length = len(tokens)

        # Emit document length statistic with a special key
        # Use a prefix 'DOCLEN_' to distinguish from regular terms.
        print(f'DOCLEN_{doc_id}\t{doc_length}')

        # Emit one record per token occurrence with key "term_docid"
        for token in tokens:
            # The composite key ensures that later reducer can group by (token, doc_id)
            print(f'{token}::{doc_id}\t1')


if __name__ == '__main__':
    main()
