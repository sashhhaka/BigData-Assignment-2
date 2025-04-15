#!/usr/bin/python
import sys
import re
import traceback

try:
    print("Starting mapper1.py", file=sys.stderr)


    def tokenize(text):
        if not isinstance(text, str):
            return []
        return re.findall(r'\w+', text.lower())


    def main():
        for i, line in enumerate(sys.stdin):
            try:
                parts = line.strip().split("\t")

                # diagnostic information
                if i < 5:
                    print(f"Read line {i}: {parts[:2]}", file=sys.stderr)

                if len(parts) < 3:
                    print(f"Skipped line (insufficient fields): {parts}", file=sys.stderr)
                    continue

                doc_id, doc_title, doc_text = parts[0], parts[1], parts[2]
                tokens = tokenize(doc_text)
                doc_length = len(tokens)

                print(f'DOCLEN_{doc_id}\t{doc_length}')

                for token in tokens:
                    print(f'{token}::{doc_id}\t1')
            except Exception as e:
                print(f"Error processing line {i}: {e}", file=sys.stderr)
                print(traceback.format_exc(), file=sys.stderr)


    if __name__ == '__main__':
        main()

except Exception as e:
    print(f"Error in mapper1.py: {e}", file=sys.stderr)
    print(traceback.format_exc(), file=sys.stderr)
    sys.exit(2)
