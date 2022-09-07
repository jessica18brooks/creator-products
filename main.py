import pandas as pd
import argparse
import os


def load_file(file):
    # extract the file name and extension
    file_name, file_extension = os.path.splitext(file)

    if file_extension == '.csv':
        df = pd.read_csv(file)
    else:
        print(f'File type {file_extension} not supported yet')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("text_file", type=str, help="location of text file to load")
    args = parser.parse_args()

    load_file(args.text_file)
