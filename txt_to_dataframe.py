import pandas as pd
import sys


def txt_to_df(text_file):
    df = pd.read_fwf(text_file, colspecs="infer", infer_nrows=811553, header=None)
    return df.to_string()


if __name__ == "__main__":
    print(txt_to_df(sys.argv[1]))
