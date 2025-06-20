# TODO: find a way to turn txt to csv/df
# TODO: create random function
# TODO : pick random df entries

import sys
import pandas as pd

"""
def txt_to_df(text_doc):
    with open(text_doc, "r") as fh:
        line_list = list()
        for line in fh:
            c_line = ";".join(line.split())
            line_list.append(c_line)
        df = pd.DataFrame(data=line_list)
    return df
"""
df = pd.read_fwf(
    "Project_Data/NumberedMPs_full.txt",
    colspecs="infer",
    infer_nrows=811553,
    header=None,
)


if __name__ == "__main__":
    #    print(txt_to_df(sys.argv[1]))
    print(df.to_string())
