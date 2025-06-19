# TODO: find a way to turn txt to csv/df
# TODO: create random function
# TODO : pick random df entries

import sys
import pandas as pd


def txt_to_df(text_doc):
    with open(text_doc, "r") as fh:
        line_list = list()
        for line in fh:
            #            c_line = ";".join(line.split(" "))
            c_line = ";".join(line.split())
            line_list.append(c_line)
    return line_list


if __name__ == "__main__":
    print(txt_to_df(sys.argv[1]))
