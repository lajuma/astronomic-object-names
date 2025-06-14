# TODO: find a way to turn txt to csv/df
# TODO: create random function
# TODO : pick random df entries

import sys


def txt_to_colon_separated(text_data):
    """Takes text file, replaces spaces with ; as delimiter, returns list of all ingested lines"""
    line_list = []
    with open(
        text_data,
        encoding="utf8",
    ) as fh:
        for line in fh:
            c_line = ";".join(line.split())
            line_list.append(c_line)
    return line_list


if __name__ == "__main__":
    print(txt_to_colon_separated(sys.argv[1]))
