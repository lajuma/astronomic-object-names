import sqlite3
import pandas

from insert_to_database import insert_asteroid, insert_named_after
from reset_database import clear_database

# Config (adapt for testing)
number_of_rows_to_process = 0 # turn to 0 for all rows
path_to_tsv_file = "../Project_Data/asteroids_17k_OpenRefine_Export.tsv"
path_to_sql_file = "../Database/astronomic-objects.db"

# Load TSV into DataFrame
df = pandas.read_csv(
    path_to_tsv_file,
    sep="\t",
    dtype={"spkid": str, "Minor Planet Center body ID": str})

# Limit to first n rows
if number_of_rows_to_process > 0:
    df = df.head(number_of_rows_to_process)

conn = sqlite3.connect(path_to_sql_file)
cursor = conn.cursor()

# clear database before
print('-------------------------------------')
clear_database(cursor)
print('-------------------------------------')

# caches for multirow data
current_asteroid_id = None
current_named_after_id = None
current_instance_id = None

# TODO: add error logging for each row (try/except)
# TODO: refactor instances, categories, subclasses to function in insert_to_database

if number_of_rows_to_process > 0:
    print(f"Start migration of first {number_of_rows_to_process} rows")
else:
    print(f"Start migration")
print('-------------------------------------')

# logging counts
asteroid_count = 0
named_after_count = 0
instance_count = 0
subclass_count = 0
row_number = 0

# Start Iteration
for i, row in df.iterrows():

    # current row number for logging
    row_number = i + 1

    # ------------------------------------------------------------------------------------------------
    # -- 1) Handle asteroids --
    # ------------------------------------------------------------------------------------------------
    spk_id = row.get('spkid')

    if not pandas.isna(spk_id) or str(spk_id).strip() == "":
        insert_asteroid(cursor, row)
        asteroid_count += 1
        current_asteroid_id = spk_id

    # print(f"row: {row_number}, asteroid_id_cache: {current_asteroid_id}")

    # ------------------------------------------------------------------------------------------------
    # -- 2) Handle named after --
    # ------------------------------------------------------------------------------------------------
    named_after = row.get('named after')

    if not pandas.isna(named_after) or str(named_after).strip() == "":

        description = row.get('Description named_after')

        # Check if already exists
        cursor.execute("SELECT named_after_id FROM named_after WHERE name=?", (named_after,))
        result = cursor.fetchone()

        if result:
            current_named_after_id = result[0]
        else:
            current_named_after_id = insert_named_after(cursor, row, current_asteroid_id)
            named_after_count += 1

    # print(f"row: {row_number}, named_after_id_cache: {current_named_after_id}")


    # ------------------------------------------------------------------------------------------------
    # -- 3) Handle instances of named after --
    # ------------------------------------------------------------------------------------------------

    instance_of_named_after = row.get('instance of named after')

    if not pandas.isna(instance_of_named_after) or str(instance_of_named_after).strip() == "":

        # Check if already exists
        cursor.execute("SELECT instance_id FROM instances WHERE label=?", (instance_of_named_after,))
        result = cursor.fetchone()

        if result:
            current_instance_id = result[0]
        else:
            cursor.execute("""
                INSERT OR IGNORE INTO instances (label)
                VALUES (?)
            """, (instance_of_named_after,))

            instance_count += 1

            # get id after database insert
            current_instance_id = cursor.lastrowid

        # update relationship table
        cursor.execute("""
                    INSERT OR IGNORE INTO 'named_after_instances' (instance_id, named_after_id)
                    VALUES (?, ?)
                """, (current_instance_id, current_named_after_id))

    # print(f"row: {row_number}, instance_id_cache: {current_instance_id}")

    # ------------------------------------------------------------------------------------------------
    # -- 4) Handle instances of instances = categories --
    # ------------------------------------------------------------------------------------------------

    instance_of_instance_of = row.get('instance of instance of')

    if not pandas.isna(instance_of_instance_of) or str(instance_of_instance_of).strip() == "":

        # Check if already exists
        cursor.execute(
            "SELECT category_id FROM categories WHERE label=?",
            (instance_of_instance_of,)
        )
        result = cursor.fetchone()

        if result:
            category_id = result[0]
        else:
            cursor.execute("""
                INSERT OR IGNORE INTO categories (label)
                VALUES (?)
            """, (instance_of_instance_of,))

            category_id = cursor.lastrowid

        # update relationship table
        cursor.execute("""
                    INSERT OR IGNORE INTO categories_instances (instance_id, category_id)
                    VALUES (?, ?)
                """, (current_instance_id, category_id))

        # print(f"row: {row_number}, instance_of_instance_of: {instance_of_instance_of}, connected to: {current_instance_id}")

    # ------------------------------------------------------------------------------------------------
    # -- 5) Handle subclasses of instances --
    # ------------------------------------------------------------------------------------------------

    subclass_of_instance_of = row.get('subclass of instance of')

    if not pandas.isna(subclass_of_instance_of) or str(subclass_of_instance_of).strip() == "":

        # Check if already exists
        cursor.execute("SELECT subclass_id FROM subclasses WHERE label=?",
                       (subclass_of_instance_of,))
        result = cursor.fetchone()

        if result:
            subclass_id = result[0]
        else:
            cursor.execute("""
                       INSERT OR IGNORE INTO subclasses (label)
                       VALUES (?)
                   """, (subclass_of_instance_of,))
            subclass_count += 1
            subclass_id = cursor.lastrowid

        # update relationship table
        cursor.execute("""
                    INSERT OR IGNORE INTO subclasses_instances (instance_id, subclass_id)
                    VALUES (?, ?)
                """, (current_instance_id, subclass_id))

        # print(f"row: {row_number}, subclass_of_instance_of: {subclass_of_instance_of}, connected to: {current_instance_id}")

    print(f"row: {row_number} done")

conn.commit()
conn.close()
print('-------------------------------------')
print("Data imported successfully.")
print('-------------------------------------')
print(f'Total rows processed: {row_number}')
print(f'Number Asteroids: {asteroid_count}')
print(f'Number Named After: {named_after_count}')
print(f'Number Instances of Named After: {instance_count}')
print(f'Number Subclasses of Instances: {subclass_count}')
print('-------------------------------------')