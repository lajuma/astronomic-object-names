# astronomic-object-names
gender implications in astronomic object names, project for databases course

# how to update the database

1. Run `create_schema.py`  
    This will create the empty database.

2. Run `migrate_asteroid_data.py`  
    Make sure to have the right paths (config section, lines 7–10).  
    This will migrate all (or parts of) the data from the TSV file into the database and will take about half a minute.

    Find the SQL database in `Database/astronomic-objects.db`.

