# astronomic-object-names
gender implications in astronomic object names, project for databases course

## how to create and update the database

1. Run [`create_schema.py`](ProjectData_Massar/Scripts/create_schema.py)
    This will create the empty database.

2. Run [`migrate_asteroid_data.py`](ProjectData_Massar/Scripts/migrate_asteroid_data.py)
    Make sure to have the right paths (config section, lines 7–10).  
    This will migrate all (or parts of) the data from the TSV file into the database and will take about half a minute.

3. The SQL database is created into `Database/astronomic-objects.db`.

## how our data is organized

The raw project data files (tsv) can be found in the folder [`Project Data`](Project_Data).

Our contributions can be found in the respective subfolders with our names. The subfolders are organized into:

1. `Queries`  
    The SQL queries, that we used for data interpretation. You might want to run them in `DB Browser for sqlite` on the above created database.
2. `Figures`  
    The Diagrams created by using Python and Matplotlib
3. `Scripts`  
    The Scripts we used for database creation, migration, statistical evaluations and visualizations
