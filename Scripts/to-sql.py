import pandas
import sqlite3

conn = sqlite3.connect("../Database/astronomic-objects.db")
cursor = conn.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS object (
    spkid INTEGER PRIMARY KEY,
    full_name TEXT,
    name TEXT,
    diameter REAL,
    first_obs DATE
)
""")

dataframe = pandas.read_csv("../Project_Data/asteroids_comets_name_diameter.csv")

print("Columns:", dataframe.columns)
print("Rows count:", len(dataframe))

# format date
dataframe['first_obs'] = pandas.to_datetime(dataframe['first_obs'], errors='coerce').dt.strftime('%Y-%m-%d')

# clean null values
dataframe['diameter'] = dataframe['diameter'].where(pandas.notnull(dataframe['diameter']), None)
dataframe['first_obs'] = dataframe['first_obs'].where(pandas.notnull(dataframe['first_obs']), None)

# trim strings
dataframe['name'] = dataframe['name'].str.strip()
dataframe['full_name'] = dataframe['full_name'].str.strip()

# insert data
for row in dataframe.itertuples(index=False):
    try:
        cursor.execute("""
        INSERT OR REPLACE INTO object (spkid, full_name, name, diameter, first_obs)
        VALUES (?, ?, ?, ?, ?)
        """, (row.spkid, row.full_name, row.name, row.diameter, row.first_obs))
    except Exception as e:
        print("Insert failed:", row)
        print("Error:", e)

conn.commit()
conn.close()
