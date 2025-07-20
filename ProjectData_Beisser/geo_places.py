import sqlite3 as sql
import pandas as pd

con = sql.connect("../Database/astronomic-objects.db")

query = str("""
SELECT asteroids.name, named_after.name AS named_after, subclasses.label, named_after.coordinates
FROM asteroids
	JOIN asteroids_named_after ON asteroids.spk_id = asteroids_named_after.spk_id
	JOIN named_after ON asteroids_named_after.named_after_id = named_after.named_after_id
	JOIN named_after_instances ON named_after.named_after_id = named_after_instances.named_after_id
	JOIN subclasses_instances ON named_after_instances.instance_id = subclasses_instances.instance_id
	JOIN subclasses ON subclasses_instances.subclass_id = subclasses.subclass_id
WHERE named_after.coordinates NOT NULL
""")


def read_data(query, con):
    with con as connection:
        results = pd.read_sql_query(query, con)
    return results


if __name__ == "__main__":
    data = read_data(query, con)
    data_noDuplicates = data.drop_duplicates(subset="name")
    print(data_noDuplicates.to_string())
    data_noDuplicates.to_csv("Figures/geo_places_noDuplicates.csv")
