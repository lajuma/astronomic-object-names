import sqlite3 as sql
import pandas as pd
from subcategories.py import import_data

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

if __name__ == "__main__":
    data = import_data(query, con)
    print(data)
