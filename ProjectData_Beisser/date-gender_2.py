import sqlite3 as sql
import matplotlib.pyplot as plt
from datetime import date

# open connection and get query results
query = str(
    """
    SELECT asteroids.first_observation, named_after.sex_or_gender
    FROM asteroids
    JOIN asteroids_named_after ON asteroids.spk_id = asteroids_named_after.spk_id
    JOIN named_after ON asteroids_named_after.named_after_id = named_after.named_after_id
    """
)


def convert_to_date(x):
    return date.fromisoformat(x)


with sql.connect("../Database/astronomic-objects.db") as db:
    results_list = list()
    for row in db.execute(query):
        new_result = tuple((convert_to_date(row[0]), row[1]))
        results_list.append(new_result)

print(results_list)
