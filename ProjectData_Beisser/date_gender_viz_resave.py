import sqlite3 as sql
import pandas as pd
import matplotlib.pyplot as plt

query = str(
"""
SELECT asteroids.first_observation, named_after.sex_or_gender
FROM asteroids
JOIN asteroids_named_after ON asteroids.spk_id = asteroids_named_after.spk_id
JOIN named_after on asteroids_named_after.named_after_id = named_after.named_after_id
"""
)

conn = sql.connect("../Database/astronomic-objects.db")

results_df = pd.read_sql_query(query, conn, parse_dates=["first_observation"])

results_df = results_df.sort_values(by="first_observation")

grouped = (
    results_df.groupby(["sex_or_gender"]).resample("5YE", on="first_observation").size()
)

print(grouped.to_string())

# create plots
fig = plt.figure()
fig, axs = plt.subplots(2, 1, sharex=True)

# create data for top graph
x_data = grouped.loc[
    ("female", "first_observation") : ("female organsim", "first_observation")
]
y_top = grouped.loc[["female", "female organism", "trans woman"]]

bar1 = axs[0].bar(x_data, y_top)

plt.savefig("img_output1.png")
plt.close()

# access all gender indices with simple .loc["gender"]
# -> x macht keinen sinn, muss tuple sein!
