import pandas as pd
import sqlite3 as sql
import squarify
import matplotlib.pyplot as plt

con = sql.connect("../Database/astronomic-objects.db")

query = str("""
SELECT subclasses.label AS subclass, COUNT(subclasses.label) as counter
FROM asteroids
JOIN asteroids_named_after ON asteroids.spk_id = asteroids_named_after.spk_id
JOIN named_after_instances ON asteroids_named_after.named_after_id = named_after_instances.named_after_id
JOIN instances ON named_after_instances.instance_id = instances.instance_id
  JOIN categories_instances ON instances.instance_id = categories_instances.instance_id
    JOIN categories ON categories_instances.category_id = categories.category_id
  JOIN subclasses_instances ON instances.instance_id = subclasses_instances.instance_id
    JOIN subclasses ON subclasses_instances.subclass_id = subclasses.subclass_id
GROUP BY subclasses.label
ORDER BY counter DESC 
""")


def read_data(query, con):
    with con as connection:
        results = pd.read_sql_query(query, con)
    return results


def prep_dataframe(data):
    sumup = data.iloc[20:].sum(axis=0, numeric_only=True)
    print(sumup)
    new_row = pd.Series(data=sumup, index=["subclass", "counter"])
    result = pd.concat([data, new_row.to_frame().T], ignore_index=False)
    result.sort_values(by="counter").reset_index()
    return result


def treemap(data):
    fig, ax = plt.subplots(1, 1)
    ax.set_axis_off()
    out = squarify.plot(sizes=data["counter"],
                        label=data["subclass"], ax=ax, pad=True)
    fig.savefig("Figures/subcategories.png", dpi=300, pad_inches=0.1)
    return out


if __name__ == "__main__":
    print(read_data(query, con))
    data = prep_dataframe(read_data(query, con))
    print(data)
    treemap(data)
