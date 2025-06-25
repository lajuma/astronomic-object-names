import sqlite3

conn = sqlite3.connect("../Database/astronomic-objects.db")
cursor = conn.cursor()

# ------------------------------------------------------------------------------------------------
# -- Entity Tables:
#    Asteroids
#    Named_After
#    Instances
#    Categories
#    Subclasses
# ------------------------------------------------------------------------------------------------

cursor.execute("""
CREATE TABLE IF NOT EXISTS asteroids (
    spk_id TEXT PRIMARY KEY,
    mpc_id INTEGER,
    name TEXT,
    full_name TEXT,
    label_full_name TEXT,
    diameter REAL,
    first_observation DATE
);
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS named_after (
    named_after_id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT,
    description TEXT,
    sex_or_gender TEXT,
    date_of_birth DATE,
    date_of_death DATE,
    coordinates TEXT,
    country_of_citizenship TEXT,
    occupation TEXT,
    taxon_rank TEXT
);
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS instances (
    instance_id INTEGER PRIMARY KEY AUTOINCREMENT,
    label TEXT
);
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS categories (
    category_id INTEGER PRIMARY KEY AUTOINCREMENT,
    label TEXT
);
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS subclasses (
    subclass_id INTEGER PRIMARY KEY AUTOINCREMENT,
    label TEXT
);
""")


# ------------------------------------------------------------------------------------------------
# -- Relationship Tables:
#    Asteroids   ->  Named_After
#    Named_After ->  Instances
#    Instances   ->  Categories
#    Instances   ->  Subclasses
# ------------------------------------------------------------------------------------------------

cursor.execute("""
CREATE TABLE IF NOT EXISTS asteroids_named_after (
    spk_id INTEGER,
    named_after_id INTEGER,
    FOREIGN KEY (spk_id) REFERENCES asteroids(spk_id),
    FOREIGN KEY (named_after_id) REFERENCES named_after(named_after_id),
    PRIMARY KEY (spk_id, named_after_id)
);
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS named_after_instances (
    named_after_id INTEGER,
    instance_id INTEGER,
    FOREIGN KEY (named_after_id) REFERENCES named_after(named_after_id),
    FOREIGN KEY (instance_id) REFERENCES instances(instance_id),
    PRIMARY KEY (named_after_id, instance_id)
);
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS categories_instances (
    category_id INTEGER,
    instance_id INTEGER,
    FOREIGN KEY (category_id) REFERENCES categories(category_id),
    FOREIGN KEY (instance_id) REFERENCES instances(instance_id),
    PRIMARY KEY (category_id, instance_id)
);
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS subclasses_instances (
    subclass_id INTEGER,
    instance_id INTEGER,
    FOREIGN KEY (subclass_id) REFERENCES subclasses(subclass_id),
    FOREIGN KEY (instance_id) REFERENCES instances(instance_id),
    PRIMARY KEY (subclass_id, instance_id)
);
""")

conn.commit()
conn.close()