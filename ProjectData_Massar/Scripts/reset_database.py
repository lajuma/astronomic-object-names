def clear_database(cursor):
    # Disable foreign key checks
    cursor.execute("PRAGMA foreign_keys = OFF;")

    # List of tables to remove data from (relationship tables come first)
    tables = [
        "named_after_instances",   # relation table
        "asteroids_named_after",  # relation table
        "categories_instances",  # relation table
        "subclasses_instances",  # relation table
        "instances",
        "subclasses",
        "categories",
        "named_after",
        "asteroids",
    ]

    # Remove data from tables
    for table in tables:
        try:
            cursor.execute(f"DELETE FROM {table};")
            # print(f"Cleared table: {table}")
        except Exception as e:
            print(f"Error clearing table {table}: {e}")

    # Reset autoincrement sequence
    try:
        cursor.execute(f"DELETE FROM sqlite_sequence WHERE name = 'named_after'")
        cursor.execute(f"DELETE FROM sqlite_sequence WHERE name = 'instances'")
        cursor.execute(f"DELETE FROM sqlite_sequence WHERE name = 'categories'")
        cursor.execute(f"DELETE FROM sqlite_sequence WHERE name = 'subclasses'")
    except Exception as e:
        print(f"Error clearing sequence: {e}")

    # Re-enable foreign key checks
    cursor.execute("PRAGMA foreign_keys = ON;")

    print(f"Cleared databases and reset sequence")
