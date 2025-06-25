def insert_asteroid(cursor, row):
    try:
        cursor.execute("""
                    INSERT OR IGNORE INTO asteroids (spk_id, mpc_id, name, full_name, label_full_name, diameter, first_observation)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
            row.get('spkid'),
            row.get('Minor Planet Center body ID'),
            row.get('name'),
            row.get('full_name'),
            row.get('Label full_name'),
            row.get('diameter'),
            row.get('first_obs')
        ))
    except Exception as e:
        print(f"Error inserting asteroid: {e}")


def insert_named_after(cursor, row, asteroid_id):
    try:
        cursor.execute("""
                    INSERT INTO named_after (name, description, sex_or_gender, date_of_birth, date_of_death, 
                                             coordinates, country_of_citizenship, occupation, taxon_rank)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
            row.get('named after'),
            row.get('Description named_after'),
            row.get('sex or gender'),
            row.get('date of birth'),
            row.get('date of death'),
            row.get('coordinate location'),
            row.get('country of citizenship'),
            row.get('occupation'),
            row.get('taxon rank')
        ))

        # get id after database insert
        current_id = cursor.lastrowid

        # update relationship table
        cursor.execute("""
                    INSERT OR IGNORE INTO 'asteroids_named_after' (spk_id, named_after_id)
                    VALUES (?, ?)
                """, (asteroid_id, current_id))

        return current_id

    except Exception as e:
        print(f"Error inserting asteroid: {e}")

