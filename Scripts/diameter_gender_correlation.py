import sqlite3
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.ticker import ScalarFormatter


def create_histogram(male, female, trans):
    plt.hist(
        [male, female, trans],
        bins=35,
        color=['green', 'orange', 'blue'],
        label=['Male', 'Female', 'Trans'],
        alpha=0.6)
    plt.title('Diameter Gender Distribution')
    formatter = ScalarFormatter()
    formatter.set_scientific(False)
    plt.gca().yaxis.set_major_formatter(formatter)
    plt.xlabel('Diameter')
    plt.ylabel('Frequency (log)')
    plt.yscale('log')
    plt.tight_layout()
    plt.savefig('../Figures/diameter_gender_distribution.png')
    plt.close()

conn = sqlite3.connect("../Database/astronomic-objects.db")
conn.row_factory = sqlite3.Row
cursor = conn.cursor()

cursor.execute("""
SELECT
    a.name as asteroid_name,
    n.name,
    n.description,
    a.diameter,
	n.sex_or_gender
FROM asteroids as a
JOIN asteroids_named_after AS an ON a.spk_id = an.spk_id
JOIN named_after AS n ON an.named_after_id = n.named_after_id
ORDER BY a.diameter DESC;
""")

# Fetch results
results = cursor.fetchall()

diameters_male = []
diameters_female = []
diameters_trans_women = []

for row in results:
    if row['sex_or_gender'] == 'female' or row['sex_or_gender'] == 'female organism':
        diameters_female.append(row['diameter'])
    elif row['sex_or_gender'] == 'male' or row['sex_or_gender'] == 'male organism':
        diameters_male.append(row['diameter'])
    elif row['sex_or_gender'] == 'trans woman':
        diameters_trans_women.append(row['diameter'])


# combined = np.concatenate([diameters_male, diameters_female, diameters_trans_women])
# combined = combined[combined > 0]  # Remove non-positive values if any

# bins = np.logspace(np.log10(min(combined[combined > 0])), np.log10(max(combined)), 50)

# Create and save histogram for the first distribution
create_histogram(diameters_male, diameters_female, diameters_trans_women)

conn.commit()
conn.close()