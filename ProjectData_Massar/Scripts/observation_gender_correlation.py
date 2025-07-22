import sqlite3
from collections import defaultdict

import matplotlib.pyplot as plt
from datetime import datetime
from statistics import median

def create_decade_histogram(gender_counts, fig_name):
    decades = sorted(gender_counts.keys())
    all_genders = ['female', 'male', 'multiple', 'none']
    gender_series = {gender: [] for gender in all_genders}

    for decade in decades:
        for gender in all_genders:
            gender_series[gender].append(decade_gender_counts[decade].get(gender, 0))
    fig, ax = plt.subplots(figsize=(12, 6))

    bottom = [0] * len(decades)
    colors = ['blue', 'orange', 'green', 'gray']

    for i, gender in enumerate(all_genders):
        ax.bar(decades, gender_series[gender], bottom=bottom, width=8,
               label=gender.capitalize(), color=colors[i % len(colors)], linewidth=0.5)
        bottom = [sum(x) for x in zip(bottom, gender_series[gender])]

    ax.set_title("Gender Distribution of Asteroids by Decade (Absolute)")
    ax.set_xlabel("Decade of First Observation")
    ax.set_ylabel("Gender Count")
    ax.legend(title="Gender", loc='upper left')
    plt.xticks(decades, rotation=45)
    plt.tight_layout()
    plt.savefig(f'../Figures/{fig_name}.png')

def create_proportion_barchart(gender_counts, fig_name):
    decades = sorted(gender_counts.keys())
    all_genders = ['female', 'male', 'multiple', 'none']
    gender_series_pct = {gender: [] for gender in all_genders}

    for decade in decades:
        total = sum(gender_counts[decade].values())
        for gender in all_genders:
            pct = (gender_counts[decade].get(gender, 0) / total) * 100 if total > 0 else 0
            gender_series_pct[gender].append(pct)
    fig, ax = plt.subplots(figsize=(12, 6))

    bottom = [0] * len(decades)
    colors = ['blue', 'orange', 'green', 'gray']

    for i, gender in enumerate(all_genders):
        ax.bar(decades, gender_series_pct[gender], bottom=bottom, width=8,
               label=gender.capitalize(), color=colors[i % len(colors)], linewidth=0.5)
        bottom = [sum(x) for x in zip(bottom, gender_series_pct[gender])]

    ax.set_title("Gender Distribution of Asteroids by Decade (Proportions)")
    ax.set_xlabel("Decade of First Observation")
    ax.set_ylabel("Gender Proportion (%)")
    ax.legend(title="Gender", loc='upper left')
    plt.xticks(decades, rotation=45)
    plt.tight_layout()
    plt.savefig(f'../Figures/{fig_name}.png')

def get_distributions(data):
    counts = defaultdict(lambda: defaultdict(int))

    for row in data:
        date = row['first_observation']
        year = datetime.strptime(date, "%Y-%m-%d").year
        decade = year - (year % 10)

        # all non gendered asteroids
        if row['sex_or_gender'] is None:
            sex_or_gender = 'none'
        else:
            sex_or_gender_list = str(row['sex_or_gender']).split(',')
            # females only
            if 'female' in sex_or_gender_list and not 'male' in sex_or_gender_list:
                sex_or_gender = 'female'
            # males only
            elif 'male' in sex_or_gender_list and not 'female' in sex_or_gender_list:
                sex_or_gender = 'male'
            else:
                sex_or_gender = 'multiple'

        counts[decade][sex_or_gender.lower()] += 1

    return counts

conn = sqlite3.connect("../../Database/astronomic-objects.db")
conn.row_factory = sqlite3.Row
cursor = conn.cursor()

cursor.execute("""
SELECT
	a.spk_id,
    a.first_observation,
    STRING_AGG(n.sex_or_gender, ',') AS sex_or_gender
FROM asteroids AS a
LEFT JOIN asteroids_named_after AS an ON a.spk_id = an.spk_id
LEFT JOIN named_after AS n ON an.named_after_id = n.named_after_id
GROUP BY a.spk_id, a.first_observation
ORDER BY a.first_observation ASC;
""")

# Fetch results
results = cursor.fetchall()

decade_gender_counts = get_distributions(results)
create_proportion_barchart(decade_gender_counts, 'first_obs_gender_barchart_proportional')
create_decade_histogram(decade_gender_counts, 'first_obs_gender_barchart')

conn.commit()
conn.close()