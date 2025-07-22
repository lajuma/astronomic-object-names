import sqlite3
import matplotlib.pyplot as plt
from statistics import median

def create_boxplot(no_gender, female, male, fig_name):

    labels = ['male', 'female', 'no gender']
    diameter_data = [male, female, no_gender]

    # Plot
    plt.figure(figsize=(8, 5))
    plt.boxplot(diameter_data, tick_labels=labels)
    plt.title("Diameter by Gender")
    plt.xlabel("Gender")
    plt.ylabel("Diameter (km)")
    plt.grid(True)
    plt.savefig(f'../Figures/{fig_name}.png')
    plt.close()

def get_distributions(data):
    d_no_gender = []
    d_female = []
    d_male = []
    d_multiple_named_afters = []
    d_multiple_sex_or_gender = []

    for row in data:
        diameter = float(row['diameter'])
        # all non gendered asteroids
        if row['sex_or_gender'] is None:
            d_no_gender.append(diameter)
        else:
            sex_or_gender = str(row['sex_or_gender']).split(',')
            # all asteroids with more than one entry in the sex and gender list
            if len(sex_or_gender) > 1:
                d_multiple_named_afters.append(sex_or_gender)
            # females only
            if 'female' in sex_or_gender and not 'male' in sex_or_gender:
                d_female.append(diameter)
            # males only
            elif 'male' in sex_or_gender and not 'female' in sex_or_gender:
                d_male.append(diameter)
            # others (trans, female and male together etc.)
            else:
                d_multiple_sex_or_gender.append(diameter)
    return d_no_gender, d_female, d_male, d_multiple_named_afters, d_multiple_sex_or_gender

def print_results(no_gender, female, male, multiple_named_after, multiple_sexes):
    print('-----------------------------')
    print('Sex or Gender Distribution:')
    print(f'females: {len(female)}')
    print(f'males: {len(male)}')
    print(f'no sex or gender: {len(no_gender)}')
    print(f'multiple named after: {len(multiple_named_after)}')
    print(f'multiple sexes or genders: {len(multiple_sexes)}')

    print('-----------------------------')
    print('Median of Distributions:')
    print(f'median male: {median(male)}')
    print(f'median female: {median(female)}')
    print(f'median no gender: {median(no_gender)}')
    print(f'median all: {median(male + female + no_gender + multiple_sexes)}')


conn = sqlite3.connect("../../Database/astronomic-objects.db")
conn.row_factory = sqlite3.Row
cursor = conn.cursor()

cursor.execute("""
SELECT
    a.spk_id as spk_id,
    a.diameter as diameter,
    STRING_AGG(n.sex_or_gender, ',') AS sex_or_gender
FROM asteroids AS a
LEFT JOIN asteroids_named_after AS an ON a.spk_id = an.spk_id
LEFT JOIN named_after AS n ON an.named_after_id = n.named_after_id
GROUP BY a.spk_id, a.diameter
ORDER BY a.diameter DESC;
""")

# Fetch results
results = cursor.fetchall()
results_100 = results[:100]

diameters_no_gender, diameters_female, diameters_male, multiple_named_afters, multiple_sexes_or_other = get_distributions(results)
diameters_no_gender_largest_100, diameters_female_largest_100, diameters_male_largest_100, multiple_named_afters_largest_100, multiple_sexes_or_other_largest_100 = get_distributions(results_100)

print_results(diameters_no_gender, diameters_female, diameters_male, multiple_named_afters, multiple_sexes_or_other)
print_results(diameters_no_gender_largest_100, diameters_female_largest_100, diameters_male_largest_100, multiple_named_afters_largest_100, multiple_sexes_or_other_largest_100)

create_boxplot(diameters_no_gender, diameters_female, diameters_male, 'diameter_gender_boxplot')

conn.commit()
conn.close()