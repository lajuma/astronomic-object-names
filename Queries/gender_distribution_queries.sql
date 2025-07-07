-- Q: What is the sex_or_gender distribution of asteroids naming?
-- A: male / male organism: 4,727 / 4
--    female / female organism: 1,036 / 3
--    trans woman: 2
--    none: 11,266
SELECT
	n.sex_or_gender,
	COUNT(DISTINCT(a.spk_id)) AS asteroid_count
FROM asteroids as a
LEFT JOIN asteroids_named_after AS an ON a.spk_id = an.spk_id
LEFT JOIN named_after AS n ON an.named_after_id = n.named_after_id
GROUP BY n.sex_or_gender
ORDER BY asteroid_count DESC;


-- Q: How many asteroids are named after natural persons and fictional persons grouped by sex_and_gender?
-- A: Natural persons:          Fictional persons:
--    male: 4,431               male: 29
--    female: 714               female: 34
--    trans women: 2            /
--    none: 79                  none: 3
SELECT
    sub.label,
	n.sex_or_gender,
    COUNT(DISTINCT(a.spk_id)) AS asteroid_count
FROM subclasses AS sub
JOIN subclasses_instances AS s ON s.subclass_id = sub.subclass_id
JOIN instances AS inst ON inst.instance_id = s.instance_id
JOIN named_after_instances AS i ON i.instance_id = inst.instance_id
JOIN named_after AS n ON i.named_after_id = n.named_after_id
JOIN asteroids_named_after AS an ON n.named_after_id = an.named_after_id
JOIN asteroids AS a ON a.spk_id = an.spk_id
WHERE sub.label IN ('natural person', 'fictional person')
GROUP BY
	sub.label,
	n.sex_or_gender
ORDER BY asteroid_count DESC;


-- Q: What are the subcategories of the remaining asteroids (not included in the fictional / natural person query)
-- A: 1) Mythological Characters: 171 male, 102 female + subcategories
--    2) Deities: 52 male, 98 female + subcategories
SELECT
    sub.label,
	n.sex_or_gender,
    COUNT(DISTINCT a.spk_id) AS asteroid_count
FROM asteroids AS a
JOIN asteroids_named_after AS an ON a.spk_id = an.spk_id
JOIN named_after AS n ON an.named_after_id = n.named_after_id
JOIN named_after_instances AS nai ON n.named_after_id = nai.named_after_id
JOIN instances AS inst ON nai.instance_id = inst.instance_id
JOIN subclasses_instances AS si ON inst.instance_id = si.instance_id
JOIN subclasses AS sub ON si.subclass_id = sub.subclass_id
WHERE n.sex_or_gender IS NOT NULL
  AND a.spk_id NOT IN (
    SELECT a1.spk_id
    FROM asteroids AS a1
    JOIN asteroids_named_after AS an1 ON a1.spk_id = an1.spk_id
    JOIN named_after AS n1 ON an1.named_after_id = n1.named_after_id
    JOIN named_after_instances AS nai1 ON n1.named_after_id = nai1.named_after_id
    JOIN instances AS inst1 ON nai1.instance_id = inst1.instance_id
    JOIN subclasses_instances AS si1 ON inst1.instance_id = si1.instance_id
    JOIN subclasses AS sub1 ON si1.subclass_id = sub1.subclass_id
    WHERE sub1.label IN ('natural person', 'fictional person')
)
GROUP BY
	n.sex_or_gender,
	sub.label
ORDER BY asteroid_count DESC;

-- Q: How many male/female deities are there? (also includes subclasses like 'greek deity' or 'waterdeity')
-- A: female: 136, male: 63
SELECT
	n.sex_or_gender,
    COUNT(DISTINCT a.spk_id) AS asteroid_count
FROM asteroids AS a
JOIN asteroids_named_after AS an ON a.spk_id = an.spk_id
JOIN named_after AS n ON an.named_after_id = n.named_after_id
JOIN named_after_instances AS nai ON n.named_after_id = nai.named_after_id
JOIN instances AS inst ON nai.instance_id = inst.instance_id
JOIN subclasses_instances AS si ON inst.instance_id = si.instance_id
JOIN subclasses AS sub ON si.subclass_id = sub.subclass_id
WHERE n.sex_or_gender IS NOT NULL AND sub.label LIKE '%deity%'
GROUP BY
	n.sex_or_gender
ORDER BY asteroid_count DESC;


-- Q: How many mythological characters are there? (also includes subclasses like 'mythological greek character')
-- A: female: 65, male: 21
SELECT
	n.sex_or_gender,
    COUNT(DISTINCT a.spk_id) AS asteroid_count
FROM asteroids AS a
JOIN asteroids_named_after AS an ON a.spk_id = an.spk_id
JOIN named_after AS n ON an.named_after_id = n.named_after_id
JOIN named_after_instances AS nai ON n.named_after_id = nai.named_after_id
JOIN instances AS inst ON nai.instance_id = inst.instance_id
JOIN subclasses_instances AS si ON inst.instance_id = si.instance_id
JOIN subclasses AS sub ON si.subclass_id = sub.subclass_id
WHERE n.sex_or_gender IS NOT NULL AND sub.label LIKE '%mythological%'
GROUP BY
	n.sex_or_gender
ORDER BY asteroid_count DESC;


-- Q: Who are the trans-women two of the asteroids are named after?
-- A: 1) Sophie (Xeon): British singer, songwriter, record producer and DJ (1986–2021)
--    2) Romy Haag: Dutch dancer, singer, actress and nightclub manager
SELECT
	n.name AS person_name,
	n.description,
	n.date_of_birth,
	n.date_of_death,
	n.occupation
FROM named_after
WHERE n.sex_or_gender = 'trans woman';


-- Q: Who are the 79 natural persons and 3 fictional persons that have no sex_or_gender tagged?
-- A: By looking at the results it seems like they are simply not tagged.
--    For further statistic calculations their gender could be researched and added manually
SELECT
	n.name,
	n.description,
	n.date_of_birth,
	n.date_of_death,
	n.occupation,
	sub.label
FROM subclasses AS sub
JOIN subclasses_instances AS s ON s.subclass_id = sub.subclass_id
JOIN instances AS inst ON inst.instance_id = s.instance_id
JOIN named_after_instances AS i ON i.instance_id = inst.instance_id
JOIN named_after AS n ON i.named_after_id = n.named_after_id
WHERE (sub.label = 'natural person' OR sub.label = 'fictional person') AND n.sex_or_gender IS NULL;