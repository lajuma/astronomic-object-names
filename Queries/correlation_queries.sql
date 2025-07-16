-- Q: Is there a correlation between asteroid size and sex_or_gender in naming?
-- A: Larger asteroids seem to be named after more females than males, but needs further (visual) examination
SELECT
    a.name AS asteroid_name,
    STRING_AGG(n.name, '; ') AS named_after,
    STRING_AGG(n.description, '; ') AS descriptions,
    a.diameter,
    STRING_AGG(n.sex_or_gender, ', ') AS sex_or_genders
FROM asteroids AS a
JOIN asteroids_named_after AS an ON a.spk_id = an.spk_id
JOIN named_after AS n ON an.named_after_id = n.named_after_id
GROUP BY a.name, a.diameter
ORDER BY a.diameter DESC;

-- Q: Get all asteroids with diameter and sex_or_gender for a boxplot diagram
--    (sometimes more than one named_after, therefore sex_and_gender needs to be aggregated)
-- A: see Figures/diameter_gender_boxplot.png and diameter_gender_correlation.py
SELECT
    a.spk_id,
    a.diameter,
    STRING_AGG(n.sex_or_gender, ', ') AS sex_or_genders
FROM asteroids AS a
LEFT JOIN asteroids_named_after AS an ON a.spk_id = an.spk_id
LEFT JOIN named_after AS n ON an.named_after_id = n.named_after_id
GROUP BY a.spk_id, a.diameter
ORDER BY a.diameter DESC;