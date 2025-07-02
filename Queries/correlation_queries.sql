-- Q: Is there a correlation between asteroid size and sex_or_gender in naming?
-- A: Needs some visualisation (diameter distribution) for further interpretation
--    At first glance the larger asteroids seem to be named after more females than males
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