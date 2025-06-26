SELECT 
	n.sex_or_gender,
	COUNT(a.spk_id) AS asteroid_count
FROM asteroids as a
LEFT JOIN asteroids_named_after AS rel_an ON a.spk_id = rel_an.spk_id
LEFT JOIN named_after AS n ON rel_an.named_after_id = n.named_after_id
GROUP BY n.sex_or_gender
ORDER BY COUNT(a.spk_id) DESC;

SELECT 
    a.name AS asteroid_name,
	n.name AS person_name,
	n.sex_or_gender,
	n.occupation,
	n.country_of_citizenship,
	a.diameter
FROM asteroids AS a
JOIN asteroids_named_after AS an ON a.spk_id = an.spk_id
JOIN named_after AS n ON an.named_after_id = n.named_after_id
JOIN named_after_instances AS i ON n.named_after_id = i.named_after_id
JOIN instances AS inst ON i.instance_id = inst.instance_id
JOIN subclasses_instances AS s ON inst.instance_id = s.instance_id
JOIN subclasses AS sub ON s.subclass_id = sub.subclass_id
WHERE sub.label  = 'person'
ORDER BY a.diameter DESC;