 --takes all objects that have coordinates with their label

SELECT asteroids.name, named_after.name AS named_after, subclasses.label, named_after.coordinates
FROM asteroids
	JOIN asteroids_named_after ON asteroids.spk_id = asteroids_named_after.spk_id
	JOIN named_after ON asteroids_named_after.named_after_id = named_after.named_after_id
	JOIN named_after_instances ON named_after.named_after_id = named_after_instances.named_after_id
	JOIN subclasses_instances ON named_after_instances.instance_id = subclasses_instances.instance_id
	JOIN subclasses ON subclasses_instances.subclass_id = subclasses.subclass_id
WHERE named_after.coordinates NOT NULL

--all objects that have a date of birth or death
 
SELECT asteroids.name, named_after.name AS named_after, named_after.sex_or_gender, named_after.date_of_birth, named_after.date_of_death
FROM asteroids
	JOIN asteroids_named_after ON asteroids.spk_id = asteroids_named_after.spk_id
	JOIN named_after ON asteroids_named_after.named_after_id = named_after.named_after_id
WHERE date_of_birth OR date_of_death NOT NULL

--gets all objects with their name, instance, category and subclass

SELECT asteroids.name, instances.label AS instance, categories.label AS category, subclasses.label AS subclass
FROM asteroids
	JOIN asteroids_named_after ON asteroids.spk_id = asteroids_named_after.spk_id
	JOIN named_after_instances ON asteroids_named_after.named_after_id = named_after_instances.named_after_id
	JOIN instances ON named_after_instances.instance_id = instances.instance_id
		JOIN categories_instances ON instances.instance_id = categories_instances.instance_id
			JOIN categories ON categories_instances.category_id = categories.category_id
		JOIN subclasses_instances ON instances.instance_id = subclasses_instances.instance_id
			JOIN subclasses ON subclasses_instances.subclass_id = subclasses.subclass_id

--gets all instances etc, but counts subclasses and orders ascending

SELECT subclasses.label AS subclass, COUNT(subclasses.label) as counter
FROM asteroids
JOIN asteroids_named_after ON asteroids.spk_id = asteroids_named_after.spk_id
JOIN named_after_instances ON asteroids_named_after.named_after_id = named_after_instances.named_after_id
JOIN instances ON named_after_instances.instance_id = instances.instance_id
  JOIN categories_instances ON instances.instance_id = categories_instances.instance_id
    JOIN categories ON categories_instances.category_id = categories.category_id
  JOIN subclasses_instances ON instances.instance_id = subclasses_instances.instance_id
    JOIN subclasses ON subclasses_instances.subclass_id = subclasses.subclass_id
GROUP BY subclasses.label
ORDER BY counter DESC 

--get all objects that have a place of citizenship

SELECT asteroids.name, named_after.name AS named_after, named_after.description, named_after.country_of_citizenship
FROM asteroids
	JOIN asteroids_named_after ON asteroids.spk_id = asteroids_named_after.spk_id
	JOIN named_after ON asteroids_named_after.named_after_id = named_after.named_after_id
WHERE named_after.country_of_citizenship NOT NULL

--count all places of citizenship

SELECT named_after.country_of_citizenship as country_of_citizenship, COUNT(country_of_citizenship) as counter
FROM asteroids
	JOIN asteroids_named_after ON asteroids.spk_id = asteroids_named_after.spk_id
	JOIN named_after ON asteroids_named_after.named_after_id = named_after.named_after_id
WHERE named_after.country_of_citizenship NOT NULL
GROUP BY country_of_citizenship
ORDER BY counter DESC
