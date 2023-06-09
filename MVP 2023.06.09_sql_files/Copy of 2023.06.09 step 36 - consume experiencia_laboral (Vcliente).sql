-- Crear tabla def de experiencia laboral dentro del cv
--<sql>--
DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_def_cv_experiencia_laboral`;
--</sql>--

--<sql>--
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_def_cv_experiencia_laboral" AS
WITH el AS (
SELECT 
	cv.id as curriculum_id,
	e.id_old,
	e.base_origen, 
	e.fecha_desde as fecha_inicio,
	e.fecha_hasta as fecha_fin,
	e.empresa_limpia as empresa,
	e.posicion_limpia as puesto
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_cv_experiencia_laboral" e
JOIN "caba-piba-staging-zone-db"."tbp_typ_def_curriculum" cv ON (e.id_cv_old=cv.id_old)
GROUP BY 
	cv.id,
	e.id_old,
	e.base_origen, 
	e.fecha_desde,
	e.fecha_hasta,
	e.empresa_limpia,
	e.posicion_limpia
),
empresas_validas AS (
	SELECT UPPER(empresa_limpia) AS empresa,
		ARRAY_JOIN(ARRAY_SORT(SPLIT(UPPER(empresa_limpia), ' ')), ' ') as empresa_ordenada,
		length(empresa_limpia) longitud,
		id_old,
		base_origen
	FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_cv_experiencia_laboral"
	WHERE empresa_valida = 1
	GROUP BY empresa_limpia, id_old, base_origen
),
organizaciones AS (
	SELECT id_organizacion,
		UPPER(COALESCE(razon_social_new, razon_social_old)) AS razon_social,
		ARRAY_JOIN(ARRAY_SORT(SPLIT(UPPER(COALESCE(razon_social_new, razon_social_old)), ' ')), ' ') as razon_social_ordenada,
		length(COALESCE(razon_social_new, razon_social_old)) longitud
	FROM "caba-piba-staging-zone-db"."tbp_typ_def_organizaciones"
),
experiencias_empresas AS ( 
SELECT 
	el.empresa,
	el.id_old,
	el.base_origen, 
	org.razon_social,
	org.id_organizacion,
	ROW_NUMBER() OVER(
		PARTITION BY 
		el.empresa_ordenada,
		org.razon_social_ordenada,
		el.id_old,
		el.base_origen 
		ORDER BY (
				(
					CAST(greatest(el.longitud, org.longitud) AS DOUBLE) - CAST(
						levenshtein_distance(el.empresa_ordenada, org.razon_social_ordenada) AS DOUBLE
					)
				) / CAST(greatest(el.longitud, org.longitud) AS DOUBLE)
			) desc
	) AS "orden_duplicado"
FROM empresas_validas el
	JOIN organizaciones org ON (
		(
			(
				CAST(greatest(el.longitud, org.longitud) AS DOUBLE) - CAST(
					levenshtein_distance(el.empresa, org.razon_social) AS DOUBLE
				)
			) / CAST(greatest(el.longitud, org.longitud) AS DOUBLE)
		) >= 0.99
		OR
		(
			(
				CAST(greatest(el.longitud, org.longitud) AS DOUBLE) - CAST(
					levenshtein_distance(el.empresa_ordenada, org.razon_social_ordenada) AS DOUBLE
				)
			) / CAST(greatest(el.longitud, org.longitud) AS DOUBLE)
		) >= 0.99
	)
)
SELECT 
	el.base_origen||el.id_old AS id,
	el.curriculum_id,
	el.id_old,
	el.base_origen, 
	el.fecha_inicio,
	el.fecha_fin,
	el.empresa as organizacion, 
	org.id_organizacion,
	el.puesto
FROM el
LEFT JOIN experiencias_empresas org
ON (el.id_old = org.id_old AND	el.base_origen = org.base_origen)
GROUP BY
	el.base_origen||el.id_old,
	el.curriculum_id,
	el.id_old,
	el.base_origen, 
	el.fecha_inicio,
	el.fecha_fin,
	el.empresa,
	org.id_organizacion,
	el.puesto
--</sql>--	