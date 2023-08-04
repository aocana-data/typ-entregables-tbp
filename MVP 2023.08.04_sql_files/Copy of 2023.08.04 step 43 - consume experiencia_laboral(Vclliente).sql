-- Crear tabla def de experiencia laboral dentro del cv
--<sql>--
DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_def_cv_experiencia_laboral`;
--</sql>--

--<sql>--
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_def_cv_experiencia_laboral" AS
WITH el AS (
SELECT 
	cv.id as id_curriculum,
	e.id_old,
	e.base_origen, 
	e.fecha_desde as fecha_inicio,
	e.fecha_hasta as fecha_fin,
	e.empresa_limpia as empresa,
	e.posicion_limpia as puesto,
	e.descripcion_empleo,
	ai.id_areas_de_interes,
	sp.id_sector_productivo
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_cv_experiencia_laboral" e
JOIN "caba-piba-staging-zone-db"."tbp_typ_def_curriculum" cv ON (e.id_cv_old=cv.id_old)
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_areas_de_interes" ai ON (e.area_id = ai.id_old AND ai.base_origen=e.base_origen)
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_sector_productivo" sp ON (CAST(e.industry_id AS VARCHAR) IN (sp.ids_mtr_portal_empleo) AND e.base_origen = 'PORTALEMPLEO')
GROUP BY 
	cv.id,
	e.id_old,
	e.base_origen, 
	e.fecha_desde,
	e.fecha_hasta,
	e.empresa_limpia,
	e.posicion_limpia,
	e.descripcion_empleo,
	ai.id_areas_de_interes,
	sp.id_sector_productivo
),
empresas_validas AS (
	SELECT UPPER(empresa_limpia) AS empresa,
		ARRAY_JOIN(ARRAY_SORT(SPLIT(UPPER(empresa_limpia), ' ')), ' ') as empresa_ordenada,
		length(empresa_limpia) longitud,
		id_old,
		base_origen,
		descripcion_empleo
	FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_cv_experiencia_laboral"
	WHERE empresa_valida = 1
	GROUP BY empresa_limpia, id_old, base_origen, descripcion_empleo
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
	el.descripcion_empleo,
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
	el.id_curriculum,
	el.id_old,
	el.base_origen, 
	el.fecha_inicio,
	el.fecha_fin,
	el.empresa as organizacion, 
	org.id_organizacion,
	el.puesto,
	el.descripcion_empleo,
	el.id_areas_de_interes,
	el.id_sector_productivo
FROM el
LEFT JOIN experiencias_empresas org
ON (el.id_old = org.id_old AND	el.base_origen = org.base_origen AND org.orden_duplicado=1)
GROUP BY
	el.base_origen||el.id_old,
	el.id_curriculum,
	el.id_old,
	el.base_origen, 
	el.fecha_inicio,
	el.fecha_fin,
	el.empresa,
	org.id_organizacion,
	el.puesto,
	el.descripcion_empleo,
	el.id_areas_de_interes,
	el.id_sector_productivo
--</sql>--