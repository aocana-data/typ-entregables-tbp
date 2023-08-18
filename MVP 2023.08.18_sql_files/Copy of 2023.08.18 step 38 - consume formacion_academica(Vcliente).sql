-- Crear tabla def de formación academica
--<sql>--
DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_def_formacion_academica`;
--</sql>--

--<sql>--
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_def_formacion_academica" AS
WITH 
instituciones AS (
	SELECT UPPER(institucion_clean) AS empresa,
		ARRAY_JOIN(ARRAY_SORT(SPLIT(UPPER(institucion_clean), ' ')), ' ') as empresa_ordenada,
		length(institucion_clean) longitud,
		codigo id_old,
		base_origen
	FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_formacion_academica"
	GROUP BY institucion_clean, codigo, base_origen
),
organizaciones AS (
	SELECT id_organizacion,
		UPPER(COALESCE(razon_social_new, razon_social_old)) AS razon_social,
		ARRAY_JOIN(ARRAY_SORT(SPLIT(UPPER(COALESCE(razon_social_new, razon_social_old)), ' ')), ' ') as razon_social_ordenada,
		length(COALESCE(razon_social_new, razon_social_old)) longitud
	FROM "caba-piba-staging-zone-db"."tbp_typ_def_organizaciones"
),
organizaciones_formadoras AS ( 
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
FROM instituciones el
	JOIN organizaciones org ON (
		(
			(
				CAST(greatest(el.longitud, org.longitud) AS DOUBLE) - CAST(
					levenshtein_distance(el.empresa, org.razon_social) AS DOUBLE
				)
			) / CAST(greatest(el.longitud, org.longitud) AS DOUBLE)
		) >= 0.80
		OR
		(
			(
				CAST(greatest(el.longitud, org.longitud) AS DOUBLE) - CAST(
					levenshtein_distance(el.empresa_ordenada, org.razon_social_ordenada) AS DOUBLE
				)
			) / CAST(greatest(el.longitud, org.longitud) AS DOUBLE)
		) >= 0.80
	)
)
SELECT 
	fa.base_origen || fa.codigo as id_formacion_academica,
	fa.codigo as id_old,
	fa.base_origen,
	
	CASE
	-- si la fecha de inicio o de fin son null o inconsistentes, se deja ambas fechas en null
		WHEN 
			try_cast(fa.fecha_inicio as date) IS NULL
			OR YEAR(try_cast(fa.fecha_inicio as date)) < (YEAR(CURRENT_DATE)-100)
			OR YEAR(try_cast(fa.fecha_inicio as date)) > (YEAR(CURRENT_DATE)+1)
			OR try_cast(fa.fecha_fin as date) IS NULL
			OR YEAR(try_cast(fa.fecha_fin as date)) < (YEAR(CURRENT_DATE)-100)
			OR YEAR(try_cast(fa.fecha_fin as date)) > (YEAR(CURRENT_DATE)+1)
		THEN CAST(NULL AS DATE)
		
		-- si la fecha de fin es menor a la de inicio se invierten
		WHEN try_cast(fa.fecha_fin as date)< try_cast(fa.fecha_inicio as date)
		THEN try_cast(fa.fecha_fin as date)
		
		ELSE try_cast(fa.fecha_inicio as date)
	END fecha_inicio,

	CASE
		-- si la fecha de inicio o de fin son null o inconsistentes, se deja ambas fechas en null
		WHEN 
			try_cast(fa.fecha_inicio as date) IS NULL
			OR YEAR(try_cast(fa.fecha_inicio as date)) < (YEAR(CURRENT_DATE)-100)
			OR YEAR(try_cast(fa.fecha_inicio as date)) > (YEAR(CURRENT_DATE)+1)
			OR try_cast(fa.fecha_fin as date) IS NULL
			OR YEAR(try_cast(fa.fecha_fin as date)) < (YEAR(CURRENT_DATE)-100)
			OR YEAR(try_cast(fa.fecha_fin as date)) > (YEAR(CURRENT_DATE)+1)
		THEN CAST(NULL AS DATE)
		
		-- si la fecha de fin es menor a la de inicio se invierten
		WHEN try_cast(fa.fecha_fin as date)< try_cast(fa.fecha_inicio as date)
		THEN try_cast(fa.fecha_inicio as date)
		
		ELSE try_cast(fa.fecha_fin as date)
	END fecha_fin,
	
	fa.estado,
	fa.descripcion_clean AS descripcion,
	UPPER(fa.institucion_clean) AS organizacion,
	org.id_organizacion as id_organizacion,
	cv.id_curriculum,
	tf.id_tipo_formacion_academica
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_formacion_academica" fa
JOIN "caba-piba-staging-zone-db"."tbp_typ_def_tipo_formacion" tf ON (fa.tipo_formacion=tf.descripcion)
JOIN "caba-piba-staging-zone-db"."tbp_typ_def_curriculum" cv ON (cv.base_origen=fa.base_origen AND cv.id_old = fa.cv_id)
LEFT JOIN organizaciones_formadoras org ON (fa.base_origen=org.base_origen and fa.codigo = org.id_old AND orden_duplicado=1)
GROUP BY
	fa.base_origen || fa.codigo,
	fa.codigo,
	fa.base_origen,
	CASE
	-- si la fecha de inicio o de fin son null o inconsistentes, se deja ambas fechas en null
		WHEN 
			try_cast(fa.fecha_inicio as date) IS NULL
			OR YEAR(try_cast(fa.fecha_inicio as date)) < (YEAR(CURRENT_DATE)-100)
			OR YEAR(try_cast(fa.fecha_inicio as date)) > (YEAR(CURRENT_DATE)+1)
			OR try_cast(fa.fecha_fin as date) IS NULL
			OR YEAR(try_cast(fa.fecha_fin as date)) < (YEAR(CURRENT_DATE)-100)
			OR YEAR(try_cast(fa.fecha_fin as date)) > (YEAR(CURRENT_DATE)+1)
		THEN CAST(NULL AS DATE)
		
		-- si la fecha de fin es menor a la de inicio se invierten
		WHEN try_cast(fa.fecha_fin as date)< try_cast(fa.fecha_inicio as date)
		THEN try_cast(fa.fecha_fin as date)
		
		ELSE try_cast(fa.fecha_inicio as date)
	END,

	CASE
		-- si la fecha de inicio o de fin son null o inconsistentes, se deja ambas fechas en null
		WHEN 
			try_cast(fa.fecha_inicio as date) IS NULL
			OR YEAR(try_cast(fa.fecha_inicio as date)) < (YEAR(CURRENT_DATE)-100)
			OR YEAR(try_cast(fa.fecha_inicio as date)) > (YEAR(CURRENT_DATE)+1)
			OR try_cast(fa.fecha_fin as date) IS NULL
			OR YEAR(try_cast(fa.fecha_fin as date)) < (YEAR(CURRENT_DATE)-100)
			OR YEAR(try_cast(fa.fecha_fin as date)) > (YEAR(CURRENT_DATE)+1)
		THEN CAST(NULL AS DATE)
		
		-- si la fecha de fin es menor a la de inicio se invierten
		WHEN try_cast(fa.fecha_fin as date)< try_cast(fa.fecha_inicio as date)
		THEN try_cast(fa.fecha_inicio as date)
		
		ELSE try_cast(fa.fecha_fin as date)
	END,    
    fa.estado,
	fa.descripcion_clean,
	fa.institucion_clean,
	org.id_organizacion,
	cv.id_curriculum,
	tf.id_tipo_formacion_academica
--</sql>--