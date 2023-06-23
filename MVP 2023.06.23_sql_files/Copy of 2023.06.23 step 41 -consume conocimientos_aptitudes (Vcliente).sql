-- 1- Se crea la tabla tbp_typ_def_nivel_conocimientos_aptitudes que contiene el nivel de conocimientos_aptitudes
-- Por ahora solo será el nivel de idiomas
--<sql>--
DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_def_nivel_conocimientos_aptitudes`;
--</sql>--

--<sql>--
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_def_nivel_conocimientos_aptitudes" AS 
SELECT 
id AS codigo,
UPPER(name) AS descripcion
FROM "caba-piba-raw-zone-db"."portal_empleo_language_level"
--</sql>--

-- 2- Se crea la tabla tbp_typ_def_nivel_conocimientos_aptitudes que contiene el tipo de conocimientos_aptitudes
-- Por ahora son grupos predefinidos en script de staging
--<sql>--
DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_def_tipo_conocimientos_aptitudes`;
--</sql>--

--<sql>--
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_def_tipo_conocimientos_aptitudes" AS 
SELECT 
row_number() OVER () AS codigo,
tipo_formacion AS descripcion
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_conocimientos_aptitudes"
GROUP BY tipo_formacion
--</sql>--

-- 3- Se crea la tabla tbp_typ_def_conocimientos_aptitudes con sus relaciones
--<sql>--
DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_def_conocimientos_aptitudes`;
--</sql>--

--<sql>--
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_def_conocimientos_aptitudes" AS
WITH 
instituciones AS (
	SELECT UPPER(institucion_clean) AS empresa,
		ARRAY_JOIN(ARRAY_SORT(SPLIT(UPPER(institucion_clean), ' ')), ' ') as empresa_ordenada,
		length(institucion_clean) longitud,
		codigo id_old,
		base_origen
	FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_conocimientos_aptitudes"
	WHERE institucion_clean IS NOT NULL
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
	fa.base_origen || fa.codigo as id,
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
	org.id_organizacion AS id_organizacion,
	UPPER(fa.observacion) AS observacion,
	cv.id as codigo_cv,
	n.descripcion as nivel,
	t.descripcion as tipo_formacion_conocimientos_aptitudes
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_conocimientos_aptitudes" fa
JOIN "caba-piba-staging-zone-db"."tbp_typ_def_curriculum" cv 
-- cuando la base origen es CRMSL se cambia el JOIN por portalempleo porque en el script de staging de 
-- asociaron los registros entre las dos bases por dni
ON (
	cv.base_origen=CASE WHEN fa.base_origen like 'CRMSL'
						THEN 'PORTALEMPLEO' ELSE fa.base_origen END 
				AND cv.id_old = fa.cv_id)
				
JOIN "caba-piba-staging-zone-db"."tbp_typ_def_tipo_conocimientos_aptitudes" t ON (t.descripcion=fa.tipo_formacion)
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_nivel_conocimientos_aptitudes" n ON (n.descripcion=fa.nivel_idioma)
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
	UPPER(fa.observacion),
	cv.id,
	n.descripcion,
	t.descripcion
--</sql>--