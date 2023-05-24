-- Crear tabla def de formación academica
--<sql>--
--DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_def_formacion_academica`;
--</sql>--

--<sql>--
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_def_formacion_academica" AS
WITH organizaciones_formadoras AS
	(SELECT 
		fa.codigo,
		fa.base_origen,
		COALESCE(org1.id_organizacion, org2.id_organizacion) id_organizacion,
		ROW_NUMBER() OVER(
		PARTITION BY fa.codigo,	fa.base_origen
		ORDER BY org1.razon_social_new, org2.razon_social_old
			) AS "orden_duplicado"
	FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_formacion_academica" fa
	LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_organizaciones" org1 ON (UPPER(fa.institucion_educativa) = org1.razon_social_new)
	LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_organizaciones" org2 ON (UPPER(fa.institucion_educativa) = org2.razon_social_old )
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
	UPPER(fa.institucion_educativa) AS institucion_educativa,
	org.id_organizacion as id_organizacion,
	cv.id as codigo_cv,
	tf.codigo as codigo_tipo_formacion
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_formacion_academica" fa
JOIN "caba-piba-staging-zone-db"."tbp_typ_def_tipo_formacion" tf ON (fa.tipo_formacion=tf.descripcion)
JOIN "caba-piba-staging-zone-db"."tbp_typ_def_curriculum" cv ON (cv.base_origen=fa.base_origen AND cv.id_old = fa.cv_id)
LEFT JOIN organizaciones_formadoras org ON (org.codigo=fa.codigo AND org.base_origen=fa.base_origen AND org.orden_duplicado=1)
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
	UPPER(fa.institucion_educativa),
	org.id_organizacion,
	cv.id,
	tf.codigo
--</sql>--