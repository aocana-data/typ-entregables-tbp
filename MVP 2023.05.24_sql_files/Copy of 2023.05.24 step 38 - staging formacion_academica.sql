-- Crear tabla tmp de formación academica
-- --<sql>--DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_tmp_formacion_academica`;--</sql>--
--<sql>--
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_formacion_academica" AS
WITH 
-- PORTALEMPLEO es la unica base origen que tiene informacion de carreras o formacion academica 
-- (no consideramos cursos como formacion academica)
datos AS (
	SELECT
		CAST(i.id AS VARCHAR) AS codigo,
		'PORTALEMPLEO' base_origen,
		i.start_date fecha_inicio, 
		i.end_date fecha_fin,
		UPPER(ls.value) as estado,
		sa.name AS descripcion,
		inst.name AS institucion_educativa,
		CAST(af.curriculum_id AS VARCHAR) AS cv_id,
		UPPER(el.value) as tipo_formacion
	FROM "caba-piba-raw-zone-db"."portal_empleo_instructions" i
	JOIN  "caba-piba-raw-zone-db"."portal_empleo_institutions" inst ON (i.institution_id=inst.id)
	JOIN  "caba-piba-raw-zone-db"."portal_empleo_academic_formations" af ON (i.id=af.id AND instruction_type LIKE 'ACADEMIC_FORMATION')
	JOIN  "caba-piba-raw-zone-db"."portal_empleo_study_area" sa ON (i.study_area_id=sa.id)
	JOIN "caba-piba-raw-zone-db"."portal_empleo_education_level_status" ls ON (af.education_level_id=ls.id)
	JOIN "caba-piba-raw-zone-db"."portal_empleo_mtr_education_level" el ON (af.education_average_id=el.id)
	),
cl1 AS (
SELECT 
	codigo,
	base_origen,
	fecha_inicio, 
	fecha_fin,
	estado,
	descripcion,
	institucion_educativa,
	cv_id,
	tipo_formacion,
	regexp_replace(regexp_replace(regexp_replace(descripcion,'[^a-zA-Z0-9ÑñÁáÉéÍíÓóÚúüÜäÄòùÇÃÊš\s#\/+\:\|\-\,\(\)\°\&\–\º\ª\_\]\[\@;.]+',''),'ò','ó'),'ù','ú') AS descripcion_clean
FROM datos
),

cl2 AS (
SELECT
	codigo,
	base_origen,
	fecha_inicio, 
	fecha_fin,
	estado,
	descripcion,
	institucion_educativa,
	cv_id,
	tipo_formacion,
	CASE
		WHEN cl1.descripcion LIKE '%.%' THEN regexp_replace(cl1.descripcion_clean,'[^a-zA-Z0-9ÑñÁáÉéÍíÓóÚúüÜäÄòùÇÃÊš\s#\/+\:\|\-\,\(\)\°\&\–\º\ª\_\]\[\@;]+',' ')
		WHEN regexp_like(cl1.descripcion ,'[a-zA-Z0-9ÑñÁáÉéÍíÓóÚúüÜäÄÇÃÊš\s?]+\s?\-\-\s?[a-zA-Z0-9ÑñÁáÉéÍíÓóÚúüÜäÄÇÃÊš\s?]+') THEN regexp_replace(cl1.descripcion_clean,'--','-')
		WHEN cl1.descripcion LIKE '%--%' AND NOT regexp_like(cl1.descripcion ,'[a-zA-Z0-9ÑñÁáÉéÍíÓóÚúüÜäÄÇÃÊš\s?]+\s?\-\-\s?[a-zA-Z0-9ÑñÁáÉéÍíÓóÚúüÜäÄÇÃÊš\s?]+') THEN regexp_replace(cl1.descripcion_clean,'-',' ')
		ELSE cl1.descripcion_clean
	END descripcion_clean
FROM cl1
),

cl3 AS (
SELECT
	codigo,
	base_origen,
	fecha_inicio, 
	fecha_fin,
	estado,
	descripcion,
	institucion_educativa,
	cv_id,
	tipo_formacion,
	CASE
		WHEN cl2.descripcion_clean = '-' THEN regexp_replace(cl2.descripcion_clean,'\-','')
		WHEN cl2.descripcion LIKE '%(' THEN regexp_replace(cl2.descripcion_clean,'\(','')
		WHEN cl2.descripcion LIKE ':%' THEN regexp_replace(cl2.descripcion_clean,'\:','')
		WHEN cl2.descripcion LIKE '%:' THEN regexp_replace(cl2.descripcion_clean,'\:','')
		WHEN cl2.descripcion LIKE '. %' THEN replace(cl2.descripcion_clean,'.','')
		WHEN cl2.descripcion LIKE '- %' THEN regexp_replace(cl2.descripcion_clean,'\-','')
		WHEN cl2.descripcion LIKE '-%' THEN regexp_replace(cl2.descripcion_clean,'\-','')
		WHEN cl2.descripcion LIKE '%-' THEN regexp_replace(cl2.descripcion_clean,'\-','')
		WHEN cl2.descripcion LIKE '%–' THEN regexp_replace(cl2.descripcion_clean,'\–','')
		WHEN cl2.descripcion LIKE '–%' THEN regexp_replace(cl2.descripcion_clean,'\–','')
		WHEN cl2.descripcion LIKE ',%' THEN regexp_replace(cl2.descripcion_clean,'\,','')
		WHEN cl2.descripcion LIKE '%,' THEN regexp_replace(cl2.descripcion_clean,'\,','')
		WHEN cl2.descripcion LIKE '%&' THEN regexp_replace(cl2.descripcion_clean,'\&','')
		WHEN cl2.descripcion LIKE '.:%' THEN regexp_replace(cl2.descripcion_clean,'.:','')
		WHEN cl2.descripcion LIKE '[%' THEN replace(replace(cl2.descripcion_clean,'[',''),']','')
		WHEN cl2.descripcion LIKE '%( %' THEN replace(replace(cl2.descripcion_clean,'( ','('),' )',')')
		WHEN cl2.descripcion LIKE '%.0%' THEN replace(cl2.descripcion_clean,' 0','.0')
		ELSE cl2.descripcion_clean
	END descripcion_clean
FROM cl2
),

cl4 AS (
SELECT
	codigo,
	base_origen,
	fecha_inicio, 
	fecha_fin,
	estado,
	descripcion,
	institucion_educativa,
	cv_id,
	tipo_formacion,
	CASE
		WHEN LTRIM(cl3.descripcion_clean) LIKE '%–%' THEN replace(LTRIM(cl3.descripcion_clean),'–','-')
		WHEN UPPER(cl3.descripcion) LIKE '%.NET%' THEN replace(UPPER(LTRIM(cl3.descripcion_clean)),'NET','.NET')
		WHEN UPPER(cl3.descripcion) LIKE '%ª CATEGORÍA%' THEN regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(UPPER(cl3.descripcion),'1ª|1.ª|1. ª','PRIMERA'),'2ª|2.ª|2. ª','SEGUNDA'),'3ª|3.ª|3. ª','TERCERA'),'4ª|4.ª|4. ª','CUARTA'),'5ª|5.ª|5. ª','QUINTA'),'6ª|6.ª|6. ª','SEXTA'),'7ª|7.ª|7. ª','SEPTIMA'),'8ª|8.ª|8. ª','OCTAVA'),'9ª|9.ª|9. ª','NOVENA'),'10ª|10.ª|10. ª','DECIMA')
		WHEN UPPER(cl3.descripcion) LIKE '%ª' THEN regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(UPPER(cl3.descripcion),'1ª|1.ª|1. ª','PRIMERA'),'2ª|2.ª|2. ª','SEGUNDA'),'3ª|3.ª|3. ª','TERCERA'),'4ª|4.ª|4. ª','CUARTA'),'5ª|5.ª|5. ª','QUINTA'),'6ª|6.ª|6. ª','SEXTA'),'7ª|7.ª|7. ª','SEPTIMA'),'8ª|8.ª|8. ª','OCTAVA'),'9ª|9.ª|9. ª','NOVENA'),'10ª|10.ª|10. ª','DECIMA')
		WHEN UPPER(cl3.descripcion) LIKE '%ª GENERACIÓN%' THEN regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(UPPER(cl3.descripcion),'1ª|1.ª|1. ª','PRIMERA'),'2ª|2.ª|2. ª','SEGUNDA'),'3ª|3.ª|3. ª','TERCERA'),'4ª|4.ª|4. ª','CUARTA'),'5ª|5.ª|5. ª','QUINTA'),'6ª|6.ª|6. ª','SEXTA'),'7ª|7.ª|7. ª','SEPTIMA'),'8ª|8.ª|8. ª','OCTAVA'),'9ª|9.ª|9. ª','NOVENA'),'10ª|10.ª|10. ª','DECIMA')
		WHEN UPPER(cl3.descripcion) LIKE '%ª EDICIÓN%' THEN regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(UPPER(cl3.descripcion),'1ª|1.ª|1. ª','PRIMERA'),'2ª|2.ª|2. ª','SEGUNDA'),'3ª|3.ª|3. ª','TERCERA'),'4ª|4.ª|4. ª','CUARTA'),'5ª|5.ª|5. ª','QUINTA'),'6ª|6.ª|6. ª','SEXTA'),'7ª|7.ª|7. ª','SEPTIMA'),'8ª|8.ª|8. ª','OCTAVA'),'9ª|9.ª|9. ª','NOVENA'),'10ª|10.ª|10. ª','DECIMA')
		ELSE UPPER(LTRIM(cl3.descripcion_clean))
	END descripcion_clean
FROM cl3
)

SELECT *
FROM cl4
ORDER BY cl4.descripcion
--</sql>--