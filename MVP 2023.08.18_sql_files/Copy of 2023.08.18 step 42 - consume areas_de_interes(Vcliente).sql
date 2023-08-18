-- 1.-- Crear la tabla def AREAS_DE_INTERES PORTALEMPLEO, 
-- Código (1+)
-- Descripción

-- 1.-- Crear la tabla def OPORTUNIDAD_LABORAL_AREAS_DE_INTERES PORTALEMPLEO, 
-- Oportunidad Laboral ID
-- Areas de Interes ID

-- 3.-- Crear la tabla def CURRICULUM_AREAS_DE_INTERES PORTALEMPLEO, 
-- Curriculum ID
-- Areas de Interes ID


--<sql>--
DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_def_areas_de_interes`;
--</sql>--

--<sql>--
DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_def_oportunidad_laboral_areas_de_interes`;
--</sql>--

--<sql>--
DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_def_curriculum_areas_de_interes`;
--</sql>--


--<sql>--
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_def_areas_de_interes" AS WITH areas_de_interes AS (
	-- AREAS DE INTERES DE LA OFERTAS LABORALES, POR EJEMPLO: 'Informática / IT / Sistemas', 'Atención al Cliente', 'Gastronomia'
	SELECT row_number() OVER () AS id_areas_de_interes,
		id AS id_old,
		base_origen,
		descripcion AS descripcion
	FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_areas_de_interes"
)
SELECT *
FROM areas_de_interes
--</sql>--


--<sql>--
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_def_oportunidad_laboral_areas_de_interes" AS WITH oportunidad_laboral_areas_de_interes AS (
	-- AREAS DE INTERES DE LA OFERTAS LABORALES, POR EJEMPLO: 'Informática / IT / Sistemas', 'Atención al Cliente', 'Gastronomia'
	SELECT row_number() OVER () AS id_oportunidad_laboral_areas_de_interes,
		ol.id_oportunidad_laboral,
		ai.id_areas_de_interes
	FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_oportunidad_laboral_areas_de_interes" olai
		JOIN "caba-piba-staging-zone-db"."tbp_typ_def_areas_de_interes" ai ON (olai.id_areas_de_interes = ai.id_old AND olai.base_origen = ai.base_origen)
		JOIN "caba-piba-staging-zone-db"."tbp_typ_def_oportunidad_laboral" ol ON (ol.id_old = olai.id_oportunidad_laboral AND ol.base_origen = ai.base_origen)

	GROUP BY
		ol.id_oportunidad_laboral,
		ai.id_areas_de_interes

)
SELECT *
FROM oportunidad_laboral_areas_de_interes
--</sql>--

--<sql>--
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_def_curriculum_areas_de_interes" AS 
WITH curriculum_areas_de_interes AS (
	-- AREAS DE INTERES DE LA OFERTAS LABORALES, POR EJEMPLO: 'Informática / IT / Sistemas', 'Atención al Cliente', 'Gastronomia'
	SELECT row_number() OVER () AS id_curriculum_areas_de_interes,
		cv.id_curriculum,
		ai.id_areas_de_interes
	FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_curriculum_areas_de_interes" cvai
		JOIN "caba-piba-staging-zone-db"."tbp_typ_def_areas_de_interes" ai ON (cvai.id_areas_de_interes = ai.id_old AND cvai.base_origen = ai.base_origen)
		JOIN "caba-piba-staging-zone-db"."tbp_typ_def_curriculum" cv ON (cv.id_old = cvai.id_curriculum AND cv.base_origen = ai.base_origen)
	GROUP BY 
		cv.id_curriculum,
		ai.id_areas_de_interes
)
SELECT *
FROM curriculum_areas_de_interes

--</sql>--