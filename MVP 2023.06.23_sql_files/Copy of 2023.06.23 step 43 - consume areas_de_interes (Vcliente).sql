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
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_def_areas_de_interes" AS
WITH 
areas_de_interes AS (
-- AREAS DE INTERES DE LA OFERTAS LABORALES, POR EJEMPLO: 'Informática / IT / Sistemas', 'Atención al Cliente', 'Gastronomia'
SELECT row_number() OVER () AS areas_de_interes_id, id AS id_old, descripcion AS descripcion FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_areas_de_interes")
SELECT * FROM areas_de_interes
--</sql>--


--<sql>--
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_def_oportunidad_laboral_areas_de_interes" AS
WITH 
oportunidad_laboral_areas_de_interes AS (
-- AREAS DE INTERES DE LA OFERTAS LABORALES, POR EJEMPLO: 'Informática / IT / Sistemas', 'Atención al Cliente', 'Gastronomia'
SELECT row_number() OVER () AS oportunidad_laboral_areas_de_interes_id, ol.oportunidad_laboral_id, ai.areas_de_interes_id FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_oportunidad_laboral_areas_de_interes" olai
JOIN "caba-piba-staging-zone-db"."tbp_typ_def_areas_de_interes" ai ON (olai.areas_de_interes_id=ai.id_old)
JOIN "caba-piba-staging-zone-db"."tbp_typ_def_oportunidad_laboral" ol ON (ol.oportunidad_laboral_id_old=olai.oportunidad_laboral_id))
SELECT * FROM oportunidad_laboral_areas_de_interes
--</sql>--

--<sql>--
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_def_curriculum_areas_de_interes" AS
WITH 
curriculum_areas_de_interes AS (
-- AREAS DE INTERES DE LA OFERTAS LABORALES, POR EJEMPLO: 'Informática / IT / Sistemas', 'Atención al Cliente', 'Gastronomia'
SELECT row_number() OVER () AS curriculum_areas_de_interes_id, cv.id AS curriculum_id, ai.areas_de_interes_id FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_curriculum_areas_de_interes" cvai
JOIN "caba-piba-staging-zone-db"."tbp_typ_def_areas_de_interes" ai ON (cvai.areas_de_interes_id=ai.id_old)
JOIN "caba-piba-staging-zone-db"."tbp_typ_def_curriculum" cv ON (cv.id_old=cvai.curriculum_id))
SELECT * FROM curriculum_areas_de_interes
--</sql>--