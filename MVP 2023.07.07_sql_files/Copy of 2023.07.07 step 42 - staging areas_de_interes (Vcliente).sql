-- 1.-- Crear la tabla tmp AREAS_DE_INTERES PORTALEMPLEO, 
-- ID
-- Descripción

-- 1.-- Crear la tabla tmp OPORTUNIDAD_LABORAL_AREAS_DE_INTERES PORTALEMPLEO, 
-- Oportunidad Laboral ID
-- Areas de Interes ID

-- 3.-- Crear la tabla tmp CURRICULUM_AREAS_DE_INTERES PORTALEMPLEO, 
-- Curriculum ID
-- Areas de Interes ID


--<sql>--
DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_tmp_areas_de_interes`;
--</sql>--

--<sql>--
DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_tmp_oportunidad_laboral_areas_de_interes`;
--</sql>--

--<sql>--
DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_tmp_curriculum_areas_de_interes`;
--</sql>--


--<sql>--
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_areas_de_interes" AS
WITH 
areas_de_interes AS (
-- Se obtienen las áreas de interés a partir de la tabla "portal_empleo_mtr_interest_areas"
-- AREAS DE INTERES DE LA OFERTAS LABORALES, POR EJEMPLO: 'Informática / IT / Sistemas', 'Atención al Cliente', 'Gastronomia'
SELECT CAST(ia.id AS VARCHAR) id, CAST(ia.name AS VARCHAR) descripcion FROM "caba-piba-raw-zone-db"."portal_empleo_mtr_interest_areas" ia
)
SELECT * FROM areas_de_interes
--</sql>--

--<sql>--
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_oportunidad_laboral_areas_de_interes" AS
WITH 
oportunidad_laboral_areas_de_interes AS (
-- Se unen dos tablas relacionadas, "portal_empleo_mtr_interest_areas" y "portal_empleo_job_offers", para  
-- obtener las áreas de interés de una oferta laboral en particular. El resultado está limitado a 10 registros.
-- AREAS DE INTERES DE LA OFERTAS LABORALES, POR EJEMPLO: 'Informática / IT / Sistemas', 'Atención al Cliente', 'Gastronomia'
SELECT CAST(jo.id AS VARCHAR) oportunidad_laboral_id, CAST(ia.id AS VARCHAR) areas_de_interes_id FROM "caba-piba-raw-zone-db"."portal_empleo_mtr_interest_areas" ia
JOIN "caba-piba-raw-zone-db"."portal_empleo_job_offers" jo
ON (ia.id=jo.area_id)
)
SELECT * FROM oportunidad_laboral_areas_de_interes
--</sql>--


--<sql>--
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_curriculum_areas_de_interes" AS
WITH 
curriculum_areas_de_interes AS (
-- Se unen varias tablas, incluyendo "portal_empleo_mtr_interest_areas", "portal_empleo_preferences_area", 
-- "portal_empleo_candidate_preferences", "portal_empleo_candidates" y portal_empleo_curriculum_vitaes, para obtener las áreas de interés de un postulante.
-- AREAS DE INTERES DEL POSTULANTE, POR EJEMPLO 'Administración', 'Atención al Cliente', 'Asistente', 'Caja', 'Almacén / Depósito / Expedición'
SELECT CAST(cv.id AS VARCHAR) curriculum_id, CAST(ia.id AS VARCHAR) areas_de_interes_id FROM "caba-piba-raw-zone-db"."portal_empleo_mtr_interest_areas" ia
JOIN "caba-piba-raw-zone-db"."portal_empleo_preferences_area" pa ON (ia.id=pa.area_id)
JOIN "caba-piba-raw-zone-db"."portal_empleo_candidate_preferences" cp ON (cp.id=pa.candidate_preference_id)
JOIN "caba-piba-raw-zone-db"."portal_empleo_candidates" ca ON (ca.id=cp.candidate_id)
JOIN "caba-piba-raw-zone-db"."portal_empleo_curriculum_vitaes" cv ON (cv.candidate_id=ca.id))
SELECT * FROM curriculum_areas_de_interes
--</sql>--