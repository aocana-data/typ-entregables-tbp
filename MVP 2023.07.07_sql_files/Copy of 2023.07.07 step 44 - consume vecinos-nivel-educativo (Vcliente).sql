-- 1.- Crear tabla tmp_vecinos con nivel educativo
--<sql>--DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_tmp_vecino_ne`;--</sql>--
--<sql>--
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_vecino_ne" AS
WITH nivel_educativo_ponderado as 
(
SELECT c.*,
		CASE
			UPPER(c.nivel_educativo)
			WHEN 'MAESTRÍA' THEN CASE
				UPPER(c.nivel_educativo_estado)
				WHEN 'GRADUADO' THEN 2
				WHEN 'EN CURSO' THEN 6
				WHEN 'ABANDONADO' THEN 10
			END
			WHEN 'DOCTORADO' THEN CASE
				UPPER(c.nivel_educativo_estado)
				WHEN 'GRADUADO' THEN 1
				WHEN 'EN CURSO' THEN 5
				WHEN 'ABANDONADO' THEN 9
			END
			WHEN 'POSTGRADO' THEN CASE
				UPPER(c.nivel_educativo_estado)
				WHEN 'GRADUADO' THEN 3
				WHEN 'EN CURSO' THEN 7
				WHEN 'ABANDONADO' THEN 11
			END
			WHEN 'UNIVERSITARIO' THEN CASE
				UPPER(c.nivel_educativo_estado)
				WHEN 'GRADUADO' THEN 4
				WHEN 'EN CURSO' THEN 8
				WHEN 'ABANDONADO' THEN 12
			END
			WHEN 'TERCIARIO' THEN CASE
				UPPER(c.nivel_educativo_estado)
				WHEN 'GRADUADO' THEN 13
				WHEN 'EN CURSO' THEN 14
				WHEN 'ABANDONADO' THEN 15
			END
			WHEN 'SECUNDARIO' THEN CASE
				UPPER(c.nivel_educativo_estado)
				WHEN 'GRADUADO' THEN 16
				WHEN 'EN CURSO' THEN 17
				WHEN 'ABANDONADO' THEN 18
			END
			WHEN 'PRIMARIO' THEN CASE
				UPPER(c.nivel_educativo_estado)
				WHEN 'GRADUADO' THEN 19
				WHEN 'EN CURSO' THEN 20
				WHEN 'ABANDONADO' THEN 21
			END
			WHEN 'OTRO' THEN CASE
				UPPER(c.nivel_educativo_estado)
				WHEN 'GRADUADO' THEN 22
				WHEN 'EN CURSO' THEN 23
				WHEN 'ABANDONADO' THEN 24
			END
				WHEN 'OTROS' THEN CASE
				UPPER(c.nivel_educativo_estado)
				WHEN 'GRADUADO' THEN 22
				WHEN 'EN CURSO' THEN 23
				WHEN 'ABANDONADO' THEN 24
			END
		END AS prioridad_nivel_educativo
	FROM "caba-piba-staging-zone-db"."tbp_typ_def_curriculum" c
),
nivel_educactivo_pe_ordenado AS (
	SELECT *,
		ROW_NUMBER() OVER(
			PARTITION BY id_vecino
			ORDER BY prioridad_nivel_educativo ASC
		) AS "orden_duplicado"
	FROM nivel_educativo_ponderado
)
SELECT v.*,
c.nivel_educativo,
c.nivel_educativo_estado
FROM "caba-piba-staging-zone-db"."tbp_typ_def_vecino" v
LEFT JOIN nivel_educactivo_pe_ordenado c ON (v.id_vecino=c.id_vecino AND c.orden_duplicado=1)
--</sql>--

-- 2- Eliminar y recrear tabla 
-- 1.- Crear tabla tbp_typ_def_vecino con nivel educativo
--<sql>--DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_def_vecino_ne`;--</sql>--
--<sql>--
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_def_vecino_ne" AS
SELECT * FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_vecino_ne"
--</sql>--