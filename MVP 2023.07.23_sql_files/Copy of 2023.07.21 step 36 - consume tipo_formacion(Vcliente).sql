-- Crear tabla def de tipo de formacion
-- --<sql>--DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_def_tipo_formacion`;--</sql>--
--<sql>--
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_def_tipo_formacion" AS
SELECT id,
	UPPER(value) AS descripcion
FROM "caba-piba-raw-zone-db"."portal_empleo_mtr_education_level"
ORDER BY value
--</sql>--