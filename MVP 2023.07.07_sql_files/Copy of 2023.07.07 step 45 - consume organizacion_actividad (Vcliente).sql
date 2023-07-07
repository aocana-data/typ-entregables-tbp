-- 1.-- Crear tabla def de organizacion_actividades
-- --<sql>--DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_def_organizacion_actividad`;--</sql>--
--<sql>--
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_def_organizacion_actividad" AS
--Se agrega la data de codigo y descripcion de actividades al universo de organizaciones
WITH org_act AS (
SELECT row_number() OVER () AS id,
cuit,
codigo_de_actividad,
descripcion_actividad
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_organizacion_actividad"
)
SELECT *
FROM org_act
--</sql>--