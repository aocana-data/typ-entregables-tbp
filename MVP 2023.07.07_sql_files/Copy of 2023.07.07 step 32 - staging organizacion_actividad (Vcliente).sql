-- 1.-- Crear tabla tmp de organizacion_actividades
-- --<sql>--DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_tmp_organizacion_actividad`;--</sql>--
--<sql>--
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_organizacion_actividad" AS
--Se agrega la data de codigo y descripcion de actividades al universo de organizaciones
WITH c AS (
SELECT
org.cuit,
ab.codigo_de_actividad,
ab.descripcion_actividad
FROM "caba-piba-staging-zone-db"."tbp_typ_def_organizaciones" org
JOIN "caba-piba-staging-zone-db"."tbp_typ_tmp_registro_laboral_formal_afip_agip_ab" ab
ON (org.cuit = ab.cuit_del_empleador)
WHERE ab.descripcion_actividad IS NOT NULL
GROUP BY 1,2,3
)
SELECT *
FROM c
--</sql>--