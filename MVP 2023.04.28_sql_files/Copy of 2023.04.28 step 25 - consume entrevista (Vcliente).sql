-- 1.-- Crear la tabla def de entrevista
-- DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_def_entrevista`;
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_def_entrevista" AS
SELECT
row_number() OVER () AS id_entrevista,
vecino_id,
oportunidad_laboral_id,
tipo_entrevista,
fecha_notificacion,
fecha_entrevista,
consiguio_trabajo,
estado_entrevista
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_entrevista"