-- 1.-- Crear tabla def de organizaciones
-- DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_def_organizaciones`;
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_def_organizaciones" AS
WITH org AS (
SELECT
cuit,
razon_social_old,
razon_social_new,
estado,
ente_gubernamental
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_organizaciones"
UNION
SELECT
CAST(NULL AS VARCHAR) AS cuit,
empresa_limpia,
CAST(NULL AS VARCHAR) AS razon_social_new,
CAST(NULL AS VARCHAR) AS estado,
CASE
    WHEN regexp_like(empresa_limpia ,'MINISTERIO|GOBIERNO|SUBSECRETARÍA|TSJ|GOB|AFIP|RENAPER|INTA|AGIP|PODER JUDICIAL|BANCO CENTRAL|ARBA') THEN 1
    ELSE 0
END ente_gubernamental
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_cv_experiencia_laboral"
WHERE empresa_limpia NOT IN (SELECT empresa FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_cv_experiencia_laboral_organizacion")
OR  empresa_limpia NOT IN (SELECT razon_social FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_cv_experiencia_laboral_organizacion" )
GROUP BY 1,2,3,4,5
ORDER BY 1
)
SELECT
row_number() OVER () AS id_organizacion,
org.*
FROM org