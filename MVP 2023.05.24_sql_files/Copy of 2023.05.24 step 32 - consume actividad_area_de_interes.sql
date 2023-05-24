-- 1.-- Crear tabla def de actividad/area de interes
-- --<sql>--DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_def_actividad_area_de_interes`;--</sql>--
--<sql>--
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_def_actividad_area_de_interes" AS
--Se realiza un cruce entre la tabla de organizacion_actividades y la tabla match sp y se para obtener el código de sector productivo correspondiente a la actividad clae
WITH c1 AS (
SELECT
org.codigo_de_actividad,
org.descripcion_actividad,
m.codigo_clae,
m.codigo_sector_productivo
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_organizacion_actividad" org
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_match_sector_estrategico_sector_productivo" m ON (org.codigo_de_actividad = TRY_CAST(m.codigo_clae AS INT))
),
--Como se detectan casos de codigo de actividad correspondientes a AGIP (que utiliza subcategorias del CLAE) que no matchean con la tabla match sp y se, entonces se realiza un cruce con la tabla del nomeclador para obtener los codigos CLAE AFIP correspondientes a los codigos SUB CLAE de AGIP y así poder matchear mas casos
c3 AS (
SELECT
c1.codigo_de_actividad,
ae.cod_actividad_agip_o_naecba AS codigo_agip,
c1.descripcion_actividad,
CASE
    WHEN TRY_CAST(c1.codigo_clae AS INT) IS NULL THEN TRY_CAST(ae.cod_actividad_afip_o_naes AS INT)
    ELSE  TRY_CAST(c1.codigo_clae AS INT) 
END codigo_clae_clean,
c1.codigo_clae,
c1.codigo_sector_productivo
FROM c1
LEFT JOIN "caba-piba-raw-zone-db"."tbp_typ_tmp_nomenclador_actividades_economicas" ae ON (c1.codigo_de_actividad = TRY_CAST(ae.cod_actividad_afip_o_naes AS INT) OR c1.codigo_de_actividad = TRY_CAST(ae.cod_actividad_agip_o_naecba AS INT))
),
c4 AS (
SELECT
CASE
    WHEN c3.codigo_clae_clean IS NULL THEN c3.codigo_de_actividad
    ELSE c3.codigo_clae_clean
END codigo_de_actividad,
c3.descripcion_actividad,
m1.codigo_sector_productivo
FROM c3
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_match_sector_estrategico_sector_productivo" m1 ON (c3.codigo_clae_clean = TRY_CAST(m1.codigo_clae AS INT))
GROUP BY 1,2,3
),
c5 AS (
SELECT
c4.codigo_de_actividad,
c4.descripcion_actividad,
CASE
    WHEN c4.codigo_sector_productivo IS NULL THEN m1.codigo_sector_productivo
    ELSE c4.codigo_sector_productivo
END codigo_sector_productivo,
m1.sector_productivo
FROM c4
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_match_sector_estrategico_sector_productivo" m1 ON (SUBSTRING(CAST(c4.codigo_de_actividad AS VARCHAR),1,4) = REPLACE(m1.codigo_4_digitos_clanae,'.',''))
)
SELECT
c5.codigo_de_actividad,
c5.descripcion_actividad,
c5.codigo_sector_productivo
FROM c5
WHERE c5.codigo_sector_productivo IS NOT NULL
GROUP BY 1,2,3
--</sql>--