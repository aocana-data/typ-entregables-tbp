-- 1.-- Crear tabla def de Sector Estrategico
-- --<sql>--DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_def_sector_estrategico`;--</sql>--
--<sql>--
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_def_sector_estrategico" AS
WITH c AS (
SELECT
id AS id_sector_estrategico,
UPPER(nombre) AS sector_estrategico,
tipo
FROM "caba-piba-raw-zone-db"."api_asi_categoria_back"
WHERE tipo IN ('sector_estrategico_asociado')
GROUP BY 1,2,3
)
SELECT
id_sector_estrategico,
REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(sector_estrategico,'"',''),'Á','A'),'É','E'),'Í','I'),'Ó','O'),'Ú','U') AS sector_estrategico
FROM c
ORDER BY 2
--</sql>--