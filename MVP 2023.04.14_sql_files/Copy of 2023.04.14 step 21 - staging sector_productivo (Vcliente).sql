-- 1.-- Crear tabla tmp de Sector_Productivo
-- DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_tmp_sector_productivo`;
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_sector_productivo" AS
WITH sp AS (
SELECT sector_productivo
FROM "caba-piba-staging-zone-db".tbp_typ_tmp_oportunidad_laboral
GROUP BY sector_productivo
)
SELECT
REPLACE(REPLACE(sp.sector_productivo,'Í','I'),'Ó','O') AS sector_productivo
FROM sp
WHERE sp.sector_productivo NOT LIKE ''
GROUP BY REPLACE(REPLACE(sp.sector_productivo,'Í','I'),'Ó','O')
ORDER BY REPLACE(REPLACE(sp.sector_productivo,'Í','I'),'Ó','O')



