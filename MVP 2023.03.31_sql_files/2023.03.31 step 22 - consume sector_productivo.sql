-- 1.-- Crear la tabla def de Sector_Productivo
-- DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_def_sector_productivo`;
-- CAMPOS REQUERIDOS EN TABLA DEF SEGUN MODELO
-- Código (1+)
-- Sector Productivo => ABASTECIMIENTO Y LOGISTICA, ADMINISTRACION, CONTABILIDAD Y FINANZAS, COMERCIAL, VENTAS Y NEGOCIOS, GASTRONOMIA, HOTELERIA Y TURISMO, HIPODROMO, OFICIOS Y OTROS, PRODUCCION Y MANUFACTURA, SALUD, MEDICINA Y FARMACIA, SECTOR PUBLICO
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_def_sector_productivo" AS
WITH sp AS (
SELECT sector_productivo
FROM "caba-piba-staging-zone-db".tbp_typ_tmp_oportunidad_laboral
GROUP BY sector_productivo
ORDER BY sector_productivo
)
SELECT
row_number() OVER () AS id_sector_productivo,
REPLACE(REPLACE(sp.sector_productivo,'Í','I'),'Ó','O') AS sector_productivo
FROM sp
WHERE sp.sector_productivo NOT LIKE ''
GROUP BY REPLACE(REPLACE(sp.sector_productivo,'Í','I'),'Ó','O')
ORDER BY id_sector_productivo
