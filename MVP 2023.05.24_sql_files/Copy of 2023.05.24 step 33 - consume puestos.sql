-- Crear tabla def de puestos
-- --<sql>--DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_def_puestos`;--</sql>--
--<sql>--
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_def_puestos" AS
SELECT 
	codigo_de_puesto_desempeniado as codigo, 
	UPPER(descripcion_de_puesto_desempeniado) AS puesto
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_registro_laboral_formal" 
WHERE 
	descripcion_de_puesto_desempeniado IS NOT NULL
	AND LENGTH(TRIM(descripcion_de_puesto_desempeniado))>0
GROUP BY 
	descripcion_de_puesto_desempeniado, 
	codigo_de_puesto_desempeniado
ORDER BY 
	descripcion_de_puesto_desempeniado
--</sql>--	