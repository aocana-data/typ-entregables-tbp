-- 1.-- Crear la tabla definitiva de REGISTRO LABORAL FORMAL
--<sql>--
DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_def_registro_laboral_formal`;
--</sql>--

--<sql>--
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_def_registro_laboral_formal" AS
SELECT 
	MAX(registro_laboral_formal_id) AS id_registro_laboral,
	id_puesto,
	id_broker,
	cuit_del_empleador,
	fecha_inicio,
	fecha_fin,
	modalidad_de_trabajo,
	remuneracion_moneda_corriente,
	remuneracion_moneda_constante
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_registro_laboral_formal_completa"
GROUP BY 
	id_puesto,
	id_broker,
	cuit_del_empleador,
	fecha_inicio,
	fecha_fin,
	modalidad_de_trabajo,
	remuneracion_moneda_corriente,
	remuneracion_moneda_constante
--</sql>--
	
-- 2.-- Crear la tabla definitiva N-N de OPORTUNIDAD LABORAL - REGISTRO LABORAL FORMAL 

--<sql>--
DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_def_oportunidad_laboral_registro_laboral_formal`;
--</sql>--

--<sql>--
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_def_oportunidad_laboral_registro_laboral_formal" AS
SELECT 
	lfc.registro_laboral_formal_id AS id_registro_laboral,
	lfc.registro_laboral_formal_id_old AS id_old, 
	lfc.base_origen AS base_origen_registro_laboral_formal,
	ol.id_oportunidad_laboral
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_registro_laboral_formal_completa" lfc
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_oportunidad_laboral"  ol 
		ON (lfc.base_origen=ol.base_origen AND lfc.oportunidad_laboral_id_old=ol.id_old)
JOIN "caba-piba-staging-zone-db"."tbp_typ_def_registro_laboral_formal" r ON (r.id_registro_laboral=lfc.registro_laboral_formal_id)
WHERE lfc.base_origen IS NOT NULL AND ol.id_oportunidad_laboral IS NOT NULL
GROUP BY 
	lfc.registro_laboral_formal_id,
	lfc.registro_laboral_formal_id_old, 
	lfc.base_origen,
	ol.id_oportunidad_laboral
--</sql>--