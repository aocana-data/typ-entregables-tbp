-- 1.-- Crear la tabla definitiva de REGISTRO LABORAL FORMAL
-- DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_def_registro_laboral_formal`;
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_def_registro_laboral_formal" AS
SELECT 
	row_number() OVER () AS registro_laboral_formal_id,
	lf.id_candidato AS registro_laboral_formal_id_old,
	lf.descripcion_de_puesto_desempeniado AS cargo,
	lf.base_origen,
	ol.id_oportunidad_laboral,
	lf.vecino_id, 
	lf.cuit_del_empleador, 
	lf.fecha_inicio_de_relacion_laboral AS fecha_inicio,
	lf.fecha_fin_de_relacion_laboral AS fecha_fin,
	lf.modalidad_contratacion AS modalidad_de_trabajo,
	lf.remuneracion_bruta AS remuneracion_moneda_corriente,
	CAST(NULL AS DECIMAL) AS remuneracion_moneda_constante
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_registro_laboral_formal"  lf
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_oportunidad_laboral"  ol 
	ON (lf.base_origen=ol.base_origen AND lf.oportunidad_laboral_id_old=ol.oportunidad_laboral_id_old)
GROUP BY 2,3,4,5,6,7,8,9,10,11,12