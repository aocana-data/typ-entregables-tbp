-- 1.- Crear tabla capacitacion
-- --<sql>--DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_tmp_capacitacion_asi`;--</sql>--
--<sql>--
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_capacitacion_asi" AS
SELECT c.id capacitacion_id,
       c.codigo codigo_capacitacion,
       c.nombre descrip_capacitacion,
       c.tipo_formacion_origen_id tipo_formacion,
	   c.programa_id,
	   p.base_origen,
       tf.nombre descrip_tipo_formacion,
	   c.modalidad_id,
	   m.nombre descrip_modalidad,
	   c.categoria_back_id,
	   cb.nombre descrip_back,
	   CASE WHEN cb.tipo = 'sector_estrategico_asociado' THEN 'SECTOR PRODUCTIVO'
		    ELSE UPPER(cb.tipo) END tipo_capacitacion,
	   c.categoria_front_id,
	   cf.nombre descrip_front,
	   c.detalle detalle_capacitacion,
	   c.estado estado_capacitacion,
       cd.seguimiento_personalizado,
       cd.soporte_online,
       cd.incentivos_terminalidad,
	   cd.exclusividad_participantes,
	   cd.otorga_certificado,
	   cd.filtro_ingreso,
	   cd.frecuencia_oferta_anual,
       c.duracion_cantidad,
       c.duracion_medida,
       c.duracion_dias,
       cd.duracion_hs_reloj,
	   cd.vacantes
FROM "caba-piba-raw-zone-db"."api_asi_capacitacion" c
INNER JOIN "caba-piba-raw-zone-db"."api_asi_tipo_formacion_origen" tf
ON (tf.id = c.tipo_formacion_origen_id)
LEFT JOIN "caba-piba-raw-zone-db"."api_asi_capacitacion_data_lake" cd
ON (cd.id = c.id)
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_programa" p ON (p.programa_id = c.programa_id) 
LEFT JOIN "caba-piba-raw-zone-db"."api_asi_categoria_back" cb ON (cb.id = c.categoria_back_id)
LEFT JOIN "caba-piba-raw-zone-db"."api_asi_categoria_front" cf ON (cf.id = c.categoria_front_id)
LEFT JOIN "caba-piba-raw-zone-db"."api_asi_modalidad" m ON (m.id = c.modalidad_id)
--</sql>--
-- 2.-  Crear tabla aptitudes para cada capacitacion
-- --<sql>--DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_tmp_aptitudes_asi`;--</sql>--
--<sql>--
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_aptitudes_asi" AS
SELECT c.id capacitacion_id,
       c.codigo codigo_capacitacion,
       ac.aptitud_id,
       a.nombre descrip_aptitud
FROM "caba-piba-raw-zone-db"."api_asi_capacitacion" c
INNER JOIN "caba-piba-raw-zone-db"."api_asi_aptitud_capacitacion" ac
ON (ac.capacitacion_id = c.id)
INNER JOIN "caba-piba-raw-zone-db"."api_asi_aptitud" a
ON (ac.aptitud_id = a.id)
--</sql>--