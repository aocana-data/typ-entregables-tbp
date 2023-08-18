-- EDICION CAPACITACION GOET, MOODLE, SIENFO Y CRMSL
-- 1.- Crear tabla edicion capacitacion definitiva
-- --<sql>--DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_def_edicion_capacitacion`;--</sql>--
--<sql>--
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_def_edicion_capacitacion" AS
SELECT row_number() OVER () AS id_edicion_capacitacion,
		ed.base_origen,
		ed.tipo_capacitacion,
		cap.id_capacitacion,
		ed.capacitacion_id_new AS id_capacitacion_new,
		ed.capacitacion_id_old AS id_capacitacion_old,
		ed.edicion_capacitacion_id AS id_edicion_capacitacion_old,
		ed.anio_inicio, 
		ed.semestre_inicio,
		ed.fecha_inicio_dictado, 
		ed.fecha_fin_dictado,
		ed.fecha_inicio_inscripcion,
		ed.fecha_limite_inscripcion,
		ed.turno,
		ed.dias_cursada,
		ed.inscripcion_abierta,
		ed.activo,
		ed.cant_inscriptos,
		ed.vacantes,
		ed.id_modalidad,
		ed.descrip_modalidad,
		ed.id_establecimiento
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_edicion_capacitacion" ed
-- en tmp hay datos provenientes de LEFT JOIN para verificar consistencia de datos
-- los mismos se quitan de la tabla def
JOIN "caba-piba-staging-zone-db"."tbp_typ_def_capacitacion" cap ON (ed.capacitacion_id_new = cap.id_new AND ed.base_origen = cap.base_origen)
WHERE ed.edicion_capacitacion_id IS NOT NULL 
AND ed.capacitacion_id_new IS NOT NULL
--</sql>--