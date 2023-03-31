-- EDICION CAPACITACION GOET, MOODLE, SIENFO Y CRMSL
-- 1.- Crear tabla edicion capacitacion definitiva
-- DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_def_edicion_capacitacion`;
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_def_edicion_capacitacion" AS
SELECT row_number() OVER () AS id,
       ed.base_origen,
	   ed.tipo_capacitacion,
       ed.capacitacion_id_new,
       ed.capacitacion_id_old,
       ed.edicion_capacitacion_id edicion_capacitacion_id_old,
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
	   ed.modalidad_id,
       ed.descrip_modalidad,
       ed.cod_origen_establecimiento
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_edicion_capacitacion" ed
-- en tmp hay datos provenientes de LEFT JOIN para verificar consistencia de datos
-- los mismos se quitan de la tabla def
WHERE ed.edicion_capacitacion_id IS NOT NULL 
AND ed.capacitacion_id_new IS NOT NULL 
