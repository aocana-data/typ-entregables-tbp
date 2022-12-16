-- EDICION CURSADA GOET, MOODLE, SIENFO Y CRMSL
-- 1.- Crear tabla edicion cursada definitiva
-- DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_def_cursada`;
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_def_cursada" AS
SELECT broker_id||'-'||CAST(edicion_capacitacion_id_new AS VARCHAR) id,
       base_origen,
	   edicion_capacitacion_id_old,
	   edicion_capacitacion_id_new,
	   identificacion_alumno identificacion_alumno_old,
       documento_broker,
       fecha_preinscripcion,
       fecha_inicio,
       fecha_abandono,
       fecha_egreso,
	   porcentaje_asistencia,
       estado,
       cant_aprobadas,
       vecino_id,
       broker_id,
	   estado_beneficiario
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_cursada"
WHERE 
edicion_capacitacion_id_new IS NOT NULL
AND
(broker_id||'-'||CAST(edicion_capacitacion_id_new AS VARCHAR)) IS NOT NULL
