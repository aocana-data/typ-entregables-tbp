-- CURSADA GOET, MOODLE, SIENFO, CRMSL Y SIU
-- 1.- Crear tabla cursada definitiva
-- DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_def_cursada`;
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_def_cursada" AS
SELECT row_number() OVER () AS id,
       base_origen,
	   edicion_capacitacion_id_old,
	   edicion_capacitacion_id_new,
	   capacitacion_id_new,
	   identificacion_alumno identificacion_alumno_old,
       documento_broker,
	   -- si hay mas de una fecha de preinscripcion se toma la menor
       MIN(fecha_preinscripcion) fecha_preinscripcion,
	   -- si hay mas de una fecha de inicio se toma la menor
	   MIN(fecha_inicio) fecha_inicio,
	   -- si hay mas de una fecha de abandono se toma la menor
	   MIN(fecha_abandono) fecha_abandono,
	   -- si hay mas de una fecha de egreso se toma la menor
	   MIN(fecha_egreso) fecha_egreso,
	   porcentaje_asistencia,
	   -- si hay de un registro de cantidad de aprobados, porque tiene mas de un registro de inscripcion, se toma el numero mayor
       MAX(cant_aprobadas) cant_aprobadas,
       vecino_id,
       broker_id,
	   CASE 
		WHEN estado_beneficiario LIKE 'FINALIZADO' THEN 'FINALIZO_CURSADA'
		WHEN estado_beneficiario LIKE 'APROBADO' THEN 'EGRESADO'
		WHEN estado_beneficiario LIKE 'NO_APLICA' THEN 'PREINSCRIPTO'
		WHEN estado_beneficiario LIKE 'BAJA' THEN 'BAJA'
		WHEN estado_beneficiario LIKE 'REGULAR' THEN 'EN_CURSO'
		ELSE estado_beneficiario
		END estado_beneficiario
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_cursada"
WHERE 
vecino_id IS NOT NULL 
AND broker_id IS NOT NULL 
AND capacitacion_id_new IS NOT NULL 
AND TRIM(documento_broker) != ''
AND 
(edicion_capacitacion_id_new IS NOT NULL
OR
-- puede no existir edicion_capacitacion_id_new cuando el estado_beneficiario es 'PREINSCRIPTO'
(estado_beneficiario LIKE 'PREINSCRIPTO' AND base_origen IN ('SIU', 'GOET'))
)
GROUP BY 
	base_origen,
	edicion_capacitacion_id_old,
	edicion_capacitacion_id_new,
	capacitacion_id_new,
	identificacion_alumno,
	documento_broker,
	porcentaje_asistencia,
	vecino_id,
	broker_id,
	estado_beneficiario
