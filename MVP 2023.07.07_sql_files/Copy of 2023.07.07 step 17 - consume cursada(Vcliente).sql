-- CURSADA GOET, MOODLE, SIENFO, CRMSL Y SIU
-- 1.- Crear tabla cursada definitiva
-- --<sql>--DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_def_cursada`;--</sql>--
--<sql>--
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_def_cursada" AS
SELECT row_number() OVER () AS id,
       cur.base_origen,
	   id_edicion_capacitacion_old AS id_edicion_capacitacion_old,
	   edicion_capacitacion_id_new AS id_edicion_capacitacion_new,
	   cap.id AS capacitacion_id,
	   id_capacitacion_new AS id_capacitacion_new,
	   identificacion_alumno identificacion_alumno_old,
       documento_broker,
	   -- si hay mas de una fecha de preinscripcion se toma la menor
       MIN(cur.fecha_preinscripcion) fecha_preinscripcion,
	   -- si hay mas de una fecha de inicio se toma la menor
	   MIN(cur.fecha_inicio) fecha_inicio,
	   -- si hay mas de una fecha de abandono se toma la menor
	   MIN(cur.fecha_abandono) fecha_abandono,
	   -- si hay mas de una fecha de egreso se toma la menor
	   MIN(cur.fecha_egreso) fecha_egreso,
	   porcentaje_asistencia,
	   -- si hay de un registro de cantidad de aprobados, porque tiene mas de un registro de inscripcion, se toma el numero mayor
       MAX(cur.cant_aprobadas) cant_aprobadas,
       id_vecino ,
       id_broker,
	   CASE 
		WHEN estado_beneficiario LIKE 'FINALIZADO' THEN 'FINALIZO_CURSADA'
		WHEN estado_beneficiario LIKE 'APROBADO' THEN 'EGRESADO'
		WHEN estado_beneficiario LIKE 'NO_APLICA' THEN 'PREINSCRIPTO'
		WHEN estado_beneficiario LIKE 'BAJA' THEN 'BAJA'
		WHEN estado_beneficiario LIKE 'REGULAR' THEN 'EN_CURSO'
		ELSE estado_beneficiario
		END estado_beneficiario
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_cursada" cur
JOIN "caba-piba-staging-zone-db"."tbp_typ_def_capacitacion" cap ON (id_capacitacion_new = cap.id_new AND cur.base_origen = cap.base_origen)
WHERE 
id_vecino IS NOT NULL 
AND id_broker IS NOT NULL 
AND id_capacitacion_new IS NOT NULL 
AND TRIM(documento_broker) != ''
AND 
(edicion_capacitacion_id_new IS NOT NULL
OR
-- puede no existir edicion_capacitacion_id_new cuando el estado_beneficiario es 'PREINSCRIPTO'
(estado_beneficiario LIKE 'PREINSCRIPTO' AND cur.base_origen IN ('SIU', 'GOET'))
)
GROUP BY 
 	cur.base_origen,
 	id_edicion_capacitacion_old,
 	edicion_capacitacion_id_new,
 	id_capacitacion_new,
 	identificacion_alumno,
 	documento_broker,
 	porcentaje_asistencia,
 	id_vecino,
 	id_broker,
 	estado_beneficiario,
 	cap.id
--</sql>--