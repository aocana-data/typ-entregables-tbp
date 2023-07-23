-- CURSADA GOET, MOODLE, SIENFO, CRMSL Y SIU
-- 1.- Crear tabla cursada definitiva
-- --<sql>--DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_def_cursada`;--</sql>--
--<sql>--
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_def_cursada" AS
WITH tmp AS (
	SELECT row_number() OVER () AS id,
		cur.base_origen,
		id_edicion_capacitacion_old AS id_edicion_capacitacion_old,
		edicion_capacitacion_id_new AS id_edicion_capacitacion_new,
		cap.id AS id_capacitacion,
		id_capacitacion_new AS id_capacitacion_new,
		identificacion_alumno identificacion_alumno_old,
		documento_broker,
		cur.fecha_preinscripcion,
		cur.fecha_inicio,
		cur.fecha_abandono,
		cur.fecha_egreso,
		porcentaje_asistencia,
		cur.cant_aprobadas,
		id_vecino,
		id_broker,
		CASE
			WHEN estado_beneficiario LIKE 'FINALIZADO' THEN 'FINALIZO_CURSADA'
			WHEN estado_beneficiario LIKE 'APROBADO' THEN 'EGRESADO'
			WHEN estado_beneficiario LIKE 'NO_APLICA' THEN 'PREINSCRIPTO'
			WHEN estado_beneficiario LIKE 'BAJA' THEN 'BAJA'
			WHEN estado_beneficiario LIKE 'REGULAR' THEN 'EN_CURSO' ELSE estado_beneficiario
		END estado_beneficiario,
		ROW_NUMBER() OVER(
			PARTITION BY cur.base_origen,
			id_edicion_capacitacion_old,
			cap.id,
			id_capacitacion_new,
			documento_broker
			ORDER BY estado_beneficiario ASC,
				cur.fecha_preinscripcion ASC,
				cur.fecha_inicio ASC,
				cur.fecha_abandono ASC,
				cur.fecha_egreso DESC
		) AS "orden"
	FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_cursada" cur
		JOIN "caba-piba-staging-zone-db"."tbp_typ_def_capacitacion" cap ON (
			id_capacitacion_new = cap.id_new
			AND cur.base_origen = cap.base_origen
		)
	WHERE id_vecino IS NOT NULL
		AND id_broker IS NOT NULL
		AND id_capacitacion_new IS NOT NULL
		AND TRIM(documento_broker) != ''
		AND (
			edicion_capacitacion_id_new IS NOT NULL
			OR -- puede no existir edicion_capacitacion_id_new cuando el estado_beneficiario es 'PREINSCRIPTO'
			(
				estado_beneficiario LIKE 'PREINSCRIPTO'
				AND cur.base_origen IN ('SIU', 'GOET')
			)
		)
) 
SELECT row_number() OVER () AS id,
	base_origen,
	id_edicion_capacitacion_old,
	id_edicion_capacitacion_new,
	id_capacitacion,
	id_capacitacion_new,
	identificacion_alumno_old,
	documento_broker,
	fecha_preinscripcion,
	fecha_inicio,
	fecha_abandono,
	fecha_egreso,
	porcentaje_asistencia,
	cant_aprobadas,
	id_vecino,
	id_broker,
	estado_beneficiario
FROM tmp
WHERE orden = 1
--</sql>--