-- TRAYECTORIA EDUCATIVA GOET, MOODLE, SIENFO Y CRMSL
-- 1.- Crear tabla trayectoria educativa definitiva
-- --<sql>--DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_def_trayectoria_educativa`;--</sql>--
--<sql>--
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_def_trayectoria_educativa" AS
SELECT 
	CAST(row_number() OVER () AS VARCHAR) AS cursado_id,
	bg.id broker_id,
	bg.nombre,
	bg.apellido,
	bg.tipo_doc_broker tipo_documento,
	bg.documento_broker numero_documento,
	vec.nacionalidad_broker nacionalidad,
	bg.genero,
	dc.capacitacion_id_asi codigo_curso,
	COALESCE(dc.descrip_capacitacion, dc.descrip_normalizada) nombre_curso,
	dc.detalle_capacitacion,
	p.codigo_programa,
	p.nombre_programa detalle,
	DATE_FORMAT(CAST(dcu.fecha_inicio AS DATE), '%d-%m-%Y') fecha_inicio_cursada,
	dcu.estado_beneficiario estado,
	-- Si existe mas de una fecha de egreso, se toma la menor
	DATE_FORMAT(MIN(CAST(dcu.fecha_egreso AS DATE)), '%d-%m-%Y') fecha_egreso
FROM "caba-piba-staging-zone-db"."tbp_broker_def_broker_general" bg  
	INNER JOIN "caba-piba-staging-zone-db"."tbp_typ_def_vecino" vec ON (bg.id = vec.broker_id)
	LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_cursada" dcu ON (dcu.broker_id = vec.broker_id) 
	LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_edicion_capacitacion" ed ON (ed.id = dcu.edicion_capacitacion_id_new AND ed.base_origen = dcu.base_origen)
	LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_capacitacion" dc ON (dc.id_new = ed.capacitacion_id_new AND dc.base_origen = ed.base_origen)
	LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_programa" p ON (p.programa_id = dc.programa_id)
WHERE 
	dc.base_origen != 'MOODLE' AND 
	dcu.estado_beneficiario IS NOT NULL
GROUP BY 
	bg.id,
	bg.apellidos_nombres,
	bg.nombre,
	bg.apellido,
	bg.tipo_doc_broker,
	bg.documento_broker,
	vec.nacionalidad_broker,
	bg.genero,
	dc.capacitacion_id_asi,
	COALESCE(dc.descrip_capacitacion, dc.descrip_normalizada),
	dc.detalle_capacitacion,
	p.codigo_programa,
	p.nombre_programa,
	DATE_FORMAT(CAST(dcu.fecha_inicio AS DATE), '%d-%m-%Y'),
	dcu.estado_beneficiario
--</sql>--