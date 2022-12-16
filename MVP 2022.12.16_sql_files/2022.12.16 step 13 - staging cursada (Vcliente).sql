-- GOET, MOODLE, SIENFO, CRMSL
-- ENTIDAD:CURSADA
-- 1.-  Crear tabla temporal de cursadas
-- DROP TABLE `caba-piba-staging-zone-db`.`tbp_typ_tmp_cursada`;
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_cursada" AS
-- GOET

SELECT 
     'GOET' base_origen,
	 CAST(gu.idusuario AS VARCHAR) identificacion_alumno,
	 CAST(gu.ndocumento AS VARCHAR) documento_broker,
	-- no esta en la BBDD, SE ASUME la fecha de inicio de la capacitacion como la fecha de preinscripcion
	date_parse(date_format(cc.iniciocurso, '%Y-%m-%d %h:%i%p'), '%Y-%m-%d %h:%i%p') fecha_preinscripcion,
	
	-- fecha de la primer asistencia del alumno a la edicion capacitacion
	date_parse(date_format(inicio_cursada.fecha_inicio, '%Y-%m-%d %h:%i%p'), '%Y-%m-%d %h:%i%p') fecha_inicio,
	
	-- para obtener la fecha de abondono se busca alumnos inscriptos que no tengan certificado y se establece 
	-- como fecha de abandono la fecha de ultima asistencia, siempre que la edicion capacitacion haya finalizado (fecha fin menor a hoy)
	-- IMPORTANTE! hay que evaluar si la logica es correcta, porque ¿talvez se pueden demorar en generar el certificado respectivo?
	date_parse(date_format(datos_abandono.fecha_abandono, '%Y-%m-%d %h:%i%p'), '%Y-%m-%d %h:%i%p') fecha_abandono,
		
	-- en el caso que la persona haya abandonado la fecha de egreso es la fecha de abandono, en caso contrario
	-- es la fecha del certificado y de no existir el mismo, es la fecha de fin de curso
	-- VER ESTADO BENEFICIARIO!!
	CASE 
		WHEN cc.fincurso IS NOT NULL AND datos_abandono.fecha_abandono IS NULL AND certificado.fecha IS NOT NULL
			THEN date_parse(date_format(certificado.fecha, '%Y-%m-%d %h:%i%p'), '%Y-%m-%d %h:%i%p')
		WHEN cc.fincurso IS NOT NULL AND datos_abandono.fecha_abandono IS NULL AND certificado.fecha IS NULL
			THEN date_parse(date_format(cc.fincurso, '%Y-%m-%d %h:%i%p'), '%Y-%m-%d %h:%i%p')
		ELSE date_parse(date_format(datos_abandono.fecha_abandono, '%Y-%m-%d %h:%i%p'), '%Y-%m-%d %h:%i%p')
	END fecha_egreso,
		
	0 porcentaje_asistencia,
    ' ' estado,
	-- Se podria calcular el promedio desde la tabla calificacion_alumnos, pero no la cantidad de materias aprobadas 
	-- dado que no esta contemplado en el modelo de GOET
	' ' cant_aprobadas,

	ed.id edicion_capacitacion_id_new,
	ed.edicion_capacitacion_id_old,
	 	
	ed.capacitacion_id_new,
	ed.capacitacion_id_old,
	ed.tipo_capacitacion,
	vec.vecino_id,
	vec.broker_id,
	eb.estado_beneficiario AS "estado_beneficiario"

FROM 
"caba-piba-raw-zone-db"."goet_nomenclador"  n 
INNER JOIN  "caba-piba-raw-zone-db"."goet_centro_habilitacion_modulos" chm ON n.IdNomenclador = chm.IdNomenclador
INNER JOIN "caba-piba-raw-zone-db"."goet_centro_codigo_curso" cc ON cc.IdCtrHbModulo = chm.IdCtrHbModulo
LEFT JOIN "caba-piba-raw-zone-db"."goet_trayecto" t ON (t.idkeytrayecto = cc.idkeytrayecto)
LEFT JOIN 
(SELECT idctrcdcurso, idusuario FROM "caba-piba-raw-zone-db"."goet_inscripcion_alumnos"  GROUP BY idctrcdcurso, idusuario) gia  ON (cc.idctrcdcurso = gia.idctrcdcurso)
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_tmp_estado_beneficiario_goet" eb ON (cc.idctrcdcurso=eb.idctrcdcurso AND eb.idusuario=gia.idusuario) 
LEFT JOIN "caba-piba-raw-zone-db"."goet_usuarios" gu on (gu.idusuario = gia.idusuario)
LEFT JOIN "caba-piba-raw-zone-db"."goet_certificado_alumnos" certificado ON (
		certificado.idctrcdcurso = cc.idctrcdcurso
		AND certificado.idusuario = gia.idusuario
		)
LEFT JOIN "caba-piba-raw-zone-db"."goet_certificado_estado" certificado_estado ON ( certificado.idcertificadoestado=certificado_estado.idcertificadoestado)
LEFT JOIN (
	SELECT min(fecha) fecha_inicio, IdUsuario, IdCtrCdCurso 
	FROM "caba-piba-raw-zone-db"."goet_asistencia_alumnos" 
	GROUP BY IdUsuario, IdCtrCdCurso
) inicio_cursada ON (inicio_cursada.idctrcdcurso = gia.idctrcdcurso and inicio_cursada.IdUsuario=gia.idusuario)

LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_edicion_capacitacion" ed ON (
		ed.edicion_capacitacion_id_old = CAST(cc.idctrcdcurso AS VARCHAR)
		AND ed.base_origen = 'GOET'
		AND ed.capacitacion_id_old=(CASE WHEN UPPER(t.detalle) IS NOT NULL THEN CAST(n.IdNomenclador AS VARCHAR)||'-'||CAST(t.IdKeyTrayecto AS VARCHAR) ELSE CAST(n.IdNomenclador AS VARCHAR) END)  
		)

LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_vecino" vec ON (
		vec.base_origen = 'GOET'
		AND vec.documento_broker = TRIM(gu.ndocumento )
		)		

LEFT JOIN (SELECT CASE 
		WHEN (
				(
					cc.fincurso IS NULL
					OR date_parse(date_format(cc.fincurso, '%Y-%m-%d %h:%i%p'), '%Y-%m-%d %h:%i%p') < CURRENT_DATE
					)
				AND certi.idctrcdcurso IS NULL
				)
			THEN fin_cursada.fecha_ultima_asistencia
		ELSE 
			NULL
		END fecha_abandono,
		cc.idctrcdcurso, 
		gia.idusuario
		
	FROM 
	(SELECT idctrcdcurso, idusuario FROM "caba-piba-raw-zone-db"."goet_inscripcion_alumnos"  GROUP BY idctrcdcurso, idusuario) gia 
	JOIN "caba-piba-raw-zone-db"."goet_centro_codigo_curso" cc  ON (cc.idctrcdcurso = gia.idctrcdcurso)

	JOIN (
		SELECT MAX(fecha) fecha_ultima_asistencia,
			IdUsuario,
			IdCtrCdCurso
		FROM "caba-piba-raw-zone-db"."goet_asistencia_alumnos"
		GROUP BY IdUsuario,
			IdCtrCdCurso
		) fin_cursada ON (fin_cursada.idctrcdcurso = gia.idctrcdcurso
			AND fin_cursada.IdUsuario = gia.idusuario)
	LEFT JOIN "caba-piba-raw-zone-db"."goet_certificado_alumnos" certi ON (
			certi.idctrcdcurso = cc.idctrcdcurso
			AND certi.idusuario = gia.idusuario
			)
) as datos_abandono	ON (datos_abandono.idusuario=gu.idusuario AND datos_abandono.idctrcdcurso=cc.idctrcdcurso)	
GROUP BY 
gu.idusuario, 
gu.ndocumento, 
cc.iniciocurso,
inicio_cursada.fecha_inicio,
datos_abandono.fecha_abandono,
cc.fincurso,
certificado.fecha,
ed.id,
ed.edicion_capacitacion_id_old,
ed.capacitacion_id_new,
ed.capacitacion_id_old,
ed.tipo_capacitacion,
vec.vecino_id,
vec.broker_id,
eb.estado_beneficiario
	
UNION
-- MOODLE
SELECT 
	'MOODLE' base_origen,
	CAST(usuario.username AS VARCHAR) identificacion_alumno,
	CAST(usuario.username AS VARCHAR) documento_broker,
	-- Analizar de donde se puede sacar el valor, cuando este el script de estado de beneficiario
	NULL fecha_preinscripcion,
	date_parse(date_format(from_unixtime(uenrolments.timestart), '%Y-%m-%d %h:%i%p'), '%Y-%m-%d %h:%i%p') fecha_inicio,
	CASE 
		WHEN uenrolments.timeend != 0
			THEN date_parse(date_format(from_unixtime(uenrolments.timeend), '%Y-%m-%d %h:%i%p'), '%Y-%m-%d %h:%i%p')
		ELSE NULL
		END fecha_abandono,
	
	--Se asume que la fecha de egreso es la fecha de finalizacion del curso, si el mismo ya termino y si el alumno no abandonó
	CASE 
		WHEN co.enddate != 0 AND uenrolments.timeend != 0
			AND date_parse(date_format(from_unixtime(co.enddate), '%Y-%m-%d %h:%i%p'), '%Y-%m-%d %h:%i%p') <= current_date
			THEN date_parse(date_format(from_unixtime(co.enddate), '%Y-%m-%d %h:%i%p'), '%Y-%m-%d %h:%i%p')
		ELSE NULL
		END fecha_egreso,		

	0 porcentaje_asistencia,
	-- no se puede determinar el significado de los valores del uenrolments.STATUS, por lo que se pone estado como ' '
	' ' estado,
	' ' cant_aprobadas,
	ed.id edicion_capacitacion_id_new,
	ed.edicion_capacitacion_id_old,
	ed.capacitacion_id_new,
	ed.capacitacion_id_old,
	
	ed.tipo_capacitacion,
	vec.vecino_id,
	vec.broker_id,
	eb.estado AS "estado_beneficiario"
FROM "caba-piba-raw-zone-db"."moodle_dgtedu01_mdl_course" co
LEFT JOIN "caba-piba-raw-zone-db"."moodle_dgtedu01_mdl_enrol" enrol ON (co.id = enrol.courseid)
LEFT JOIN "caba-piba-raw-zone-db"."moodle_dgtedu01_mdl_user_enrolments" uenrolments ON (uenrolments.enrolid = enrol.id)
LEFT JOIN "caba-piba-raw-zone-db"."moodle_dgtedu01_mdl_user" usuario ON (usuario.id = uenrolments.userid)
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_edicion_capacitacion" ed ON (
		ed.edicion_capacitacion_id_old = CAST(co.id AS VARCHAR)
		AND ed.base_origen = 'MOODLE'
		)
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_vecino" vec ON (
		vec.base_origen = 'MOODLE'
		AND vec.documento_broker = REGEXP_REPLACE(UPPER(CAST(CASE WHEN REGEXP_LIKE(usuario.username, '@') THEN 
					CASE WHEN REGEXP_LIKE(LOWER(usuario.username), '\.ar') THEN 
						split_part(split_part(usuario.username, '@', 2),'.',4) 
					ELSE split_part(split_part(usuario.username, '@', 2),'.',3) END
					ELSE split_part(usuario.username, '.', 1) END AS VARCHAR)),'[A-Za-z]+|\.','')
		)
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_tmp_estado_beneficiario_moodle" eb ON (eb.id_curso=co.id AND eb.alumno_id=usuario.id)		
GROUP BY 
	usuario.username,
	uenrolments.timestart,
	uenrolments.timeend,
	co.enddate,
	ed.id,
	ed.edicion_capacitacion_id_old,
	ed.capacitacion_id_new,
	ed.capacitacion_id_old,
	ed.tipo_capacitacion,
	vec.vecino_id,
	vec.broker_id,
	eb.estado 
UNION
-- SIENFO  ver xq hay fichas cuyo nro de documento no esta en la tabla vecinos!!!
SELECT 'SIENFO' base_origen,
       --sf.codigo_ct edicion_capacitacion_id_old,
	   CAST(sf.nrodoc AS VARCHAR) identificacion_alumno,
       CAST(sf.nrodoc AS VARCHAR) documento_broker,
       sfp.fecha_inc fecha_preinscripcion,
       sf.fecha_inc fecha_inicio,
       sf.fechabaja fecha_abandono,
       ed.fecha_fin_dictado fecha_egreso,
	   0 porcentaje_asistencia,
       '' estado,
       sf.aprobado cant_aprobadas,
       ed.id edicion_capacitacion_id_new,
	   ed.edicion_capacitacion_id_old,
       ed.capacitacion_id_new,
	   ed.capacitacion_id_old,
       ed.tipo_capacitacion,
       vec.vecino_id,
       vec.broker_id,
	   ebs.estado_beneficiario
FROM "caba-piba-raw-zone-db"."sienfo_fichas" sf
LEFT JOIN "caba-piba-raw-zone-db"."sienfo_alumnos" a ON (sf.nrodoc = a.nrodoc)
LEFT JOIN "caba-piba-raw-zone-db"."sienfo_tdoc" d ON (a.tipodoc = d.tipodoc)
LEFT JOIN "caba-piba-raw-zone-db"."sienfo_fichas_preinscripcion" sfp ON (sfp.codigo_ct = sf.codigo_ct AND sf.nrodoc = sfp.nrodoc)
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_edicion_capacitacion" ed ON (ed.edicion_capacitacion_id_old = sf.codigo_ct AND ed.base_origen = 'SIENFO')
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_vecino" vec ON 
(vec.base_origen = 'SIENFO' 
AND vec.documento_broker = CASE WHEN d.nombre IN ('D.N.I.', 'L.C.', 'L.E.', 'C.I.', 'CUIT', 'CUIL') THEN REGEXP_REPLACE(UPPER(a.nrodoc),'[A-Za-z]+|\.|\,','') ELSE
           CAST(a.nrodoc AS VARCHAR) END 
)
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_tmp_estado_beneficiario_sienfo" ebs 
ON (ebs.nrodoc = sf.nrodoc AND (
	(ebs.codigo_ct = ed.edicion_capacitacion_id_old AND ebs.tipo_formacion like 'CURSO' ) 
	OR 
	(split_part(ebs.llave_doc_idcap,'-',2) = ed.capacitacion_id_old AND ebs.tipo_formacion like 'CARRERA' AND ebs.codigo_ct = ed.edicion_capacitacion_id_old))
	)
UNION
-- CRMSL
SELECT 'CRMSL' base_origen,
       --ofc.op_oportun1d35rmacion_ida edicion_capacitacion_id_old,
       vec.cod_origen indentificacion_alumno,
       CAST(cs.numero_documento_c AS VARCHAR) documento_broker,
       ofc.date_modified fecha_preinscripcion,
       NULL fecha_inicio,
       NULL fecha_abandono,
       NULL fecha_egreso,
       sc.porcentaje_asistencia_c porcentaje_asistencia,
       sc.estado_c estado,
       NULL cant_aprobadas,
       ed.id edicion_capacitacion_id_new,
	   ed.edicion_capacitacion_id_old,
       ed.capacitacion_id_new,
	   ed.capacitacion_id_old,
       ed.tipo_capacitacion,
       vec.vecino_id,
       vec.broker_id,
	   ebc.estado_beneficiario
FROM "caba-piba-raw-zone-db"."crm_sociolaboral_op_oportunidades_formacion_contacts_c" ofc
INNER JOIN "caba-piba-raw-zone-db"."crm_sociolaboral_contacts" co ON (co.id = ofc.op_oportunidades_formacion_contactscontacts_idb)
INNER JOIN "caba-piba-raw-zone-db"."crm_sociolaboral_contacts_cstm" cs ON (ofc.op_oportunidades_formacion_contactscontacts_idb = cs.id_c)
LEFT JOIN "caba-piba-raw-zone-db"."crm_sociolaboral_se_seguimiento_cstm" sc ON (sc.id_c = ofc.op_oportunidades_formacion_contactscontacts_idb)
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_edicion_capacitacion" ed ON (ed.edicion_capacitacion_id_old = ofc.op_oportun1d35rmacion_ida AND ed.base_origen = 'CRMSL')
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_vecino" vec ON (vec.base_origen = 'CRMSL' AND vec.documento_broker = CAST(cs.numero_documento_c AS VARCHAR))
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_tmp_estado_beneficiario_crmsl" ebc ON (ebc.edicion_capacitacion_id_old = ed.edicion_capacitacion_id_old AND ebc.alumno_id_old = vec.cod_origen )
WHERE (co.lead_source = 'sociolaboral'
OR ((co.lead_source = 'rib') AND cs.forma_parte_interm_lab_c = 'si')) 
AND cs.numero_documento_c IS NOT NULL