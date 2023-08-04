-- GOET, MOODLE, SIENFO, CRMSL, SIU
-- ENTIDAD:CURSADA
-- 1.-  Crear tabla temporal de cursadas sin IEL
-- --<sql>--DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_tmp_cursada_1`;--</sql>--
--<sql>--
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_cursada_1" AS
-- GOET

SELECT 
     'GOET' base_origen,
	 CAST(gu.idusuario AS VARCHAR) identificacion_alumno,
	 REGEXP_REPLACE(CONCAT(' ', CAST(gu.ndocumento AS VARCHAR)), '[A-Za-z\.\-\,\(\)\@\_\ñ\ ]+', '') documento_broker,
	-- no esta en la BBDD, SE ASUME la fecha de inicio de la capacitacion como la fecha de preinscripcion
	date_parse(date_format(cc.iniciocurso, '%Y-%m-%d %h:%i%p'), '%Y-%m-%d %h:%i%p') fecha_preinscripcion,
	
	-- se utiliza la fecha de la primer asistencia del alumno a la edicion capacitacion
	-- en caso de null y que el estado de beneficiario no sea 'PREINSCRIPTO' ni 'INSCRIPTO' se utiliza 
	-- el campo cc.iniciocurso, null en otro caso
	CASE WHEN eb.estado_beneficiario NOT IN ('PREINSCRIPTO', 'INSCRIPTO') AND inicio_cursada.fecha_inicio IS NULL
	THEN date_parse(date_format(cc.iniciocurso, '%Y-%m-%d %h:%i%p'), '%Y-%m-%d %h:%i%p')
	ELSE date_parse(date_format(inicio_cursada.fecha_inicio, '%Y-%m-%d %h:%i%p'), '%Y-%m-%d %h:%i%p') 
	END fecha_inicio,
	
	-- para obtener la fecha de abondono se busca alumnos inscriptos que no tengan certificado y se establece 
	-- como fecha de abandono la fecha de ultima asistencia, siempre que la edicion capacitacion haya finalizado (fecha fin menor a hoy)
	-- IMPORTANTE! hay que evaluar si la logica es correcta, porque ¿talvez se pueden demorar en generar el certificado respectivo?
	date_parse(date_format(datos_abandono.fecha_abandono, '%Y-%m-%d %h:%i%p'), '%Y-%m-%d %h:%i%p') fecha_abandono,
		
	-- se utiliza la fecha fin de estado del beneficiario, en caso de null y que el caso que la persona haya abandonado la fecha de egreso es la fecha de abandono, en caso contrario
	-- es la fecha del certificado y de no existir el mismo, es la fecha de fin de curso
	-- IMPORTANTE! hay que evaluar si la logica es correcta
	CASE 
		WHEN eb.FinCurso IS NOT NULL 
			THEN date_parse(date_format(eb.FinCurso, '%Y-%m-%d %h:%i%p'), '%Y-%m-%d %h:%i%p')
		WHEN  eb.FinCurso IS NULL AND cc.fincurso IS NOT NULL AND datos_abandono.fecha_abandono IS NULL AND certificado.fecha IS NOT NULL
			THEN date_parse(date_format(certificado.fecha, '%Y-%m-%d %h:%i%p'), '%Y-%m-%d %h:%i%p')
		WHEN eb.FinCurso IS NULL AND cc.fincurso IS NOT NULL AND datos_abandono.fecha_abandono IS NULL AND certificado.fecha IS NULL
			THEN date_parse(date_format(cc.fincurso, '%Y-%m-%d %h:%i%p'), '%Y-%m-%d %h:%i%p')
		ELSE date_parse(date_format(datos_abandono.fecha_abandono, '%Y-%m-%d %h:%i%p'), '%Y-%m-%d %h:%i%p')
		 
	END fecha_egreso,
		
	CAST(NULL AS INTEGER) porcentaje_asistencia,
	-- Se podria calcular el promedio desde la tabla calificacion_alumnos, pero no la cantidad de materias aprobadas 
	-- dado que no esta contemplado en el modelo de GOET
	CAST(NULL AS VARCHAR) cant_aprobadas,

	ed.id edicion_capacitacion_id_new,
	ed.id_edicion_capacitacion_old,
	 	
	ed.id_capacitacion_new,
	ed.tipo_capacitacion,
	vec.id_vecino,
	vec.id_broker,
	eb.estado_beneficiario AS "estado_beneficiario"

FROM 
"caba-piba-raw-zone-db"."goet_nomenclador"  n 
INNER JOIN  "caba-piba-raw-zone-db"."goet_centro_habilitacion_modulos" chm ON n.IdNomenclador = chm.IdNomenclador
INNER JOIN "caba-piba-raw-zone-db"."goet_centro_codigo_curso" cc ON cc.IdCtrHbModulo = chm.IdCtrHbModulo
LEFT JOIN "caba-piba-raw-zone-db"."goet_trayecto" t ON (t.idkeytrayecto = cc.idkeytrayecto)
LEFT JOIN 
(SELECT idctrcdcurso, idusuario FROM "caba-piba-raw-zone-db"."goet_inscripcion_alumnos"  GROUP BY idctrcdcurso, idusuario) gia  ON (cc.idctrcdcurso = gia.idctrcdcurso)
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_tmp_estado_beneficiario_goet" eb ON (cc.idctrcdcurso=eb.idctrcdcurso AND eb.idusuario=gia.idusuario) 
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_tmp_view_goet_usuarios_no_duplicates" gu on (gu.idusuario = gia.idusuario)
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
		ed.id_edicion_capacitacion_old = CAST(cc.idctrcdcurso AS VARCHAR)
		AND ed.base_origen = 'GOET'
		AND ed.id_capacitacion_old=(CASE WHEN UPPER(t.detalle) IS NOT NULL THEN CAST(n.IdNomenclador AS VARCHAR)||'-'||CAST(t.IdKeyTrayecto AS VARCHAR) ELSE CAST(n.IdNomenclador AS VARCHAR) END)
		)
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_establecimientos" est ON (est.id_old = CAST(chm.IdCentro AS VARCHAR) AND ed.id_establecimiento=est.id AND est.base_origen = ed.base_origen)


LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_vecino" vec ON (
		vec.base_origen = 'GOET'
		AND vec.documento_broker = TRIM(gu.ndocumento )
		AND vec.genero_broker = gu.sexo
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
eb.FinCurso,
ed.id,
ed.id_edicion_capacitacion_old,
ed.id_capacitacion_new,
ed.tipo_capacitacion,
vec.id_vecino,
vec.id_broker,
eb.estado_beneficiario

	
UNION
-- MOODLE
SELECT 
	DISTINCT 'MOODLE' base_origen,
	CAST(usuario.username AS VARCHAR) identificacion_alumno,
	REGEXP_REPLACE(CONCAT(' ', CAST(usuario.username AS VARCHAR)), '[A-Za-z\.\-\,\(\)\@\_\ñ\ ]+', '') documento_broker,
	
	date_parse(date_format(from_unixtime(uenrolments.timestart), '%Y-%m-%d %h:%i%p'), '%Y-%m-%d %h:%i%p') fecha_preinscripcion, 
	
	-- se utiliza la fecha de la inscripcion del alumno a la edicion capacitacion para la fecha de inicio, en el caso que 
	-- el estado de beneficiario no sea 'PREINSCRIPTO' ni 'INSCRIPTO', null en otro caso
	CASE WHEN eb.estado NOT IN ('PREINSCRIPTO', 'INSCRIPTO') 
	THEN date_parse(date_format(from_unixtime(uenrolments.timestart), '%Y-%m-%d %h:%i%p'), '%Y-%m-%d %h:%i%p')
	ELSE NULL
	END fecha_inicio,

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

	CAST(NULL AS INTEGER) porcentaje_asistencia,
	-- no se puede determinar el significado de los valores del uenrolments.STATUS, por lo que se pone estado como ' '
	CAST(NULL AS VARCHAR) cant_aprobadas,
	ed.id edicion_capacitacion_id_new,
	ed.id_edicion_capacitacion_old,
	ed.id_capacitacion_new,
	
	ed.tipo_capacitacion,
	vec.id_vecino,
	vec.id_broker,
	eb.estado AS "estado_beneficiario"
FROM "caba-piba-raw-zone-db"."moodle_dgtedu01_mdl_course" co
JOIN "caba-piba-raw-zone-db"."moodle_dgtedu01_mdl_course_categories" cc ON (co.category = cc.id)
LEFT JOIN "caba-piba-raw-zone-db"."moodle_dgtedu01_mdl_enrol" enrol ON (co.id = enrol.courseid)
LEFT JOIN "caba-piba-raw-zone-db"."moodle_dgtedu01_mdl_user_enrolments" uenrolments ON (uenrolments.enrolid = enrol.id)
LEFT JOIN "caba-piba-raw-zone-db"."moodle_dgtedu01_mdl_user" usuario ON (usuario.id = uenrolments.userid)
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_edicion_capacitacion" ed ON (
		ed.id_edicion_capacitacion_old = CAST(cc.id AS VARCHAR)
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
-- se excluyen registros que son de test
WHERE vec.id_broker NOT IN  ('DNI1XARG','DNI2XARG','DNI6XARG','DNI3XARG','DNI40XARG')	
GROUP BY 
	usuario.username,
	uenrolments.timestart,
	uenrolments.timeend,
	co.enddate,
	ed.id,
	ed.id_edicion_capacitacion_old,
	ed.id_capacitacion_new,
	ed.tipo_capacitacion,
	vec.id_vecino,
	vec.id_broker,
	eb.estado
UNION

-- SIENFO
SELECT 'SIENFO' base_origen,
	CAST(sf.nrodoc AS VARCHAR) identificacion_alumno,
	REGEXP_REPLACE(CONCAT(' ', UPPER(sf.nrodoc)), '[A-Za-z\.\-\,\(\)\@\+\/\ ]+', '') documento_broker,

	-- En el caso que no se encuentre la fecha de preinscripcion se utiliza la de inscripcion, en otro caso null
	CASE WHEN sfp.fecha_inc IS NOT NULL
	THEN sfp.fecha_inc
	ELSE sf.fecha_inc 
	END fecha_preinscripcion,

	-- se utiliza la fecha de la inscripcion del alumno a la edicion capacitacion para la fecha de inicio, en el caso que 
	-- el estado de beneficiario no sea 'PREINSCRIPTO' ni 'INSCRIPTO', null en otro caso
	CASE WHEN ebs.estado_beneficiario NOT IN ('PREINSCRIPTO', 'INSCRIPTO') 
	THEN sf.fecha_inc 
	ELSE NULL
	END fecha_inicio,

	sf.fechabaja fecha_abandono,
	ed.fecha_fin_dictado fecha_egreso,
	CAST(NULL AS INTEGER) porcentaje_asistencia,
	CAST(sf.aprobado AS VARCHAR) cant_aprobadas,
	ed.id edicion_capacitacion_id_new,
	ed.id_edicion_capacitacion_old,
	ed.id_capacitacion_new,
	ed.tipo_capacitacion,
	vec.id_vecino,
	vec.id_broker,
	ebs.estado_beneficiario
FROM "caba-piba-staging-zone-db"."goayvd_typ_vw_sienfo_fichas" sf
LEFT JOIN "caba-piba-raw-zone-db"."sienfo_alumnos" a ON (sf.nrodoc = a.nrodoc)
LEFT JOIN "caba-piba-raw-zone-db"."sienfo_tgenero" g ON (CAST(g.id AS INT) = CAST(a.sexo AS INT))	
LEFT JOIN "caba-piba-raw-zone-db"."sienfo_tdoc" d ON (a.tipodoc = d.tipodoc)
LEFT JOIN "caba-piba-staging-zone-db"."goayvd_typ_vw_sienfo_fichas_preinscripcion" sfp ON (sfp.codigo_ct = sf.codigo_ct AND sf.nrodoc = sfp.nrodoc)
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_edicion_capacitacion" ed ON (ed.id_edicion_capacitacion_old = sf.codigo_ct AND ed.base_origen = 'SIENFO')
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_establecimientos" est ON (est.id_old = CAST(sf.id_centro AS VARCHAR) AND ed.id_establecimiento=est.id AND est.base_origen = ed.base_origen)
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_vecino" vec ON 
(vec.base_origen = 'SIENFO' 
AND vec.documento_broker = CASE WHEN d.nombre IN ('D.N.I.', 'L.C.', 'L.E.', 'C.I.', 'CUIT', 'CUIL') THEN REGEXP_REPLACE(UPPER(a.nrodoc),'[A-Za-z]+|\.|\,','') ELSE
           CAST(a.nrodoc AS VARCHAR) END 
AND vec.genero_broker = (CASE WHEN (g.sexo = 'Masculino') THEN 'M' WHEN (g.sexo = 'Femenino') THEN 'F' ELSE 'X' END)
)
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_tmp_estado_beneficiario_sienfo" ebs 
ON (ebs.nrodoc = sf.nrodoc AND (
	(ebs.codigo_ct = ed.id_edicion_capacitacion_old AND ebs.tipo_formacion like 'CURSO' ) 
	OR 
	(split_part(ebs.llave_doc_idcap,'-',2) = ed.id_capacitacion_old AND ebs.tipo_formacion like 'CARRERA' AND ebs.codigo_ct = ed.id_edicion_capacitacion_old))
	)
GROUP BY 
	sf.nrodoc,
	sfp.fecha_inc,
	ebs.estado_beneficiario,
	sf.fecha_inc,
	sf.fechabaja,
	ed.fecha_fin_dictado,
	sf.aprobado,
	ed.id,
	ed.id_edicion_capacitacion_old,
	ed.id_capacitacion_new,
	ed.tipo_capacitacion,
	vec.id_vecino,
	vec.id_broker

UNION
-- CRMSL
SELECT 'CRMSL' base_origen,
       vec.cod_origen indentificacion_alumno,
       CAST(cs.numero_documento_c AS VARCHAR) documento_broker,
       ofc.date_modified fecha_preinscripcion,
	   
		-- se utiliza la fecha de estado del beneficiario, en caso de null y que el estado de beneficiario no sea 'PREINSCRIPTO' ni 'INSCRIPTO' se utiliza 
		-- el campo ofc.date_modified, null en otro caso
		CASE WHEN ebc.estado_beneficiario NOT IN ('PREINSCRIPTO', 'INSCRIPTO') AND ebc.inicio IS NULL
		THEN ofc.date_modified
		ELSE ebc.inicio
		END fecha_inicio,
	   
	   -- no se encuentra valor en la base origen para este atributo
	   CAST(NULL AS DATE) fecha_abandono,
	   
	   -- se obtiene el dato desde el script/tabla de estado del beneficiario
       ebc.fin fecha_egreso,
       CAST(sc.porcentaje_asistencia_c AS INTEGER) porcentaje_asistencia,
       CAST(NULL AS VARCHAR) cant_aprobadas,
       ed.id edicion_capacitacion_id_new,
	   ed.id_edicion_capacitacion_old,
       ed.id_capacitacion_new,
       ed.tipo_capacitacion,
       vec.id_vecino,
       vec.id_broker,
	   ebc.estado_beneficiario
FROM "caba-piba-raw-zone-db"."crm_sociolaboral_op_oportunidades_formacion_contacts_c" ofc

LEFT JOIN "caba-piba-raw-zone-db"."crm_sociolaboral_op_oportunidades_formacion" of ON (of.id = ofc.op_oportun1d35rmacion_ida)
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_establecimientos" est ON (of.sede in (est.nombres_old))

INNER JOIN "caba-piba-raw-zone-db"."crm_sociolaboral_contacts" co ON (co.id = ofc.op_oportunidades_formacion_contactscontacts_idb)
INNER JOIN "caba-piba-staging-zone-db"."tbp_typ_tmp_view_crm_sociolaboral_contacts_cstm_no_duplicates" cs ON (ofc.op_oportunidades_formacion_contactscontacts_idb = cs.id_c)
LEFT JOIN "caba-piba-raw-zone-db"."crm_sociolaboral_se_seguimiento_cstm" sc ON (sc.id_c = ofc.op_oportunidades_formacion_contactscontacts_idb)
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_edicion_capacitacion" ed ON (ed.id_edicion_capacitacion_old = ofc.op_oportun1d35rmacion_ida AND ed.id_establecimiento = est.id AND ed.base_origen = 'CRMSL')

LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_vecino" vec 
	ON (
		vec.base_origen = 'CRMSL' 
		AND vec.documento_broker = CAST(cs.numero_documento_c AS VARCHAR) 
		AND (CASE 
				WHEN cs.genero_c LIKE 'masculino' OR SUBSTRING(CAST(cs.cuil2_c AS VARCHAR),1,2) = '20' THEN 'M'
				WHEN cs.genero_c LIKE 'femenino' OR SUBSTRING(CAST(cs.cuil2_c AS VARCHAR),1,2) = '27' THEN 'F'
				ELSE 'X' END = vec.genero_broker
			)
	)
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_tmp_estado_beneficiario_crmsl" ebc ON (ebc.edicion_capacitacion_id_old = ed.id_edicion_capacitacion_old AND ebc.alumno_id_old = vec.cod_origen )
WHERE (LOWER(co.lead_source) = 'sociolaboral'
OR ((LOWER(co.lead_source) = 'rib') AND LOWER(cs.forma_parte_interm_lab_c) = 'si')) 
AND cs.numero_documento_c IS NOT NULL
GROUP BY
vec.cod_origen,
cs.numero_documento_c,
ofc.date_modified,
ebc.estado_beneficiario,
ebc.inicio,
ebc.fin,
sc.porcentaje_asistencia_c,
ed.id,
ed.id_edicion_capacitacion_old,
ed.id_capacitacion_new,
ed.tipo_capacitacion,
vec.id_vecino,
vec.id_broker
	   
UNION
-- SIU
SELECT 
	'SIU' base_origen,
	CAST(alumnos.alumno AS VARCHAR) identificacion_alumno,
	REGEXP_REPLACE(CONCAT(' ', CAST(nmpd.nro_documento AS VARCHAR)), '[A-Za-z\.\-\,\(\)\@\_\ñ\ ]+', '') documento_broker,
	
	-- Se obtiene el dato a partir de la tabla de estado del beneficiario 
	-- en caso de null, se toma de  siu_preinscripcion_public_sga_preinscripcion
	-- en caso de null nuevamnete, se toma la fecha de inicio
	CASE WHEN eb.fecha_preinscripcion IS NOT NULL
		THEN date_parse(date_format(eb.fecha_preinscripcion, '%Y-%m-%d %h:%i%p'), '%Y-%m-%d %h:%i%p')
	WHEN P.fecha_registro IS NOT NULL
		THEN date_parse(date_format(p.fecha_registro, '%Y-%m-%d %h:%i%p'), '%Y-%m-%d %h:%i%p')
	ELSE
		date_parse(date_format(finicio.fecha_inicio, '%Y-%m-%d %h:%i%p'), '%Y-%m-%d %h:%i%p')
	END fecha_preinscripcion,
	
	-- se utiliza la fecha de primer asistencia a la comision y clase, en caso de null y que el estado de beneficiario no sea 'PREINSCRIPTO' ni 'INSCRIPTO' se utiliza 
	-- el campo eb.fecha_preinscripcion de estado de beneficiario, null en otro caso
	CASE WHEN eb.estado_beneficiario NOT IN ('PREINSCRIPTO', 'INSCRIPTO') AND finicio.fecha_inicio IS NULL
		THEN date_parse(date_format(eb.fecha_preinscripcion, '%Y-%m-%d %h:%i%p'), '%Y-%m-%d %h:%i%p')
	WHEN eb.estado_beneficiario NOT IN ('PREINSCRIPTO')
		THEN date_parse(date_format(finicio.fecha_inicio, '%Y-%m-%d %h:%i%p'), '%Y-%m-%d %h:%i%p')
	ELSE NULL
	END fecha_inicio,
	
	-- Se obtiene el dato a partir de la tabla de estado del beneficiario 
	-- (el script de estado de beneficiario utiliza la tabla "siu_toba_3_3_negocio_sga_perdida_regularidad")
	-- aparentemente la perdida de regularidad es por alumno y carrera y no por edicion capacitacion, por lo que si 
	-- la fecha de perdida de regularidad esta entre la fecha de inicio y fin de la edicion capacitacion se considera 
	-- que el alumno abandonó
	CASE WHEN 
			eb.fecha_baja IS NOT NULL
			AND eb.fecha_baja >= ed.fecha_inicio_dictado
			AND eb.fecha_baja <= ed.fecha_fin_dictado
		THEN date_parse(date_format(eb.fecha_baja, '%Y-%m-%d %h:%i%p'), '%Y-%m-%d %h:%i%p')
		ELSE NULL END fecha_abandono,
	
	date_parse(date_format(cert.fecha_egreso, '%Y-%m-%d %h:%i%p'), '%Y-%m-%d %h:%i%p') fecha_egreso,
	
	CAST(NULL AS INTEGER) porcentaje_asistencia,
    	
	-- Se obtiene el dato a partir de la tabla de estado del beneficiario
	CAST(eb.cantidad_materias_aprobadas AS VARCHAR) cant_aprobadas,

	-- Cuando el estado de beneficiario es preinscripto no tiene edicion capacitacion por lo tanto es NULL la edicion_capacitacion_id_new
	CASE WHEN eb.estado_beneficiario LIKE 'PREINSCRIPTO' 
	THEN NULL
	ELSE ed.id
	END edicion_capacitacion_id_new,
	
	-- Cuando el estado de beneficiario es preinscripto no tiene edicion capacitacion por lo tanto es NULL la id_edicion_capacitacion_old
	CASE WHEN eb.estado_beneficiario LIKE 'PREINSCRIPTO' 
	THEN NULL
	ELSE ed.id_edicion_capacitacion_old
	END id_edicion_capacitacion_old,
	 	
	-- Cuando el estado de beneficiario es preinscripto el id_capacitacion se toma de la tabla capacitacion en lugar de def edicion
	CASE WHEN eb.estado_beneficiario LIKE 'PREINSCRIPTO' 
	THEN dcmatch.id_new
	ELSE ed.id_capacitacion_new
	END id_capacitacion_new,
	
	-- Cuando el estado de beneficiario es preinscripto el id_capacitacion se toma de la tabla capacitacion en lugar de def edicion
	CASE WHEN eb.estado_beneficiario LIKE 'PREINSCRIPTO' 
	THEN dcmatch.tipo_capacitacion
	ELSE ed.tipo_capacitacion
	END tipo_capacitacion,
	
	vec.id_vecino,
	vec.id_broker,
	eb.estado_beneficiario AS estado_beneficiario

FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_view_siu_toba_3_3_negocio_mdp_personas_no_duplicates" nmp
LEFT JOIN "caba-piba-raw-zone-db"."siu_preinscripcion_public_sga_preinscripcion" pre ON (nmp.persona = pre.persona)
LEFT JOIN "caba-piba-raw-zone-db"."siu_preinscripcion_public_sga_preinscripcion_propuestas" pre_pro ON (pre.id_preinscripcion = pre_pro.id_preinscripcion)
LEFT JOIN "caba-piba-raw-zone-db"."siu_toba_3_3_negocio_mdp_personas_documentos" nmpd ON (nmpd.documento = nmp.documento_principal)
LEFT JOIN  "caba-piba-raw-zone-db"."siu_toba_3_3_negocio_sga_alumnos" alumnos ON (alumnos.persona=nmp.persona)
LEFT JOIN "caba-piba-raw-zone-db"."siu_toba_3_3_negocio_sga_insc_cursada" insc_cur ON (insc_cur.alumno=alumnos.alumno)
LEFT JOIN "caba-piba-raw-zone-db"."siu_toba_3_3_negocio_sga_comisiones" comi ON (comi.comision=insc_cur.comision)
LEFT JOIN "caba-piba-raw-zone-db"."siu_toba_3_3_negocio_sga_comisiones_propuestas" cp ON (comi.comision = cp.comision)
LEFT JOIN "caba-piba-raw-zone-db"."siu_toba_3_3_negocio_sga_planes" spl ON (spl.propuesta = cp.propuesta)
LEFT JOIN "caba-piba-raw-zone-db"."siu_toba_3_3_negocio_sga_turnos_cursadas" turno_c ON (turno_c.turno=comi.turno)
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_capacitacion_match" dcmatch ON (dcmatch.id_old = CAST(spl.plan AS VARCHAR) AND dcmatch.base_origen = 'SIU')
LEFT JOIN "caba-piba-raw-zone-db"."siu_toba_3_3_negocio_sga_periodos_lectivos" pl ON (comi.periodo_lectivo=pl.periodo_lectivo)
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_capacitacion" dc ON (dcmatch.id_new = dc.id_new AND dcmatch.base_origen = dc.base_origen)
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_establecimientos" est ON (est.id_old=CAST(comi.ubicacion AS VARCHAR) AND est.base_origen = dc.base_origen)
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_edicion_capacitacion" ed ON 
		(ed.id_edicion_capacitacion_old=
									(CAST(spl.plan AS VARCHAR) || '-' || CAST(spl.propuesta AS VARCHAR) || '-' || 
									CAST(pl.periodo_lectivo AS VARCHAR)  || '-' || CAST(est.id AS VARCHAR) || '-' ||
									CAST(dc.id_modalidad AS VARCHAR) || '-' || CAST(turno_c.turno AS VARCHAR)
									)
		--AND ed.id_establecimiento = comi.ubicacion
		AND ed.id_establecimiento = est.id
		AND ed.base_origen = 'SIU')

LEFT JOIN 
		(SELECT ca.alumno, ca_acum.comision, MIN(cla.fecha) fecha_inicio
		FROM 
		"caba-piba-raw-zone-db"."siu_toba_3_3_negocio_sga_clases_asistencia" ca
		LEFT JOIN "caba-piba-raw-zone-db"."siu_toba_3_3_negocio_sga_clases" cla ON (cla.clase = ca.clase)
		LEFT JOIN "caba-piba-raw-zone-db"."siu_toba_3_3_negocio_sga_clases_asistencia_acum" ca_acum ON (ca_acum.alumno = ca.alumno)
		GROUP BY ca.alumno, ca_acum.comision) finicio ON (finicio.comision=insc_cur.comision AND finicio.alumno=alumnos.alumno)
LEFT JOIN "caba-piba-raw-zone-db"."siu_toba_3_3_negocio_sga_certificados_otorg" cert ON (cert.alumno=alumnos.alumno AND cert.plan_version=alumnos.plan_version)
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_vecino" vec ON (
		vec.base_origen = 'SIU'
		AND vec.documento_broker = CAST(nmpd.nro_documento AS VARCHAR)
		AND vec.genero_broker = nmp.sexo
		)
LEFT JOIN "caba-piba-raw-zone-db"."siu_preinscripcion_public_sga_preinscripcion" p ON (p.persona=alumnos.persona)
LEFT JOIN "caba-piba-raw-zone-db"."siu_preinscripcion_public_sga_preinscripcion_propuestas" pp ON (p.id_preinscripcion = pp.id_preinscripcion and pp.propuesta=alumnos.propuesta)
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_tmp_estado_beneficiario_siu" eb 
	ON (
		(eb.estado_beneficiario!='PREINSCRIPTO' AND eb.id_broker=vec.id_broker 
		AND eb.plan_version=alumnos.plan_version AND eb.propuesta=alumnos.propuesta)
		-- cuando el estado es preinscripto no tiene plan_version
		OR
		(eb.estado_beneficiario='PREINSCRIPTO' AND eb.id_broker=vec.id_broker AND eb.propuesta=pre_pro.propuesta)
	)
GROUP BY
	alumnos.alumno,
	nmpd.nro_documento,
	eb.fecha_preinscripcion,
	p.fecha_registro,
	finicio.fecha_inicio,
	eb.fecha_baja,
	ed.fecha_inicio_dictado,
	ed.fecha_fin_dictado,
	cert.fecha_egreso,
	eb.cantidad_materias_aprobadas,
	ed.id,
	ed.id_edicion_capacitacion_old,
	ed.id_capacitacion_new,
	ed.tipo_capacitacion,
	dcmatch.id_new,
	CAST(dcmatch.id_old AS VARCHAR),
	dcmatch.tipo_capacitacion,
	vec.id_vecino,
	vec.id_broker,
	eb.estado_beneficiario
--</sql>--

-- 2.-  Crear tabla temporal incluyendo IEL
-- --<sql>--DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_tmp_cursada`;--</sql>--
--<sql>--
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_cursada" AS
SELECT * FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_cursada_1"
-- IEL
UNION
SELECT df.base_origen,
	   iel.nrodocumento identificacion_alumno,
	   iel.nrodocumento documento_broker,
	   null fecha_preinscripcion,
	   null fecha_inicio,
	   null fecha_abandono,
	   null fecha_egreso,
	   0 porcentaje_asistencia,
	   '0' cant_aprobadas,
	   null edicion_capacitacion_id_new,
	   null id_edicion_capacitacion_old,
	   df.id_new id_capacitacion_new,
	   df.tipo_capacitacion,
	   iel.broker_id||'-'||df.base_origen vecino_id,
	   iel.broker_id,
	   'PREINSCRIPTO' estado_beneficiario
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_analisis_iel" iel
INNER JOIN "caba-piba-staging-zone-db"."tbp_typ_def_capacitacion" df ON 
(levenshtein_distance(df.descrip_normalizada,iel.descrip_normalizada) <= 0.9) AND df.base_origen = iel.base_origen
WHERE NOT EXISTS (SELECT 1 FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_cursada_1" dcu
				  WHERE dcu.id_capacitacion_new = df.id_new AND 
						dcu.base_origen = df.base_origen AND 
						dcu.documento_broker = iel.nrodocumento)
GROUP BY
	df.base_origen,
	iel.nrodocumento,
	iel.nrodocumento,
	df.id_new,
	iel.curso,
	df.tipo_capacitacion,
	iel.broker_id||'-'||df.base_origen,
	iel.broker_id
--</sql>--