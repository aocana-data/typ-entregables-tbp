-- Copy of 2022.11.25 create_tbp_typ_def_capacitacion (Vcliente).sql 



--- CRM SOCIO LABORAL

CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_def_capacitacion" AS
SELECT base_origen||'-'||tipo_capacitacion||'-'||CAST(id AS VARCHAR) id,
	   id id_new,
       base_origen,
       tipo_capacitacion,
       descrip_normalizada,
       fecha_inicio,
       fecha_fin,
       categoria,
       estado
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_crmsl_capacitaciones"
UNION ALL
SELECT base_origen||'-'||tipo_capacitacion||'-'||CAST(id AS VARCHAR),
	   id,
       base_origen,
       tipo_capacitacion,
       descrip_normalizada,
       fecha_inicio,
       fecha_fin,
       categoria,
       estado
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_sienfo_capacitaciones"
UNION ALL
SELECT base_origen||'-'||tipo_capacitacion||'-'||CAST(id AS VARCHAR),
	   id,
       base_origen,
       tipo_capacitacion,
       descrip_normalizada,
       fecha_inicio,
       fecha_fin,
       categoria,
       estado
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_goet_capacitaciones"
UNION ALL
SELECT base_origen||'-'||tipo_capacitacion||'-'||CAST(id AS VARCHAR),
	   id,
       base_origen,
       tipo_capacitacion,
       descrip_normalizada,
       fecha_inicio,
       fecha_fin,
       categoria,
       estado
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_moodle_capacitaciones"
UNION ALL
SELECT base_origen||'-'||tipo_capacitacion||'-'||CAST(id AS VARCHAR),
	   id,
       base_origen,
       tipo_capacitacion,
       descrip_normalizada,
       fecha_inicio,
       fecha_fin,
       categoria,
       estado
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_siu_capacitaciones"


CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_def_capacitacion_match" AS
SELECT base_origen,
       tipo_capacitacion,
	   id_new,
       id_old
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_crmsl_match"
UNION ALL
SELECT base_origen,
       tipo_capacitacion,
	   id_new,
       id_old
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_sienfo_match"
UNION ALL
SELECT base_origen,
       tipo_capacitacion,
	   id_new,
       id_old
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_goet_match"
UNION ALL
SELECT base_origen,
       tipo_capacitacion,
	   id_new,
       id_old
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_moodle_match"
UNION ALL
SELECT base_origen,
       tipo_capacitacion,
	   id_new,
       id_old
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_siu_match"



-- Copy of 2022.11.25 create_tbp_typ_def_cursada (Vcliente).sql 



-- EDICION CAPACITACION SIENFO
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_cursada" AS
SELECT 'SIENFO' base_origen,
       sf.codigo_ct edicion_capacitacion_id_old,
	   sf.nrodoc identificacion_alumno,
       sf.nrodoc documento_broker,
       sfp.fecha_inc fecha_preinscripcion,
       sf.fecha_inc fecha_inicio,
       sf.fechabaja fecha_abandono,
       ed.fecha_fin_dictado fecha_egreso,
	   0 porcentaje_asistencia,
       '' estado,
       sf.aprobado cant_aprobadas,
       ed.id edicion_capacitacion_id,
       ed.capacitacion_id,
       ed.tipo_capacitacion,
       vec.vecino_id,
       vec.broker_id,
	   ebs.estado_beneficiario
FROM "caba-piba-raw-zone-db"."sienfo_fichas" sf
LEFT JOIN "caba-piba-raw-zone-db"."sienfo_fichas_preinscripcion" sfp ON (sfp.codigo_ct = sf.codigo_ct AND sf.nrodoc = sfp.nrodoc)
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_edicion_capacitacion" ed ON (ed.edicion_capacitacion_id_old = sf.codigo_ct AND ed.base_origen = 'SIENFO')
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_vecino" vec ON (vec.base_origen = 'SIENFO' AND vec.documento_broker = TRIM(sf.nrodoc))
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_tmp_estado_beneficiario_sienfo" ebs ON (ebs.nrodoc = sf.nrodoc AND ebs.codigo_ct = ed.edicion_capacitacion_id_old)

UNION
SELECT 'CRMSL' base_origen,
       ofc.op_oportun1d35rmacion_ida edicion_capacitacion_id_old,
       ofc.op_oportunidades_formacion_contactscontacts_idb identificacion_alumno,
       cs.numero_documento_c documento_broker,
       ofc.date_modified fecha_preinscripcion,
       NULL fecha_inicio,
       NULL fecha_abandono,
       NULL fecha_egreso,
       sc.porcentaje_asistencia_c porcentaje_asistencia,
       sc.estado_c estado,
       NULL cant_aprobadas,
       ed.id edicion_capacitacion_id,
       ed.capacitacion_id,
       ed.tipo_capacitacion,
       vec.vecino_id,
       vec.broker_id,
	   ebc.estado_beneficiario
FROM "caba-piba-raw-zone-db"."crm_sociolaboral_op_oportunidades_formacion_contacts_c" ofc
INNER JOIN "caba-piba-raw-zone-db"."crm_sociolaboral_contacts" co ON (co.id = ofc.op_oportunidades_formacion_contactscontacts_idb)
INNER JOIN "caba-piba-raw-zone-db"."crm_sociolaboral_contacts_cstm" cs ON (ofc.op_oportunidades_formacion_contactscontacts_idb = cs.id_c)
LEFT JOIN "caba-piba-raw-zone-db"."crm_sociolaboral_se_seguimiento_cstm" sc ON (sc.id_c = ofc.op_oportunidades_formacion_contactscontacts_idb)
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_edicion_capacitacion" ed ON (ed.edicion_capacitacion_id_old = ofc.op_oportun1d35rmacion_ida AND ed.base_origen = 'CRMSL')
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_vecino" vec ON (vec.base_origen = 'CRMSL' AND vec.cod_origen = ofc.op_oportunidades_formacion_contactscontacts_idb)
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_tmp_estado_beneficiario_crmsl" ebc ON (vec.base_origen = 'CRMSL' AND ebc.last_name = vec.apellido AND ebc.first_name = vec.nombre)
WHERE (co.lead_source = 'sociolaboral'
OR ((co.lead_source = 'rib') AND cs.forma_parte_interm_lab_c = 'si'))
AND cs.numero_documento_c IS NOT NULL



-- Copy of 2022.11.25 create_tbp_typ_def_edicion_capacitacion (Vcliente).sql 



-- EDICION CAPACITACION SIENFO
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_def_edicion_capacitacion" AS
SELECT row_number() OVER () AS id,
       ed.capacitacion_id_new,
	   ed.capacitacion_id,
	   ed.base_origen,
	   ed.tipo_capacitacion,
       ed.edicion_capacitacion_id edicion_capacitacion_id_old,
       ed.anio_dictado,
       ed.semestre_dictado,
       ed.fecha_inicio_dictado,
       ed.fecha_fin_dictado,
       ed.fecha_tope_movimientos,
       ed.nombre_turno,
	   ed.descrip_turno,
	   ed.inscripcion_habilitada,
       ed.activo,
	   ed.cant_inscriptos,
	   ed.cupo,
	   ed.modalidad,
       ed.nombre_modalidad,
       ed.descrip_modalidad,
       ed.cod_origen_establecimiento
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_edicion_capacitacion" ed



-- Copy of 2022.11.25 create_tbp_typ_tmp_capacitacion (Vcliente).sql 



--- CRM SOCIO LABORAL

CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_crmsl_1" AS
SELECT 'CRMSL' base_origen,
       'CURSO' tipo_capacitacion,
        CAST(of.id AS VARCHAR) capacitacion_id,
		CASE WHEN TRIM(UPPER(of.area)) IN ('GESTIÓN COMERCIAL','LIMPIEZA Y MANTENIMIENTO', 'MOZO Y CAMARERA', 'OPERARIO CALIFICADO') THEN
			TRIM(UPPER(of.area))
		 ELSE TRIM(REGEXP_REPLACE(TRIM(REGEXP_REPLACE(TRIM(REGEXP_REPLACE(REGEXP_REPLACE(UPPER(CASE
			WHEN of.name LIKE '%|%' THEN split_part(of.name,'|',2)
			WHEN of.name LIKE '%/%' THEN split_part(of.name,'/',2)
		ELSE of.name
		END),'[0-9]+|\ II$|\ I$|TURNO|MAÑANA|NOCHE|TARDE|TURNOS|LUNES|MARTES|MIERCOLES|JUEVES|VIERNES|BARRIO|MIÉRCOLES|MARZO|SEPTIEMBRE',''),'\.|\-', ' ')),'\ Y$|','')),'\(\ \)|\º|\  S$|PLAYON DE CHACARITA| RODRIGO BUENO Y  MUGICA|\ S$|RICCIARDELLI|FRAGA','')) END descrip_normalizada,
		of.name descrip_capacitacion,
        of.inicio fecha_inicio_dictado,
        of.fin fecha_fin_dictado,
		CASE WHEN UPPER(of.estado_curso) = 'FINALIZADA' THEN 0 ELSE 1 END estado,
		TRIM(UPPER(of.area)) categoria
FROM "caba-piba-raw-zone-db"."crm_sociolaboral_op_oportunidades_formacion" OF
INNER JOIN "caba-piba-raw-zone-db"."crm_sociolaboral_op_oportunidades_formacion_contacts_c" ofc  ON (of.id = ofc.op_oportun1d35rmacion_ida)
INNER JOIN "caba-piba-raw-zone-db"."crm_sociolaboral_contacts" co ON (co.id = ofc.op_oportunidades_formacion_contactscontacts_idb)
INNER JOIN "caba-piba-raw-zone-db"."crm_sociolaboral_contacts_cstm" cs ON (co.id = cs.id_c)
WHERE (co.lead_source = 'sociolaboral'
OR ((co.lead_source = 'rib') AND cs.forma_parte_interm_lab_c = 'si'))
GROUP BY CAST(of.id AS VARCHAR),
       of.name,
       of.inicio,
       of.fin,
       UPPER(of.estado_curso),
	   UPPER(of.area)

CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_crmsl_capacitaciones" AS
SELECT row_number() OVER () AS id,
       base_origen,
       tipo_capacitacion,
       descrip_normalizada,
       MIN(fecha_inicio_dictado) fecha_inicio,
       MAX(fecha_fin_dictado) fecha_fin,
       categoria,
       CASE WHEN SUM(estado) > 0 THEN 'ACTIVO' ELSE 'BAJA' END estado
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_crmsl_1" s1
GROUP BY base_origen,
       tipo_capacitacion,
       descrip_normalizada,
       categoria

CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_crmsl_match" AS
SELECT sc.base_origen,
       sc.tipo_capacitacion,
	   sc.id id_new,
       s1.capacitacion_id id_old
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_crmsl_1" s1
INNER JOIN "caba-piba-staging-zone-db"."tbp_typ_tmp_crmsl_capacitaciones" sc ON (sc.descrip_normalizada = s1.descrip_normalizada) AND (COALESCE(CAST(sc.categoria AS VARCHAR),'-') = COALESCE(CAST(s1.categoria AS VARCHAR),'-'))

-- SIENFO

CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_sienfo_1" AS
WITH sienfo_cap AS
(SELECT 'SIENFO' base_origen,
      'CARRERA' tipo_capacitacion,
       CAST(ca.id_carrera AS VARCHAR) capacitacion_id,
	   TRIM(REGEXP_REPLACE(TRIM(REGEXP_REPLACE(REGEXP_REPLACE(UPPER(ca.nom_carrera),'Í','I'),'[0-9]+|\ II$|\ I$|\(MÓDULO?\)', '')),'\(?MODULO\s?[A-Za-z0-9\s]+\)?|\(?MÓDULO\s?[A-Za-z0-9\s]+\)?','')) descrip_normalizada,
       UPPER(ca.nom_carrera) descrip_capacitacion,
       MIN(DATE_PARSE(CASE WHEN t.fecha = '0000-00-00' THEN NULL ELSE t.fecha END, '%Y-%m-%d')) fecha_inicio_dictado,
       MAX(DATE_PARSE(CASE WHEN t.fecha_fin = '0000-00-00' THEN NULL ELSE t.fecha_fin END, '%Y-%m-%d')) fecha_fin_dictado,
	   CASE WHEN SUM(CASE WHEN UPPER(te.nombre) = 'ACTIVO' THEN 1 ELSE 0 END) > 0 THEN 1 ELSE 0 END estado,
       UPPER(tc.nom_categoria) categoria
FROM "caba-piba-raw-zone-db"."sienfo_carreras" ca
INNER JOIN "caba-piba-raw-zone-db"."sienfo_talleres" t ON (t.id_carrera = ca.id_carrera)
LEFT JOIN "caba-piba-raw-zone-db"."sienfo_testado" te ON (ca.estado = te.valor)
-- REVISAR
LEFT JOIN "caba-piba-raw-zone-db"."sienfo_tcategorias" tc ON (ca.id_tcarrera = tc.id_tcategoria)
WHERE COALESCE(t.id_carrera,0) != 0
GROUP BY  CAST(ca.id_carrera AS VARCHAR),
       ca.nom_carrera,
       UPPER(tc.nom_categoria)
UNION
SELECT 'SIENFO' base_origen,
       'CURSO',
       CAST(cu.id_curso AS VARCHAR) capacitacion_id,
	   TRIM(REGEXP_REPLACE(TRIM(UPPER(cu.nom_curso)),'\(?MODULO\ CONTABLE\)?|\(?MÓDULO\ CONTABLE\)?|\(?MODULO\ JURIDICO\)?|\(?MÓDULO\ JURÍDICO\)?| \(NIDO SOLDATI\-CARRILLO\)?|\(RETIRO NORTE\)?|\(PRINGLES\)?|\(ARANGUREN\)?|\(ARANGUREN\-?JUNCAL\)?|\(ARANGUREN\-\MANANTIALES\)?|\(LAMBARÉ\)?|\(SAN NICOLÁS\)?|\(CORRALES\)?|\(JUNCAL\)?|\(JUAN A\.\ \GARCIA\)?|\(SARAZA\)?|\(RETIRO\)?|\(DON BOSCO\)?|\(ANCHORENA\)?','')) descrip_normalizada,
       UPPER(cu.nom_curso) descrip_capacitacion,
       MIN(DATE_PARSE(CASE WHEN t.fecha = '0000-00-00' THEN NULL ELSE t.fecha END, '%Y-%m-%d')) fecha_inicio_dictado,
       MAX(DATE_PARSE(CASE WHEN t.fecha_fin = '0000-00-00' THEN NULL ELSE t.fecha_fin END, '%Y-%m-%d')) fecha_fin_dictado,
       CASE WHEN SUM(CASE WHEN UPPER(te.nombre) = 'ACTIVO' THEN 1 ELSE 0 END) > 0 THEN 1 ELSE 0 END estado,
       UPPER(tc.nom_categoria)
FROM "caba-piba-raw-zone-db"."sienfo_cursos" cu
INNER JOIN "caba-piba-raw-zone-db"."sienfo_talleres" t ON (t.id_curso = cu.id_curso)
LEFT JOIN "caba-piba-raw-zone-db"."sienfo_testado" te ON (t.estado = te.valor)
LEFT JOIN "caba-piba-raw-zone-db"."sienfo_tcategorias" tc ON (tc.id_tcategoria = cu.id_tcategoria)
WHERE COALESCE(t.id_carrera,0) = 0
GROUP BY  CAST(cu.id_curso AS VARCHAR),
       cu.nom_curso,
       UPPER(tc.nom_categoria))
SELECT sienfo_cap.*
FROM sienfo_cap

CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_sienfo_capacitaciones" AS
SELECT row_number() OVER () AS id,
       base_origen,
       tipo_capacitacion,
       descrip_normalizada,
       MIN(fecha_inicio_dictado) fecha_inicio,
       MAX(fecha_fin_dictado) fecha_fin,
       categoria,
       CASE WHEN SUM(estado) > 0 THEN 'ACTIVO' ELSE 'BAJA' END estado
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_sienfo_1" s1
GROUP BY base_origen,
       tipo_capacitacion,
       descrip_normalizada,
       categoria

CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_sienfo_match" AS
SELECT sc.base_origen,
       sc.tipo_capacitacion,
	   sc.id id_new,
       s1.capacitacion_id id_old
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_sienfo_1" s1
INNER JOIN "caba-piba-staging-zone-db"."tbp_typ_tmp_sienfo_capacitaciones" sc ON (sc.tipo_capacitacion = s1.tipo_capacitacion AND sc.descrip_normalizada = s1.descrip_normalizada) AND (COALESCE(CAST(sc.categoria AS VARCHAR),'-') = COALESCE(CAST(s1.categoria AS VARCHAR),'-'))


-- GOET



CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_goet_1" AS
SELECT  'GOET' base_origen,
        CASE WHEN UPPER(t.detalle) IS NOT NULL THEN 'CARRERA' ELSE 'CURSO' END tipo_capacitacion,
        CASE WHEN UPPER(t.detalle) IS NOT NULL THEN CAST(n.IdNomenclador AS VARCHAR)||'-'||CAST(t.IdKeyTrayecto AS VARCHAR) ELSE CAST(n.IdNomenclador AS VARCHAR) END capacitacion_id,
        TRIM(REGEXP_REPLACE(REGEXP_REPLACE(TRIM(REGEXP_REPLACE(CASE WHEN UPPER(t.detalle) IS NOT NULL THEN UPPER(t.Detalle) ELSE UPPER(n.Detalle) END,'[0-9]+|\ II$|\ I$|\ III$|\ IV$|\ V$|\ VI$|\(MÓDULO?\)', '')),'\(?MODULO\s?[A-Za-z0-9\s]+\)?|\(?MÓDULO\s?[A-Za-z0-9\s]+\)?',''),'\(?NIVEL\s?[A-Za-z0-9\s]+\)?','')) descrip_normalizada,
		CASE WHEN UPPER(t.detalle) IS NOT NULL THEN UPPER(t.Detalle) ELSE UPPER(n.Detalle) END descrip_capacitacion,
        MIN(cc.iniciocurso) fecha_inicio_dictado,
	    MAX(cc.fincurso) fecha_fin_dictado,
	    CASE WHEN UPPER(en.Detalle) = 'ACTIVO' THEN 1 ELSE 0 END estado,
        COALESCE(UPPER(a.Detalle),  UPPER(f.Detalle)) AS categoria
FROM
    "caba-piba-raw-zone-db"."goet_centro_habilitacion_modulos" chm
INNER JOIN "caba-piba-raw-zone-db"."goet_centro_formacion" cf ON cf.IdCentro  = chm.IdCentro
INNER JOIN "caba-piba-raw-zone-db"."goet_centro_codigo_curso" cc ON cc.IdCtrHbModulo = chm.IdCtrHbModulo
INNER JOIN "caba-piba-raw-zone-db"."goet_nomenclador"  n ON n.IdNomenclador = chm.IdNomenclador
LEFT JOIN "caba-piba-raw-zone-db"."goet_area" a ON a.IdArea = n.IdArea
LEFT JOIN "caba-piba-raw-zone-db"."goet_familia" f ON f.IdFamilia = n.IdFamilia
LEFT JOIN "caba-piba-raw-zone-db"."goet_mudulosxtrayecto" m ON m.IdNomenclador = n.IdNomenclador
LEFT JOIN "caba-piba-raw-zone-db"."goet_trayecto" t ON t.IdKeyTrayecto = m.IdKeyTrayecto
LEFT JOIN "caba-piba-raw-zone-db"."goet_trayecto_nivel" tn ON tn.IdNivelTrayecto = t.IdNivelTrayecto
LEFT JOIN "caba-piba-raw-zone-db"."goet_estado_nomenclador" en ON en.IdEstadoNomenclador = n.IdEstadoNomenclador
GROUP BY UPPER(t.detalle),
         CAST(n.IdNomenclador AS VARCHAR)||'-'||CAST(t.IdKeyTrayecto AS VARCHAR),
		 CAST(n.IdNomenclador AS VARCHAR),
         UPPER(n.Detalle),
         UPPER(en.Detalle),
         UPPER(a.Detalle),
         UPPER(f.Detalle)

CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_goet_capacitaciones" AS
SELECT row_number() OVER () AS id,
       base_origen,
       tipo_capacitacion,
       CASE WHEN length(descrip_normalizada) = 0 THEN descrip_capacitacion ELSE descrip_normalizada END descrip_normalizada,
       MIN(fecha_inicio_dictado) fecha_inicio,
       MAX(fecha_fin_dictado) fecha_fin,
       categoria,
       CASE WHEN SUM(estado) > 0 THEN 'ACTIVO' ELSE 'BAJA' END estado
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_goet_1" s1
GROUP BY base_origen,
       tipo_capacitacion,
       CASE WHEN length(descrip_normalizada) = 0 THEN descrip_capacitacion ELSE descrip_normalizada END,
       categoria

CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_goet_match" AS
SELECT sc.base_origen,
       sc.tipo_capacitacion,
	   sc.id id_new,
       s1.capacitacion_id id_old
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_goet_1" s1
INNER JOIN "caba-piba-staging-zone-db"."tbp_typ_tmp_goet_capacitaciones" sc ON (sc.tipo_capacitacion = s1.tipo_capacitacion AND sc.descrip_normalizada = (CASE WHEN length(s1.descrip_normalizada) = 0 THEN s1.descrip_capacitacion ELSE s1.descrip_normalizada END)) AND (COALESCE(CAST(sc.categoria AS VARCHAR),'-') = COALESCE(CAST(s1.categoria AS VARCHAR),'-'))


-- MOODLE

CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_moodle_1" AS
SELECT 'MOODLE' base_origen,
       'CURSO' tipo_capacitacion,
       CAST(co.id AS VARCHAR) capacitacion_id,
       TRIM(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(UPPER(co.fullname),'[0-9]+|\ II$|\ I$|\(MÓDULO?\)', ''),'TURNO|MAÑANA|NOCHE|TARDE|\_ENE|\_FEB|\_MAR|\_ABR|\_MAY|\_JUN|\_JUL|\_AGO|\_SEP|\_OCT|\_NOV|\_DIC|\(\COMISIÓN \)|\_AULAA|CURSO\:|\_AULAB|\_AULAC|\_AULAD|\_AULA|\_F|\_E|_A|_B|_C|_D|\(\COMISIÓN\)|\ C$',''),'\.|\/|\__',' '),'\(?MODULO\s?[A-Za-z0-9\s]+\)?|\(?MÓDULO\s?[A-Za-z0-9\s]+\)?|MODULO','')) descrip_normalizada,
       UPPER(co.fullname) descrip_capacitacion,
       MIN(date_parse(date_format(from_unixtime(co.startdate),'%Y-%m-%d %h:%i%p'),'%Y-%m-%d %h:%i%p')) fecha_inicio_dictado,
       MAX(CASE WHEN co.enddate != 0 THEN
       date_parse(date_format(from_unixtime(co.enddate),'%Y-%m-%d %h:%i%p'),'%Y-%m-%d %h:%i%p')
       ELSE NULL END) fecha_fin_dictado,
       CASE WHEN co.enddate = 0 THEN 1 ELSE 0 END estado,
       TRIM(REGEXP_REPLACE(REGEXP_REPLACE(UPPER(cc.name),'[0-9]+|\ II$|\ I$|\(MÓDULO?\)', ''),'_','')) categoria
FROM "caba-piba-raw-zone-db"."moodle_dgtedu01_mdl_course" co
LEFT JOIN "caba-piba-raw-zone-db"."moodle_dgtedu01_mdl_course_categories" cc ON (co.category = cc.id)
WHERE cc.idnumber LIKE 'CAC%'
    -- CAC
    OR cc.idnumber LIKE 'FPE%'
    -- Habilidades/Formación para la empleabilidad
GROUP BY CAST(co.id AS VARCHAR),
       UPPER(co.fullname),
       co.enddate,
       UPPER(cc.name)

CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_moodle_capacitaciones" AS
SELECT row_number() OVER () AS id,
       base_origen,
       tipo_capacitacion,
       descrip_normalizada,
       MIN(fecha_inicio_dictado) fecha_inicio,
       MAX(fecha_fin_dictado) fecha_fin,
       categoria,
       CASE WHEN SUM(estado) > 0 THEN 'ACTIVO' ELSE 'BAJA' END estado
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_moodle_1" s1
GROUP BY base_origen,
       tipo_capacitacion,
       descrip_normalizada,
       categoria

CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_moodle_match" AS
SELECT sc.base_origen,
       sc.tipo_capacitacion,
	   sc.id id_new,
       s1.capacitacion_id id_old
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_moodle_1" s1
INNER JOIN "caba-piba-staging-zone-db"."tbp_typ_tmp_moodle_capacitaciones" sc ON (sc.tipo_capacitacion = s1.tipo_capacitacion AND sc.descrip_normalizada = (CASE WHEN length(s1.descrip_normalizada) = 0 THEN s1.descrip_capacitacion ELSE s1.descrip_normalizada END)) AND (COALESCE(CAST(sc.categoria AS VARCHAR),'-') = COALESCE(CAST(s1.categoria AS VARCHAR),'-'))

-- SIU

CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_siu_1" AS
WITH siu AS
(SELECT 'SIU' base_origen,
       'CARRERA' tipo_capacitacion,
       CAST(spl.plan AS VARCHAR) capacitacion_id,
       TRIM(REGEXP_REPLACE(UPPER(spl.nombre),'[0-9]+|\ II$|\ I$|\(MÓDULO?\)|IFTS|MODALIDAD|MODALIDAD I|\-', '')) descrip_normalizada,
       spl.nombre descrip_capacitacion,
       spl.fecha_entrada_vigencia fecha_inicio_dictado,
       spl.fecha_baja fecha_fin_dictado,
       CASE WHEN spl.estado = 'V' THEN 1 ELSE 0 END estado,
       'SIN CATEGORIA' categoria
FROM "caba-piba-raw-zone-db"."siu_toba_3_3_negocio_sga_planes" spl
GROUP BY CAST(spl.plan AS VARCHAR),
       spl.nombre,
       spl.fecha_entrada_vigencia,
       spl.fecha_baja,
       spl.estado)
SELECT *
FROM siu
WHERE siu.descrip_normalizada != ''

CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_siu_capacitaciones" AS
SELECT row_number() OVER () AS id,
       base_origen,
       tipo_capacitacion,
       descrip_normalizada,
       MIN(fecha_inicio_dictado) fecha_inicio,
       MAX(fecha_fin_dictado) fecha_fin,
       categoria,
       CASE WHEN SUM(estado) > 0 THEN 'ACTIVO' ELSE 'BAJA' END estado
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_siu_1" s1
GROUP BY base_origen,
       tipo_capacitacion,
       descrip_normalizada,
       categoria

CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_siu_match" AS
SELECT sc.base_origen,
       sc.tipo_capacitacion,
	   sc.id id_new,
       s1.capacitacion_id id_old
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_siu_1" s1
INNER JOIN "caba-piba-staging-zone-db"."tbp_typ_tmp_siu_capacitaciones" sc ON (sc.tipo_capacitacion = s1.tipo_capacitacion AND sc.descrip_normalizada = (CASE WHEN length(s1.descrip_normalizada) = 0 THEN s1.descrip_capacitacion ELSE s1.descrip_normalizada END)) AND (COALESCE(CAST(sc.categoria AS VARCHAR),'-') = COALESCE(CAST(s1.categoria AS VARCHAR),'-'))



-- Copy of 2022.11.25 create_tbp_typ_tmp_cursada (Vcliente).sql 



-- EDICION CAPACITACION SIENFO
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_cursada" AS
SELECT 'SIENFO' base_origen,
       sf.codigo_ct edicion_capacitacion_id_old,
	   sf.nrodoc identificacion_alumno,
       sf.nrodoc documento_broker,
       sfp.fecha_inc fecha_preinscripcion,
       sf.fecha_inc fecha_inicio,
       sf.fechabaja fecha_abandono,
       ed.fecha_fin_dictado fecha_egreso,
	   0 porcentaje_asistencia,
       '' estado,
       sf.aprobado cant_aprobadas,
       ed.id edicion_capacitacion_id,
       ed.capacitacion_id,
       ed.tipo_capacitacion,
       vec.vecino_id,
       vec.broker_id,
	   ebs.estado_beneficiario
FROM "caba-piba-raw-zone-db"."sienfo_fichas" sf
LEFT JOIN "caba-piba-raw-zone-db"."sienfo_fichas_preinscripcion" sfp ON (sfp.codigo_ct = sf.codigo_ct AND sf.nrodoc = sfp.nrodoc)
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_edicion_capacitacion" ed ON (ed.edicion_capacitacion_id_old = sf.codigo_ct AND ed.base_origen = 'SIENFO')
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_vecino" vec ON (vec.base_origen = 'SIENFO' AND vec.documento_broker = TRIM(sf.nrodoc))
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_tmp_estado_beneficiario_sienfo" ebs ON (ebs.nrodoc = sf.nrodoc AND ebs.codigo_ct = ed.edicion_capacitacion_id_old)

UNION
SELECT 'CRMSL' base_origen,
       ofc.op_oportun1d35rmacion_ida edicion_capacitacion_id_old,
       vec.cod_origen indentificacion_alumno,
       cs.numero_documento_c documento_broker,
       ofc.date_modified fecha_preinscripcion,
       NULL fecha_inicio,
       NULL fecha_abandono,
       NULL fecha_egreso,
       sc.porcentaje_asistencia_c porcentaje_asistencia,
       sc.estado_c estado,
       NULL cant_aprobadas,
       ed.id edicion_capacitacion_id,
       ed.capacitacion_id,
       ed.tipo_capacitacion,
       vec.vecino_id,
       vec.broker_id--,
	   --ebc.estado_beneficiario
FROM "caba-piba-raw-zone-db"."crm_sociolaboral_op_oportunidades_formacion_contacts_c" ofc
INNER JOIN "caba-piba-raw-zone-db"."crm_sociolaboral_contacts" co ON (co.id = ofc.op_oportunidades_formacion_contactscontacts_idb)
INNER JOIN "caba-piba-raw-zone-db"."crm_sociolaboral_contacts_cstm" cs ON (ofc.op_oportunidades_formacion_contactscontacts_idb = cs.id_c)
LEFT JOIN "caba-piba-raw-zone-db"."crm_sociolaboral_se_seguimiento_cstm" sc ON (sc.id_c = ofc.op_oportunidades_formacion_contactscontacts_idb)
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_edicion_capacitacion" ed ON (ed.edicion_capacitacion_id_old = ofc.op_oportun1d35rmacion_ida AND ed.base_origen = 'CRMSL')
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_vecino" vec ON (vec.base_origen = 'CRMSL' AND vec.cod_origen = CAST(co.id AS VARCHAR))
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_tmp_estado_beneficiario_crmsl" ebc ON (ebc.edicion_capacitacion_id_old = ed.edicion_capacitacion_id_old AND ebc.alumno_id_old = vec.codigo_origen )
WHERE (co.lead_source = 'sociolaboral'
OR ((co.lead_source = 'rib') AND cs.forma_parte_interm_lab_c = 'si'))
AND cs.numero_documento_c IS NOT NULL



-- Copy of 2022.11.25 create_tbp_typ_tmp_edicion_capacitacion (Vcliente).sql 



-- EDICION CAPACITACION SIENFO
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_edicion_capacitacion" AS
SELECT cm.base_origen,
	   cm.tipo_capacitacion,
       cm.id_new capacitacion_id_new,
	   dc.id capacitacion_id,
	   dc.descrip_normalizada,
	   dc.categoria,
       CAST(ca.id_carrera AS VARCHAR) capacitacion_id_old,
       ca.nom_carrera descrip_capacitacion_old,
       t.codigo_ct edicion_capacitacion_id,
       --'Carrera '||COALESCE(t.obs,' ') descrip_ed_capacitacion,
       SPLIT_PART(CASE WHEN t.fecha = '0000-00-00' THEN NULL ELSE t.fecha END,'-',1) anio_dictado,
       CASE WHEN CAST(SPLIT_PART(CASE WHEN t.fecha = '0000-00-00' THEN NULL ELSE t.fecha END,'-',2) AS INTEGER) <= 6 THEN 1 ELSE 2 END semestre_dictado,
       DATE_PARSE(CASE WHEN t.fecha = '0000-00-00' THEN NULL ELSE t.fecha END, '%Y-%m-%d') fecha_inicio_dictado,
       DATE_PARSE(CASE WHEN t.fecha_fin = '0000-00-00' THEN NULL ELSE t.fecha_fin END, '%Y-%m-%d') fecha_fin_dictado,
       DATE_PARSE('1900-01-01', '%Y-%m-%d') fecha_tope_movimientos, ---t.inscripcionh se puede usar esta columna sumada a la fecha de inicio
       CASE WHEN CAST(SUBSTR(COALESCE(t.horaini_lu, t.horaini_ma, t.horaini_mi, t.horaini_ju, t.horaini_vi, t.horaini_sa),1,2) AS INTEGER) BETWEEN 7 AND 12 THEN 'Mañana'
            WHEN CAST(SUBSTR(COALESCE(t.horaini_lu, t.horaini_ma, t.horaini_mi, t.horaini_ju, t.horaini_vi, t.horaini_sa),1,2) AS INTEGER) BETWEEN 13 AND 17 THEN 'Tarde'
			WHEN CAST(SUBSTR(COALESCE(t.horaini_lu, t.horaini_ma, t.horaini_mi, t.horaini_ju, t.horaini_vi, t.horaini_sa),1,2) AS INTEGER) BETWEEN 18 AND 24 THEN 'Noche'
			ELSE NULL END nombre_turno,
	   CASE WHEN t.lu = 1 THEN 'Lunes ' ELSE '' END ||
	   CASE WHEN t.ma = 1 THEN 'Martes ' ELSE '' END ||
	   CASE WHEN t.mi = 1 THEN 'Miercoles ' ELSE '' END ||
	   CASE WHEN t.ju = 1 THEN 'Jueves ' ELSE '' END ||
	   CASE WHEN t.vi = 1 THEN 'Viernes ' ELSE '' END ||
	   CASE WHEN t.sa = 1 THEN 'Sabado ' ELSE '' END descrip_turno,
	   CASE WHEN COALESCE(t.fecha_fin,'0000-00-00') = '0000-00-00' THEN (CASE WHEN te.nombre = 'Activo' THEN 'S' ELSE 'N' END) ELSE (CASE WHEN DATE_PARSE(CASE WHEN t.fecha_fin = '0000-00-00' THEN NULL ELSE t.fecha_fin END, '%Y-%m-%d') > CURRENT_DATE THEN 'S' ELSE 'N' END) END inscripcion_habilitada,
       CASE WHEN COALESCE(t.fecha_fin,'0000-00-00') = '0000-00-00' THEN (CASE WHEN te.nombre = 'Activo' THEN 'S' ELSE 'N' END) ELSE (CASE WHEN DATE_PARSE(CASE WHEN t.fecha_fin = '0000-00-00' THEN NULL ELSE t.fecha_fin END, '%Y-%m-%d') > CURRENT_DATE THEN 'S' ELSE 'N' END) END activo,
	   t.altas_total cant_inscriptos,
	   CAST(t.vacantes AS VARCHAR) cupo,
	   ' ' modalidad,
       ' ' nombre_modalidad,
       ' ' descrip_modalidad,
       CAST(t.id_centro AS VARCHAR) cod_origen_establecimiento
FROM "caba-piba-raw-zone-db"."sienfo_carreras" ca
INNER JOIN "caba-piba-raw-zone-db"."sienfo_talleres" t ON (t.id_carrera = ca.id_carrera)
LEFT JOIN "caba-piba-raw-zone-db"."sienfo_testado" te ON (t.estado = te.valor)
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_capacitacion_match" cm ON (cm.base_origen = 'SIENFO' AND cm.tipo_capacitacion = 'CARRERA' AND cm.id_old = CAST(ca.id_carrera AS VARCHAR))
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_capacitacion" dc ON (dc.id_new = cm.id_new AND dc.base_origen = cm.base_origen AND dc.tipo_capacitacion = cm.tipo_capacitacion)
WHERE t.id_carrera != 0
UNION
SELECT cm.base_origen,
	   cm.tipo_capacitacion,
       cm.id_new capacitacion_id_new,
	   dc.id,
	   dc.descrip_normalizada,
	   dc.categoria,
       CAST(cu.id_curso AS VARCHAR) capacitacion_id_old,
       cu.nom_curso descrip_capacitacion_old,
       t.codigo_ct edicion_capacitacion_id,
       --'Curso '||COALESCE(t.obs,' ') descrip_ed_capacitacion,
	   SPLIT_PART(CASE WHEN t.fecha = '0000-00-00' THEN NULL ELSE t.fecha END,'-',1) anio_dictado,
       CASE WHEN CAST(SPLIT_PART(CASE WHEN t.fecha = '0000-00-00' THEN NULL ELSE t.fecha END,'-',2) AS INTEGER) <= 6 THEN 1 ELSE 2 END semestre_dictado,
       DATE_PARSE(CASE WHEN t.fecha = '0000-00-00' THEN NULL ELSE t.fecha END, '%Y-%m-%d') fecha_inicio_dictado,
       DATE_PARSE(CASE WHEN t.fecha_fin = '0000-00-00' THEN NULL ELSE t.fecha_fin END, '%Y-%m-%d') fecha_fin_dictado,
       NULL fecha_tope_movimientos, ---t.inscripcionh se puede usar esta columna sumada a la fecha de inicio
       CASE WHEN CAST(SUBSTR(COALESCE(t.horaini_lu, t.horaini_ma, t.horaini_mi, t.horaini_ju, t.horaini_vi, t.horaini_sa),1,2) AS INTEGER) BETWEEN 7 AND 12 THEN 'Mañana'
            WHEN CAST(SUBSTR(COALESCE(t.horaini_lu, t.horaini_ma, t.horaini_mi, t.horaini_ju, t.horaini_vi, t.horaini_sa),1,2) AS INTEGER) BETWEEN 13 AND 17 THEN 'Tarde'
			WHEN CAST(SUBSTR(COALESCE(t.horaini_lu, t.horaini_ma, t.horaini_mi, t.horaini_ju, t.horaini_vi, t.horaini_sa),1,2) AS INTEGER) BETWEEN 18 AND 24 THEN 'Noche'
			ELSE NULL END nombre_turno,
	   CASE WHEN t.lu = 1 THEN 'Lunes ' ELSE '' END ||
	   CASE WHEN t.ma = 1 THEN 'Martes ' ELSE '' END ||
	   CASE WHEN t.mi = 1 THEN 'Miercoles ' ELSE '' END ||
	   CASE WHEN t.ju = 1 THEN 'Jueves ' ELSE '' END ||
	   CASE WHEN t.vi = 1 THEN 'Viernes ' ELSE '' END ||
	   CASE WHEN t.sa = 1 THEN 'Sabado ' ELSE '' END descrip_turno,
	   CASE WHEN COALESCE(t.fecha_fin,'0000-00-00') = '0000-00-00' THEN (CASE WHEN te.nombre = 'Activo' THEN 'S' ELSE 'N' END) ELSE (CASE WHEN DATE_PARSE(CASE WHEN t.fecha_fin = '0000-00-00' THEN NULL ELSE t.fecha_fin END, '%Y-%m-%d') > CURRENT_DATE THEN 'S' ELSE 'N' END) END inscripcion_habilitada,
       CASE WHEN COALESCE(t.fecha_fin,'0000-00-00') = '0000-00-00' THEN (CASE WHEN te.nombre = 'Activo' THEN 'S' ELSE 'N' END) ELSE (CASE WHEN DATE_PARSE(CASE WHEN t.fecha_fin = '0000-00-00' THEN NULL ELSE t.fecha_fin END, '%Y-%m-%d') > CURRENT_DATE THEN 'S' ELSE 'N' END) END activo,
	   t.altas_total cant_inscriptos,
	   CAST(t.vacantes AS VARCHAR) cupo,
	   NULL modalidad,
       NULL nombre_modalidad,
       NULL descrip_modalidad,
       CAST(t.id_centro AS VARCHAR) cod_origen_establecimiento
FROM "caba-piba-raw-zone-db"."sienfo_cursos" cu
INNER JOIN "caba-piba-raw-zone-db"."sienfo_talleres" t ON (t.id_curso = cu.id_curso)
LEFT JOIN "caba-piba-raw-zone-db"."sienfo_testado" te ON (t.estado = te.valor)
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_capacitacion_match" cm ON (cm.base_origen = 'SIENFO' AND cm.tipo_capacitacion = 'CURSO' AND cm.id_old = CAST(cu.id_curso AS VARCHAR))
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_capacitacion" dc ON (dc.id_new = cm.id_new AND dc.base_origen = cm.base_origen AND dc.tipo_capacitacion = cm.tipo_capacitacion)
WHERE COALESCE(t.id_carrera,0) = 0
UNION
-- EDICIONES CAPACITACIONES CRMSL

SELECT cm.base_origen,
	   cm.tipo_capacitacion,
       cm.id_new capacitacion_id_new,
	   dc.id,
	   dc.descrip_normalizada,
	   dc.categoria,
       CAST(of.id AS VARCHAR) capacitacion_id_old,
       of.name descrip_capacitacion_old,
       CAST(of.id AS VARCHAR) edicion_capacitacion_id,
       --of.name descrip_ed_capacitacion,
	   CASE WHEN of.inicio IS NOT NULL THEN SPLIT_PART(CAST(of.inicio AS VARCHAR),'-',1) ELSE NULL END anio_dictado,
       CASE WHEN of.inicio IS NOT NULL THEN (CASE WHEN CAST(SPLIT_PART(CAST(of.inicio AS VARCHAR),'-',2) AS INTEGER) <= 6 THEN 1 ELSE 2 END) ELSE NULL END semestre_dictado,
       of.inicio fecha_inicio_dictado,
       of.fin fecha_fin_dictado,
       NULL fecha_tope_movimientos,
       NULL nombre_turno,
       NULL descrip_turno,
       CASE WHEN of.fin IS NULL THEN (CASE WHEN estado_inscripcion = 'finalizada' THEN 'N' ELSE 'S' END) ELSE (CASE WHEN of.fin > CURRENT_DATE THEN 'S' ELSE 'N' END) END inscripcion_habilitada,
       CASE WHEN of.fin IS NULL THEN (CASE WHEN estado_curso = 'finalizada' THEN 'N' ELSE 'S' END) ELSE (CASE WHEN of.fin > CURRENT_DATE THEN 'S' ELSE 'N' END) END activo,
       of.inscriptos cant_inscriptos,
       CAST(of.cupos AS VARCHAR) cupo,
       CASE WHEN of.modalidad = 'virtual' THEN 'V'
			WHEN of.modalidad = 'semi' THEN 'S'
			WHEN of.modalidad = 'presencial' THEN 'P' END modalidad, -- P, S, V
       CASE WHEN of.modalidad = 'virtual' THEN 'Virtual'
			WHEN of.modalidad = 'semi' THEN 'Presencial y Virtual'
			WHEN of.modalidad = 'presencial' THEN 'Presencial' END nombre_modalidad, -- presencial, semipresncial, virtual
       NULL descrip_modalidad,
       CAST(of.sede AS VARCHAR) cod_origen_establecimiento
FROM "caba-piba-raw-zone-db"."crm_sociolaboral_op_oportunidades_formacion" OF
INNER JOIN "caba-piba-raw-zone-db"."crm_sociolaboral_op_oportunidades_formacion_contacts_c" ofc  ON (of.id = ofc.op_oportun1d35rmacion_ida)
INNER JOIN "caba-piba-raw-zone-db"."crm_sociolaboral_contacts" co ON (co.id = ofc.op_oportunidades_formacion_contactscontacts_idb)
INNER JOIN "caba-piba-raw-zone-db"."crm_sociolaboral_contacts_cstm" cs ON (co.id = cs.id_c)
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_capacitacion_match" cm ON (cm.base_origen = 'CRMSL' AND cm.tipo_capacitacion = 'CURSO' AND cm.id_old = CAST(of.id AS VARCHAR))
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_capacitacion" dc ON (dc.id_new = cm.id_new AND dc.base_origen = cm.base_origen AND dc.tipo_capacitacion = cm.tipo_capacitacion)
WHERE (co.lead_source = 'sociolaboral'
OR ((co.lead_source = 'rib') AND cs.forma_parte_interm_lab_c = 'si'))
GROUP BY cm.base_origen,
	   cm.tipo_capacitacion,
       cm.id_new,
	   dc.id,
	   dc.descrip_normalizada,
	   dc.categoria,
       CAST(of.id AS VARCHAR),
	   of.name,
       CAST(of.id AS VARCHAR),
	   of.inicio,
       of.fin,
	   of.estado_inscripcion,
	   of.estado_curso,
	   of.inscriptos,
       CAST(of.cupos AS VARCHAR),
       of.modalidad,
	   CAST(of.sede AS VARCHAR)



-- Copy of 2022.11.25 create_tbp_typ_tmp_estado_beneficiario_crmsl (Vcliente).sql 



-- Query Estado de Beneficiario para athena
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_estado_beneficiario_crmsl" AS
-- Query Estado de Beneficiario para athena
WITH seguimientos_calculado0 AS (
SELECT
	off.id,
	ofs.op_oportunidades_formacion_se_seguimiento_1se_seguimiento_idb ,
	s.name
	 , s.date_modified
	 , dense_rank() OVER(PARTITION BY off.id , s.name ORDER BY s.date_modified DESC) AS max_date
FROM
	"caba-piba-raw-zone-db".crm_sociolaboral_op_oportunidades_formacion OFF
LEFT JOIN "caba-piba-raw-zone-db".crm_sociolaboral_op_oportunidades_formacion_se_seguimiento_1_c ofs ON
	ofs.op_oportun868armacion_ida = off.id
LEFT JOIN "caba-piba-raw-zone-db".crm_sociolaboral_se_seguimiento s ON
	s.id = ofs.op_oportunidades_formacion_se_seguimiento_1se_seguimiento_idb
	-- group by off.id, s.name
	),
seguimientos_calculado AS (
-- max_date : es usado como col de seguimiento0
-- se usa dense_rank buscando la fecha maxima
	SELECT
		sc0.id,
		sc0.op_oportunidades_formacion_se_seguimiento_1se_seguimiento_idb ,
		sc0.name
		, sc0.date_modified
		FROM seguimientos_calculado0 AS sc0
		 WHERE sc0.max_date = 1
),
resultado AS (
SELECT
	c.id,
	c.first_name,
	c.last_name,
	c.lead_source,
	cc.forma_parte_interm_lab_c,
	off.id AS id_formacion,
	off.name,
	off.inicio,
	off.fin,
	CASE
		WHEN estado_c = 'incompleto' THEN 'baja'
		-- convertir sus opciones a nuestras opciones
		WHEN estado_c = 'en_curso' THEN 'regular'
		WHEN estado_c = 'finalizado' THEN 'egresado'
		WHEN estado_c = 'nunca_inicio' THEN 'baja'
		-- abandono al inicio
		WHEN estado_c IS NOT NULL THEN estado_c
		WHEN estado_c IS NULL THEN (
	CASE
			-- when off.name is null then 'no aplica_c'
		WHEN off.name IS NOT NULL THEN (
		CASE
				WHEN off.inicio < off.fin THEN 'egresado'
				WHEN off.fin IS NULL THEN 'regular'
				WHEN /*off.inicio is null and*/
				off.fin < CURRENT_DATE THEN 'egresado'
				-- when off.inicio > off.fin then 'error_fecha_c'
			END
		 )
		END)
	END AS estado_n,
	sc.estado_c
FROM
	"caba-piba-raw-zone-db".crm_sociolaboral_contacts c
INNER JOIN "caba-piba-raw-zone-db".crm_sociolaboral_contacts_cstm cc ON
	cc.id_c = c.id
INNER JOIN "caba-piba-raw-zone-db".crm_sociolaboral_op_oportunidades_formacion_contacts_c ofc ON
	ofc.op_oportunidades_formacion_contactscontacts_idb = c.id
INNER JOIN "caba-piba-raw-zone-db".crm_sociolaboral_op_oportunidades_formacion OFF ON
	off.id = ofc.op_oportun1d35rmacion_ida
	-- left join crm_sociolaboral_op_oportunidades_formacion_se_seguimiento_1_c ofs ON ofs.op_oportun868armacion_ida = off.id -- acÃ¡ limitr para que solo traiga el que coincide con c.id
LEFT JOIN seguimientos_calculado SCN ON
	off.id = scn.id
	AND replace(trim(concat_ws(' ', c.first_name, c.last_name)), ' ', ' ') = replace(trim(scn.name), '  ', ' ')
LEFT JOIN "caba-piba-raw-zone-db".crm_sociolaboral_op_oportunidades_formacion_se_seguimiento_1_c ofs ON
	ofs.op_oportunidades_formacion_se_seguimiento_1se_seguimiento_idb = scn.op_oportunidades_formacion_se_seguimiento_1se_seguimiento_idb
LEFT JOIN "caba-piba-raw-zone-db".crm_sociolaboral_se_seguimiento s ON
	s.id = ofs.op_oportunidades_formacion_se_seguimiento_1se_seguimiento_idb
LEFT JOIN "caba-piba-raw-zone-db".crm_sociolaboral_se_seguimiento_cstm sc ON
	sc.id_c = s.id
WHERE
	(c.lead_source IN ('sociolaboral')
		OR ((c.lead_source IN ('rib'))
			AND forma_parte_interm_lab_c IN ('si')
		))
		)
SELECT id alumno_id_old,
	   id_formacion edicion_capacitacion_id_old,
	   UPPER(first_name) first_name,
	   UPPER(last_name) last_name,
	   name descrip_capacitacion,
	   inicio,
	   fin,
	   UPPER(estado_n) estado_beneficiario
FROM resultado



