-- Copy of 2022.11.18 create_tbp_typ_def_capacitacion (Vcliente).sql 



--- CRM SOCIO LABORAL

CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_def_capacitacion" AS
SELECT id,
       base_origen,
       tipo_capacitacion,
       descrip_normalizada,
       fecha_inicio,
       fecha_fin,
       categoria,
       estado
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_crmsl_capacitaciones"
UNION ALL
SELECT id,
       base_origen,
       tipo_capacitacion,
       descrip_normalizada,
       fecha_inicio,
       fecha_fin,
       categoria,
       estado
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_sienfo_capacitaciones"
UNION ALL
SELECT id,
       base_origen,
       tipo_capacitacion,
       descrip_normalizada,
       fecha_inicio,
       fecha_fin,
       categoria,
       estado
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_goet_capacitaciones"
UNION ALL
SELECT id,
       base_origen,
       tipo_capacitacion,
       descrip_normalizada,
       fecha_inicio,
       fecha_fin,
       categoria,
       estado
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_moodle_capacitaciones"
UNION ALL
SELECT id,
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



-- Copy of 2022.11.18 create_tbp_typ_def_vecino (Vcliente).sql 



-- DEBERÍA SOLUCIONARSE EL PROBLEMA DE LOS DUPLICADOS

CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_def_vecino" AS
WITH tmp_vec AS
(SELECT vec.broker_id_din||'-'||vec.base_origen vecino_id,
	   vec.base_origen,
	   vec.cod_origen,
	   vec.broker_id_din broker_id,
	   vec.broker_id_est,
	   vec.tipo_documento,
	   vec.tipo_doc_broker,
	   vec.documento_broker,
	   vec.nombre,
	   vec.apellido,
	   vec.fecha_nacimiento,
	   vec.genero_broker,
	   vec.nacionalidad,
	   vec.descrip_nacionalidad,
	   vec.nacionalidad_broker,
	   vec.nombre_valido,
	   vec.apellido_valido,
	   CASE WHEN bo.id = vec.broker_id_din THEN 1 ELSE 0 END broker_id_valido,
	   CASE WHEN CAST(do.documento_broker AS VARCHAR) = vec.documento_broker THEN 1 ELSE 0 END dni_valido
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_vecino" vec
LEFT JOIN "caba-piba-staging-zone-db"."tbp_broker_def_broker_general" bo ON (bo.id = vec.broker_id_din)
LEFT JOIN "caba-piba-staging-zone-db"."tbp_broker_def_broker_general" DO ON (CAST(do.documento_broker AS VARCHAR) = vec.documento_broker AND do.id != vec.broker_id_din)
WHERE vec.broker_id_din IS NOT NULL
GROUP BY vec.broker_id_din,
		 vec.base_origen,
		 vec.base_origen,
	     vec.cod_origen,
	     vec.broker_id_din,
	     vec.broker_id_est,
	     vec.tipo_documento,
	     vec.tipo_doc_broker,
	     vec.documento_broker,
	     vec.nombre,
	     vec.apellido,
	     vec.fecha_nacimiento,
	     vec.genero_broker,
	     vec.nacionalidad,
	     vec.descrip_nacionalidad,
	     vec.nacionalidad_broker,
	     vec.nombre_valido,
	     vec.apellido_valido,
		 bo.id,
		 CAST(do.documento_broker AS VARCHAR))
SELECT tmp_vec.vecino_id,
	   tmp_vec.base_origen,
	   tmp_vec.cod_origen,
	   tmp_vec.broker_id,
	   tmp_vec.broker_id_est,
	   tmp_vec.tipo_documento,
	   tmp_vec.tipo_doc_broker,
	   tmp_vec.documento_broker,
	   tmp_vec.nombre,
	   tmp_vec.apellido,
	   tmp_vec.fecha_nacimiento,
	   tmp_vec.genero_broker,
	   tmp_vec.nacionalidad,
	   tmp_vec.descrip_nacionalidad,
	   tmp_vec.nacionalidad_broker,
	   tmp_vec.nombre_valido,
	   tmp_vec.apellido_valido,
	   tmp_vec.broker_id_valido,
	   CASE WHEN tmp_vec.broker_id_valido = 1 AND tmp_vec.dni_valido = 1 THEN 0
	        WHEN tmp_vec.broker_id_valido = 0 AND tmp_vec.dni_valido = 1 THEN 1
			ELSE 0 END dni_valido
FROM tmp_vec



-- Copy of 2022.11.18 create_tbp_typ_tmp_capacitacion (Vcliente).sql 



--- CRM SOCIO LABORAL

CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_crmsl_1" AS
SELECT 'CRMSL' base_origen,
       'CURSO' tipo_capacitacion,
        CAST(of.id AS VARCHAR) capacitacion_id,
		TRIM(REGEXP_REPLACE(TRIM(REGEXP_REPLACE(REGEXP_REPLACE(UPPER(CASE
			WHEN of.name LIKE '%|%' THEN split_part(of.name,'|',2)
			WHEN of.name LIKE '%/%' THEN split_part(of.name,'/',2)
		ELSE of.name
		END),'[0-9]+|\ II$|\ I$|TURNO|MAÑANA|NOCHE|TARDE|TURNOS|LUNES|MARTES|MIERCOLES|JUEVES|VIERNES|BARRIO|MIÉRCOLES|MARZO|SEPTIEMBRE',''),'\.|\-', ' ')),'\ Y$|','')) descrip_normalizada,
		of.name descrip_capacitacion,
        of.inicio fecha_inicio_dictado,
        of.fin fecha_fin_dictado,
		CASE WHEN UPPER(of.estado_curso) = 'FINALIZADA' THEN 0 ELSE 1 END estado,
		UPPER(of.area) categoria
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
	   TRIM(REGEXP_REPLACE(TRIM(REGEXP_REPLACE(UPPER(ca.nom_carrera),'[0-9]+|\ II$|\ I$|\(MÓDULO?\)', '')),'\(?MODULO\s?[A-Za-z0-9\s]+\)?|\(?MÓDULO\s?[A-Za-z0-9\s]+\)?','')) descrip_normalizada,
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
       TRIM(REGEXP_REPLACE(TRIM(REGEXP_REPLACE(REGEXP_REPLACE(UPPER(cu.nom_curso),'Í','I'),'[0-9]+|\ II$|\ I$|\(MÓDULO?\)', '')),'\(?MODULO\s?[A-Za-z0-9\s]+\)?|\(?MÓDULO\s?[A-Za-z0-9\s]+\)?','')),
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



-- Copy of 2022.11.18 create_tbp_typ_tmp_capacitacion ASI (Vcliente).sql 



DROP TABLE IF EXISTS "caba-piba-staging-zone-db"."tbp_typ_tmp_capacitacion_asi";
DROP TABLE `tbp_typ_tmp_capacitacion_asi`;

-- 1.- Crear tabla capacitacion
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_capacitacion_asi" AS
SELECT c.id capacitacion_id,
       c.codigo codigo_capacitacion,
       c.nombre descrip_capacitacion,
       c.tipo_formacion_origen_id tipo_formacion,
	   c.programa_id,
	   p.base_origen,
       tf.nombre descrip_tipo_formacion,
	   c.modalidad_id,
	   m.nombre descrip_modalidad,
	   c.categoria_back_id,
	   cb.nombre descrip_back,
	   CASE WHEN cb.tipo = 'sector_estrategico_asociado' THEN 'SECTOR PRODUCTIVO'
		    ELSE UPPER(cb.tipo) END tipo_capacitacion,
	   c.categoria_front_id,
	   cf.nombre descrip_front,
	   c.detalle detalle_capacitacion,
	   c.estado estado_capacitacion,
       cd.seguimiento_personalizado,
       cd.soporte_online,
       cd.incentivos_terminalidad,
	   cd.exclusividad_participantes,
	   cd.otorga_certificado,
	   cd.filtro_ingreso,
	   cd.frecuencia_oferta_anual,
       c.duracion_cantidad,
       c.duracion_medida,
       c.duracion_dias,
       cd.duracion_hs_reloj,
	   cd.vacantes
FROM "caba-piba-raw-zone-db"."api_asi_capacitacion" c
INNER JOIN "caba-piba-raw-zone-db"."api_asi_tipo_formacion_origen" tf
ON (tf.id = c.tipo_formacion_origen_id)
LEFT JOIN "caba-piba-raw-zone-db"."api_asi_capacitacion_data_lake" cd
ON (cd.id = c.id)
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_programa" p ON (p.programa_id = c.programa_id)
LEFT JOIN "caba-piba-raw-zone-db"."api_asi_categoria_back" cb ON (cb.id = c.categoria_back_id)
LEFT JOIN "caba-piba-raw-zone-db"."api_asi_categoria_front" cf ON (cf.id = c.categoria_front_id)
LEFT JOIN "caba-piba-raw-zone-db"."api_asi_modalidad" m ON (m.id = c.modalidad_id)

-- 2.-  Crear tabla aptitudes para cada capacitacion
DROP TABLE `tbp_typ_def_aptitudes_asi`;
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_aptitudes_asi" AS
SELECT c.id capacitacion_id,
       c.codigo codigo_capacitacion,
       ac.aptitud_id,
       a.nombre descrip_aptitud
FROM "caba-piba-raw-zone-db"."api_asi_capacitacion" c
INNER JOIN "caba-piba-raw-zone-db"."api_asi_aptitud_capacitacion" ac
ON (ac.capacitacion_id = c.id)
INNER JOIN "caba-piba-raw-zone-db"."api_asi_aptitud" a
ON (ac.aptitud_id = a.id)



-- Copy of 2022.11.18 create_tbp_typ_tmp_vecino (Vcliente).sql 



-- Entidad Vecino Basi SIU
-- BROKER_ID estática 4 pos tipo de documento - 11 posiciones numero doc - 1 posicion genero - 3 posiciones nac
DROP TABLE IF EXISTS "caba-piba-staging-zone-db"."tbp_typ_tmp_vecino";
DROP TABLE `tbp_typ_tmp_vecino`;



CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_vecino" AS
SELECT 'SIU' base_origen,
       CAST(nmp.persona AS VARCHAR) cod_origen,
	   CONCAT((CASE WHEN nmtd.desc_abreviada IN ('DNI',
                                         'LC',
                                         'LE',
                                         'CI',
                                         'CUIT',
                                         'CUIL') THEN nmtd.desc_abreviada
            WHEN nmtd.desc_abreviada IN ('CM', 'CD') THEN 'CE'
            WHEN nmtd.desc_abreviada = 'PAS' THEN 'PE'
            WHEN nmtd.desc_abreviada = 'DNT' THEN 'DNI'
            WHEN nmtd.desc_abreviada IN ('CDI', 'CC') THEN 'NN'
            ELSE 'NN'
            END
            ),
		   CASE WHEN nmtd.desc_abreviada IN ('DNI',
                                         'LC',
                                         'LE',
                                         'CI',
                                         'CUIT',
                                         'CUIL') THEN REGEXP_REPLACE(UPPER(nmpd.nro_documento),'[A-Za-z]+|\.','') ELSE
           CAST(nmpd.nro_documento AS VARCHAR) END,
           nmp.sexo,
          (CASE WHEN UPPER(SUBSTR(nmn.descripcion,1,3)) = 'ARG' THEN 'ARG'
           ELSE 'NN'
           END)) broker_id_din,
	   CONCAT(RPAD((CASE WHEN nmtd.desc_abreviada IN ('DNI',
                                         'LC',
                                         'LE',
                                         'CI',
                                         'CUIT',
                                         'CUIL') THEN nmtd.desc_abreviada
            WHEN nmtd.desc_abreviada IN ('CM', 'CD') THEN 'CE'
            WHEN nmtd.desc_abreviada = 'PAS' THEN 'PE'
            WHEN nmtd.desc_abreviada = 'DNT' THEN 'DNI'
            WHEN nmtd.desc_abreviada IN ('CI', 'CDI', 'CC') THEN 'NN'
            ELSE 'NN'
            END
            ),4,' '),
           LPAD((CASE WHEN nmtd.desc_abreviada IN ('DNI',
                                         'LC',
                                         'LE',
                                         'CI',
                                         'CUIT',
                                         'CUIL') THEN REGEXP_REPLACE(UPPER(nmpd.nro_documento),'[A-Za-z]+|\.','') ELSE
           CAST(nmpd.nro_documento AS VARCHAR) END),11,'0'),
           nmp.sexo,
          (CASE WHEN UPPER(SUBSTR(nmn.descripcion,1,3)) = 'ARG' THEN 'ARG'
           ELSE 'NNN'
           END)) broker_id_est,
       CAST(nmpd.tipo_documento AS VARCHAR) tipo_documento,
       CASE WHEN nmtd.desc_abreviada IN ('DNI',
                                         'LC',
                                         'LE',
                                         'CI',
                                         'CUIT',
                                         'CUIL') THEN nmtd.desc_abreviada
            WHEN nmtd.desc_abreviada IN ('CM', 'CD') THEN 'CE'
            WHEN nmtd.desc_abreviada = 'PAS' THEN 'PE'
            WHEN nmtd.desc_abreviada = 'DNT' THEN 'DNI'
            WHEN nmtd.desc_abreviada IN ('CI', 'CDI', 'CC') THEN 'NN'
            ELSE 'NN'
            END tipo_doc_broker,
       CASE WHEN nmtd.desc_abreviada IN ('DNI',
                                         'LC',
                                         'LE',
                                         'CI',
                                         'CUIT',
                                         'CUIL') THEN REGEXP_REPLACE(UPPER(nmpd.nro_documento),'[A-Za-z]+|\.','') ELSE
           CAST(nmpd.nro_documento AS VARCHAR) END documento_broker,
       UPPER(nmp.nombres) nombre,
       UPPER(nmp.apellido) apellido,
       nmp.fecha_nacimiento,
       nmp.sexo genero_broker,
       CAST(nmp.nacionalidad AS VARCHAR) nacionalidad,
       nmn.descripcion descrip_nacionalidad,
       CASE WHEN UPPER(SUBSTR(nmn.descripcion,1,3)) = 'ARG' THEN 'ARG'
       ELSE 'NN'
       END nacionalidad_broker,
	   (CASE WHEN ((UPPER(nmp.nombres) IS NULL) OR (("length"(UPPER(nmp.nombres)) < 3) AND (NOT ("upper"(UPPER(nmp.nombres)) IN ('BO', 'GE', 'HE', 'LI', 'LU', 'QI', 'WU', 'XI', 'XU', 'YE', 'YI', 'YU')))) OR (UPPER(nmp.nombres) LIKE '%PRUEBA%')) THEN 0 ELSE 1 END) nombre_valido,
	   (CASE WHEN ((UPPER(nmp.apellido) IS NULL) OR (("length"(UPPER(nmp.apellido)) < 3) AND (NOT ("upper"(UPPER(nmp.apellido)) IN ('AL', 'AM', 'AN', 'BO', U&'B\00D3', 'CO', 'DE', 'DO', 'DU', 'FU', 'GE', 'GO', 'GU', 'HA', 'HE', 'HO', 'HU', 'IM', 'IN', 'IS', 'JI', 'JO', 'JU', 'KE', 'KI', 'KO', 'KU', 'LI', 'LO', 'LU', 'MA', 'MO', 'MU', 'NA', 'NG', 'NI', 'NO', 'OH', 'OU', 'PI', 'PO', 'PY', 'QI', 'QU', 'RA', 'RE', U&'R\00C9', 'RO', 'RU', 'SA', U&'S\00C1', 'SO', 'SU', 'TU', 'UM', 'UZ', 'WU', 'XU', 'YA', 'YE', 'YI', 'YO', 'YU')))) OR (UPPER(nmp.apellido) LIKE '%PRUEBA%')) THEN 0 ELSE 1 END) apellido_valido,
	   CAST(nmpd.nro_documento AS VARCHAR) documento_original
FROM "caba-piba-raw-zone-db"."siu_toba_3_3_negocio_mdp_personas" nmp
INNER JOIN "caba-piba-raw-zone-db"."siu_toba_3_3_negocio_sga_alumnos" nsa ON nsa.persona = nmp.persona
LEFT JOIN "caba-piba-raw-zone-db"."siu_toba_3_3_negocio_mdp_nacionalidades" nmn ON nmn.nacionalidad = nmp.nacionalidad
LEFT JOIN "caba-piba-raw-zone-db"."siu_toba_3_3_negocio_mdp_personas_documentos" nmpd ON nmpd.documento = nmp.documento_principal
LEFT JOIN "caba-piba-raw-zone-db"."siu_toba_3_3_negocio_mdp_tipo_documento" nmtd ON nmtd.tipo_documento = nmpd.tipo_documento
GROUP BY nmp.persona,
		 nmtd.desc_abreviada,
		 nmpd.nro_documento,
		 nmp.sexo,
		 nmn.descripcion,
		 nmpd.nro_documento,
		 UPPER(nmp.nombres),
		 UPPER(nmp.apellido),
		 nmp.fecha_nacimiento,
		 nmp.nacionalidad,
		 CAST(nmp.persona AS VARCHAR),
		 CAST(nmp.nacionalidad AS VARCHAR),
		 CAST(nmpd.tipo_documento AS VARCHAR)
UNION ALL

SELECT 'GOET' base_origen,
	    CAST(goetu.idusuario AS VARCHAR) codigo_origen,
		CONCAT((CASE WHEN (goettd.detalle IN ('DNI', 'CUIT', 'CUIL', 'LC', 'LE', 'CI')) THEN goettd.detalle
		        ELSE 'NN' END),
				CASE WHEN goettd.detalle IN ('DNI',
                                         'LC',
                                         'LE',
                                         'CI',
                                         'CUIT',
                                         'CUIL') THEN REGEXP_REPLACE(UPPER(goetu.ndocumento),'[A-Za-z]+|\.','') ELSE
					CAST(goetu.ndocumento AS VARCHAR) END,
				goetu.sexo,
				(CASE WHEN (goettd.detalle IN ('DNI', 'CUIT', 'CUIL', 'LC', 'LE', 'CI')) THEN 'ARG' ELSE 'NN' END)) broker_id_din,
	    CONCAT(RPAD(CASE WHEN (goettd.detalle IN ('DNI', 'CUIT', 'CUIL', 'LC', 'LE', 'CI')) THEN goettd.detalle
		        ELSE 'NN' END,4,' '),
				LPAD((CASE WHEN goettd.detalle IN ('DNI',
                                         'LC',
                                         'LE',
                                         'CI',
                                         'CUIT',
                                         'CUIL') THEN REGEXP_REPLACE(UPPER(goetu.ndocumento),'[A-Za-z]+|\.','') ELSE
           CAST(goetu.ndocumento AS VARCHAR) END),11,'0'),
				goetu.sexo,
				(CASE WHEN (goettd.detalle IN ('DNI', 'CUIT', 'CUIL', 'LC', 'LE', 'CI')) THEN 'ARG' ELSE 'NNN' END)) broker_id_est,
		goettd.detalle tipo_documento,
		(CASE WHEN (goettd.detalle IN ('DNI', 'CUIT', 'CUIL', 'LC', 'LE', 'CI')) THEN goettd.detalle
		        ELSE 'NN' END) tipo_doc_broker,
		CASE WHEN goettd.detalle IN ('DNI',
                                         'LC',
                                         'LE',
                                         'CI',
                                         'CUIT',
                                         'CUIL') THEN REGEXP_REPLACE(UPPER(goetu.ndocumento),'[A-Za-z]+|\.','') ELSE
           CAST(goetu.ndocumento AS VARCHAR) END documento_broker,
		UPPER(goetu.nombres) nombre,
        UPPER(goetu.apellidos) apellido,
		goetu.fechanacimiento fecha_nacimiento,
		goetu.sexo genero_broker,
		CASE WHEN (goettd.detalle IN ('DNI', 'CUIT', 'CUIL', 'LC', 'LE', 'CI')) THEN 'ARG' ELSE 'NNN' END nacionalidad,
		NULL descrip_nacionalidad,
		CASE WHEN (goettd.detalle IN ('DNI', 'CUIT', 'CUIL', 'LC', 'LE', 'CI')) THEN 'ARG' ELSE 'NNN' END nacionalidad_broker,
		(CASE WHEN ((UPPER(goetu.nombres) IS NULL) OR (("length"(UPPER(goetu.nombres)) < 3) AND (NOT ("upper"(UPPER(goetu.nombres)) IN ('BO', 'GE', 'HE', 'LI', 'LU', 'QI', 'WU', 'XI', 'XU', 'YE', 'YI', 'YU')))) OR (UPPER(goetu.nombres) LIKE '%PRUEBA%')) THEN 0 ELSE 1 END) nombre_valido,
	    (CASE WHEN ((UPPER(goetu.apellidos) IS NULL) OR (("length"(UPPER(goetu.apellidos)) < 3) AND (NOT ("upper"(UPPER(goetu.apellidos)) IN ('AL', 'AM', 'AN', 'BO', U&'B\00D3', 'CO', 'DE', 'DO', 'DU', 'FU', 'GE', 'GO', 'GU', 'HA', 'HE', 'HO', 'HU', 'IM', 'IN', 'IS', 'JI', 'JO', 'JU', 'KE', 'KI', 'KO', 'KU', 'LI', 'LO', 'LU', 'MA', 'MO', 'MU', 'NA', 'NG', 'NI', 'NO', 'OH', 'OU', 'PI', 'PO', 'PY', 'QI', 'QU', 'RA', 'RE', U&'R\00C9', 'RO', 'RU', 'SA', U&'S\00C1', 'SO', 'SU', 'TU', 'UM', 'UZ', 'WU', 'XU', 'YA', 'YE', 'YI', 'YO', 'YU')))) OR (UPPER(goetu.apellidos) LIKE '%PRUEBA%')) THEN 0 ELSE 1 END) apellido_valido,
		CAST(goetu.ndocumento AS VARCHAR) documento_original
FROM  "caba-piba-raw-zone-db"."goet_usuarios" goetu
LEFT JOIN "caba-piba-raw-zone-db"."goet_tipo_documento" goettd ON (goetu.idtipodoc = goettd.idtipodoc)
UNION ALL

SELECT 'SIENFO' base_origen,
	    CAST(a.id_alumno AS VARCHAR) codigo_origen,
		CONCAT((CASE WHEN (d.nombre = 'D.N.I.') THEN 'DNI'
				WHEN (d.nombre = 'C.I.') THEN 'CI'
				WHEN (d.nombre = 'L.E.') THEN 'LE'
				WHEN (d.nombre = 'L.C.') THEN 'LC'
				WHEN (d.nombre = 'CUIL') THEN 'CUIL'
				WHEN (d.nombre = 'CUIT') THEN 'CUIT'
				WHEN (d.nombre = 'Pasaporte') THEN 'PE'
				WHEN (d.nombre IN ('C.E. Cl', 'C.I. Py', 'C.I. Bo', 'C.I. Br', 'C.I Uy')) THEN 'CE'
				WHEN (d.nombre = 'otros') THEN 'OTRO' ELSE 'NN' END),
				CASE WHEN d.nombre IN ('D.N.I.', 'L.C.', 'L.E.', 'C.I.', 'CUIT', 'CUIL') THEN REGEXP_REPLACE(UPPER(a.nrodoc),'[A-Za-z]+|\.|\,','') ELSE
           CAST(a.nrodoc AS VARCHAR) END,
				(CASE WHEN (g.sexo = 'Masculino') THEN 'M' WHEN (g.sexo = 'Femenino') THEN 'F' ELSE 'X' END),
				(CASE WHEN (d.nombre IN ('D.N.I.', 'C.I.', 'L.E.', 'L.C.', 'CUIL', 'CUIT')) THEN 'ARG'
				 ELSE 'NN' END)) broker_id_din,
		CONCAT(RPAD(CASE WHEN (d.nombre = 'D.N.I.') THEN 'DNI'
				WHEN (d.nombre = 'C.I.') THEN 'CI'
				WHEN (d.nombre = 'L.E.') THEN 'LE'
				WHEN (d.nombre = 'L.C.') THEN 'LC'
				WHEN (d.nombre = 'CUIL') THEN 'CUIL'
				WHEN (d.nombre = 'CUIT') THEN 'CUIT'
				WHEN (d.nombre = 'Pasaporte') THEN 'PE'
				WHEN (d.nombre IN ('C.E. Cl', 'C.I. Py', 'C.I. Bo', 'C.I. Br', 'C.I Uy')) THEN 'CE'
				WHEN (d.nombre = 'otros') THEN 'OTRO' ELSE 'NN' END,4,' '),
				LPAD((CASE WHEN d.nombre IN ('D.N.I.', 'L.C.', 'L.E.', 'C.I.', 'CUIT', 'CUIL') THEN REGEXP_REPLACE(UPPER(a.nrodoc),'[A-Za-z]+|\.|\,','') ELSE
           CAST(a.nrodoc AS VARCHAR) END),11,'0'),
				(CASE WHEN (g.sexo = 'Masculino') THEN 'M' WHEN (g.sexo = 'Femenino') THEN 'F' ELSE 'X' END),
				(CASE WHEN (d.nombre IN ('D.N.I.', 'C.I.', 'L.E.', 'L.C.', 'CUIL', 'CUIT')) THEN 'ARG'
				 ELSE 'NNN' END)) broker_id_est,
		d.nombre tipo_documento,
		CASE WHEN (d.nombre = 'D.N.I.') THEN 'DNI'
				WHEN (d.nombre = 'C.I.') THEN 'CI'
				WHEN (d.nombre = 'L.E.') THEN 'LE'
				WHEN (d.nombre = 'L.C.') THEN 'LC'
				WHEN (d.nombre = 'CUIL') THEN 'CUIL'
				WHEN (d.nombre = 'CUIT') THEN 'CUIT'
				WHEN (d.nombre = 'Pasaporte') THEN 'PE'
				WHEN (d.nombre IN ('C.E. Cl', 'C.I. Py', 'C.I. Bo', 'C.I. Br', 'C.I Uy')) THEN 'CE'
				WHEN (d.nombre = 'otros') THEN 'OTRO' ELSE 'NN' END tipo_doc_broker,
		CASE WHEN d.nombre IN ('D.N.I.', 'L.C.', 'L.E.', 'C.I.', 'CUIT', 'CUIL') THEN REGEXP_REPLACE(UPPER(a.nrodoc),'[A-Za-z]+|\.|\,','') ELSE
           CAST(a.nrodoc AS VARCHAR) END documento_broker,
		UPPER(CASE WHEN (a.apenom LIKE '%,%') THEN "trim"("split_part"(apenom, ',', 2)) ELSE a.apenom END) nombre,
        UPPER((CASE WHEN (a.apenom LIKE '%,%') THEN "trim"("split_part"(apenom, ',', 1)) ELSE a.apenom END)) apellido,
		a.fechanac fecha_nacimiento,
		(CASE WHEN (g.sexo = 'Masculino') THEN 'M' WHEN (g.sexo = 'Femenino') THEN 'F' ELSE 'X' END) genero_broker,
		CAST(a.nacionalidad AS VARCHAR) nacionalidad,
		n.nombre descrip_nacionalidad,
		(CASE WHEN (d.nombre IN ('D.N.I.', 'C.I.', 'L.E.', 'L.C.', 'CUIL', 'CUIT')) THEN 'ARG'
				 ELSE 'NN' END) nacionalidad_broker,
		(CASE WHEN ((UPPER(a.apenom) IS NULL) OR (("length"(UPPER(a.apenom)) < 3) AND (NOT ("upper"(UPPER(a.apenom)) IN ('BO', 'GE', 'HE', 'LI', 'LU', 'QI', 'WU', 'XI', 'XU', 'YE', 'YI', 'YU')))) OR (UPPER(a.apenom) LIKE '%PRUEBA%')) THEN 0 ELSE 1 END) nombre_valido,
	    (CASE WHEN ((UPPER(a.apenom) IS NULL) OR (("length"(UPPER(a.apenom)) < 3) AND (NOT ("upper"(UPPER(a.apenom)) IN ('AL', 'AM', 'AN', 'BO', U&'B\00D3', 'CO', 'DE', 'DO', 'DU', 'FU', 'GE', 'GO', 'GU', 'HA', 'HE', 'HO', 'HU', 'IM', 'IN', 'IS', 'JI', 'JO', 'JU', 'KE', 'KI', 'KO', 'KU', 'LI', 'LO', 'LU', 'MA', 'MO', 'MU', 'NA', 'NG', 'NI', 'NO', 'OH', 'OU', 'PI', 'PO', 'PY', 'QI', 'QU', 'RA', 'RE', U&'R\00C9', 'RO', 'RU', 'SA', U&'S\00C1', 'SO', 'SU', 'TU', 'UM', 'UZ', 'WU', 'XU', 'YA', 'YE', 'YI', 'YO', 'YU')))) OR (UPPER(a.apenom) LIKE '%PRUEBA%')) THEN 0 ELSE 1 END) apellido_valido,
		CAST(a.nrodoc AS VARCHAR) documento_original
FROM "caba-piba-raw-zone-db"."sienfo_tdoc" d
INNER JOIN "caba-piba-raw-zone-db"."sienfo_alumnos" a ON (a.tipodoc = d.tipodoc)
INNER JOIN "caba-piba-raw-zone-db"."sienfo_tgenero" g ON (CAST(g.id AS INT) = CAST(a.sexo AS INT))
INNER JOIN "caba-piba-raw-zone-db"."sienfo_tnacionalidad"  n ON (a.nacionalidad = n.nacionalidad)
UNION ALL

SELECT 'CRMSL' base_origen,
	    CAST(co.id AS VARCHAR) codigo_origen,
		CONCAT((CASE WHEN (cs.tipo_documento_c = 'dni') THEN 'DNI' ELSE 'NN' END),
				CAST(cs.numero_documento_c AS varchar),
				(CASE WHEN (cs.genero_c = 'masculino') THEN 'M' WHEN (cs.genero_c = 'femenino') THEN 'F' ELSE 'X' END),
				(CASE WHEN (cs.tipo_documento_c = 'dni') THEN 'ARG' ELSE 'NN' END)) broker_id_din,
		CONCAT(RPAD(CASE WHEN (cs.tipo_documento_c = 'dni') THEN 'DNI' ELSE 'NN' END,4,' '),
				LPAD(CAST(cs.numero_documento_c AS VARCHAR),11,'0'),
				(CASE WHEN (cs.genero_c = 'masculino') THEN 'M' WHEN (cs.genero_c = 'femenino') THEN 'F' ELSE 'X' END),
				(CASE WHEN (cs.tipo_documento_c = 'dni') THEN 'ARG' ELSE 'NNN' END)) broker_id_est,
		cs.tipo_documento_c tipo_documento,
		(CASE WHEN (cs.tipo_documento_c = 'dni') THEN 'DNI' ELSE 'NN' END) tipo_doc_broker,
		CAST(cs.numero_documento_c AS VARCHAR) documento_broker,
		UPPER(co.first_name) nombre,
		UPPER(co.last_name) apellido,
		co.birthdate fecha_nacimiento,
		(CASE WHEN (cs.genero_c = 'masculino') THEN 'M' WHEN (cs.genero_c = 'femenino') THEN 'F' ELSE 'X' END) genero_broker,
		NULL nacionalidad,
		NULL descrip_nacionalidad,
		(CASE WHEN (cs.tipo_documento_c = 'dni') THEN 'ARG' ELSE 'NNN' END) nacionalidad_broker,
		(CASE WHEN ((UPPER(co.first_name) IS NULL) OR (("length"(UPPER(co.first_name)) < 3) AND (NOT ("upper"(UPPER(co.first_name)) IN ('BO', 'GE', 'HE', 'LI', 'LU', 'QI', 'WU', 'XI', 'XU', 'YE', 'YI', 'YU')))) OR (UPPER(co.first_name) LIKE '%PRUEBA%')) THEN 0 ELSE 1 END) nombre_valido,
	    (CASE WHEN ((UPPER(co.last_name) IS NULL) OR (("length"(UPPER(co.last_name)) < 3) AND (NOT ("upper"(UPPER(co.last_name)) IN ('AL', 'AM', 'AN', 'BO', U&'B\00D3', 'CO', 'DE', 'DO', 'DU', 'FU', 'GE', 'GO', 'GU', 'HA', 'HE', 'HO', 'HU', 'IM', 'IN', 'IS', 'JI', 'JO', 'JU', 'KE', 'KI', 'KO', 'KU', 'LI', 'LO', 'LU', 'MA', 'MO', 'MU', 'NA', 'NG', 'NI', 'NO', 'OH', 'OU', 'PI', 'PO', 'PY', 'QI', 'QU', 'RA', 'RE', U&'R\00C9', 'RO', 'RU', 'SA', U&'S\00C1', 'SO', 'SU', 'TU', 'UM', 'UZ', 'WU', 'XU', 'YA', 'YE', 'YI', 'YO', 'YU')))) OR (UPPER(co.last_name) LIKE '%PRUEBA%')) THEN 0 ELSE 1 END) apellido_valido,
		CAST(cs.numero_documento_c AS VARCHAR) documento_original
FROM "caba-piba-raw-zone-db"."crm_sociolaboral_contacts" co
INNER JOIN "caba-piba-raw-zone-db"."crm_sociolaboral_contacts_cstm" cs ON (co.id = cs.id_c)
WHERE (co.lead_source = 'sociolaboral'
OR ((co.lead_source = 'rib') AND cs.forma_parte_interm_lab_c = 'si'))
AND cs.numero_documento_c IS NOT NULL

UNION ALL

SELECT 'MOODLE' base_origen,
	    CAST(mu.id AS VARCHAR) codigo_origen,
		CONCAT('DNI',
				REGEXP_REPLACE(UPPER(CAST(CASE WHEN REGEXP_LIKE(mu.username, '@') THEN
					CASE WHEN REGEXP_LIKE(LOWER(mu.username), '\.ar') THEN
						split_part(split_part(mu.username, '@', 2),'.',4)
					ELSE split_part(split_part(mu.username, '@', 2),'.',3) END
					ELSE split_part(mu.username, '.', 1) END AS VARCHAR)),'[A-Za-z]+|\.',''),
                COALESCE(do.genero,'X'),
				'ARG') broker_id_din,
		CONCAT('DNI ',
				LPAD((REGEXP_REPLACE(UPPER(CAST(CASE WHEN REGEXP_LIKE(mu.username, '@') THEN
					CASE WHEN REGEXP_LIKE(LOWER(mu.username), '\.ar') THEN
						split_part(split_part(mu.username, '@', 2),'.',4)
					ELSE split_part(split_part(mu.username, '@', 2),'.',3) END
					ELSE split_part(mu.username, '.', 1) END AS VARCHAR)),'[A-Za-z]+|\.','')),11,'0'),
                COALESCE(do.genero,'X'),
				'ARG') broker_id_est,
		'DNI' tipo_documento,
		'DNI' tipo_doc_broker,
		REGEXP_REPLACE(UPPER(CAST(CASE WHEN REGEXP_LIKE(mu.username, '@') THEN
					CASE WHEN REGEXP_LIKE(LOWER(mu.username), '\.ar') THEN
						split_part(split_part(mu.username, '@', 2),'.',4)
					ELSE split_part(split_part(mu.username, '@', 2),'.',3) END
					ELSE split_part(mu.username, '.', 1) END AS VARCHAR)),'[A-Za-z]+|\.','') documento_broker,
		UPPER(mu.firstname) nombre,
		UPPER(mu.lastname) apellido,
		NULL fecha_nacimiento,
		COALESCE(do.genero,'X') genero_broker,
		NULL nacionalidad,
		NULL descrip_nacionalidad,
		'ARG' nacionalidad_broker,
		(CASE WHEN ((UPPER(mu.firstname) IS NULL) OR (("length"(UPPER(mu.firstname)) < 3) AND (NOT ("upper"(UPPER(mu.firstname)) IN ('BO', 'GE', 'HE', 'LI', 'LU', 'QI', 'WU', 'XI', 'XU', 'YE', 'YI', 'YU')))) OR (UPPER(mu.firstname) LIKE '%PRUEBA%')) THEN 0 ELSE 1 END) nombre_valido,
	    (CASE WHEN ((UPPER(mu.lastname) IS NULL) OR (("length"(UPPER(mu.lastname)) < 3) AND (NOT ("upper"(UPPER(mu.lastname)) IN ('AL', 'AM', 'AN', 'BO', U&'B\00D3', 'CO', 'DE', 'DO', 'DU', 'FU', 'GE', 'GO', 'GU', 'HA', 'HE', 'HO', 'HU', 'IM', 'IN', 'IS', 'JI', 'JO', 'JU', 'KE', 'KI', 'KO', 'KU', 'LI', 'LO', 'LU', 'MA', 'MO', 'MU', 'NA', 'NG', 'NI', 'NO', 'OH', 'OU', 'PI', 'PO', 'PY', 'QI', 'QU', 'RA', 'RE', U&'R\00C9', 'RO', 'RU', 'SA', U&'S\00C1', 'SO', 'SU', 'TU', 'UM', 'UZ', 'WU', 'XU', 'YA', 'YE', 'YI', 'YO', 'YU')))) OR (UPPER(mu.lastname) LIKE '%PRUEBA%')) THEN 0 ELSE 1 END) apellido_valido,
		CAST(CASE WHEN REGEXP_LIKE(mu.username, '@') THEN
					CASE WHEN REGEXP_LIKE(LOWER(mu.username), '\.ar') THEN
						split_part(split_part(mu.username, '@', 2),'.',4)
					ELSE split_part(split_part(mu.username, '@', 2),'.',3) END
					ELSE split_part(mu.username, '.', 1) END AS VARCHAR) documento_original
FROM "caba-piba-raw-zone-db"."moodle_dgtedu01_mdl_user" mu
LEFT JOIN "caba-piba-raw-zone-db"."moodle_dgtedu01_mdl_user_info_data" ui ON (mu.id = ui.userid AND ui.fieldid = 7)
LEFT JOIN "caba-piba-raw-zone-db"."moodle_dgtedu01_mdl_user_enrolments" mue ON (mue.userid = mu.id)
LEFT JOIN "caba-piba-raw-zone-db"."moodle_dgtedu01_mdl_enrol" me ON (me.id = mue.enrolid)
LEFT JOIN "caba-piba-raw-zone-db"."moodle_dgtedu01_mdl_course" mc ON(mc.id = me.courseid)
LEFT JOIN "caba-piba-raw-zone-db"."moodle_dgtedu01_mdl_course_categories" cc ON (mc.category = cc.id)
LEFT JOIN "caba-piba-staging-zone-db"."tbp_broker_def_broker_general" DO ON (CAST(do.documento_broker AS VARCHAR) = CAST(CASE WHEN REGEXP_LIKE(mu.username, '@') THEN
					CASE WHEN REGEXP_LIKE(LOWER(mu.username), '\.ar') THEN
						split_part(split_part(mu.username, '@', 2),'.',4)
					ELSE split_part(split_part(mu.username, '@', 2),'.',3) END
					ELSE split_part(mu.username, '.', 1) END AS VARCHAR))
WHERE (CASE WHEN REGEXP_LIKE(mu.username, '@') THEN
        CASE WHEN REGEXP_LIKE(LOWER(mu.username), '\.ar') THEN
          split_part(split_part(mu.username, '@', 2),'.',4)
        ELSE split_part(split_part(mu.username, '@', 2),'.',3) END
       ELSE split_part(mu.username, '.', 1)  END) IS NOT NULL
	   AND (cc.idnumber LIKE 'CAC%' OR cc.idnumber LIKE 'FPE%')
    -- Habilidades/Formación para la empleabilidad
GROUP BY mu.id,
		 mu.username,
		 ui.data,
		 mu.firstname,
		 mu.lastname,
		 do.genero



/*ASPIRANTE
INSCRIPTO
REGULAR
LIBRE/BAJA
EGRESADO
TRABAJANDO -->AFIP*/



-- Copy of 2022.11.18 script_completitud (Vcliente).sql 



-- CANTIDAD DE REGISTROS POR BASE
SELECT dv.base_origen,
       COUNT(1) cant_vecinos
FROM "caba-piba-staging-zone-db"."tbp_typ_def_vecino" dv
GROUP BY dv.base_origen

SELECT SUM(cant_vecinos)
FROM (SELECT dv.base_origen,
       COUNT(1) cant_vecinos
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_vecino" dv
GROUP BY dv.base_origen)

SELECT COUNT(1) cant_vecinos
FROM "caba-piba-staging-zone-db"."tbp_typ_def_vecino" dv

-- DUPLICADOS DNI POR BASE
SELECT base_origen, COUNT(1)
FROM
(
SELECT vec.broker_id, vec.base_origen
FROM "caba-piba-staging-zone-db"."tbp_typ_def_vecino" vec
--WHERE vec.base_origen LIKE 'CRM%'--= 'SIENFO'
GROUP BY vec.broker_id, vec.base_origen
HAVING COUNT(1)>1
)
GROUP BY base_origen

SELECT base_origen, COUNT(1)
FROM
(
SELECT vec.broker_id_din, vec.base_origen
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_vecino" vec
--WHERE vec.base_origen LIKE 'CRM%'--= 'SIENFO'
GROUP BY vec.broker_id_din, vec.base_origen
HAVING COUNT(1)>1
)
GROUP BY base_origen

-- REGISTROS DUPLICADOS
SELECT vec.*
FROM "caba-piba-staging-zone-db"."tbp_typ_def_vecino" vec,
(
SELECT vec.broker_id, vec.base_origen
FROM "caba-piba-staging-zone-db"."tbp_typ_def_vecino" vec
--WHERE vec.base_origen LIKE 'CRM%'--= 'SIENFO'
GROUP BY vec.broker_id, vec.base_origen
HAVING COUNT(1)>1
) tmp
WHERE tmp.broker_id = vec.broker_id
AND tmp.base_origen = vec.base_origen


-- ANALISIS DE COMPLETITUD

SELECT dv.base_origen,
       COUNT(1) cant_vecinos,
       SUM(CASE WHEN dv.nombre IS NULL THEN 0 ELSE 1 END) completitud_nombre,
       SUM(CASE WHEN dv.apellido IS NULL THEN 0 ELSE 1 END) completitud_apellido,
       SUM(CASE WHEN dv.fecha_nacimiento IS NULL THEN 0 ELSE 1 END) completitud_fec_nac,
       SUM(CASE WHEN dv.descrip_nacionalidad IS NULL THEN 0 ELSE 1 END) completitud_nacionalidad,
       SUM(dv.nombre_valido) nombre_valido,
       SUM(dv.apellido_valido) apellido_valido
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_vecino" dv
GROUP BY dv.base_origen

SELECT COUNT(1) cant_reg,
       SUM(CASE WHEN ministerio_id IS NULL THEN 0 ELSE 1 END) cant_min,
       SUM(CASE WHEN reparticion_id IS NULL THEN 0 ELSE 1 END) cant_rep,
       SUM(CASE WHEN tipo_programa IS NULL THEN 0 ELSE 1 END) cant_tipo_prog,
       SUM(CASE WHEN duracion_estimada IS NULL THEN 0 ELSE 1 END) cant_dur_est,
       SUM(CASE WHEN fecha_inscripcion IS NULL THEN 0 ELSE 1 END) cant_fec_insc
FROM "caba-piba-staging-zone-db"."tbp_typ_def_programa"


SELECT COUNT(1) cant_capacitaciones,
       SUM(CASE WHEN dc.descrip_capacitacion IS NULL THEN 0 ELSE 1 END) completitud_descripcion,
       SUM(CASE WHEN dc.tipo_formacion IS NULL THEN 0 ELSE 1 END) completitud_tipo_formacion,
       SUM(CASE WHEN dc.modalidad_id IS NULL THEN 0 ELSE 1 END) completitud_modalidad,
       SUM(CASE WHEN dc.categoria_back_id IS NULL THEN 0 ELSE 1 END) completitud_categoria_back,
       SUM(CASE WHEN dc.categoria_front_id IS NULL THEN 0 ELSE 1 END) completitud_categoria_front,
       SUM(CASE WHEN dc.estado_capacitacion IS NULL THEN 0 ELSE 1 END) completitud_estado,
       SUM(CASE WHEN dc.seguimiento_personalizado IS NULL THEN 0 ELSE 1 END) completitud_seg_personalizado,
       SUM(CASE WHEN dc.soporte_online IS NULL THEN 0 ELSE 1 END) completitud_soporte_online,
       SUM(CASE WHEN dc.incentivos_terminalidad IS NULL THEN 0 ELSE 1 END) completitud_incentivos_terminalidad,
       SUM(CASE WHEN dc.exclusividad_participantes IS NULL THEN 0 ELSE 1 END) completitud_exclusividad_participantes,
       SUM(CASE WHEN dc.otorga_certificado IS NULL THEN 0 ELSE 1 END) completitud_otorga_certificado,
       SUM(CASE WHEN dc.filtro_ingreso IS NULL THEN 0 ELSE 1 END) completitud_filtro_ingreso,
       SUM(CASE WHEN dc.frecuencia_oferta_anual IS NULL THEN 0 ELSE 1 END) completitud_frec_oferta_anual,
       SUM(CASE WHEN dc.duracion_cantidad IS NULL THEN 0 ELSE 1 END) completitud_duracion_cantidad,
       SUM(CASE WHEN dc.duracion_medida IS NULL THEN 0 ELSE 1 END) completitud_duracion_medida,
       SUM(CASE WHEN dc.duracion_dias IS NULL THEN 0 ELSE 1 END) completitud_duracion_dias,
       SUM(CASE WHEN dc.duracion_hs_reloj IS NULL THEN 0 ELSE 1 END) completitud_duracion_hs_reloj,
	   SUM(CASE WHEN dc.vacantes IS NULL THEN 0 ELSE 1 END) completitud_vacantes
FROM "caba-piba-staging-zone-db"."tbp_typ_def_capacitacion" dc


SELECT COUNT(1) cant_ed_capacitacion,
       SUM(CASE WHEN ec.capacitacion_id_typ IS NULL THEN 0 ELSE 1 END) completitud_capacitacion_id_typ,
       SUM(CASE WHEN ec.descrip_capacitacion IS NULL THEN 0 ELSE 1 END) completitud_descrip_capacitacion,
       SUM(CASE WHEN ec.descrip_capacitacion_abrev IS NULL THEN 0 ELSE 1 END) completitud_descrip_capacitacion_abrev,
       SUM(CASE WHEN ec.descrip_ed_capacitacion IS NULL THEN 0 ELSE 1 END) completitud_descrip_ed_capacitacion,
       SUM(CASE WHEN ec.fecha_inicio_dictado IS NULL THEN 0 ELSE 1 END) completitud_fecha_inicio_dictado,
       SUM(CASE WHEN ec.fecha_fin_dictado IS NULL THEN 0 ELSE 1 END) completitud_fecha_fin_dictado,
       SUM(CASE WHEN ec.fecha_tope_movimientos IS NULL THEN 0 ELSE 1 END) completitud_fecha_tope_movimientos,
       SUM(CASE WHEN ec.nombre_turno IS NULL THEN 0 ELSE 1 END) completitud_nombre_turno,
       SUM(CASE WHEN ec.descrip_turno IS NULL THEN 0 ELSE 1 END) completitud_descrip_turno,
	   SUM(CASE WHEN ec.inscripcion_habilitada IS NULL THEN 0 ELSE 1 END) completitud_inscripcion_habilitada,
       SUM(CASE WHEN ec.activo IS NULL THEN 0 ELSE 1 END) completitud_activo,
       SUM(CASE WHEN ec.cupo IS NULL THEN 0 ELSE 1 END) completitud_cupo,
       SUM(CASE WHEN ec.modalidad IS NULL THEN 0 ELSE 1 END) completitud_modalidad,
	   SUM(CASE WHEN ec.nombre_modalidad IS NULL THEN 0 ELSE 1 END) completitud_nombre_modalidad,
       SUM(CASE WHEN ec.descrip_modalidad IS NULL THEN 0 ELSE 1 END) completitud_descrip_modalidad,
       SUM(CASE WHEN ec.cod_origen_establecimiento IS NULL THEN 0 ELSE 1 END) completitud_cod_origen_establecimiento
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_edicion_capacitacion" ec

-- CANTIDAD DE VECINOS CON BROKER ID VALIDO
SELECT COUNT(1) cant_vecinos
FROM "caba-piba-staging-zone-db"."tbp_typ_def_vecino" dv
WHERE broker_id_valido = 1 AND dni_valido = 0

-- CANTIDAD DE VECINOS CON DNI VALIDO
SELECT COUNT(1) cant_vecinos
FROM "caba-piba-staging-zone-db"."tbp_typ_def_vecino" dv
WHERE broker_id_valido = 0 AND dni_valido = 1

-- CANTIDAD DE VECINOS SIN MATCH
SELECT COUNT(1) cant_vecinos
FROM "caba-piba-staging-zone-db"."tbp_typ_def_vecino" dv
WHERE broker_id_valido = 0 AND dni_valido = 0

-- CANTIDAD DE VECINOS SIN MATCH POR BASE
SELECT dv.base_origen,
       COUNT(1) cant_vecinos
FROM "caba-piba-staging-zone-db"."tbp_typ_def_vecino" dv
WHERE broker_id_valido = 0 AND dni_valido = 0
GROUP BY dv.base_origen

SELECT nacionalidad, SUM(cantidad) cant
FROM (
SELECT CASE WHEN descrip_nacionalidad IN ('Argentina','Argentina') THEN 'Argentino'
		    WHEN descrip_nacionalidad IS NULL THEN 'Sin Nacionalidad'
			ELSE 'Extranjeros' END nacionalidad,
	   COUNT(1) cantidad
FROM "caba-piba-staging-zone-db"."tbp_typ_def_vecino"
WHERE broker_id_valido = 0 AND dni_valido = 0
GROUP BY descrip_nacionalidad)
GROUP BY nacionalidad

SELECT count(1)
FROM "caba-piba-staging-zone-db"."tbp_typ_def_vecino" vec
WHERE broker_id_valido = 0 AND dni_valido = 0


-- Registros no enviados a SINTYS


-- CANTIDAD DE VECINOS ENVIADOS A SINTYS CON ERROR
SELECT count(1)
FROM "caba-piba-staging-zone-db"."tbp_typ_def_vecino" vec
WHERE broker_id_valido = 0 AND dni_valido = 0
AND EXISTS (SELECT 1
                  FROM  "caba-piba-raw-zone-db"."sintys_base_origen" sbo
                  WHERE vec.documento_broker = sbo.ndoc_orig)



-- CANTIDAD DE VECINOS NO ENVIADOS A SINTYS
SELECT count(1)
FROM "caba-piba-staging-zone-db"."tbp_typ_def_vecino" vec
WHERE broker_id_valido = 0 AND dni_valido = 0
AND NOT EXISTS (SELECT 1
                  FROM  "caba-piba-raw-zone-db"."sintys_base_origen" sbo
                  WHERE vec.documento_broker = sbo.ndoc_orig)

-- CANTIDAD DE VECINOS ENVIADOS A SINTYS CON ERROR CON LONGITUD VALIDA
SELECT count(1)
FROM "caba-piba-staging-zone-db"."tbp_typ_def_vecino" vec
WHERE broker_id_valido = 0 AND dni_valido = 0 AND
LENGTH(CASE WHEN tipo_doc_broker NOT IN ('CUIT', 'CUIL') THEN CAST(documento_broker AS VARCHAR)
			WHEN tipo_doc_broker IN ('CUIT', 'CUIL') THEN SUBSTR(CAST(documento_broker AS VARCHAR), 3, 8) END) IN (7,8)
AND EXISTS (SELECT 1
                  FROM  "caba-piba-raw-zone-db"."sintys_base_origen" sbo
                  WHERE vec.documento_broker = sbo.ndoc_orig)


-- CANTIDAD DE VECINOS CON LONGITUD INVALIDA
SELECT count(1)
FROM "caba-piba-staging-zone-db"."tbp_typ_def_vecino" vec
WHERE broker_id_valido = 0 AND dni_valido = 0 AND
LENGTH(CASE WHEN tipo_doc_broker NOT IN ('CUIT', 'CUIL') THEN CAST(documento_broker AS VARCHAR)
			WHEN tipo_doc_broker IN ('CUIT', 'CUIL') THEN SUBSTR(CAST(documento_broker AS VARCHAR), 3, 8) END) NOT IN (7,8)
AND EXISTS (SELECT 1
                  FROM  "caba-piba-raw-zone-db"."sintys_base_origen" sbo
                  WHERE vec.documento_broker = sbo.ndoc_orig)

-- CANTIDAD DE VECINOS ERRONEOS LONGITUD VALIDA POR NACIONALIDAD
SELECT descrip_nacionalidad, count(1)
FROM "caba-piba-staging-zone-db"."tbp_typ_def_vecino" vec
WHERE broker_id_valido = 0 AND dni_valido = 0 AND
LENGTH(CASE WHEN tipo_doc_broker NOT IN ('CUIT', 'CUIL') THEN CAST(documento_broker AS VARCHAR)
			WHEN tipo_doc_broker IN ('CUIT', 'CUIL') THEN SUBSTR(CAST(documento_broker AS VARCHAR), 3, 8) END) IN (7,8)
AND EXISTS (SELECT 1
                  FROM  "caba-piba-raw-zone-db"."sintys_base_origen" sbo
                  WHERE vec.documento_broker = sbo.ndoc_orig)
GROUP BY descrip_nacionalidad

-- CANTIDAD DE VECINOS ERRONEOS LONGITUD VALIDA POR GENERO
SELECT genero_broker, count(1)
FROM "caba-piba-staging-zone-db"."tbp_typ_def_vecino" vec
WHERE broker_id_valido = 0 AND dni_valido = 0 AND
LENGTH(CASE WHEN tipo_doc_broker NOT IN ('CUIT', 'CUIL') THEN CAST(documento_broker AS VARCHAR)
			WHEN tipo_doc_broker IN ('CUIT', 'CUIL') THEN SUBSTR(CAST(documento_broker AS VARCHAR), 3, 8) END) IN (7,8)
AND EXISTS (SELECT 1
                  FROM  "caba-piba-raw-zone-db"."sintys_base_origen" sbo
                  WHERE vec.documento_broker = sbo.ndoc_orig)
GROUP BY genero_broker


-- CANTIDAD DE VECINOS ERRONEOS LONGITUD VALIDA POR TIPO_DNI
SELECT tipo_doc_broker, count(1)
FROM "caba-piba-staging-zone-db"."tbp_typ_def_vecino" vec
WHERE broker_id_valido = 0 AND dni_valido = 0 AND
LENGTH(CASE WHEN tipo_doc_broker NOT IN ('CUIT', 'CUIL') THEN CAST(documento_broker AS VARCHAR)
			WHEN tipo_doc_broker IN ('CUIT', 'CUIL') THEN SUBSTR(CAST(documento_broker AS VARCHAR), 3, 8) END) IN (7,8)
AND EXISTS (SELECT 1
                  FROM  "caba-piba-raw-zone-db"."sintys_base_origen" sbo
                  WHERE vec.documento_broker = sbo.ndoc_orig)
GROUP BY tipo_doc_broker

-- CANTIDAD DE PERDONAS EN BROKER Con Y sin  mi BA
SELECT SUM(CASE WHEN login2_id IS NOT NULL THEN 1 ELSE 0 END) cant_con_login2,
        SUM(CASE WHEN login2_id IS NULL THEN 1 ELSE 0 END) cant_sin_login2
FROM  "caba-piba-staging-zone-db"."tbp_broker_def_broker_general"

--CANTIDAD VECINOS EN BROKER CON Y SIN MI BA
SELECT CASE WHEN bo.login2_id IS NULL THEN 0 ELSE 1 END con_login,
	   COUNT(1) cant
FROM "caba-piba-staging-zone-db"."tbp_typ_def_vecino" vec
LEFT JOIN "caba-piba-staging-zone-db".tbp_broker_def_broker_general bo ON (bo.id = vec.broker_id)
GROUP BY CASE WHEN bo.login2_id IS NULL THEN 0 ELSE 1 END

-- VALIDACIONES LOGIN 2 RENAPER

SELECT count(1)
FROM "caba-piba-staging-zone-db"."tbp_typ_def_vecino" vec
INNER JOIN "caba-piba-consume-zone-db"."login2_citizen" cit ON (CAST(cit.dni AS VARCHAR) = vec.documento_broker)

SELECT CASE WHEN COALESCE(cit.validated_renaper,0) != 0 THEN 'S' ELSE 'N' END valid_renaper,
    count(1)
FROM "caba-piba-staging-zone-db"."tbp_typ_def_vecino" vec
INNER JOIN "caba-piba-consume-zone-db"."login2_citizen" cit ON (CAST(cit.dni AS VARCHAR) = vec.documento_broker)
GROUP BY CASE WHEN COALESCE(cit.validated_renaper,0) != 0 THEN 'S' ELSE 'N' END

SELECT CASE WHEN COALESCE(cit.validated_renaper,0) != 0 THEN 'S' ELSE 'N' END valid_renaper,
    count(1)
FROM "caba-piba-staging-zone-db"."tbp_typ_def_vecino" vec
INNER JOIN "caba-piba-consume-zone-db"."login2_citizen" cit ON (CAST(cit.dni AS VARCHAR) = vec.documento_broker)


SELECT count(1)
FROM "caba-piba-staging-zone-db"."tbp_broker_def_broker_general" bg
INNER JOIN "caba-piba-consume-zone-db"."login2_citizen" cit ON (cit.citizen_id = bg.login2_id)
INNER JOIN "caba-piba-staging-zone-db"."tbp_typ_def_vecino" vec ON (vec.broker_id = bg.id)

SELECT count(1)
FROM "caba-piba-staging-zone-db"."tbp_broker_def_broker_general" bg
INNER JOIN "caba-piba-consume-zone-db"."login2_citizen" cit ON (cit.citizen_id = bg.login2_id)
INNER JOIN "caba-piba-staging-zone-db"."tbp_typ_def_vecino" vec ON (vec.documento_broker = CAST(bg.documento_broker AS VARCHAR))



