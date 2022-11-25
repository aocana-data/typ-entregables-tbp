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
FROM "caba-piba-raw-zone-db"."crm_sociolaboral_op_oportunidades_formacion" of
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
        COALESCE(UPPER(a.Detalle),  UPPER(f.Detalle)) as categoria
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


