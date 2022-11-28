-- 1.-- Crear EDICION CAPACITACION SIENFO Y CRMSL
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
	   CASE WHEN COALESCE(t.fecha_fin,'0000-00-00') = '0000-00-00' THEN (CASE WHEN te.nombre = 'Activo' THEN 'S' ELSE 'N' END) ELSE (CASE WHEN DATE_PARSE(CASE WHEN t.fecha_fin = '0000-00-00' THEN NULL ELSE t.fecha_fin END, '%Y-%m-%d') > current_date THEN 'S' ELSE 'N' END) END inscripcion_habilitada,
       CASE WHEN COALESCE(t.fecha_fin,'0000-00-00') = '0000-00-00' THEN (CASE WHEN te.nombre = 'Activo' THEN 'S' ELSE 'N' END) ELSE (CASE WHEN DATE_PARSE(CASE WHEN t.fecha_fin = '0000-00-00' THEN NULL ELSE t.fecha_fin END, '%Y-%m-%d') > current_date THEN 'S' ELSE 'N' END) END activo,
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
	   CASE WHEN COALESCE(t.fecha_fin,'0000-00-00') = '0000-00-00' THEN (CASE WHEN te.nombre = 'Activo' THEN 'S' ELSE 'N' END) ELSE (CASE WHEN DATE_PARSE(CASE WHEN t.fecha_fin = '0000-00-00' THEN NULL ELSE t.fecha_fin END, '%Y-%m-%d') > current_date THEN 'S' ELSE 'N' END) END inscripcion_habilitada,
       CASE WHEN COALESCE(t.fecha_fin,'0000-00-00') = '0000-00-00' THEN (CASE WHEN te.nombre = 'Activo' THEN 'S' ELSE 'N' END) ELSE (CASE WHEN DATE_PARSE(CASE WHEN t.fecha_fin = '0000-00-00' THEN NULL ELSE t.fecha_fin END, '%Y-%m-%d') > current_date THEN 'S' ELSE 'N' END) END activo,
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
       CASE WHEN of.fin IS NULL THEN (CASE WHEN estado_inscripcion = 'finalizada' THEN 'N' ELSE 'S' END) ELSE (CASE WHEN of.fin > current_date THEN 'S' ELSE 'N' END) END inscripcion_habilitada,
       CASE WHEN of.fin IS NULL THEN (CASE WHEN estado_curso = 'finalizada' THEN 'N' ELSE 'S' END) ELSE (CASE WHEN of.fin > current_date THEN 'S' ELSE 'N' END) END activo,
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
FROM "caba-piba-raw-zone-db"."crm_sociolaboral_op_oportunidades_formacion" of 
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