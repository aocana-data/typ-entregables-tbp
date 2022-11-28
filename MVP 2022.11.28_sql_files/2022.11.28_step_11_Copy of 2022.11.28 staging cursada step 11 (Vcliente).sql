-- EDICION CAPACITACION SIENFO
-- 1.-  Crear tabla temporal de cursadas
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_cursada" AS
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
LEFT JOIN "caba-piba-raw-zone-db"."sienfo_fichas_preinscripcion" sfp ON (sfp.codigo_ct = sf.codigo_ct AND sf.nrodoc = sfp.nrodoc)
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_edicion_capacitacion" ed ON (ed.edicion_capacitacion_id_old = sf.codigo_ct AND ed.base_origen = 'SIENFO')
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_vecino" vec ON (vec.base_origen = 'SIENFO' AND vec.documento_broker = TRIM(sf.nrodoc))
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_tmp_estado_beneficiario_sienfo" ebs ON (ebs.nrodoc = sf.nrodoc AND ebs.codigo_ct = ed.edicion_capacitacion_id_old)
UNION
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
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_vecino" vec ON (vec.base_origen = 'CRMSL' AND vec.cod_origen = CAST(co.id AS VARCHAR))
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_tmp_estado_beneficiario_crmsl" ebc ON (ebc.edicion_capacitacion_id_old = ed.edicion_capacitacion_id_old AND ebc.alumno_id_old = vec.cod_origen )
WHERE (co.lead_source = 'sociolaboral'
OR ((co.lead_source = 'rib') AND cs.forma_parte_interm_lab_c = 'si')) 
AND cs.numero_documento_c IS NOT NULL