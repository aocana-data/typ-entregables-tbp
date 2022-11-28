--- CRM SOCIO LABORAL

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

CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_def_capacitacion" AS
SELECT tc.id,
	   tc.id_new,
       tc.base_origen,
       tc.tipo_capacitacion,
       tc.descrip_normalizada,
       tc.fecha_inicio,
       tc.fecha_fin,
       tc.categoria,
       tc.estado,
	   ca.capacitacion_id capacitacion_id_asi,
	   ca.programa_id,
	   ca.descrip_capacitacion,
	   ca.tipo_formacion,
	   ca.descrip_tipo_formacion,
	   ca.modalidad_id,
	   ca.descrip_modalidad,
	   ca.tipo_capacitacion tipo_capacitacion_asi,
	   ca.estado_capacitacion,
	   ca.seguimiento_personalizado,
	   ca.soporte_online,
	   ca.incentivos_terminalidad,
	   ca.exclusividad_participantes,
	   ca.categoria_back_id, 
	   ca.descrip_back,
	   ca.categoria_front_id,
	   ca.descrip_front,
	   ca.detalle_capacitacion,
	   ca.otorga_certificado,
	   ca.filtro_ingreso,
	   ca.frecuencia_oferta_anual,
	   ca.duracion_cantidad,
	   ca.duracion_medida,
	   ca.duracion_dias,
	   ca.duracion_hs_reloj,
	   ca.vacantes
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_capacitacion" tc
INNER JOIN "caba-piba-staging-zone-db"."tbp_typ_def_capacitacion_match" cm ON (cm.base_origen = tc.base_origen AND cm.id_new = tc.id_new)
INNER JOIN "caba-piba-staging-zone-db"."tbp_typ_tmp_capacitacion_asi" ca ON (ca.base_origen = tc.base_origen AND ca.codigo_capacitacion = cm.id_old)
GROUP BY tc.id,
	   tc.id_new,
       tc.base_origen,
       tc.tipo_capacitacion,
       tc.descrip_normalizada,
       tc.fecha_inicio,
       tc.fecha_fin,
       tc.categoria,
       tc.estado,
	   ca.capacitacion_id,
	   ca.programa_id,
	   ca.descrip_capacitacion,
	   ca.tipo_formacion,
	   ca.descrip_tipo_formacion,
	   ca.modalidad_id,
	   ca.descrip_modalidad,
	   ca.tipo_capacitacion,
	   ca.estado_capacitacion,
	   ca.seguimiento_personalizado,
	   ca.soporte_online,
	   ca.incentivos_terminalidad,
	   ca.exclusividad_participantes,
	   ca.categoria_back_id, 
	   ca.descrip_back,
	   ca.categoria_front_id,
	   ca.descrip_front,
	   ca.detalle_capacitacion,
	   ca.otorga_certificado,
	   ca.filtro_ingreso,
	   ca.frecuencia_oferta_anual,
	   ca.duracion_cantidad,
	   ca.duracion_medida,
	   ca.duracion_dias,
	   ca.duracion_hs_reloj,
	   ca.vacantes
UNION
SELECT tc.id,
	   tc.id_new,
       tc.base_origen,
       tc.tipo_capacitacion,
       tc.descrip_normalizada,
       tc.fecha_inicio,
       tc.fecha_fin,
       tc.categoria,
       tc.estado,
	   NULL capacitacion_id_asi,
	   NULL programa_id,
	   NULL descrip_capacitacion,
	   NULL tipo_formacion,
	   NULL descrip_tipo_formacion,
	   NULL modalidad_id,
	   NULL descrip_modalidad,
	   NULL tipo_capacitacion_asi,
	   NULL estado_capacitacion,
	   NULL seguimiento_personalizado,
	   NULL soporte_online,
	   NULL incentivos_terminalidad,
	   NULL exclusividad_participantes,
	   NULL categoria_back_id, 
	   NULL descrip_back,
	   NULL categoria_front_id,
	   NULL descrip_front,
	   NULL detalle_capacitacion,
	   NULL otorga_certificado,
	   NULL filtro_ingreso,
	   NULL frecuencia_oferta_anual,
	   NULL duracion_cantidad,
	   NULL duracion_medida,
	   NULL duracion_dias,
	   NULL duracion_hs_reloj,
	   NULL vacantes
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_capacitacion" tc
WHERE NOT EXISTS (SELECT 1 FROM "caba-piba-staging-zone-db"."tbp_typ_def_capacitacion_match" cm,
	                          "caba-piba-staging-zone-db"."tbp_typ_tmp_capacitacion_asi" ca  
			    WHERE cm.base_origen = tc.base_origen AND 
					  cm.id_new = tc.id_new AND
					  ca.base_origen = tc.base_origen AND 
					  ca.codigo_capacitacion = cm.id_old)
GROUP BY tc.id,
	   tc.id_new,
       tc.base_origen,
       tc.tipo_capacitacion,
       tc.descrip_normalizada,
       tc.fecha_inicio,
       tc.fecha_fin,
       tc.categoria,
       tc.estado