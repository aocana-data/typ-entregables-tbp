CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_def_trayectoria_educativa" AS
SELECT row_number() OVER () AS tray_edu_id,
	   bg.id,
       bg.apellidos_nombres,
       bg.nombre,
       bg.apellido,
       bg.tipo_doc_broker,
       bg.documento_broker,
       vec.nacionalidad_broker,
       bg.genero,
       dc.id capacitacion_id,
       dc.descrip_normalizada descrip_capacitacion,
       dc.programa_id,
	   p.codigo_programa,
	   p.nombre_programa descrip_programa,
       dcu.fecha_inicio fecha_inicio_cursada,
       dcu.estado_beneficiario
       FROM "caba-piba-staging-zone-db"."tbp_broker_def_broker_general" bg  
       INNER JOIN "caba-piba-staging-zone-db"."tbp_typ_def_vecino" vec ON (bg.id = vec.broker_id)
       LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_cursada" dcu ON (dcu.broker_id = vec.broker_id) 
	   LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_edicion_capacitacion" ed ON (ed.id = dcu.edicion_capacitacion_id_new AND ed.base_origen = dcu.base_origen)
       LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_capacitacion" dc ON (dc.id_new = ed.capacitacion_id_new AND dc.base_origen = ed.base_origen)
	   LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_programa" p ON (p.programa_id = dc.programa_id)
GROUP BY bg.id,
       bg.apellidos_nombres,
       bg.nombre,
       bg.apellido,
       bg.tipo_doc_broker,
       bg.documento_broker,
       vec.nacionalidad_broker,
       bg.genero,
       dc.id,
       dc.descrip_normalizada,
       dc.programa_id,
	   p.codigo_programa,
	   p.nombre_programa,
       dcu.fecha_inicio,
       dcu.estado_beneficiario