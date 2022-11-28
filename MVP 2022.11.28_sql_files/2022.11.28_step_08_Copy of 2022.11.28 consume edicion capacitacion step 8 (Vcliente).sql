-- EDICION CAPACITACION SIENFO
-- 1.- Crear tabla edicion capacitacion definitiva
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_def_edicion_capacitacion" AS
SELECT row_number() OVER () AS id,
       ed.base_origen,
	   ed.tipo_capacitacion,
       ed.capacitacion_id_new,
       ed.capacitacion_id_old,
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
