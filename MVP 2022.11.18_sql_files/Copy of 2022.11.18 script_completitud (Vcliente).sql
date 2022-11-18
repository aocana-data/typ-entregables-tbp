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
WHERE broker_id_valido = 1 and dni_valido = 0

-- CANTIDAD DE VECINOS CON DNI VALIDO
SELECT COUNT(1) cant_vecinos
FROM "caba-piba-staging-zone-db"."tbp_typ_def_vecino" dv
WHERE broker_id_valido = 0 and dni_valido = 1

-- CANTIDAD DE VECINOS SIN MATCH
SELECT COUNT(1) cant_vecinos
FROM "caba-piba-staging-zone-db"."tbp_typ_def_vecino" dv
WHERE broker_id_valido = 0 and dni_valido = 0

-- CANTIDAD DE VECINOS SIN MATCH POR BASE
SELECT dv.base_origen,
       COUNT(1) cant_vecinos
FROM "caba-piba-staging-zone-db"."tbp_typ_def_vecino" dv
WHERE broker_id_valido = 0 and dni_valido = 0
GROUP BY dv.base_origen

SELECT nacionalidad, SUM(cantidad) cant
FROM (
SELECT CASE WHEN descrip_nacionalidad IN ('Argentina','Argentina') THEN 'Argentino' 
		    WHEN descrip_nacionalidad IS NULL THEN 'Sin Nacionalidad'
			ELSE 'Extranjeros' END nacionalidad,
	   COUNT(1) cantidad
FROM "caba-piba-staging-zone-db"."tbp_typ_def_vecino" 
WHERE broker_id_valido = 0 and dni_valido = 0
GROUP BY descrip_nacionalidad)
GROUP BY nacionalidad

SELECT count(1)
FROM "caba-piba-staging-zone-db"."tbp_typ_def_vecino" vec 
WHERE broker_id_valido = 0 and dni_valido = 0


-- Registros no enviados a SINTYS


-- CANTIDAD DE VECINOS ENVIADOS A SINTYS CON ERROR
SELECT count(1)
FROM "caba-piba-staging-zone-db"."tbp_typ_def_vecino" vec 
WHERE broker_id_valido = 0 and dni_valido = 0
AND EXISTS (SELECT 1
                  FROM  "caba-piba-raw-zone-db"."sintys_base_origen" sbo
                  WHERE vec.documento_broker = sbo.ndoc_orig)
				  
				  

-- CANTIDAD DE VECINOS NO ENVIADOS A SINTYS 
SELECT count(1)
FROM "caba-piba-staging-zone-db"."tbp_typ_def_vecino" vec 
WHERE broker_id_valido = 0 and dni_valido = 0
AND NOT EXISTS (SELECT 1
                  FROM  "caba-piba-raw-zone-db"."sintys_base_origen" sbo
                  WHERE vec.documento_broker = sbo.ndoc_orig)
				  
-- CANTIDAD DE VECINOS ENVIADOS A SINTYS CON ERROR CON LONGITUD VALIDA
SELECT count(1)
FROM "caba-piba-staging-zone-db"."tbp_typ_def_vecino" vec 
WHERE broker_id_valido = 0 and dni_valido = 0 AND 
LENGTH(CASE WHEN tipo_doc_broker NOT IN ('CUIT', 'CUIL') THEN CAST(documento_broker AS VARCHAR)
			WHEN tipo_doc_broker IN ('CUIT', 'CUIL') THEN SUBSTR(CAST(documento_broker AS VARCHAR), 3, 8) END) IN (7,8)
AND EXISTS (SELECT 1
                  FROM  "caba-piba-raw-zone-db"."sintys_base_origen" sbo
                  WHERE vec.documento_broker = sbo.ndoc_orig)					  
				  
				  
-- CANTIDAD DE VECINOS CON LONGITUD INVALIDA
SELECT count(1)
FROM "caba-piba-staging-zone-db"."tbp_typ_def_vecino" vec 
WHERE broker_id_valido = 0 and dni_valido = 0 AND 
LENGTH(CASE WHEN tipo_doc_broker NOT IN ('CUIT', 'CUIL') THEN CAST(documento_broker AS VARCHAR)
			WHEN tipo_doc_broker IN ('CUIT', 'CUIL') THEN SUBSTR(CAST(documento_broker AS VARCHAR), 3, 8) END) NOT IN (7,8)
AND EXISTS (SELECT 1
                  FROM  "caba-piba-raw-zone-db"."sintys_base_origen" sbo
                  WHERE vec.documento_broker = sbo.ndoc_orig)					

-- CANTIDAD DE VECINOS ERRONEOS LONGITUD VALIDA POR NACIONALIDAD
SELECT descrip_nacionalidad, count(1)
FROM "caba-piba-staging-zone-db"."tbp_typ_def_vecino" vec 
WHERE broker_id_valido = 0 and dni_valido = 0 AND 
LENGTH(CASE WHEN tipo_doc_broker NOT IN ('CUIT', 'CUIL') THEN CAST(documento_broker AS VARCHAR)
			WHEN tipo_doc_broker IN ('CUIT', 'CUIL') THEN SUBSTR(CAST(documento_broker AS VARCHAR), 3, 8) END) IN (7,8)
AND EXISTS (SELECT 1
                  FROM  "caba-piba-raw-zone-db"."sintys_base_origen" sbo
                  WHERE vec.documento_broker = sbo.ndoc_orig)	
GROUP BY descrip_nacionalidad				  

-- CANTIDAD DE VECINOS ERRONEOS LONGITUD VALIDA POR GENERO
SELECT genero_broker, count(1)
FROM "caba-piba-staging-zone-db"."tbp_typ_def_vecino" vec 
WHERE broker_id_valido = 0 and dni_valido = 0 AND 
LENGTH(CASE WHEN tipo_doc_broker NOT IN ('CUIT', 'CUIL') THEN CAST(documento_broker AS VARCHAR)
			WHEN tipo_doc_broker IN ('CUIT', 'CUIL') THEN SUBSTR(CAST(documento_broker AS VARCHAR), 3, 8) END) IN (7,8)
AND EXISTS (SELECT 1
                  FROM  "caba-piba-raw-zone-db"."sintys_base_origen" sbo
                  WHERE vec.documento_broker = sbo.ndoc_orig)	
GROUP BY genero_broker


-- CANTIDAD DE VECINOS ERRONEOS LONGITUD VALIDA POR TIPO_DNI
SELECT tipo_doc_broker, count(1)
FROM "caba-piba-staging-zone-db"."tbp_typ_def_vecino" vec 
WHERE broker_id_valido = 0 and dni_valido = 0 AND 
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
