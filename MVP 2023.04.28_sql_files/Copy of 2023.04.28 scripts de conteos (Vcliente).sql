-- ***************************************************************************************************************
-- ARCHIVO DE CONTEOS PARA TYP
--
-- IMPORTANTE!!!! LEER!!!
-- Para realizar los conteos se debe:
-- 
-- 1- Abrir el archivo "ConteosEntidadesTyP V2" disponible en drive en la ruta 1_Desarrollo\1_Proyectos\3. Trabajo y Progreso\3. Modelado (Bases Unficadas)
-- 
-- 2- Completar las distintas secciones con las queries de este script
-- 
-- 3- Exportar el archivo a .xlsx


-- HOJA "Atributos Vecinos"
SELECT dv.base_origen,
       COUNT(1) cant_vecinos,
       SUM(CASE WHEN dv.nombre IS NULL THEN 0 ELSE 1 END) completitud_nombre, 
       SUM(CASE WHEN dv.apellido IS NULL THEN 0 ELSE 1 END) completitud_apellido, 
       SUM(CASE WHEN dv.fecha_nacimiento IS NULL THEN 0 ELSE 1 END) completitud_fec_nac, 
       SUM(CASE WHEN dv.descrip_nacionalidad IS NULL THEN 0 ELSE 1 END) completitud_nacionalidad,
       SUM(dv.nombre_valido) nombre_valido, 
       SUM(dv.apellido_valido) apellido_valido
FROM "caba-piba-staging-zone-db"."tbp_typ_def_vecino" dv
GROUP BY dv.base_origen
ORDER BY dv.base_origen


-- ***************************************************************************************************************
-- HOJA "Registros Duplicados Vecinos"
SELECT vec.vecino_id, vec.base_origen, vec.cod_origen, vec.broker_id, vec.tipo_doc_broker, vec.documento_broker, vec.genero_broker, vec.nombre, vec.apellido
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

-- ***************************************************************************************************************
-- HOJA "Vecinos"
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
ORDER BY base_origen

-- CANTIDAD DE VECINOS CON BROKER ID VALIDO
SELECT COUNT(1) cant_vecinos
FROM "caba-piba-staging-zone-db"."tbp_typ_def_vecino" dv
WHERE broker_id_valido = 1 and dni_valido = 0

-- CANTIDAD DE VECINOS CON DNI VALIDO
SELECT COUNT(1) cant_vecinos
FROM "caba-piba-staging-zone-db"."tbp_typ_def_vecino" dv
WHERE broker_id_valido = 0 and dni_valido = 1

-- CANTIDAD DE RENAPER  VALIDO
SELECT COUNT(1) cant_vecinos
FROM "caba-piba-staging-zone-db"."tbp_typ_def_vecino" dv
WHERE broker_id_valido = 0  and renaper_valido=1

-- RENAPER: VALIDOS RENAPER POR DNI Y FECHA NAC
SELECT coincidencia, count(1) 
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_view_ciudadanos_renaper_no_duplicates"
group by coincidencia

--CANTIDAD VECINOS EN BROKER CON Y SIN MI BA (broker_valido_sin_login, broker_valido_con_login)
SELECT CASE WHEN bo.login2_id IS NULL THEN 0 ELSE 1 END con_login,
	   COUNT(1) cant
FROM "caba-piba-staging-zone-db"."tbp_typ_def_vecino" vec
LEFT JOIN "caba-piba-staging-zone-db".tbp_broker_def_broker_general bo ON (bo.id = vec.broker_id)
GROUP BY CASE WHEN bo.login2_id IS NULL THEN 0 ELSE 1 END

-- DNI VALIDO sin login y con login (dni_valido_sin_login dni_valido_con_login)
SELECT CASE WHEN cit.dni IS NULL THEN 'SIN LOGIN' ELSE 'CON LOGIN' END login, 
	   count(1)
FROM "caba-piba-staging-zone-db"."tbp_typ_def_vecino" vec 
LEFT JOIN "caba-piba-consume-zone-db"."login2_citizen" cit ON (CAST(cit.dni AS VARCHAR) = vec.documento_broker) 
WHERE vec.dni_valido = 1 
GROUP BY CASE WHEN cit.dni IS NULL THEN 'SIN LOGIN' ELSE 'CON LOGIN' END 

-- RENAPER VALIDO sin login y con login (renaper_valido_sin_login renaper_valido_con_login)
SELECT CASE WHEN cit.dni IS NULL THEN 'SIN LOGIN' ELSE 'CON LOGIN' END login, 
	   count(1)
FROM "caba-piba-staging-zone-db"."tbp_typ_def_vecino" vec 
LEFT JOIN "caba-piba-consume-zone-db"."login2_citizen" cit ON (CAST(cit.dni AS VARCHAR) = vec.documento_broker) 
WHERE vec.renaper_valido = 1 
GROUP BY CASE WHEN cit.dni IS NULL THEN 'SIN LOGIN' ELSE 'CON LOGIN' END 

--  tabla excel "Sin Match con Broker"
SELECT dv.base_origen,
       COUNT(1) cant_vecinos
FROM "caba-piba-staging-zone-db"."tbp_typ_def_vecino" dv
WHERE broker_id_valido = 0 
GROUP BY dv.base_origen
ORDER BY dv.base_origen

-- tabla excel "Sin Match con Broker" seccion nacionalidad
SELECT nacionalidad, SUM(cantidad) cant
FROM (
SELECT CASE WHEN descrip_nacionalidad IN ('Argentina','Argentina') THEN 'Argentino' 
		    WHEN descrip_nacionalidad IS NULL THEN 'Sin Nacionalidad'
			ELSE 'Extranjeros' END nacionalidad,
	   COUNT(1) cantidad
FROM "caba-piba-staging-zone-db"."tbp_typ_def_vecino" 
WHERE broker_id_valido = 0
GROUP BY descrip_nacionalidad)
GROUP BY nacionalidad

-- ***************************************************************************************************************
-- HOJA "Match Maestro Capacitacion ASI"
-- total de capacitaciones
SELECT COUNT(1)
FROM "caba-piba-staging-zone-db"."tbp_typ_def_capacitacion" 

-- CON MATCH	
SELECT COUNT(1) 
FROM  "caba-piba-staging-zone-db"."tbp_typ_def_capacitacion" tc
WHERE EXISTS (SELECT 1 FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_capacitacion_asi" ca 
WHERE ca.capacitacion_id = tc.capacitacion_id_asi AND ca.base_origen = tc.base_origen)

-- SIN MATCH (No va en el excel, es solo para control dado que el valor se calcula en el excel desde otras celdas)
SELECT COUNT(1) 
FROM  "caba-piba-staging-zone-db"."tbp_typ_def_capacitacion" tc
WHERE NOT EXISTS (SELECT 1 FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_capacitacion_asi" ca 
WHERE ca.capacitacion_id = tc.capacitacion_id_asi AND ca.base_origen = tc.base_origen)

-- (columnas: "match_asi, estado, cantidad")
SELECT 
CASE WHEN ca.base_origen IS NULL THEN 'N' ELSE 'S' END match_asi, tc.estado, COUNT(1) cantidad
FROM  "caba-piba-staging-zone-db"."tbp_typ_def_capacitacion" tc
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_tmp_capacitacion_asi" ca 
	ON (ca.capacitacion_id = tc.capacitacion_id_asi AND ca.base_origen = tc.base_origen)
GROUP BY tc.estado, CASE WHEN ca.base_origen IS NULL THEN 'N' ELSE 'S' END

-- MAESTRO CAPACITACIONES -CATALOGO ASI POR NOMBRE 90%	
-- para calcular el porcentaje de diferencia entre strings se utiliza la funcion levenshtein_distance
-- conjuntamente con greatest para que el porcentaje sea como maximo el 100% independientemente
-- de cual es el string mas extenso
-- Ej.: 15 de 15 caracteres iguales es el 100% => 1.0
-- Ej.: 3 carateres de diferencia es el (15-3)/15 => 80% => 0.8
SELECT count(1)
FROM "caba-piba-staging-zone-db"."tbp_typ_def_capacitacion" tc
INNER JOIN "caba-piba-staging-zone-db"."tbp_typ_tmp_capacitacion_asi" ca
ON (
((cast(greatest(length(ca.descrip_capacitacion), length(tc.descrip_normalizada)) AS DOUBLE)
-CAST(levenshtein_distance(UPPER(ca.descrip_capacitacion),tc.descrip_normalizada) AS DOUBLE))
/CAST(greatest(length(ca.descrip_capacitacion), length(tc.descrip_normalizada)) AS DOUBLE))
>= 0.9 AND tc.capacitacion_id_asi IS NULL)

-- MAESTRO CAPACITACIONES -CATALOGO ASI POR NOMBRE 80%	
SELECT count(1)
FROM "caba-piba-staging-zone-db"."tbp_typ_def_capacitacion" tc
INNER JOIN "caba-piba-staging-zone-db"."tbp_typ_tmp_capacitacion_asi" ca
ON (
((cast(greatest(length(ca.descrip_capacitacion), length(tc.descrip_normalizada)) AS DOUBLE)
-CAST(levenshtein_distance(UPPER(ca.descrip_capacitacion),tc.descrip_normalizada) AS DOUBLE))
/CAST(greatest(length(ca.descrip_capacitacion), length(tc.descrip_normalizada)) AS DOUBLE))
>= 0.8 AND tc.capacitacion_id_asi IS NULL)

-- CATALOGO ASI -MAESTRO CAPACITACIONES POR CODIGO -- cantidad total
SELECT COUNT(1) FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_capacitacion_asi"  

-- CATALOGO ASI -MAESTRO CAPACITACIONES POR CODIGO -- sin match
SELECT COUNT(1) 
FROM  "caba-piba-staging-zone-db"."tbp_typ_tmp_capacitacion_asi" ca 
WHERE NOT EXISTS (SELECT 1 FROM "caba-piba-staging-zone-db"."tbp_typ_def_capacitacion" tc
WHERE ca.capacitacion_id = tc.capacitacion_id_asi AND ca.base_origen = tc.base_origen)

-- CATALOGO ASI -MAESTRO CAPACITACIONES POR CODIGO -- con match
SELECT COUNT(1) 
FROM  "caba-piba-staging-zone-db"."tbp_typ_tmp_capacitacion_asi" ca 
WHERE EXISTS (SELECT 1 FROM "caba-piba-staging-zone-db"."tbp_typ_def_capacitacion" tc
WHERE ca.capacitacion_id = tc.capacitacion_id_asi AND ca.base_origen = tc.base_origen)

-- CATALOGO ASI -MAESTRO CAPACITACIONES POR NOMBRE -- sin match
SELECT COUNT(1) 
FROM  "caba-piba-staging-zone-db"."tbp_typ_tmp_capacitacion_asi" ca 
WHERE NOT EXISTS (SELECT 1 FROM "caba-piba-staging-zone-db"."tbp_typ_def_capacitacion" tc
WHERE ((cast(greatest(length(ca.descrip_capacitacion), length(tc.descrip_normalizada)) AS DOUBLE)
-CAST(levenshtein_distance(UPPER(ca.descrip_capacitacion),tc.descrip_normalizada) AS DOUBLE))
/CAST(greatest(length(ca.descrip_capacitacion), length(tc.descrip_normalizada)) AS DOUBLE)) >= 0.9 AND ca.base_origen = tc.base_origen)

-- CATALOGO ASI -MAESTRO CAPACITACIONES POR NOMBRE -- con match
SELECT COUNT(1) 
FROM  "caba-piba-staging-zone-db"."tbp_typ_tmp_capacitacion_asi" ca 
WHERE  EXISTS (SELECT 1 FROM "caba-piba-staging-zone-db"."tbp_typ_def_capacitacion" tc
WHERE ((cast(greatest(length(ca.descrip_capacitacion), length(tc.descrip_normalizada)) AS DOUBLE)
-CAST(levenshtein_distance(UPPER(ca.descrip_capacitacion),tc.descrip_normalizada) AS DOUBLE))
/CAST(greatest(length(ca.descrip_capacitacion), length(tc.descrip_normalizada)) AS DOUBLE)) >= 0.9 AND ca.base_origen = tc.base_origen)

-- REGISTROS CON MATCH POR SIMILITUD DE NOMBRES
SELECT 
tc.descrip_normalizada "descrip_maestro", 
upper(ca.descrip_capacitacion) "descrip asi",
tc.base_origen,
tc.id "id maestro", 
tc.tipo_capacitacion, 
tc.estado, 
ca.capacitacion_id "codigo_capacitacion_asi",
'90%' "grado de confianza"
FROM "caba-piba-staging-zone-db"."tbp_typ_def_capacitacion" tc 
INNER JOIN "caba-piba-staging-zone-db"."tbp_typ_tmp_capacitacion_asi" ca
ON (
((cast(greatest(length(ca.descrip_capacitacion), length(tc.descrip_normalizada)) AS DOUBLE)
-CAST(levenshtein_distance(UPPER(ca.descrip_capacitacion),tc.descrip_normalizada) AS DOUBLE))
/CAST(greatest(length(ca.descrip_capacitacion), length(tc.descrip_normalizada)) AS DOUBLE)) >= 0.9 AND tc.capacitacion_id_asi IS NULL)

-- ***************************************************************************************************************
-- HOJA "Atributos tablas def"
-- Ejecución de script
-- git\estandarizacion-proceso-de-gobernanza-de-datos\4-scripts\conexion_aws_y_calculo_calidad.py
-- Al ejecutarlo se generará un archivo .xlsx cuyo contenido debe copiarse en la hoja "Atributos tablas def"
