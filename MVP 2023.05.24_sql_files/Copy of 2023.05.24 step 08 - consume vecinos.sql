-- 1.- Crear la tabla definitiva de vecinos
-- --<sql>--DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_def_vecino`;--</sql>--
--<sql>--
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_def_vecino" AS
WITH tmp_vec_broker AS
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
LEFT JOIN "caba-piba-staging-zone-db"."tbp_broker_def_broker_general" do ON (CAST(do.documento_broker AS VARCHAR) = vec.documento_broker AND do.id != vec.broker_id_din)
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
		 CAST(do.documento_broker AS VARCHAR)),
tmp_vec_renaper AS(
	   SELECT tvb.vecino_id,
	   tvb.base_origen,
	   tvb.cod_origen,
	   tvb.broker_id,
	   tvb.broker_id_est,
	   tvb.tipo_documento,
	   tvb.tipo_doc_broker,
	   tvb.documento_broker,
	   tvb.nombre,
	   tvb.apellido,
	   UPPER(cr.apellidos) apellido_renaper,
	   UPPER(cr.nombres) nombre_renaper,
	   COALESCE(DATE_PARSE(cr.fecha_nacimiento,'%Y-%m-%d'),tvb.fecha_nacimiento) fecha_nacimiento,
	   tvb.genero_broker,
	   tvb.nacionalidad,
	   tvb.descrip_nacionalidad,
	   tvb.nacionalidad_broker,
	   tvb.nombre_valido,
	   tvb.apellido_valido,
	   tvb.broker_id_valido,
	   CASE WHEN tvb.broker_id_valido = 1 AND tvb.dni_valido = 1 THEN 0    
	        WHEN tvb.broker_id_valido = 0 AND tvb.dni_valido = 1 THEN 1
			ELSE 0 END dni_valido,
	   CASE WHEN cr.coincidencia = 'si' AND tvb.broker_id_valido = 0 AND tvb.dni_valido = 0 THEN 1
		    WHEN cr.coincidencia = 'no' AND 
				 UPPER(observaciones) LIKE UPPER('%la fecha de nacimiento informada por renaper%') AND 
				 levenshtein_distance(UPPER(cr.apellidos),tvb.apellido) <= 0.9 AND
				 levenshtein_distance(UPPER(cr.nombres),tvb.nombre) <= 0.9 AND 
				 tvb.broker_id_valido = 0 AND tvb.dni_valido = 0 THEN 1
			ELSE 0 END renaper_valido
FROM tmp_vec_broker tvb
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_tmp_view_ciudadanos_renaper_no_duplicates" cr ON (cr.dni = tvb.documento_broker AND cr.sexo = tvb.genero_broker))
SELECT tvc.vecino_id,
	   tvc.base_origen,
	   tvc.cod_origen,
	   tvc.broker_id,
	   tvc.broker_id_est,
	   tvc.tipo_documento,
	   tvc.tipo_doc_broker,
	   tvc.documento_broker,
	   tvc.nombre,
	   tvc.apellido,
	   tvc.nombre_renaper,
	   tvc.apellido_renaper,
	   tvc.fecha_nacimiento,
	   tvc.genero_broker,
	   tvc.nacionalidad,
	   tvc.descrip_nacionalidad,
	   tvc.nacionalidad_broker,
	   tvc.nombre_valido,
	   tvc.apellido_valido,
	   tvc.broker_id_valido,
	   tvc.dni_valido,
	   tvc.renaper_valido
FROM tmp_vec_renaper tvc
--</sql>--