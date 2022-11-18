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
