-- Crear tabla def de curriculum
-- --<sql>--DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_def_curriculum`;--</sql>--
--<sql>--
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_def_curriculum" AS
SELECT cv.base_origen || cv.cod_origen as id,
	cv.cod_origen id_old,
	cv.base_origen,
	vec.vecino_id,
	cv.modalidad,
	TRY_CAST(cv.fecha_publicacion AS DATE) fecha_publicacion,
	cv.disponibilidad,
	cv.presentacion,
	cv.estado,
	cv.metas,
	CASE WHEN UPPER(cv.nivel_educativo) LIKE 'OTRO%' THEN 'OTRO' ELSE UPPER(cv.nivel_educativo) END nivel_educativo,
	cv.nivel_educativo_estado,
	cv.capacidades_diferentes tipo_discapacidad
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_curriculum" cv
	JOIN "caba-piba-staging-zone-db"."tbp_typ_def_vecino" vec ON (
		cv.base_origen = vec.base_origen
		AND cv.tipo_doc_broker = vec.tipo_doc_broker
		AND CAST(cv.documento_broker AS VARCHAR) = vec.documento_broker
	)
GROUP BY  
	cv.base_origen || cv.cod_origen,
	cv.cod_origen,
	cv.base_origen,
	vec.vecino_id,
	cv.modalidad,
	TRY_CAST(cv.fecha_publicacion AS DATE),
	cv.disponibilidad,
	cv.presentacion,
	cv.estado,
	cv.metas,
	CASE WHEN UPPER(cv.nivel_educativo) LIKE 'OTRO%' THEN 'OTRO' ELSE UPPER(cv.nivel_educativo) END,
	cv.nivel_educativo_estado,
	cv.capacidades_diferentes
--</sql>--	