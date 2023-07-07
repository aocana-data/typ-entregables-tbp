-- Crear tabla def de curriculum
--<sql>--
DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_def_curriculum`;
--</sql>--

--<sql>--
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_def_curriculum" AS
WITH nivel_educativo_ponderado as 
(
SELECT c.*,
		CASE
			UPPER(c.nivel_educativo)
			WHEN 'MAESTRÍA' THEN CASE
				UPPER(c.nivel_educativo_estado)
				WHEN 'GRADUADO' THEN 2
				WHEN 'EN CURSO' THEN 6
				WHEN 'ABANDONADO' THEN 10
			END
			WHEN 'DOCTORADO' THEN CASE
				UPPER(c.nivel_educativo_estado)
				WHEN 'GRADUADO' THEN 1
				WHEN 'EN CURSO' THEN 5
				WHEN 'ABANDONADO' THEN 9
			END
			WHEN 'POSTGRADO' THEN CASE
				UPPER(c.nivel_educativo_estado)
				WHEN 'GRADUADO' THEN 3
				WHEN 'EN CURSO' THEN 7
				WHEN 'ABANDONADO' THEN 11
			END
			WHEN 'UNIVERSITARIO' THEN CASE
				UPPER(c.nivel_educativo_estado)
				WHEN 'GRADUADO' THEN 4
				WHEN 'EN CURSO' THEN 8
				WHEN 'ABANDONADO' THEN 12
			END
			WHEN 'TERCIARIO' THEN CASE
				UPPER(c.nivel_educativo_estado)
				WHEN 'GRADUADO' THEN 13
				WHEN 'EN CURSO' THEN 14
				WHEN 'ABANDONADO' THEN 15
			END
			WHEN 'SECUNDARIO' THEN CASE
				UPPER(c.nivel_educativo_estado)
				WHEN 'GRADUADO' THEN 16
				WHEN 'EN CURSO' THEN 17
				WHEN 'ABANDONADO' THEN 18
			END
			WHEN 'PRIMARIO' THEN CASE
				UPPER(c.nivel_educativo_estado)
				WHEN 'GRADUADO' THEN 19
				WHEN 'EN CURSO' THEN 20
				WHEN 'ABANDONADO' THEN 21
			END
			WHEN 'OTRO' THEN CASE
				UPPER(c.nivel_educativo_estado)
				WHEN 'GRADUADO' THEN 22
				WHEN 'EN CURSO' THEN 23
				WHEN 'ABANDONADO' THEN 24
			END
				WHEN 'OTROS' THEN CASE
				UPPER(c.nivel_educativo_estado)
				WHEN 'GRADUADO' THEN 22
				WHEN 'EN CURSO' THEN 23
				WHEN 'ABANDONADO' THEN 24
			END
		END AS prioridad_nivel_educativo
	FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_curriculum" c
),
nivel_educactivo_pe_ordenado AS (
	SELECT *,
		ROW_NUMBER() OVER(
			PARTITION BY tipo_doc_broker, documento_broker
			ORDER BY prioridad_nivel_educativo ASC
		) AS "orden_duplicado"
	FROM nivel_educativo_ponderado
),
cv_con_duplicados AS (
	SELECT cv.base_origen || cv.cod_origen as id,
	cv.cod_origen id_old,
	cv.base_origen,
	vec.id_vecino,
	cv.modalidad,
	TRY_CAST(cv.fecha_publicacion AS DATE) fecha_publicacion,
	cv.disponibilidad,
	cv.presentacion,
	cv.estado,
	cv.metas,
	CASE WHEN UPPER(cv.nivel_educativo) LIKE 'OTRO%' THEN 'OTRO' ELSE UPPER(cv.nivel_educativo) END nivel_educativo,
	cv.nivel_educativo_estado,
	cv.capacidades_diferentes tipo_discapacidad,
	cv.licencia_conducir,
	ROW_NUMBER() OVER(
			PARTITION BY cv.tipo_doc_broker, cv.documento_broker
			ORDER BY CASE cv.tipo_doc_broker
					WHEN 'DNI' THEN 1
					WHEN 'CUIL' THEN 2
					WHEN 'LC' THEN 3
					WHEN 'LE' THEN 4
					WHEN 'CE' THEN 5
					WHEN 'CI' THEN 6
					WHEN 'PE' THEN 7
					WHEN 'OTRO' THEN 8
					WHEN 'NN' THEN 9
					ELSE 10
				  END
		) AS "orden_duplicado_vecino"
FROM 
	"nivel_educactivo_pe_ordenado" cv
	JOIN "caba-piba-staging-zone-db"."tbp_typ_def_vecino" vec ON (
		cv.base_origen = vec.base_origen
		AND cv.tipo_doc_broker = vec.tipo_doc_broker
		AND CAST(cv.documento_broker AS VARCHAR) = vec.documento_broker
	)
WHERE cv.orden_duplicado = 1
GROUP BY  
	cv.base_origen || cv.cod_origen,
	cv.cod_origen,
	cv.base_origen,
	vec.id_vecino,
	cv.modalidad,
	TRY_CAST(cv.fecha_publicacion AS DATE),
	cv.disponibilidad,
	cv.presentacion,
	cv.estado,
	cv.metas,
	CASE WHEN UPPER(cv.nivel_educativo) LIKE 'OTRO%' THEN 'OTRO' ELSE UPPER(cv.nivel_educativo) END,
	cv.nivel_educativo_estado,
	cv.capacidades_diferentes,
	cv.licencia_conducir,
	cv.tipo_doc_broker, 
	cv.documento_broker
)
SELECT  id,
	id_old,
	base_origen,
	id_vecino,
	modalidad,
	fecha_publicacion,
	disponibilidad,
	presentacion,
	estado,
	metas,
	nivel_educativo,
	nivel_educativo_estado,
	tipo_discapacidad,
	licencia_conducir
FROM cv_con_duplicados
WHERE orden_duplicado_vecino = 1
--</sql>--	