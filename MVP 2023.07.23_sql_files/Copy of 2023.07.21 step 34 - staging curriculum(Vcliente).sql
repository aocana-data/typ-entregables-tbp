-- Crear tabla tmp de curriculum
--<sql>--
DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_tmp_curriculum`;
--</sql>--

--<sql>--
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_curriculum" AS 
WITH datos_broker AS (
	SELECT bg.*
	FROM "caba-piba-raw-zone-db"."crm_empleo_experiencia_laboral__c" el
		INNER JOIN "caba-piba-raw-zone-db"."crm_empleo_entrevista__c" e ON (
			el.postulante__c = e.postulante__c
			AND e.dni__c IS NOT NULL
		)
		INNER JOIN "caba-piba-staging-zone-db"."tbp_broker_def_broker_general" bg ON (bg.documento_broker = CAST(e.dni__c AS INT))
	WHERE 1 = (
			SELECT COUNT(1)
			FROM "caba-piba-staging-zone-db"."tbp_broker_def_broker_general" bg1
			WHERE bg1.documento_broker = bg.documento_broker
			GROUP BY bg1.documento_broker
		)
),
datos_broker_1 AS (
	SELECT bg.*
	FROM "caba-piba-raw-zone-db"."crm_sociolaboral_contacts_cstm"  cc
		INNER JOIN "caba-piba-staging-zone-db"."tbp_broker_def_broker_general" bg ON (bg.documento_broker = cc.numero_documento_c)
	WHERE 1 = (
			SELECT COUNT(1)
			FROM "caba-piba-staging-zone-db"."tbp_broker_def_broker_general" bg1
			WHERE bg1.documento_broker = bg.documento_broker
			GROUP BY bg1.documento_broker
		)
),
nivel_educativo AS (
	SELECT cc.id_c,
		ee.document_name,
		CASE
			WHEN UPPER(TRIM(ee.document_name)) IN (
				'SECUNDARIO FINALIZADO',
				'SECUNDARIO',
				UPPER('Secundario Técnico')
			) THEN 'Secundario'
			WHEN UPPER(TRIM(ee.document_name)) IN (
				'TERCIARIO COMPLETO',
				'TERCIARIO FINALIZADO',
				'TERCIARIO'
			) THEN 'Terciario'
			WHEN UPPER(TRIM(ee.document_name)) IN (
				'UNIVERSITARIO COMPLETO',
				'UNIVERSITARIO FINALIZADO',
				'UNIVERSITARIO',
				UPPER('ingienieria en sistema')
			) THEN 'Universitario'
			WHEN UPPER(TRIM(ee.document_name)) LIKE 'BACHILLER%' THEN 'Secundario'
			WHEN UPPER(TRIM(ee.document_name)) LIKE 'SECUNDARI%COMPLE%' THEN 'Secundario'
			WHEN UPPER(TRIM(ee.document_name)) LIKE 'LICENCIATURA%' THEN 'Universitario'
			WHEN UPPER(TRIM(ee.document_name)) LIKE '%EN%CURSO%' THEN 'En curso' ELSE 'Otros'
		END nivel_educativo
	FROM "caba-piba-raw-zone-db"."crm_sociolaboral_es_estudios" ee
		INNER JOIN "caba-piba-raw-zone-db"."crm_sociolaboral_es_estudios_contacts_c" ecc ON (ecc.es_estudios_contactses_estudios_idb = ee.id)
		INNER JOIN "caba-piba-staging-zone-db"."tbp_typ_tmp_view_crm_sociolaboral_contacts_cstm_no_duplicates" cc ON (cc.id_c = ecc.es_estudios_contactscontacts_ida)
	WHERE ee.active_date = (
			SELECT MAX(ee1.active_date)
			FROM "caba-piba-raw-zone-db"."crm_sociolaboral_es_estudios" ee1
				INNER JOIN "caba-piba-raw-zone-db"."crm_sociolaboral_es_estudios_contacts_c" ecc1 ON (
					ecc1.es_estudios_contactses_estudios_idb = ee1.id
				)
			WHERE cc.id_c = ecc1.es_estudios_contactscontacts_ida
		)
	GROUP BY cc.id_c,
		ee.document_name
),
nivel_educactivo_pe_ponderado AS (
	SELECT CAST(af.curriculum_id AS VARCHAR) AS cv_id,
		UPPER(ls.value) as estado,
		UPPER(el.value) as nivel_capacitacion,
		CASE
			UPPER(el.value)
			WHEN 'MAESTRÍA' THEN CASE
				UPPER(ls.value)
				WHEN 'GRADUADO' THEN 2
				WHEN 'EN CURSO' THEN 6
				WHEN 'ABANDONADO' THEN 10
			END
			WHEN 'DOCTORADO' THEN CASE
				UPPER(ls.value)
				WHEN 'GRADUADO' THEN 1
				WHEN 'EN CURSO' THEN 5
				WHEN 'ABANDONADO' THEN 9
			END
			WHEN 'POSTGRADO' THEN CASE
				UPPER(ls.value)
				WHEN 'GRADUADO' THEN 3
				WHEN 'EN CURSO' THEN 7
				WHEN 'ABANDONADO' THEN 11
			END
			WHEN 'UNIVERSITARIO' THEN CASE
				UPPER(ls.value)
				WHEN 'GRADUADO' THEN 4
				WHEN 'EN CURSO' THEN 8
				WHEN 'ABANDONADO' THEN 12
			END
			WHEN 'TERCIARIO' THEN CASE
				UPPER(ls.value)
				WHEN 'GRADUADO' THEN 13
				WHEN 'EN CURSO' THEN 14
				WHEN 'ABANDONADO' THEN 15
			END
			WHEN 'SECUNDARIO' THEN CASE
				UPPER(ls.value)
				WHEN 'GRADUADO' THEN 16
				WHEN 'EN CURSO' THEN 17
				WHEN 'ABANDONADO' THEN 18
			END
			WHEN 'PRIMARIO' THEN CASE
				UPPER(ls.value)
				WHEN 'GRADUADO' THEN 19
				WHEN 'EN CURSO' THEN 20
				WHEN 'ABANDONADO' THEN 21
			END
			WHEN 'OTRO' THEN CASE
				UPPER(ls.value)
				WHEN 'GRADUADO' THEN 22
				WHEN 'EN CURSO' THEN 23
				WHEN 'ABANDONADO' THEN 24
			END
		END AS prioridad_nivel_educativo
	FROM "caba-piba-raw-zone-db"."portal_empleo_instructions" i
		JOIN "caba-piba-raw-zone-db"."portal_empleo_academic_formations" af ON (
			i.id = af.id
			AND instruction_type LIKE 'ACADEMIC_FORMATION'
		)
		JOIN "caba-piba-raw-zone-db"."portal_empleo_education_level_status" ls ON (i.education_level_status_id = ls.id)
		JOIN "caba-piba-raw-zone-db"."portal_empleo_mtr_education_level" el ON (af.education_level_id = el.id)
),
nivel_educactivo_pe_ordenado AS (
	SELECT *,
		ROW_NUMBER() OVER(
			PARTITION BY cv_id
			ORDER BY prioridad_nivel_educativo ASC
		) AS "orden_duplicado"
	FROM nivel_educactivo_pe_ponderado
),
nivel_educactivo_pe AS (
	SELECT cv_id, estado, nivel_capacitacion
	FROM nivel_educactivo_pe_ordenado
	WHERE orden_duplicado = 1
)
SELECT CAST(cv.id AS VARCHAR) cod_origen,
	'PORTALEMPLEO' base_origen,
	MAX(i.start_date) AS fecha_publicacion,
	-- assumption ultima fecha de postulacion es la ultima fecha de publicacion del cv
	COALESCE(CAST(ja.application_date AS timestamp), MIN(i.end_date)) AS fecha_ultima_modificacion,
	UPPER(ct.name) capacidades_diferentes,
	cv.presentation presentacion,
	cv.goals metas,
	CASE
		WHEN cv.disabled = 0 THEN 'NO VIGENTE' ELSE 'VIGENTE'
	END estado,
	c.nationality nacionalidad,
	CASE
		WHEN (
			c.doc_type IN ('DNI', 'LC', 'CI', 'LE', 'CUIL')
		) THEN c.doc_type
		WHEN (c.doc_type = 'PAS') THEN 'PE'
		WHEN (c.doc_type = 'DE') THEN 'CE'
		WHEN (c.doc_type = 'CRP') THEN 'OTRO' ELSE 'NN'
	END tipo_doc_broker,
	c.doc_number  documento_broker,
	CASE
		WHEN (c.gender NOT IN ('F', 'M')) THEN 'X' ELSE c.gender
	END genero_broker,
	CAST(c.miba_id AS VARCHAR) login2_id,
	CASE
		WHEN UPPER(SUBSTR(c.document_nationality, 1, 3)) = 'ARG' THEN 'ARG' ELSE 'NN'
	END nacionalidad_broker,
	ta.name disponibilidad,
	m.name modalidad,
 	ne.nivel_capacitacion AS nivel_educativo,
 	ne.estado AS nivel_educativo_estado,
	CONCAT(
		(
			CASE
				WHEN (
					c.doc_type IN ('DNI', 'LC', 'CI', 'LE', 'CUIL')
				) THEN c.doc_type ELSE 'NN'
			END
		),
		CAST(c.doc_number AS varchar),
		(
			CASE
				WHEN (c.gender = 'M') THEN 'M'
				WHEN (c.gender = 'F') THEN 'F' ELSE 'X'
			END
		),
		(
			CASE
				WHEN UPPER(SUBSTR(c.document_nationality, 1, 3)) = 'ARG' THEN 'ARG' ELSE 'NN'
			END
		)
	) broker_id,
	dr.code || '|' || dr.value AS licencia_conducir
FROM "caba-piba-raw-zone-db"."portal_empleo_curriculum_vitaes" cv
INNER JOIN "caba-piba-raw-zone-db"."portal_empleo_candidates" c ON (c.id = cv.candidate_id)
	LEFT JOIN "caba-piba-raw-zone-db"."portal_empleo_academic_formations" af ON (af.curriculum_id = cv.id)
	LEFT JOIN "caba-piba-raw-zone-db"."portal_empleo_education_level_status" eds ON (eds.id = af.education_level_id)
	JOIN "caba-piba-raw-zone-db"."portal_empleo_instructions" i ON (
			i.id = af.id AND instruction_type LIKE 'ACADEMIC_FORMATION')
 	LEFT JOIN nivel_educactivo_pe ne ON (ne.cv_id = CAST(cv.id AS VARCHAR))
	LEFT JOIN "caba-piba-raw-zone-db"."portal_empleo_condition_types" ct ON (ct.id = cv.condition_type_id)
	LEFT JOIN "caba-piba-raw-zone-db"."portal_empleo_mtr_time_availability" ta ON (ta.id = cv.availability_id)
	LEFT JOIN "caba-piba-raw-zone-db"."portal_empleo_mtr_working_modalities" m ON (m.id = modality_id)
	LEFT JOIN "caba-piba-raw-zone-db"."portal_empleo_job_applications" ja ON (ja.candidate_id = cv.candidate_id)
	LEFT JOIN "caba-piba-raw-zone-db"."portal_empleo_driving_records" dr ON (dr.id=cv.driving_licence_id)
GROUP BY cv.id,
	UPPER(ct.name),
	cv.presentation,
	cv.goals,
	cv.disabled,
	c.nationality,
	CASE
		WHEN (
			c.doc_type IN ('DNI', 'LC', 'CI', 'LE', 'CUIL')
		) THEN c.doc_type
		WHEN (c.doc_type = 'PAS') THEN 'PE'
		WHEN (c.doc_type = 'DE') THEN 'CE'
		WHEN (c.doc_type = 'CRP') THEN 'OTRO' ELSE 'NN'
	END,
	c.doc_number,
	CASE
		WHEN (c.gender NOT IN ('F', 'M')) THEN 'X' ELSE c.gender
	END,
	c.miba_id,
	CASE
		WHEN UPPER(SUBSTR(c.document_nationality, 1, 3)) = 'ARG' THEN 'ARG' ELSE 'NN'
	END,
	ta.name,
	m.name,
 	ne.nivel_capacitacion,
 	ne.estado,
	CONCAT(
		(
			CASE
				WHEN (
					c.doc_type IN ('DNI', 'LC', 'CI', 'LE', 'CUIL')
				) THEN c.doc_type ELSE 'NN'
			END
		),
		CAST(c.doc_number AS varchar),
		(
			CASE
				WHEN (c.gender = 'M') THEN 'M'
				WHEN (c.gender = 'F') THEN 'F' ELSE 'X'
			END
		),
		(
			CASE
				WHEN UPPER(SUBSTR(c.document_nationality, 1, 3)) = 'ARG' THEN 'ARG' ELSE 'NN'
			END
		)
	),
	dr.code || '|' || dr.value,
	CAST(ja.application_date AS timestamp),
	i.start_date, 
	i.end_date
UNION
SELECT e.id,
	'CRMEMPLEO',
	COALESCE(MAX(e.fecha_de_entrevista__c), current_date) fecha_publicacion,
	-- assumption ultima fecha de entrevista es la ultima fecha de publicacion del cv
	e.lastmodifieddate AS fecha_ultima_modificacion,
	NULL capacidades_diferentes,
	NULL presentacion,
	e.resumen_de_la_entrevista__c metas,
	CASE
		WHEN el.isdeleted = 'true' then 'VIGENTE' ELSE 'NO VIGENTE'
	END estado,
	NULL nacionalidad,
	'DNI' tipo_doc_broker,
	CAST(e.dni__c AS INT) documento_broker,
	b.genero genero_broker,
	CAST(b.login2_id AS VARCHAR) login2_id,
	NULL nacionalidad_broker,
	NULL disponibilidad,
	NULL modalidad,
	e.nivel_de_instruccion__c nivel_educativo,
	CASE WHEN e.nivel_de_instruccion__c IS NOT NULL AND LENGTH(TRIM(TRY_CAST(e.nivel_de_instruccion__c AS VARCHAR)))>0 THEN
	'GRADUADO' ELSE CAST(NULL AS VARCHAR) END, 
	b.id,
	CAST(NULL AS VARCHAR) AS licencia_conducir
FROM 
	"caba-piba-raw-zone-db"."crm_empleo_entrevista__c" e 
	LEFT JOIN "caba-piba-raw-zone-db"."crm_empleo_experiencia_laboral__c" el 
	ON (
		el.postulante__c = e.postulante__c
		AND e.dni__c IS NOT NULL
	)
	LEFT JOIN datos_broker b ON (b.documento_broker = CAST(e.dni__c AS INT))
GROUP BY e.id,
	e.resumen_de_la_entrevista__c,
	e.estado__c,
	CAST(e.dni__c AS INT),
	e.nivel_de_instruccion__c,
	CASE WHEN e.nivel_de_instruccion__c IS NOT NULL AND LENGTH(TRIM(TRY_CAST(e.nivel_de_instruccion__c AS VARCHAR)))>0 THEN
	'GRADUADO' ELSE CAST(NULL AS VARCHAR) END,
	b.id,
	b.genero,
	CAST(b.login2_id AS VARCHAR),
	el.isdeleted,
	e.lastmodifieddate
UNION
SELECT COALESCE(ecc.id, NULL),
	'CRMSL',
	COALESCE(MAX(ecc.active_date), MAX(NULL), current_date) fecha_publicacion,
	-- assumption ultima fecha de entrevista es la ultima fecha de publicacion del cv
	ecc.date_modified AS fecha_ultima_modificacion,
	CASE
		WHEN cc.tipo_discapacidad_c = 'Motriz' THEN 'MOTOR'
		WHEN cc.tipo_discapacidad_c = 'Nada' THEN NULL
		WHEN UPPER(cc.tipo_discapacidad_c) IN ('VISUAL', 'AUDITIVA', 'MENTAL', 'VISCERAL') THEN UPPER(cc.tipo_discapacidad_c) ELSE NULL
	END capacidades_diferentes,
	ecc.description presentacion,
	NULL metas,
	CASE
		WHEN ecc.status_id = 'Active' THEN 'VIGENTE' ELSE 'NO VIGENTE'
	END estado,
	cc.nacionalidad_c nacionalidad,
	
	CASE
			WHEN UPPER(cc.tipo_documento_c) IN ('DNI','LC','LE','CI','CUIT','CUIL') THEN UPPER(cc.tipo_documento_c)
			WHEN cc.tipo_documento_c = 'PAS' THEN 'PE' ELSE 'NN'
		END tipo_doc_broker,
		
		
	CAST(
		COALESCE(
				CAST(cc.numero_documento_c AS VARCHAR),
				SUBSTR(CAST(cc.cuil2_c AS VARCHAR),	3,LENGTH(CAST(cc.cuil2_c AS VARCHAR)) -3)
				) 
		AS DECIMAL) documento_broker,
	
	CASE
		WHEN cc.genero_c LIKE 'masculino' OR SUBSTRING(CAST(cc.cuil2_c AS VARCHAR),1,2) = '20' THEN 'M'
		WHEN cc.genero_c LIKE 'femenino' OR SUBSTRING(CAST(cc.cuil2_c AS VARCHAR),1,2) = '27' THEN 'F'
		ELSE 'X'
	END genero_broker,

	
	CAST(b.login2_id AS VARCHAR) login2_id,
	CASE
		WHEN UPPER(SUBSTR(cc.nacionalidad_c, 1, 3)) = 'ARG' THEN 'ARG' ELSE 'NN'
	END nacionalidad_broker,

	NULL disponibilidad,
	NULL modalidad,
	ne.nivel_educativo nivel_educativo,
	CASE WHEN ne.nivel_educativo IS NOT NULL AND LENGTH(TRIM(TRY_CAST(ne.nivel_educativo AS VARCHAR)))>0 THEN
	'GRADUADO' ELSE CAST(NULL AS VARCHAR) END, 
	b.id,
	CAST(NULL AS VARCHAR) AS licencia_conducir
FROM "caba-piba-raw-zone-db"."crm_sociolaboral_re_experiencia_laboral" ecc
	INNER JOIN "caba-piba-raw-zone-db"."crm_sociolaboral_re_experiencia_laboral_contacts_c" c ON (
		c.re_experiencia_laboral_contactsre_experiencia_laboral_idb = ecc.id
	)
	INNER JOIN "caba-piba-raw-zone-db"."crm_sociolaboral_contacts_cstm" cc ON (
		cc.id_c = c.re_experiencia_laboral_contactscontacts_ida
	)
	LEFT JOIN datos_broker_1 b ON (b.documento_broker = cc.numero_documento_c)
	LEFT JOIN nivel_educativo ne ON (ne.id_c = cc.id_c)
WHERE cc.numero_documento_c IS NULL
GROUP BY ecc.id,
	cc.discapacidad_c,
	ecc.description,
	ecc.status_id,
	cc.nacionalidad_c,
	cc.tipo_documento_c,
	cc.numero_documento_c,
	cc.genero_c,
	b.login2_id,
	UPPER(SUBSTR(cc.nacionalidad_c, 1, 3)),
	cc.trabaja_actualmente_c,
	b.id,
	ne.nivel_educativo,
	CASE WHEN ne.nivel_educativo IS NOT NULL AND LENGTH(TRIM(TRY_CAST(ne.nivel_educativo AS VARCHAR)))>0 THEN
	'GRADUADO' ELSE CAST(NULL AS VARCHAR) END, 
	cc.tipo_discapacidad_c,
	cc.cuil2_c,
	ecc.date_modified
--</sql>--	