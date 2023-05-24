-- Crear tabla tmp de curriculum
-- DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_tmp_curriculum`;
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
)
SELECT CAST(cv.id AS VARCHAR) cod_origen,
	'PORTALEMPLEO' base_origen,
	COALESCE(MAX(application_date), current_date) fecha_publicacion,
	-- assumption ultima fecha de postulacion es la ultima fecha de publicacion del cv
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
	eds.value nivel_educativo,
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
	) broker_id
FROM "caba-piba-raw-zone-db"."portal_empleo_curriculum_vitaes" cv
	INNER JOIN "caba-piba-raw-zone-db"."portal_empleo_academic_formations" af ON (af.curriculum_id = cv.id)
	INNER JOIN "caba-piba-raw-zone-db"."portal_empleo_education_level_status" eds ON (eds.id = af.education_level_id)
	INNER JOIN "caba-piba-raw-zone-db"."portal_empleo_candidates" c ON (c.id = cv.candidate_id)
	LEFT JOIN "caba-piba-raw-zone-db"."portal_empleo_condition_types" ct ON (ct.id = cv.condition_type_id)
	LEFT JOIN "caba-piba-raw-zone-db"."portal_empleo_mtr_time_availability" ta ON (ta.id = cv.availability_id)
	LEFT JOIN "caba-piba-raw-zone-db"."portal_empleo_mtr_working_modalities" m ON (m.id = modality_id)
	LEFT JOIN "caba-piba-raw-zone-db"."portal_empleo_job_applications" ja ON (ja.candidate_id = cv.candidate_id)
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
	eds.value,
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
	)
UNION
SELECT e.id,
	'CRMEMPLEO',
	COALESCE(MAX(e.fecha_de_entrevista__c), current_date) fecha_publicacion,
	-- assumption ultima fecha de entrevista es la ultima fecha de publicacion del cv
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
	b.id
FROM "caba-piba-raw-zone-db"."crm_empleo_experiencia_laboral__c" el
	INNER JOIN "caba-piba-raw-zone-db"."crm_empleo_entrevista__c" e ON (
		el.postulante__c = e.postulante__c
		AND e.dni__c IS NOT NULL
	)
	LEFT JOIN datos_broker b ON (b.documento_broker = CAST(e.dni__c AS INT))
GROUP BY e.id,
	e.resumen_de_la_entrevista__c,
	e.estado__c,
	CAST(e.dni__c AS INT),
	e.nivel_de_instruccion__c,
	b.id,
	b.genero,
	CAST(b.login2_id AS VARCHAR),
	el.isdeleted
UNION
SELECT COALESCE(ecc.id, NULL),
	'CRMSL',
	COALESCE(MAX(ecc.active_date), MAX(NULL), current_date) fecha_publicacion,
	-- assumption ultima fecha de entrevista es la ultima fecha de publicacion del cv
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
	b.id
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
	cc.tipo_discapacidad_c,
	cc.cuil2_c