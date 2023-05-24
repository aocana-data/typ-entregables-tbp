-- 1.-- Crear ENTREVISTA LABORAL
-- --<sql>--DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_tmp_entrevista`; --</sql>--
--<sql>--
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_entrevista" AS
-- PORTALEMPLEO
SELECT vec.base_origen,
	vec.vecino_id,
	ol.oportunidad_laboral_id,
	CAST(NULL AS VARCHAR) as "tipo_entrevista",
	-- es la fecha de alta del registro, no hay una fecha de entrevista
	CAST(jac.created_at AS DATE) as "fecha_entrevista",
	-- es la fecha de postulacion del candidato, no hay una fecha de notificacion
	CAST(ja.application_date AS DATE) as "fecha_notificacion",
	-- 1 si fue contratado, 0 en caso contrario
	ja.hired as "consiguio_trabajo",
	CASE
	WHEN ja.contacted = 1 OR ja.hired = 1 THEN 'COMPLETA'
	WHEN ja.contacted = 0 AND ja.hired = 0 THEN 'PENDIENTE'
	END estado_entrevista
FROM "caba-piba-raw-zone-db"."portal_empleo_job_applications" ja
	JOIN "caba-piba-raw-zone-db"."portal_empleo_candidates" pec ON (pec.id = ja.candidate_id)
	JOIN "caba-piba-raw-zone-db"."portal_empleo_job_application_comments" jac ON (ja.id = jac.job_application_id)
	JOIN "caba-piba-raw-zone-db"."portal_empleo_job_postings" jp ON (ja.job_posting_id = jp.id)
	JOIN "caba-piba-staging-zone-db"."tbp_typ_def_vecino" vec ON (
		vec.genero_broker = (
			CASE
				WHEN (pec.gender = 'M') THEN 'M'
				WHEN (pec.gender = 'F') THEN 'F' ELSE 'X'
			END
		)
		AND vec.documento_broker = CAST(pec.doc_number AS VARCHAR)
		AND vec.tipo_doc_broker = CASE
			WHEN (
				pec.doc_type IN ('DNI', 'LC', 'CI', 'LE', 'CUIL')
			) THEN pec.doc_type
			WHEN (pec.doc_type = 'PAS') THEN 'PE'
			WHEN (pec.doc_type = 'DE') THEN 'CE'
			WHEN (pec.doc_type = 'CRP') THEN 'OTRO' ELSE 'NN'
		END
		AND vec.base_origen = 'PORTALEMPLEO'
	)
	LEFT JOIN  "caba-piba-staging-zone-db"."tbp_typ_def_oportunidad_laboral" ol ON (ol.oportunidad_laboral_id_old=CAST(jp.id AS VARCHAR) and ol.base_origen= 'PORTALEMPLEO') 
	GROUP BY 
	vec.base_origen,
	vec.vecino_id,
	ol.oportunidad_laboral_id,
	jac.created_at,
	ja.application_date,
	ja.hired,
	ja.contacted
UNION
-- CRMEMPLEO
SELECT vec.base_origen,
	 vec.vecino_id,
	ol.oportunidad_laboral_id,
	CASE
		WHEN e.tipo_de_entrevista__c LIKE 'Telefónica' THEN 'Virtual Individual'
		WHEN e.tipo_de_entrevista__c LIKE 'Presencial Individual' THEN 'Presencial Individual' 
		ELSE ''
	END as "tipo_entrevista",
	CAST(e.fecha_de_entrevista__c AS DATE) as "fecha_entrevista",
	-- es la fecha de creacion del registro, no hay una fecha de notificacion
	CAST(e.createddate AS DATE) as "fecha_notificacion",
	-- 1 si fue contratado, 0 en caso contrario
	CASE WHEN p.estado__c LIKE 'Contratado' THEN 1 ELSE 0 END  as "consiguio_trabajo",
	CASE
	WHEN p.estado__c IN ('Contratado', 'Entrevista realizada', 'Enviado al solicitante') THEN 'COMPLETA'
	WHEN p.estado__c IN ('Contactado') THEN 'PENDIENTE'
	END estado_entrevista
FROM "caba-piba-raw-zone-db"."crm_empleo_entrevista__c" e
LEFT JOIN "caba-piba-raw-zone-db"."crm_empleo_account"  cea ON (e.dni__c=cea.numero_de_documento__c)
LEFT JOIN"caba-piba-raw-zone-db"."crm_empleo_anuncio_postulante__c" p ON (cea.id=p.cuenta__c
)
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_vecino" vec ON (
		vec.genero_broker = (CASE 
		WHEN (cea.genero__c = 'MASCULINIO' OR cea.genero__c = 'Masculino' ) THEN 'M' 
		WHEN (cea.genero__c = 'FEMENINA' OR cea.genero__c = 'Femenino') THEN 'F' ELSE 'X' END)
		
		AND vec.documento_broker = CAST(cea.numero_de_documento__c AS VARCHAR)
		
		AND vec.tipo_doc_broker = (CASE
		WHEN (cea.tipo_de_documento__c = 'DNI') THEN 'DNI' 
		WHEN (cea.tipo_de_documento__c = 'DNIEx') THEN 'CE' 
		WHEN (cea.tipo_de_documento__c = 'CI' OR UPPER(cea.tipo_de_documento__c) LIKE '%IDENTIDAD%') THEN 'CI' 
		WHEN (UPPER(cea.tipo_de_documento__c) LIKE '%Libreta C%') THEN 'LC' 
		WHEN (cea.tipo_de_documento__c IN ('Pasaporte', 'PAS')) THEN 'PE' 
		WHEN (cea.tipo_de_documento__c IN ('DNIEx', 'DNI extranjero')) THEN 'CE' 
		WHEN (cea.tipo_de_documento__c IN ('DNIEx', 'DNI extranjero')) THEN 'CE' 
		WHEN (cea.tipo_de_documento__c IN ('PRECARIA', 'Credencial Residencia Precaria')) THEN 'OTRO' ELSE 'NN' END)
		AND vec.base_origen = 'CRMEMPLEO'
	) 
	LEFT JOIN "caba-piba-raw-zone-db"."crm_empleo_anuncio__c" anu ON (anu.id=p.anuncio__c)
	LEFT JOIN  "caba-piba-staging-zone-db"."tbp_typ_def_oportunidad_laboral" ol ON (ol.oportunidad_laboral_id_old=CAST(anu.id AS VARCHAR) and ol.base_origen= 'CRMEMPLEO') 
	WHERE e.isdeleted = false
GROUP BY 
	vec.base_origen,
	vec.vecino_id,
	ol.oportunidad_laboral_id,
	CASE
		WHEN e.tipo_de_entrevista__c LIKE 'Telefónica' THEN 'Virtual Individual'
		WHEN e.tipo_de_entrevista__c LIKE 'Presencial Individual' THEN 'Presencial Individual' 
		ELSE ''
	END,
	e.fecha_de_entrevista__c,
	e.createddate,
	CASE WHEN p.estado__c LIKE 'Contratado' THEN 1 ELSE 0 END,
	CASE
	WHEN p.estado__c IN ('Contratado', 'Entrevista realizada', 'Enviado al solicitante') THEN 'COMPLETA'
	WHEN p.estado__c IN ('Contactado') THEN 'PENDIENTE'
	END
	
UNION
-- CRMEMPLEO - TABLAS HISTORICAS
SELECT vec.base_origen,
	 vec.vecino_id,
	ol.oportunidad_laboral_id,
	CASE
		WHEN e.tipo_de_entrevista__c LIKE 'Telefónica' THEN 'Virtual Individual'
		WHEN e.tipo_de_entrevista__c LIKE 'Presencial Individual' THEN 'Presencial Individual' 
		WHEN e.tipo_de_entrevista__c LIKE 'Presencial Grupal' THEN 'Presencial Grupal' 
		ELSE ''
	END as "tipo_entrevista",
	CAST(e.fecha_de_entrevista__c AS DATE) as "fecha_entrevista",
	-- se deje en NULL porque no existe fecha de notificacion y la fecha de creacion es inexacta
	CAST(NULL AS DATE) as "fecha_notificacion",
	-- 1 si fue contratado, 0 en caso contrario
	CASE WHEN p.estado__c LIKE 'Contratado' THEN 1 ELSE 0 END  as "consiguio_trabajo",
	CASE
	WHEN p.estado__c IN ('Contratado', 'Entrevista realizada', 'Enviado al solicitante') THEN 'COMPLETA'
	WHEN p.estado__c IN ('Contactado') THEN 'PENDIENTE'
	END estado_entrevista
FROM "caba-piba-raw-zone-db"."crm_empleo_historico_entrevista__c" e
LEFT JOIN "caba-piba-raw-zone-db"."crm_empleo_account"  cea ON (e.dni__c=cea.numero_de_documento__c)
LEFT JOIN"caba-piba-raw-zone-db"."crm_empleo_hist_rico_anuncio_postulante__c" p ON (cea.id=p.cuenta__c
)
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_vecino" vec ON (
		vec.genero_broker = (CASE 
		WHEN (cea.genero__c = 'MASCULINIO' OR cea.genero__c = 'Masculino' ) THEN 'M' 
		WHEN (cea.genero__c = 'FEMENINA' OR cea.genero__c = 'Femenino') THEN 'F' ELSE 'X' END)
		
		AND vec.documento_broker = CAST(cea.numero_de_documento__c AS VARCHAR)
		
		AND vec.tipo_doc_broker = (CASE
		WHEN (cea.tipo_de_documento__c = 'DNI') THEN 'DNI' 
		WHEN (cea.tipo_de_documento__c = 'DNIEx') THEN 'CE' 
		WHEN (cea.tipo_de_documento__c = 'CI' OR UPPER(cea.tipo_de_documento__c) LIKE '%IDENTIDAD%') THEN 'CI' 
		WHEN (UPPER(cea.tipo_de_documento__c) LIKE '%Libreta C%') THEN 'LC' 
		WHEN (cea.tipo_de_documento__c IN ('Pasaporte', 'PAS')) THEN 'PE' 
		WHEN (cea.tipo_de_documento__c IN ('DNIEx', 'DNI extranjero')) THEN 'CE' 
		WHEN (cea.tipo_de_documento__c IN ('DNIEx', 'DNI extranjero')) THEN 'CE' 
		WHEN (cea.tipo_de_documento__c IN ('PRECARIA', 'Credencial Residencia Precaria')) THEN 'OTRO' ELSE 'NN' END)
		AND vec.base_origen = 'CRMEMPLEO'
	) 
	LEFT JOIN "caba-piba-raw-zone-db"."crm_empleo_anuncio__c" anu ON (anu.id=p.hist_rico_anuncio__c)
	LEFT JOIN  "caba-piba-staging-zone-db"."tbp_typ_def_oportunidad_laboral" ol ON (ol.oportunidad_laboral_id_old=(CAST(anu.id AS VARCHAR) || 'H') and ol.base_origen= 'CRMEMPLEO') 
	WHERE e.isdeleted = 'false'
GROUP BY 
	vec.base_origen,
	vec.vecino_id,
	ol.oportunidad_laboral_id,
	CASE
		WHEN e.tipo_de_entrevista__c LIKE 'Telefónica' THEN 'Virtual Individual'
		WHEN e.tipo_de_entrevista__c LIKE 'Presencial Individual' THEN 'Presencial Individual' 
		WHEN e.tipo_de_entrevista__c LIKE 'Presencial Grupal' THEN 'Presencial Grupal' 
		ELSE ''
	END,
	e.fecha_de_entrevista__c,
	CASE WHEN p.estado__c LIKE 'Contratado' THEN 1 ELSE 0 END,
	CASE
	WHEN p.estado__c IN ('Contratado', 'Entrevista realizada', 'Enviado al solicitante') THEN 'COMPLETA'
	WHEN p.estado__c IN ('Contactado') THEN 'PENDIENTE'
	END
	
UNION

--CRMSL
SELECT vec.base_origen,
	vec.vecino_id,
	NULL AS oportunidad_laboral_id,
	c.tipo_entrevista,
	CAST(c.fecha_notificacion AS DATE) AS fecha_notificacion,
	CAST(c.fecha_entrevista AS DATE) AS fecha_entrevista,
	c.consiguio_trabajo,
	c.estado_entrevista
FROM (
	SELECT el.id AS id_entrevista_old,
		el.date_entered AS fecha_notificacion,
		CASE
			WHEN el.fecha_entrevista IS NULL
			OR LENGTH(TRIM(CAST(el.fecha_entrevista AS VARCHAR))) = 0 THEN el.date_modified ELSE el.fecha_entrevista
		END fecha_entrevista,
		CASE
			WHEN el.tipo_entrevista = 'telefonica' THEN 'Virtual Individual'
			WHEN el.tipo_entrevista = 'presencial' THEN 'Presencial Individual'
		END tipo_entrevista,
		CASE
			WHEN cstm.estado_entrevista_c = 'completa' OR cstm.estado_entrevista_c = 'completa_con_cv' THEN 'COMPLETA'
			WHEN cstm.estado_entrevista_c = 'incompleta' THEN 'INCOMPLETA'
			ELSE 'PENDIENTE'
		END estado_entrevista,
		cc.per_entrevista_laboral_contactscontacts_ida,
		CAST(NULL AS INT) AS consiguio_trabajo,
		CASE WHEN (ccstm.tipo_documento_c = 'dni') THEN 'DNI' ELSE 'NN' END tipo_documento,
		CAST(ccstm.numero_documento_c AS VARCHAR) numero_documento,
		CASE
			WHEN ccstm.genero_c LIKE 'masculino' OR SUBSTRING(CAST(ccstm.cuil2_c AS VARCHAR),1,2) = '20' THEN 'M'
			WHEN ccstm.genero_c LIKE 'femenino' OR SUBSTRING(CAST(ccstm.cuil2_c AS VARCHAR),1,2) = '27' THEN 'F'
			ELSE 'X'
		END genero
	FROM "caba-piba-raw-zone-db".crm_sociolaboral_per_entrevista_laboral el
		JOIN (
			SELECT id_c,
				estado_entrevista_c
			FROM "caba-piba-raw-zone-db".crm_sociolaboral_per_entrevista_laboral_cstm
		) cstm ON (el.id = cstm.id_c)
		JOIN (
			SELECT per_entrevista_laboral_contactscontacts_ida,
				per_entrevista_laboral_contactsper_entrevista_laboral_idb
			FROM "caba-piba-raw-zone-db".crm_sociolaboral_per_entrevista_laboral_contacts_c
			WHERE deleted = false
		) cc ON (
			el.id = cc.per_entrevista_laboral_contactsper_entrevista_laboral_idb
		)
		JOIN "caba-piba-staging-zone-db"."tbp_typ_tmp_view_crm_sociolaboral_contacts_cstm_no_duplicates" ccstm ON (
			cc.per_entrevista_laboral_contactscontacts_ida = ccstm.id_c
		)
) c
	JOIN "caba-piba-staging-zone-db"."tbp_typ_def_vecino" vec ON (
		vec.genero_broker = c.genero
		AND vec.documento_broker = c.numero_documento
		AND vec.tipo_doc_broker = c.tipo_documento
		AND vec.base_origen = 'CRMSL'
	)
GROUP BY 
	vec.base_origen,
	vec.vecino_id,
	CAST(NULL AS INT),
	CAST(c.fecha_notificacion AS DATE),
	CAST(c.fecha_entrevista AS DATE),
	c.tipo_entrevista,
	c.consiguio_trabajo,
	c.estado_entrevista
--</sql>--	