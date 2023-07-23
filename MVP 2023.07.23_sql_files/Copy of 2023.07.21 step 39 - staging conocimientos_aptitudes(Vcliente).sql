-- 1- Se crea la tabla tbp_typ_tmp_conocimientos_aptitudes_1 que contiene informacion de CURSOS
-- las carreras estan en la tabla de formacion academica
--<sql>--
DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_tmp_conocimientos_aptitudes_1`;
--</sql>--

--<sql>--
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_conocimientos_aptitudes_1" AS 
WITH seguimientos_calculado0 AS (
	SELECT off.id,
		ofs.op_oportunidades_formacion_se_seguimiento_1se_seguimiento_idb,
		s.name,
		s.date_modified,
		dense_rank() OVER(
			PARTITION BY off.id,
			s.name
			order by s.date_modified desc
		) as max_date
	FROM "caba-piba-raw-zone-db".crm_sociolaboral_op_oportunidades_formacion off
		LEFT JOIN "caba-piba-raw-zone-db".crm_sociolaboral_op_oportunidades_formacion_se_seguimiento_1_c ofs ON (ofs.op_oportun868armacion_ida = off.id)
		LEFT JOIN "caba-piba-raw-zone-db".crm_sociolaboral_se_seguimiento s ON (
			s.id = ofs.op_oportunidades_formacion_se_seguimiento_1se_seguimiento_idb
		)
),
seguimientos_calculado AS (
	SELECT sc0.id,
		sc0.op_oportunidades_formacion_se_seguimiento_1se_seguimiento_idb,
		sc0.name,
		sc0.date_modified
	FROM seguimientos_calculado0 as sc0
	WHERE sc0.max_date = 1
)
SELECT 'CRMSL' base_origen,
	CAST(cm.id_new AS VARCHAR) || '-' || CAST(cv.id AS VARCHAR) || '-' || CAST(of.id AS VARCHAR) AS codigo,
	CAST(
		DATE_PARSE(
			CASE
				WHEN fechas.fecha_inicio = '0000-00-00' THEN date_format(cast(of.inicio as date), '%Y-%m-%d %h:%i%p') ELSE date_format(
					cast(fechas.fecha_inicio as date),
					'%Y-%m-%d %h:%i%p'
				)
			END,
			'%Y-%m-%d %h:%i%p'
		) AS DATE
	) fecha_inicio,
	CAST(
		DATE_PARSE(
			CASE
				WHEN fechas.fecha_fin = '0000-00-00' THEN date_format(of.fin, '%Y-%m-%d %h:%i%p') ELSE date_format(
					cast(fechas.fecha_fin as date),
					'%Y-%m-%d %h:%i%p'
				)
			END,
			'%Y-%m-%d %h:%i%p'
		) AS DATE
	) fecha_fin,
	cm.tipo_capacitacion AS tipo_formacion,
	of.organismo AS institucion_educativa,
	CAST(cv.id AS VARCHAR) AS cv_id,
	of.name descripcion,
	dc.descrip_capacitacion AS observacion,
	sc.estado_c estado
FROM "caba-piba-raw-zone-db"."crm_sociolaboral_op_oportunidades_formacion" of
	LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_establecimientos" est ON (of.sede in (est.nombres_old))
	LEFT JOIN "caba-piba-raw-zone-db"."crm_sociolaboral_op_oportunidades_formacion_contacts_c" ofc ON (of.id = ofc.op_oportun1d35rmacion_ida)
	LEFT JOIN "caba-piba-raw-zone-db"."crm_sociolaboral_contacts" co ON (
		co.id = ofc.op_oportunidades_formacion_contactscontacts_idb
	)
	LEFT JOIN seguimientos_calculado scn ON (
		of.id = scn.id
		AND replace(
			trim(concat_ws(' ', co.first_name, co.last_name)),
			' ',
			' '
		) = replace(trim(scn.name), '  ', ' ')
	)
	LEFT JOIN "caba-piba-raw-zone-db".crm_sociolaboral_op_oportunidades_formacion_se_seguimiento_1_c ofs ON (
		ofs.op_oportunidades_formacion_se_seguimiento_1se_seguimiento_idb = scn.op_oportunidades_formacion_se_seguimiento_1se_seguimiento_idb
	)
	LEFT JOIN "caba-piba-raw-zone-db".crm_sociolaboral_se_seguimiento s ON (
		s.id = ofs.op_oportunidades_formacion_se_seguimiento_1se_seguimiento_idb
	)
	LEFT JOIN "caba-piba-raw-zone-db".crm_sociolaboral_se_seguimiento_cstm sc ON (sc.id_c = s.id)
	LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_tmp_view_crm_sociolaboral_contacts_cstm_no_duplicates" cs ON (co.id = cs.id_c)
	LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_capacitacion_match" cm ON (
		cm.base_origen = 'CRMSL'
		AND cm.tipo_capacitacion = 'CURSO'
		AND cm.id_old = CAST(of.id AS VARCHAR)
	)
	LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_capacitacion" dc ON (
		cm.id_new = dc.id_new
		AND cm.base_origen = dc.base_origen
	)
	INNER JOIN "caba-piba-raw-zone-db"."portal_empleo_candidates" c ON (c.doc_number = cs.numero_documento_c)
	INNER JOIN "caba-piba-raw-zone-db"."portal_empleo_curriculum_vitaes" cv ON (cv.candidate_id = c.id)
	LEFT JOIN (
		SELECT edicion_capacitacion_id_old,
			CAST(MIN(inicio) AS VARCHAR) fecha_inicio,
			CAST(MAX(fin) AS VARCHAR) fecha_fin,
			CAST(MIN(inicio) AS VARCHAR) fecha_inicio_inscripcion,
			CAST(MAX(inicio) AS VARCHAR) fecha_fin_inscripcion,
			COUNT(*) cantidad_inscriptos
		FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_estado_beneficiario_crmsl"
		GROUP BY edicion_capacitacion_id_old
	) fechas ON (
		CAST(of.id AS VARCHAR) = fechas.edicion_capacitacion_id_old
	)
WHERE (
		co.lead_source = 'sociolaboral'
		OR (
			(co.lead_source = 'rib')
			AND cs.forma_parte_interm_lab_c = 'si'
		)
	)
GROUP BY cm.base_origen,
	cm.tipo_capacitacion,
	cm.id_new,
	of.id,
	of.name,
	of.inicio,
	dc.fecha_inicio,
	of.fin,
	dc.fecha_fin,
	of.description,
	of.organismo,
	cv.id,
	dc.descrip_capacitacion,
	fechas.fecha_inicio,
	fechas.fecha_fin,
	sc.estado_c
UNION
SELECT 'PORTALEMPLEO' base_origen,
	CAST(i.id AS VARCHAR) AS codigo,
	i.start_date fecha_inicio,
	i.end_date fecha_fin,
	'CURSO' AS tipo_formacion,
	inst.name AS organizacion,
	CAST(c.curriculum_id AS VARCHAR) AS cv_id,
	c.name AS descripcion,
	c.observation AS observacion,
	LOWER(els.value) estado
FROM "caba-piba-raw-zone-db"."portal_empleo_instructions" i
	JOIN "caba-piba-raw-zone-db"."portal_empleo_institutions" inst ON (i.institution_id = inst.id)
	JOIN "caba-piba-raw-zone-db"."portal_empleo_courses" c ON (
		i.id = c.id
		AND i.instruction_type LIKE 'COURSE'
	)
	JOIN "caba-piba-raw-zone-db"."portal_empleo_education_level_status" els ON (els.id = i.education_level_status_id)
UNION
SELECT -- CURSOS REALIZADO QUE SE PUDIERON TRACKEAR CON EL VECINO ENTREVISTADO
	'CRMEMPLEO' base_origen,
	ins.id AS codigo,
	CAST(c.avx_inicio_del_curso__c AS DATE) fecha_inicio,
	CAST(c.avx_finalizacion_del_curso__c AS DATE) fecha_fin,
	'CURSO' AS tipo_formacion,
	c.avx_institucion_capacitadora__c AS organizacion,
	e.id AS cv_id,
	-- c.name tiene codigos y no contamos con tabla parametrica por ahora, se quita hasta tenerla?
	c.name AS capacitacion,
	null observacion,
	CASE
		WHEN ins.avx_estado__c = 'Finalizado' THEN 'graduado' ELSE LOWER(ins.avx_estado__c)
	END AS estado
FROM "caba-piba-raw-zone-db"."crm_empleo_avx_asistente__c" ins
	JOIN "caba-piba-raw-zone-db"."crm_empleo_account" a ON (ins.alumno__c = a.id)
	JOIN "caba-piba-raw-zone-db"."crm_empleo_avx_curso__c" c ON (ins.curso__c = c.id)
	JOIN "caba-piba-raw-zone-db"."crm_empleo_entrevista__c" e ON (e.postulante__c = a.id)
--</sql>--

-- 2- Se crea la tabla tbp_typ_tmp_conocimientos_aptitudes_2 que contiene informacion nomenclada de idiomas e informatica
--<sql>--
DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_tmp_conocimientos_aptitudes_2`;
--</sql>--

--<sql>--
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_conocimientos_aptitudes_2" AS 
-- IDIOMAS
SELECT 'PORTALEMPLEO' base_origen,
	CAST(cvl.id AS VARCHAR) ||'-'|| 'IDIOMA' AS codigo,
	'IDIOMA' AS tipo_formacion,
	CAST(cv.id AS VARCHAR) AS cv_id,
	UPPER(l.name) AS descripcion,
	UPPER(ll.name) AS  nivel_idioma
FROM "caba-piba-raw-zone-db"."portal_empleo_curriculum_vitaes" cv
	INNER JOIN "caba-piba-raw-zone-db"."portal_empleo_academic_formations" af ON (af.curriculum_id = cv.id)
	INNER JOIN "caba-piba-raw-zone-db"."portal_empleo_cv_languages" cvl ON (cvl.curriculum_id = af.curriculum_id)
	INNER JOIN "caba-piba-raw-zone-db"."portal_empleo_language" l ON (l.id = cvl.language_id)
	LEFT JOIN "caba-piba-raw-zone-db"."portal_empleo_language_level" ll ON (ll.id = cvl.language_level_id)
UNION	
-- HERRAMIENTAS INFORMATICAS MICROSOFT OFFICE
SELECT 'PORTALEMPLEO' base_origen,
	CAST(io.informatic_id AS VARCHAR) ||'-MO-'|| CAST(io.office_id AS VARCHAR) AS codigo,
	'HERRAMIENTAS INFORMÁTICAS' AS tipo_formacion,
	CAST(cv.id AS VARCHAR) AS cv_id,
	UPPER(os.name) AS descripcion,
	CAST(NULL AS VARCHAR)
FROM "caba-piba-raw-zone-db"."portal_empleo_curriculum_vitaes" cv
	INNER JOIN "caba-piba-raw-zone-db"."portal_empleo_academic_formations" af ON (af.curriculum_id = cv.id)
	INNER JOIN "caba-piba-raw-zone-db"."portal_empleo_informatics" i ON (i.id = cv.informatic_id)
	INNER JOIN "caba-piba-raw-zone-db"."portal_empleo_informatics_office" io ON (io.informatic_id = cv.informatic_id)
	INNER JOIN "caba-piba-raw-zone-db"."portal_empleo_office_software" os ON (os.id = io.office_id)
UNION
-- HERRAMIENTAS INFORMATICAS PROGRAMACION	
SELECT 'PORTALEMPLEO' base_origen,
	CAST(ip.informatic_id AS VARCHAR) ||'-PR-'|| CAST(ip.programming_knowledge_id AS VARCHAR) AS codigo,
	'HERRAMIENTAS INFORMÁTICAS' AS tipo_formacion,
	CAST(cv.id AS VARCHAR) AS cv_id,
	CASE WHEN UPPER(pk.name) LIKE 'OTRO'  THEN 'OTRO LENGUAJE DE PROGRAMACIÓN'
	ELSE UPPER(pk.name) END AS descripcion,
	CAST(NULL AS VARCHAR)
FROM "caba-piba-raw-zone-db"."portal_empleo_curriculum_vitaes" cv
	INNER JOIN "caba-piba-raw-zone-db"."portal_empleo_academic_formations" af ON (af.curriculum_id = cv.id)
	INNER JOIN "caba-piba-raw-zone-db"."portal_empleo_informatics" i ON (i.id = cv.informatic_id)
	INNER JOIN "caba-piba-raw-zone-db"."portal_empleo_informatics_programming" ip ON (ip.informatic_id = cv.informatic_id)
	INNER JOIN "caba-piba-raw-zone-db"."portal_empleo_programming_knowledge" pk ON (pk.id = ip.programming_knowledge_id)	
UNION	
-- HERRAMIENTAS INFORMATICAS BASES DE DATOS	
SELECT 'PORTALEMPLEO' base_origen,
	CAST(id.informatic_id AS VARCHAR) ||'-DB-'|| CAST(id.database_id AS VARCHAR) AS codigo,
	'HERRAMIENTAS INFORMÁTICAS' AS tipo_formacion,
	CAST(cv.id AS VARCHAR) AS cv_id,
	CASE WHEN UPPER(db.name) LIKE 'OTRO'  THEN 'OTRA BASE DE DATOS'
	ELSE UPPER(db.name) END AS descripcion,
	CAST(NULL AS VARCHAR)
FROM "caba-piba-raw-zone-db"."portal_empleo_curriculum_vitaes" cv
	INNER JOIN "caba-piba-raw-zone-db"."portal_empleo_academic_formations" af ON (af.curriculum_id = cv.id)
	INNER JOIN "caba-piba-raw-zone-db"."portal_empleo_informatics" i ON (i.id = cv.informatic_id)
	INNER JOIN "caba-piba-raw-zone-db"."portal_empleo_informatics_databases" id ON (id.informatic_id = cv.informatic_id)
	INNER JOIN "caba-piba-raw-zone-db"."portal_empleo_database_knowledge" db ON (db.id = id.database_id)
UNION	
-- HERRAMIENTAS INFORMATICAS SISTEMAS CONTABLES
SELECT 'PORTALEMPLEO' base_origen,
	CAST(ia.informatic_id AS VARCHAR) ||'-SC-'|| CAST(ia.accounting_knowledge_id AS VARCHAR) AS codigo,
	'HERRAMIENTAS INFORMÁTICAS' AS tipo_formacion,
	CAST(cv.id AS VARCHAR) AS cv_id,
	CASE WHEN UPPER(kas.name) LIKE 'OTRO'  THEN 'OTRO SISTEMA CONTABLE'
	ELSE UPPER(kas.name) END AS descripcion,
	CAST(NULL AS VARCHAR)
FROM "caba-piba-raw-zone-db"."portal_empleo_curriculum_vitaes" cv
	INNER JOIN "caba-piba-raw-zone-db"."portal_empleo_academic_formations" af ON (af.curriculum_id = cv.id)
	INNER JOIN "caba-piba-raw-zone-db"."portal_empleo_informatics" i ON (i.id = cv.informatic_id)
	INNER JOIN "caba-piba-raw-zone-db"."portal_empleo_informatics_accouting" ia ON (ia.informatic_id = cv.informatic_id)
	INNER JOIN "caba-piba-raw-zone-db"."portal_empleo_know_account_systems" kas ON (kas.id = ia.accounting_knowledge_id)	
UNION	
-- HERRAMIENTAS INFORMATICAS INTERNET
SELECT 'PORTALEMPLEO' base_origen,
	CAST(i.id AS VARCHAR) ||'-INTERNET' AS codigo,
	'HERRAMIENTAS INFORMÁTICAS' AS tipo_formacion,
	CAST(cv.id AS VARCHAR) AS cv_id,
	'INTERNET' AS descripcion,
	CAST(NULL AS VARCHAR)
FROM "caba-piba-raw-zone-db"."portal_empleo_curriculum_vitaes" cv
	INNER JOIN "caba-piba-raw-zone-db"."portal_empleo_academic_formations" af ON (af.curriculum_id = cv.id)
	INNER JOIN "caba-piba-raw-zone-db"."portal_empleo_informatics" i ON (i.id = cv.informatic_id)
WHERE i.internet=1	
--</sql>--

-- 3- Se crea la tabla tbp_typ_tmp_conocimientos_aptitudes que consiste en la union de las dos anteriores
--<sql>--
DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_tmp_conocimientos_aptitudes`;
--</sql>--

--<sql>--
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_conocimientos_aptitudes" AS 
WITH 
datos AS 
(
SELECT base_origen,
	codigo,
	fecha_inicio,
	fecha_fin,
	tipo_formacion,
	institucion_educativa,
	cv_id,
	descripcion,
	observacion,
	estado,
	CAST(NULL AS VARCHAR) AS nivel_idioma
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_conocimientos_aptitudes_1"
UNION
SELECT base_origen,
	codigo,
	CAST(NULL AS DATE) fecha_inicio,
	CAST(NULL AS DATE) fecha_fin,
	tipo_formacion,
	CAST(NULL AS VARCHAR) AS organizacion,
	cv_id,
	descripcion,
	CAST(NULL AS VARCHAR) AS observacion,
	CAST(NULL AS VARCHAR) AS estado,
	nivel_idioma
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_conocimientos_aptitudes_2"
),
cl1 AS (
SELECT 
	codigo,
	base_origen,
	fecha_inicio, 
	fecha_fin,
	UPPER(estado) estado,
	descripcion,
	institucion_educativa,
	cv_id,
	tipo_formacion,
	observacion,
	nivel_idioma,
	regexp_replace(regexp_replace(regexp_replace(descripcion,'[^a-zA-Z0-9ÑñÁáÉéÍíÓóÚúüÜäÄòùÇÃÊš\s#\/+\:\|\-\,\(\)\°\&\–\º\ª\_\]\[\@;.]+',''),'ò','ó'),'ù','ú') AS descripcion_clean
FROM datos
),
cl2 AS (
SELECT
	codigo,
	base_origen,
	fecha_inicio, 
	fecha_fin,
	CASE 
	WHEN estado IN ('GRADUADO','ABANDONADO', 'EN CURSO') THEN estado
	WHEN estado IN ('INCOMPLETO','EN_CURSO') THEN 'EN CURSO' 
	WHEN estado LIKE 'FINALIZADO' THEN 'GRADUADO' 
	WHEN estado LIKE 'NUNCA_INICIO' THEN 'ABANDONADO' 
	END estado,
	descripcion,
	institucion_educativa,
	cv_id,
	tipo_formacion,
	observacion,
	nivel_idioma,
	CASE
		WHEN cl1.descripcion LIKE '%.%' THEN regexp_replace(cl1.descripcion_clean,'[^a-zA-Z0-9ÑñÁáÉéÍíÓóÚúüÜäÄòùÇÃÊš\s#\/+\:\|\-\,\(\)\°\&\–\º\ª\_\]\[\@;]+',' ')
		WHEN regexp_like(cl1.descripcion ,'[a-zA-Z0-9ÑñÁáÉéÍíÓóÚúüÜäÄÇÃÊš\s?]+\s?\-\-\s?[a-zA-Z0-9ÑñÁáÉéÍíÓóÚúüÜäÄÇÃÊš\s?]+') THEN regexp_replace(cl1.descripcion_clean,'--','-')
		WHEN cl1.descripcion LIKE '%--%' AND NOT regexp_like(cl1.descripcion ,'[a-zA-Z0-9ÑñÁáÉéÍíÓóÚúüÜäÄÇÃÊš\s?]+\s?\-\-\s?[a-zA-Z0-9ÑñÁáÉéÍíÓóÚúüÜäÄÇÃÊš\s?]+') THEN regexp_replace(cl1.descripcion_clean,'-',' ')
		ELSE cl1.descripcion_clean
	END descripcion_clean
FROM cl1
),
cl3 AS (
SELECT
	codigo,
	base_origen,
	fecha_inicio, 
	fecha_fin,
	estado,
	descripcion,
	institucion_educativa,
	cv_id,
	tipo_formacion,
	observacion,
	nivel_idioma,
	CASE
		WHEN cl2.descripcion_clean = '-' THEN regexp_replace(cl2.descripcion_clean,'\-','')
		WHEN cl2.descripcion LIKE '%(' THEN regexp_replace(cl2.descripcion_clean,'\(','')
		WHEN cl2.descripcion LIKE ':%' THEN regexp_replace(cl2.descripcion_clean,'\:','')
		WHEN cl2.descripcion LIKE '%:' THEN regexp_replace(cl2.descripcion_clean,'\:','')
		WHEN cl2.descripcion LIKE '. %' THEN replace(cl2.descripcion_clean,'.','')
		WHEN cl2.descripcion LIKE '- %' THEN regexp_replace(cl2.descripcion_clean,'\-','')
		WHEN cl2.descripcion LIKE '-%' THEN regexp_replace(cl2.descripcion_clean,'\-','')
		WHEN cl2.descripcion LIKE '%-' THEN regexp_replace(cl2.descripcion_clean,'\-','')
		WHEN cl2.descripcion LIKE '%–' THEN regexp_replace(cl2.descripcion_clean,'\–','')
		WHEN cl2.descripcion LIKE '–%' THEN regexp_replace(cl2.descripcion_clean,'\–','')
		WHEN cl2.descripcion LIKE ',%' THEN regexp_replace(cl2.descripcion_clean,'\,','')
		WHEN cl2.descripcion LIKE '%,' THEN regexp_replace(cl2.descripcion_clean,'\,','')
		WHEN cl2.descripcion LIKE '%&' THEN regexp_replace(cl2.descripcion_clean,'\&','')
		WHEN cl2.descripcion LIKE '.:%' THEN regexp_replace(cl2.descripcion_clean,'.:','')
		WHEN cl2.descripcion LIKE '[%' THEN replace(replace(cl2.descripcion_clean,'[',''),']','')
		WHEN cl2.descripcion LIKE '%( %' THEN replace(replace(cl2.descripcion_clean,'( ','('),' )',')')
		WHEN cl2.descripcion LIKE '%.0%' THEN replace(cl2.descripcion_clean,' 0','.0')
		ELSE cl2.descripcion_clean
	END descripcion_clean
FROM cl2
),
cl4 AS (
SELECT
	codigo,
	base_origen,
	fecha_inicio, 
	fecha_fin,
	estado,
	descripcion as capacitacion,
	institucion_educativa,
	cv_id,
	tipo_formacion,
	observacion,
	nivel_idioma,
	CASE
		WHEN LTRIM(cl3.descripcion_clean) LIKE '%–%' THEN replace(LTRIM(cl3.descripcion_clean),'–','-')
		WHEN UPPER(cl3.descripcion) LIKE '%.NET%' THEN replace(UPPER(LTRIM(cl3.descripcion_clean)),'NET','.NET')
		WHEN UPPER(cl3.descripcion) LIKE '%ª CATEGORÍA%' THEN regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(UPPER(cl3.descripcion),'1ª|1.ª|1. ª','PRIMERA'),'2ª|2.ª|2. ª','SEGUNDA'),'3ª|3.ª|3. ª','TERCERA'),'4ª|4.ª|4. ª','CUARTA'),'5ª|5.ª|5. ª','QUINTA'),'6ª|6.ª|6. ª','SEXTA'),'7ª|7.ª|7. ª','SEPTIMA'),'8ª|8.ª|8. ª','OCTAVA'),'9ª|9.ª|9. ª','NOVENA'),'10ª|10.ª|10. ª','DECIMA')
		WHEN UPPER(cl3.descripcion) LIKE '%ª' THEN regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(UPPER(cl3.descripcion),'1ª|1.ª|1. ª','PRIMERA'),'2ª|2.ª|2. ª','SEGUNDA'),'3ª|3.ª|3. ª','TERCERA'),'4ª|4.ª|4. ª','CUARTA'),'5ª|5.ª|5. ª','QUINTA'),'6ª|6.ª|6. ª','SEXTA'),'7ª|7.ª|7. ª','SEPTIMA'),'8ª|8.ª|8. ª','OCTAVA'),'9ª|9.ª|9. ª','NOVENA'),'10ª|10.ª|10. ª','DECIMA')
		WHEN UPPER(cl3.descripcion) LIKE '%ª GENERACIÓN%' THEN regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(UPPER(cl3.descripcion),'1ª|1.ª|1. ª','PRIMERA'),'2ª|2.ª|2. ª','SEGUNDA'),'3ª|3.ª|3. ª','TERCERA'),'4ª|4.ª|4. ª','CUARTA'),'5ª|5.ª|5. ª','QUINTA'),'6ª|6.ª|6. ª','SEXTA'),'7ª|7.ª|7. ª','SEPTIMA'),'8ª|8.ª|8. ª','OCTAVA'),'9ª|9.ª|9. ª','NOVENA'),'10ª|10.ª|10. ª','DECIMA')
		WHEN UPPER(cl3.descripcion) LIKE '%ª EDICIÓN%' THEN regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(UPPER(cl3.descripcion),'1ª|1.ª|1. ª','PRIMERA'),'2ª|2.ª|2. ª','SEGUNDA'),'3ª|3.ª|3. ª','TERCERA'),'4ª|4.ª|4. ª','CUARTA'),'5ª|5.ª|5. ª','QUINTA'),'6ª|6.ª|6. ª','SEXTA'),'7ª|7.ª|7. ª','SEPTIMA'),'8ª|8.ª|8. ª','OCTAVA'),'9ª|9.ª|9. ª','NOVENA'),'10ª|10.ª|10. ª','DECIMA')
		ELSE UPPER(LTRIM(cl3.descripcion_clean))
	END descripcion_clean,
	CASE
        WHEN UPPER(cl3.institucion_educativa) LIKE '%INSTITUTO UNIVERSITARIO DE CIENCIAS DE LA SALUD%' THEN 'INSTITUTO UNIVERSITARIO DE CIENCIAS DE LA SALUD FUNDACIÓN H.A. BARCELÓ'
        WHEN UPPER(cl3.institucion_educativa) LIKE '%INSTITUTO UNIVERSITARIO DEL EJÉCITO%' THEN 'INSTITUTO UNIVERSITARIO DEL EJÉRCITO "MAYOR FRANCISCO ROMERO"'
        WHEN UPPER(cl3.institucion_educativa) LIKE '%INSTITUTO UNIVERSITARIO DEL EJÉRCITO ” MAYOR FRANCISCO ROMERO”%' THEN 'INSTITUTO UNIVERSITARIO DEL EJÉRCITO "MAYOR FRANCISCO ROMERO"'
        WHEN UPPER(cl3.institucion_educativa) LIKE '%INSTITUTO UNIVERSITARIO ESCUELA ARGENTINA DE NEGOCIOS%' THEN 'ESCUELA ARGENTINA DE NEGOCIOS'
        WHEN UPPER(cl3.institucion_educativa) LIKE '%UNIVERSIDAD CATÓLICA ARGENTINA%' THEN 'UNIVERSIDAD CATÓLICA ARGENTINA SANTA MARÍA DE LOS BUENOS AIRES'
        WHEN UPPER(cl3.institucion_educativa) LIKE '%AUSTRAL DE ROSARIO%' THEN 'UNIVERSIDAD AUSTRAL - SEDE ROSARIO'
        ELSE regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(UPPER(cl3.institucion_educativa), 'INSTITUTO DE ESTUDIOS PAR LA EXCELENCIA COMPETITIVA', 'INSTITUTO DE ESTUDIOS PARA LA EXCELENCIA COMPETITIVA'), 'INSTITUTO UNIVERSITARIO DE CIENCIAS DE LA SALUD', 'INSTITUTO UNIVERSITARIO DE CIENCIAS DE LA SALUD FUNDACIÓN H.A. BARCELÓ'), 'INSTITUTO UNIVERSITARIO DEL EJÉCITO', 'INSTITUTO UNIVERSITARIO DEL EJÉRCITO "MAYOR FRANCISCO ROMERO"'),'INSTITUTO UNIVERSITARIO DEL EJÉRCITO ” MAYOR FRANCISCO ROMERO”', 'INSTITUTO UNIVERSITARIO DEL EJÉRCITO "MAYOR FRANCISCO ROMERO"'), 'INSTITUTO UNIVERSITARIO ESCUELA ARGENTINA DE NEGOCIOS', 'ESCUELA ARGENTINA DE NEGOCIOS'), 'UNIVERSIDAD CATÓLICA ARGENTINA', 'UNIVERSIDAD CATÓLICA ARGENTINA SANTA MARÍA DE LOS BUENOS AIRES'), 'AUSTRAL DE ROSARIO', 'UNIVERSIDAD AUSTRAL - SEDE ROSARIO')
    END AS institucion_clean
FROM cl3
)
SELECT *
FROM cl4
--</sql>--