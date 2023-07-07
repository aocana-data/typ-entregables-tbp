-- 1.-- Crear REGISTRO LABORAL SIN CRUCE AGIP/AFIP
--<sql>--
DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_tmp_registro_laboral_formal_1`;
--</sql>--

--<sql>--
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_registro_laboral_formal_1" AS
-- CRMSL
SELECT 
	vec.id_broker,
	vec.base_origen,
	CAST(crmsl_candidatos.id_candidato AS VARCHAR)  id_candidato,
	crmsl_candidatos.tipo_doc_broker,
	crmsl_candidatos.documento_broker,
	crmsl_candidatos.genero_broker,
	crmsl_candidatos.fecha_empleo,
	CAST(NULL AS VARCHAR) tipo_contratacion,
	CAST(NULL AS VARCHAR) organizacion_empleadora_cuit,
	CAST(crmsl_candidatos.oportunidad_laboral_id_old AS VARCHAR) oportunidad_laboral_id_old
FROM (
		SELECT id_c AS id_candidato,
			(
				CASE
					WHEN (cs.tipo_documento_c = 'dni') THEN 'DNI' ELSE 'NN'
				END
			) AS tipo_doc_broker,
			CAST(cs.numero_documento_c AS VARCHAR) AS documento_broker,
			cs.tipo_documento_c AS tipo_documento_c_origen,
			cs.numero_documento_c AS numero_documento_origen,
			cs.cuil2_c AS cuil_origen,
			cc.id AS oportunidad_laboral_id_old,
			-- dado que no hay otra fecha, se utiliza fecha de modificacion del registro, como fecha de empleo
			CAST(cc.date_modified AS DATE) AS fecha_empleo,
			CASE
				WHEN cs.genero_c LIKE 'masculino'
				OR SUBSTRING(CAST(cs.cuil2_c AS VARCHAR), 1, 2) = '20' THEN 'M'
				WHEN cs.genero_c LIKE 'femenino'
				OR SUBSTRING(CAST(cs.cuil2_c AS VARCHAR), 1, 2) = '27' THEN 'F' ELSE 'X'
			END genero_broker
		FROM "caba-piba-staging-zone-db".tbp_typ_tmp_view_crm_sociolaboral_contacts_cstm_no_duplicates cs
			JOIN "caba-piba-raw-zone-db".crm_sociolaboral_op_oportunidades_laborales_contacts_c cc ON (
				cs.id_c = cc.op_oportunidades_laborales_contactscontacts_idb
			)
		WHERE cc.deleted = false
	) crmsl_candidatos
	JOIN "caba-piba-staging-zone-db"."tbp_typ_def_vecino" vec ON (
		vec.genero_broker = crmsl_candidatos.genero_broker
		AND vec.documento_broker = crmsl_candidatos.documento_broker
		AND vec.tipo_doc_broker = crmsl_candidatos.tipo_doc_broker
		AND vec.base_origen = 'CRMSL'
	)
WHERE crmsl_candidatos.id_candidato IS NOT NULL
GROUP BY 
vec.id_broker,
vec.base_origen,
crmsl_candidatos.id_candidato,
crmsl_candidatos.tipo_doc_broker,
crmsl_candidatos.documento_broker,
crmsl_candidatos.genero_broker,
crmsl_candidatos.fecha_empleo,
crmsl_candidatos.oportunidad_laboral_id_old 

UNION 
-- PORTALEMPLEO
SELECT 
	vec.id_broker,
	vec.base_origen,
	CAST(portal_empleo_candidatos.cid AS VARCHAR) id_candidato,
	portal_empleo_candidatos.tipo_doc_broker,
	portal_empleo_candidatos.documento_broker,
	portal_empleo_candidatos.genero_broker,
	portal_empleo_candidatos.fecha_empleo,
	portal_empleo_candidatos.tipo_contratacion,
	portal_empleo_candidatos.organizacion_empleadora_cuit,
	CAST(portal_empleo_candidatos.oportunidad_laboral_id_old AS VARCHAR) oportunidad_laboral_id_old
FROM (
		SELECT aux.cid,
			'PORTALEMPLEO' base_origen,
			-- Campo doc_type : A fin de homogeneizar criterios, se adpotará la tipología utilizada para tipos de documentos en las tablas tbp_broker_def_broker_general y tbp_typ_def_vecino
			--Tipos del campo doc_type (origen)
			--DE: Documento extranjero
			--CRP: Cdelua de Residencia Precaria
			--CI: Cédula de Identidad de Capital Federal
			--CUIL: Clave única de Identificación Laboral. Los casos que figuran con tipo de documento CUIL poseen una longitud de numeros de documento que es menor a los 11 digito. Es por ello que se asume este campo se encuentra mal catalogado y que en realidad es el tipo de documento DNI
			--LE: Libreta de Enrolamiento
			--LC: Libreta Civica
			--DNI: Documento Nacional de Identidad
			--PAS: Pasaporte. En este cmapo figuran casos de nacionalidad argentina
			CASE
				WHEN (
					pec.doc_type IN ('DNI', 'LC', 'CI', 'LE', 'CUIL')
				) THEN pec.doc_type
				WHEN (pec.doc_type = 'PAS') THEN 'PE'
				WHEN (pec.doc_type = 'DE') THEN 'CE'
				WHEN (pec.doc_type = 'CRP') THEN 'OTRO' ELSE 'NN'
			END tipo_doc_broker,
			(
				CASE
					WHEN (pec.gender = 'M') THEN 'M'
					WHEN (pec.gender = 'F') THEN 'F' ELSE 'X'
				END
			) genero_broker,
			CAST(pec.doc_number AS VARCHAR) documento_broker,
			--Campo fecha_empleo: Se toman las variables "start_date" (de la tabla "portal_empleo_volunteerings"),"application_date" (de la tabla "portal_empleo_job_applications"), "close_date" (de la tabla "portal_empleo_job_hirings") de la fuente de PORTAL EMPLEO como fecha vinculada al contrato laboral
			aux.fecha_empleo,
			aux.tipo_contratacion,
			aux.organizacion_empleadora_cuit,
			aux.oportunidad_laboral_id_old
		FROM (
				SELECT DISTINCT(pev.candidate_id) cid, pev.application_date AS fecha_empleo, 
					CAST(NULL AS VARCHAR) tipo_contratacion,  CAST(org.lei_code AS VARCHAR) organizacion_empleadora_cuit,
					CAST(jo.id AS VARCHAR) oportunidad_laboral_id_old
				FROM "caba-piba-raw-zone-db"."portal_empleo_job_applications" pev
				LEFT JOIN "caba-piba-raw-zone-db"."portal_empleo_job_postings" jp ON (jp.id=pev.job_posting_id)
				LEFT JOIN "caba-piba-raw-zone-db"."portal_empleo_organizations" org ON (org.id=jp.organization_id) 
				LEFT JOIN "caba-piba-raw-zone-db"."portal_empleo_job_offers" jo ON (jo.id=jp.job_offer_id)
				
				UNION
				
				SELECT DISTINCT(pev.candidate_id) cid, pev.close_date AS fecha_empleo, 
					CAST(NULL AS VARCHAR) tipo_contratacion, CAST(org.lei_code AS VARCHAR) organizacion_empleadora_cuit,
					CAST(jo.id AS VARCHAR) oportunidad_laboral_id_old
				FROM "caba-piba-raw-zone-db"."portal_empleo_job_hirings" pev
			    LEFT JOIN "caba-piba-raw-zone-db"."portal_empleo_job_postings" jp ON (jp.id=pev.job_posting_id)
				LEFT JOIN "caba-piba-raw-zone-db"."portal_empleo_organizations" org ON (org.id=jp.organization_id) 
				LEFT JOIN "caba-piba-raw-zone-db"."portal_empleo_job_offers" jo ON (jo.id=jp.job_offer_id)
				
			) aux
			JOIN "caba-piba-raw-zone-db"."portal_empleo_candidates" pec ON (pec.id = aux.cid)
	) portal_empleo_candidatos
	JOIN "caba-piba-staging-zone-db"."tbp_typ_def_vecino" vec ON (
		vec.genero_broker = portal_empleo_candidatos.genero_broker
		AND vec.documento_broker = portal_empleo_candidatos.documento_broker
		AND vec.tipo_doc_broker = portal_empleo_candidatos.tipo_doc_broker
		AND vec.base_origen = portal_empleo_candidatos.base_origen
	)
WHERE portal_empleo_candidatos.cid IS NOT NULL
GROUP BY 
vec.id_broker,
vec.base_origen,
portal_empleo_candidatos.cid,
portal_empleo_candidatos.tipo_doc_broker,
portal_empleo_candidatos.documento_broker,
portal_empleo_candidatos.genero_broker,
portal_empleo_candidatos.fecha_empleo,
portal_empleo_candidatos.tipo_contratacion,
portal_empleo_candidatos.organizacion_empleadora_cuit,
portal_empleo_candidatos.oportunidad_laboral_id_old
--</sql>--
	
-- 2.-- Crear REGISTRO LABORAL AFIP AGIP ALTAS BAJAS
--Campo descripcion_modalidad_contratacion. Existen casos con mas de una descripción para un mismo codigo de modalidad de contratación. Los mismos provienen de la tabla "afip_agip_tipo_contratacion". Se optó por elegír una única descripción basada estrictamente en la descripción que figura en la tabla de modalidades de contratación proveniente de la web de AFIP
 
--<sql>--
DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_tmp_registro_laboral_formal_afip_agip_ab`;
--</sql>--

--<sql>--
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_registro_laboral_formal_afip_agip_ab" AS
WITH aa_ab AS (
	SELECT 'DNI' AS tipo_documento,
		SUBSTRING(ab.cuil_del_empleado, 3, 8) AS numero_documento,
		ab.cuit_del_empleador,
		CASE
			WHEN SUBSTRING(ab.cuil_del_empleado, 1, 2) = '20' THEN 'M'
			WHEN SUBSTRING(ab.cuil_del_empleado, 1, 2) = '23'
			AND SUBSTRING(ab.cuil_del_empleado, 11, 1) = '9' THEN 'M'
			WHEN SUBSTRING(ab.cuil_del_empleado, 1, 2) = '27' THEN 'F'
			WHEN SUBSTRING(ab.cuil_del_empleado, 1, 2) = '23'
			AND SUBSTRING(ab.cuil_del_empleado, 11, 1) = '4' THEN 'F' ELSE 'X'
		END genero,
		-- se convierte la fecha a date, si las fechas de inicio queda con el valor NULL
		CASE
			WHEN ab.fecha_inicio_de_relacion_laboral IN ('9999-12-31','999-12-31','3202-03-09','7202-08-17','4202-02-23','2109-02-25')
			OR (LENGTH(TRIM(ab.fecha_inicio_de_relacion_laboral)) > 0 AND try_cast(ab.fecha_inicio_de_relacion_laboral as date) IS NULL) 
			THEN CAST(NULL AS DATE)
			ELSE try_cast(ab.fecha_inicio_de_relacion_laboral as date)
		END fecha_inicio_de_relacion_laboral,
		
		-- se convierte la fecha a date, si las fechas de inicio o fin son inconsistentes quedan con el valor NULL
		CASE
			WHEN ab.fecha_fin_de_relacion_laboral IN ('9999-12-31','999-12-31','3202-03-09','7202-08-17','4202-02-23','2109-02-25')
			OR (LENGTH(TRIM(ab.fecha_inicio_de_relacion_laboral)) > 0 AND try_cast(ab.fecha_inicio_de_relacion_laboral as date) IS NULL) 
			OR (LENGTH(TRIM(ab.fecha_fin_de_relacion_laboral)) > 0 AND try_cast(ab.fecha_fin_de_relacion_laboral as date) IS NULL) 
			THEN CAST(NULL AS DATE)
			ELSE try_cast(ab.fecha_fin_de_relacion_laboral as date)
		END fecha_fin_de_relacion_laboral,
		
		TRY_CAST(ab.codigo_modalidad_de_contratato AS INT) AS codigo_modalidad_contratacion,
		--Campo descripcion_modalidad_contratacion: Existen casos con mas de una descripción para un mismo codigo de modalidad de contratación. Los mismos provienen de la tabla "afip_agip_tipo_contratacion". Se optó por elegír una única descripción basada estrictamente en la descripción que figura en la tabla de modalidades de contratación proveniente de la web de AFIP
		CASE
			WHEN TRY_CAST(ab.codigo_modalidad_de_contratato AS INT) = 0 THEN 'Contrato Modalidad Promovida. Reducción 0%'
			WHEN TRY_CAST(ab.codigo_modalidad_de_contratato AS INT) = 10 THEN 'Pasantías Ley N° 25.165 Decreto N° 340/92'
			WHEN TRY_CAST(ab.codigo_modalidad_de_contratato AS INT) = 27 THEN 'Pasantías Decreto N° 1.227/01' ELSE tc."descripción"
		END descripcion_modalidad_contratacion,
		ab.codigo_de_puesto_desempeniado,
		--Campo descripcion_de_puesto_desempeniado: Se asume este campo como input para obtener el cargo de un empleado.
		pd.descripcion AS descripcion_de_puesto_desempeniado,
		ab.codigo_de_movimiento,
		cm."descripción" AS descripcion_movimiento,
		ab.fecha_de_movimiento,
		TRY_CAST(ab.codigo_de_actividad AS INT) AS codigo_de_actividad,
		CASE 
		    WHEN aa.desc_actividad_afip_o_naes IS NULL OR LENGTH(TRIM(aa.desc_actividad_afip_o_naes)) = 0 THEN ba.desc_actividad_agip_o_naecba
		    ELSE aa.desc_actividad_afip_o_naes
		END descripcion_actividad,

		CASE 
		    WHEN (aa.desc_actividad_afip_o_naes IS NULL OR LENGTH(TRIM(aa.desc_actividad_afip_o_naes)) = 0) 
			AND LENGTH(TRIM(ba.desc_actividad_agip_o_naecba)) > 0 
				THEN 'AGIP-CLAE'
				
			WHEN (ba.desc_actividad_agip_o_naecba IS NULL OR LENGTH(TRIM(ba.desc_actividad_agip_o_naecba)) = 0) 
			AND LENGTH(TRIM(aa.desc_actividad_afip_o_naes)) > 0 
				THEN 'AFIP-CLAE'
		    
			ELSE ''
		END origen_descripcion_actividad,
		TRY_CAST(ab.codigo_de_situacion_de_baja AS INT) AS codigo_de_situacion_de_baja,
		sb.descripcion AS descripcion_situacion_baja,
		sb.situacion_de_revista,
		ab.remuneracion_bruta
	FROM "caba-piba-raw-zone-db"."afip_agip_altas_bajas" ab
		LEFT JOIN "caba-piba-raw-zone-db".afip_agip_codigos_movimientos cm ON (ab.codigo_de_movimiento = cm."código")
		LEFT JOIN "caba-piba-raw-zone-db".afip_agip_tipo_contratacion tc ON (
			TRY_CAST(ab.codigo_modalidad_de_contratato AS INT) = TRY_CAST(tc."código" AS INT)
		)
		LEFT JOIN "caba-piba-raw-zone-db".afip_agip_situacion_bajas sb ON (
			TRY_CAST(ab.codigo_de_situacion_de_baja AS INT) = TRY_CAST(sb.codigo AS INT)
		)
		LEFT JOIN "caba-piba-staging-zone-db".tbp_typ_def_puestos pd ON (ab.codigo_de_puesto_desempeniado = pd.id_puesto)
		--Agregamos los codigos y descripciones de  de naecba y naes (AGIP Y AFIP)
		LEFT JOIN (
			SELECT cod_actividad_afip_o_naes,
				desc_actividad_afip_o_naes
			FROM "caba-piba-raw-zone-db".tbp_typ_tmp_nomenclador_actividades_economicas
			ORDER BY cod_actividad_afip_o_naes
		) aa ON (
			TRY_CAST(ab.codigo_de_actividad AS INT) = TRY_CAST(aa.cod_actividad_afip_o_naes AS INT)
		)
		LEFT JOIN (
			SELECT cod_actividad_agip_o_naecba,
                desc_actividad_agip_o_naecba
			FROM "caba-piba-raw-zone-db".tbp_typ_tmp_nomenclador_actividades_economicas
			ORDER BY cod_actividad_agip_o_naecba
		) ba ON (
			TRY_CAST(ab.codigo_de_actividad AS INT) = TRY_CAST(ba.cod_actividad_agip_o_naecba AS INT)
		)
		-- se limita el resultado a las personas que estan en la tabla de vecinos
		JOIN "caba-piba-staging-zone-db"."tbp_typ_def_vecino" vec ON (
		vec.genero_broker = CASE
			WHEN SUBSTRING(ab.cuil_del_empleado, 1, 2) = '20' THEN 'M'
			WHEN SUBSTRING(ab.cuil_del_empleado, 1, 2) = '23'
			AND SUBSTRING(ab.cuil_del_empleado, 11, 1) = '9' THEN 'M'
			WHEN SUBSTRING(ab.cuil_del_empleado, 1, 2) = '27' THEN 'F'
			WHEN SUBSTRING(ab.cuil_del_empleado, 1, 2) = '23'
			AND SUBSTRING(ab.cuil_del_empleado, 11, 1) = '4' THEN 'F' ELSE 'X'
		END 
		AND vec.documento_broker = SUBSTRING(ab.cuil_del_empleado, 3, 8)
		AND vec.tipo_doc_broker LIKE 'DNI'
		)		
	WHERE SUBSTRING(ab.cuil_del_empleado, 1, 2) NOT IN ('IS')
),
abf1 AS (
	SELECT aa_ab.tipo_documento,
		aa_ab.numero_documento,
		aa_ab.cuit_del_empleador,
		aa_ab.genero,
		aa_ab.fecha_inicio_de_relacion_laboral,
		aa_ab.fecha_fin_de_relacion_laboral,
		aa_ab.codigo_modalidad_contratacion,
		--Campo descripcion_modalidad_contratacion: Existen casos que cuentan con este campo nulo. Se puede inferir que aquellos valores que no tengan descripción corresponde a la introducción de un código de modalidad de contratacion erróneo o con un código que no figura o no se actualizado en la tablas paramétricas de AFIP AGIP
		aa_ab.descripcion_modalidad_contratacion,
		--Para el campo modalidad_contratacion, las opciones se reducen a cuatro: "RELACIÓN DE DEPENDENCIA", "CONTRATO", "PASANTÍA" y "AD HONÓREM". Para poder realizar esta selección, se utiliza como input el campo "descripcion_modalidad_contratacion", tomando en cuenta las categorías establecidas por la tabla de AFIP/AGIP ALTAS y BAJAS que se muestran en la tabla paramétrica afip_agip_tipo_contratacion
		CASE
			WHEN aa_ab.codigo_modalidad_contratacion IN (
				1,
				4,
				5,
				6,
				8,
				14,
				15,
				17,
				18,
				19,
				20,
				24,
				25,
				26,
				31,
				38,
				42,
				47,
				49,
				99,
				102,
				110,
				112,
				115,
				201,
				202,
				203,
				204,
				205,
				206,
				301,
				302,
				303,
				307,
				308,
				309,
				310,
				311,
				312,
				313,
				314,
				315,
				985,
				987,
				989,
				990,
				991,
				992,
				994,
				995,
				996,
				997,
				998,
				999
			) THEN 'RELACION DE DEPENDENCIA'
			WHEN aa_ab.codigo_modalidad_contratacion IN (
				0,
				2,
				7,
				11,
				12,
				13,
				16,
				21,
				22,
				23,
				28,
				29,
				30,
				32,
				33,
				34,
				35,
				36,
				37,
				39,
				40,
				41,
				43,
				45,
				46,
				48,
				50,
				95,
				96,
				98,
				100,
				111,
				113,
				114,
				211,
				212,
				213,
				221,
				222,
				223,
				304,
				305,
				306
			) THEN 'CONTRATO'
			WHEN aa_ab.codigo_modalidad_contratacion IN (3, 9, 10, 27, 51, 97) THEN 'PASANTIA'
			WHEN aa_ab.codigo_modalidad_contratacion IN (44) THEN 'AD HONOREM' ELSE ''
		END modalidad_contratacion,
		aa_ab.codigo_de_puesto_desempeniado,
		--Campo descripcion_puesto_desempenado: Existen casos que cuentan con este campo nulo. Se puede inferir que aquellos valores que no tengan descripción corresponde a la introducción de un código de puesto desempeniado erróneo o con un código que no figura o no se actualizado en la tablas paramétricas de AFIP AGIP
		aa_ab.descripcion_de_puesto_desempeniado,
		aa_ab.codigo_de_movimiento,
		aa_ab.descripcion_movimiento,
		aa_ab.fecha_de_movimiento AS fecha_de_movimiento_origen,
		aa_ab.codigo_de_actividad,
		--Campo descripction_actividad: Existen casos que cuentan con este campo nulo. Se puede inferir que aquellas actividades que no tengan descripción corresponde a la introducción de un código de actividad erróneo o con un código que no figura o no se actualizado en la tablas paramétricas de AFIP AGIP
		aa_ab.descripcion_actividad,
		aa_ab.origen_descripcion_actividad,
		aa_ab.codigo_de_situacion_de_baja,
		aa_ab.descripcion_situacion_baja,
		aa_ab.situacion_de_revista,
		aa_ab.remuneracion_bruta
	FROM aa_ab
),
abf2 AS (
	SELECT abf1.tipo_documento,
		abf1.numero_documento,
		abf1.cuit_del_empleador,
		abf1.genero,
		abf1.fecha_inicio_de_relacion_laboral,
		abf1.fecha_fin_de_relacion_laboral,
		abf1.codigo_modalidad_contratacion,
		abf1.descripcion_modalidad_contratacion,
		abf1.modalidad_contratacion,
		abf1.codigo_de_puesto_desempeniado,
		abf1.descripcion_de_puesto_desempeniado,
		abf1.codigo_de_movimiento,
		abf1.descripcion_movimiento,
		abf1.fecha_de_movimiento_origen,
		--fecha_de_movimiento: A fin de evitar duplicidad de casos, se toma el último movimiento registrado en función del numero de documento del empleado y su genero y el CUIT del empleador
		ROW_NUMBER() OVER(
			PARTITION BY abf1.numero_documento,
			abf1.cuit_del_empleador,
			abf1.genero
			ORDER BY abf1.fecha_de_movimiento_origen desc
		) AS "orden",
		abf1.codigo_de_actividad,
		CASE
		    WHEN abf1.descripcion_actividad IS NULL OR LENGTH(TRIM(abf1.descripcion_actividad)) = 0 THEN cl.descripcion
		    ELSE abf1.descripcion_actividad
		END descripcion_actividad,
		
		CASE
			WHEN (abf1.descripcion_actividad IS NULL OR LENGTH(TRIM(abf1.descripcion_actividad)) = 0)
			AND	LENGTH(TRIM(cl.descripcion)) > 0 THEN 'AFIP-CLANAE-5DIGITOS'
			ELSE abf1.origen_descripcion_actividad
		END origen_descripcion_actividad,
		
		abf1.codigo_de_situacion_de_baja,
		abf1.descripcion_situacion_baja,
		abf1.situacion_de_revista,
		abf1.remuneracion_bruta
	FROM abf1
	LEFT JOIN "caba-piba-raw-zone-db"."mec_codigos_clanae_ctividades_economicas" cl
	ON (SUBSTRING(CAST(abf1.codigo_de_actividad AS VARCHAR),1,5) = REPLACE(cl.codigo_5_digitos,'.',''))
)
SELECT *
FROM abf2
WHERE orden = 1
--</sql>--

-- 3.-- Crear REGISTRO LABORAL FORMAL CON CRUCE AGIP/AFIP
--<sql>--
DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_tmp_registro_laboral_formal`;
--</sql>--

--<sql>--
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_registro_laboral_formal" AS
--fecha_inicio_de_relacion_laboral: Dado que no hasta el día de la fecha no se cuenta con fuentes completas o fidedignas para determinar si un caso efectivamente participo de un proceso de alguna oportunidad laboral y fue contratado y dado de alta en AFIP (CUIT de organizaciones, fecha de contrato laboral) se establece como condicion que la fecha de inicio de la relación laboral no supere los 6 meses de la fecha de aplicación a una vacante 
SELECT *,
	-- se agrega una columna para indicar si el cuit del empleador de la base origen coincide
	-- con el cuil informado por AGIP/AFIP
	CASE
		WHEN CAST(a.cuit_del_empleador AS VARCHAR) = CAST(r.organizacion_empleadora_cuit AS VARCHAR) THEN 1 ELSE 0
	END AS mismo_empleador
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_registro_laboral_formal_afip_agip_ab" a
	JOIN "caba-piba-staging-zone-db"."tbp_typ_tmp_registro_laboral_formal_1" r ON (
		r.tipo_doc_broker LIKE 'DNI'
		AND r.documento_broker = a.numero_documento
		AND r.genero_broker = a.genero
	)
--</sql>--	

-- 4.-- Crear REGISTRO LABORAL FORMAL CON CRUCE AGIP/AFIP Y VECINOS
--<sql>--
DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_tmp_registro_laboral_formal_sin_ol`;
--</sql>--

--<sql>--
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_registro_laboral_formal_sin_ol" AS
-- se crea una tabla de registro laboral de aquellos vecinos que no han participado de una oportunidad laboral
SELECT 
a.codigo_de_puesto_desempeniado,
a.codigo_modalidad_contratacion,
a.descripcion_modalidad_contratacion,
a.codigo_de_actividad,
a.descripcion_actividad,
a.origen_descripcion_actividad,
CAST(NULL AS VARCHAR) id_candidato,
a.descripcion_de_puesto_desempeniado,
CAST(NULL AS VARCHAR) oportunidad_laboral_id,
vec.id_broker,
vec.base_origen,
a.cuit_del_empleador, 
a.fecha_inicio_de_relacion_laboral,
a.fecha_fin_de_relacion_laboral,
a.modalidad_contratacion,
a.remuneracion_bruta	
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_registro_laboral_formal_afip_agip_ab" a
	JOIN "caba-piba-staging-zone-db"."tbp_typ_def_vecino" vec ON (
	vec.genero_broker = a.genero 
	AND vec.documento_broker = a.numero_documento
	AND vec.tipo_doc_broker LIKE 'DNI'
	)
	LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_tmp_registro_laboral_formal" r ON (
		r.tipo_doc_broker LIKE 'DNI'
		AND r.documento_broker = a.numero_documento
		AND r.genero_broker = a.genero 
	)
WHERE r.tipo_doc_broker IS NULL
--</sql>--

-- 5.-- Crear REGISTRO LABORAL FORMAL COMPLETO CON ATRIBUTO BASE_ORIGEN
--<sql>--
DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_tmp_registro_laboral_formal_completa`;
--</sql>--

--<sql>--
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_registro_laboral_formal_completa" AS
SELECT 
row_number() OVER () AS registro_laboral_formal_id,
registros.*
FROM
(
	SELECT 
		lf.base_origen,
		lf.id_candidato AS registro_laboral_formal_id_old,
		lf.codigo_de_puesto_desempeniado AS id_puesto,
		lf.oportunidad_laboral_id_old,
		lf.id_broker,
		lf.cuit_del_empleador, 
		lf.fecha_inicio_de_relacion_laboral AS fecha_inicio,
		lf.fecha_fin_de_relacion_laboral AS fecha_fin,
		lf.modalidad_contratacion AS modalidad_de_trabajo,
		lf.remuneracion_bruta AS remuneracion_moneda_corriente,
		CAST(NULL AS DECIMAL) AS remuneracion_moneda_constante
	FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_registro_laboral_formal"  lf
	GROUP BY 
		lf.base_origen,
		lf.id_candidato,
		lf.codigo_de_puesto_desempeniado,
		lf.oportunidad_laboral_id_old,
		lf.id_broker,
		lf.cuit_del_empleador, 
		lf.fecha_inicio_de_relacion_laboral,
		lf.fecha_fin_de_relacion_laboral,
		lf.modalidad_contratacion,
		lf.remuneracion_bruta
	
	UNION
	
	SELECT
		'' AS base_origen,
		rf_sin_oportunidad.id_candidato AS registro_laboral_formal_id_old,
		rf_sin_oportunidad.codigo_de_puesto_desempeniado AS id_puesto,
		NULL oportunidad_laboral_id_old,
		rf_sin_oportunidad.id_broker,
		rf_sin_oportunidad.cuit_del_empleador, 
		rf_sin_oportunidad.fecha_inicio_de_relacion_laboral AS fecha_inicio,
		rf_sin_oportunidad.fecha_fin_de_relacion_laboral AS fecha_fin,
		rf_sin_oportunidad.modalidad_contratacion AS modalidad_de_trabajo,
		rf_sin_oportunidad.remuneracion_bruta AS remuneracion_moneda_corriente,
		CAST(NULL AS DECIMAL) AS remuneracion_moneda_constante
	FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_registro_laboral_formal_sin_ol"  rf_sin_oportunidad
	GROUP BY 
		rf_sin_oportunidad.id_candidato,
		rf_sin_oportunidad.codigo_de_puesto_desempeniado,
		rf_sin_oportunidad.id_broker,
		rf_sin_oportunidad.cuit_del_empleador, 
		rf_sin_oportunidad.fecha_inicio_de_relacion_laboral,
		rf_sin_oportunidad.fecha_fin_de_relacion_laboral,
		rf_sin_oportunidad.modalidad_contratacion,
		rf_sin_oportunidad.remuneracion_bruta
) registros
--</sql>--