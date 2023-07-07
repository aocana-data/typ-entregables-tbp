-- 1.-- Crear OPORTUNIDAD_LABORAL CRMEMPLEO, CRMSL, PORTALEMPLEO, 
-- CAMPOS REQUERIDOS EN TABLA DEF SEGUN MODELO (Oferta Laboral, Prácticas Formativas (Pasantías)):
-- Código (1+)
-- Descripción
-- Estado => ABIERTO, CANCELADO, CERRADO
-- Apto discapacitados => S, N
-- Vacantes
-- Modalidad de Trabajo => RELACION DE DEPENDENCIA, CONTRATIO, PASANTIA, AD HONOREM
-- Edad Mínima
-- Edad Máxima
-- Vacantes Cubiertas
-- Tipo de Puesto
-- Turno de Trabajo => MAÑANA, MAÑANA-TARDE, MAÑANA-TARDE-NOCHE, TARDE, TARDE-NOCHE, NOCHE
-- Grado de Estudio => SECUNDARIO, TERCIARIO, UNIVERSITARIO, OTROS
-- Duracion Practica formativa
-- Sector Productivo => ABASTECIMIENTO Y LOGISTICA, ADMINISTRACION, CONTABILIDAD Y FINANZAS,ATENCION AL CLIENTE, CALL CENTER Y TELEMARKETING,ADUANA Y COMERCIO EXTERIOR, COMERCIAL, VENTAS Y NEGOCIOS, GASTRONOMIA, HOTELERIA Y TURISMO, INGENIERIAS , LIMPIEZA Y MANTENIMIENTO (SIN EDIFICIOS), OFICIOS Y OTROS, PRODUCCION Y MANUFACTURA (SIN TEXTIL, ELECTRONICA Y AUTOMOTRIZ), SALUD, MEDICINA, FARMACIA Y ASISTENCIA SOCIAL, SECTOR PUBLICO,MINERIA, ENERGIA, PETROLEO, AGUA Y GAS
-- Nota: la tabla deberá estar relacionada con la entidad "Registro laboral formal" si toma el empleo 
-- y con la entidad "Programa"

--<sql>--
DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_tmp_oportunidad_laboral`;
--</sql>--

--<sql>--
DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_tmp_oportunidad_laboral_idiomas`;
--</sql>--

--<sql>--
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_oportunidad_laboral_idiomas" AS
WITH oportunidad_laboral_idiomas AS (
SELECT CAST(jo.id AS VARCHAR) oportunidad_laboral_id,
-- idioma requerido para el puesto
UPPER(pel.name) AS idioma_requerido

FROM "caba-piba-raw-zone-db"."portal_empleo_job_offers" jo
LEFT JOIN "caba-piba-raw-zone-db"."portal_empleo_job_postings" jp ON (jo.id=jp.job_offer_id)
LEFT JOIN "caba-piba-raw-zone-db"."portal_empleo_job_requirements" jr ON (jp.job_requirement_id=jr.id)
LEFT JOIN 
"caba-piba-raw-zone-db"."portal_empleo_job_req_language" jrl ON (jr.id=jrl.job_requirement_id)
LEFT JOIN 
"caba-piba-raw-zone-db"."portal_empleo_language" pel ON (jrl.language_id=pel.id)
WHERE jp.deleted = 0 AND UPPER(pel.name) NOT LIKE 'OTRO' AND pel.name IS NOT NULL
)
SELECT *
FROM oportunidad_laboral_idiomas
--</sql>--

--<sql>--
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_oportunidad_laboral" AS
-- CRM EMPLEO
WITH oportunidad_laboral AS (
SELECT 
	'CRMEMPLEO' base_origen,
	CAST(id AS VARCHAR) id,
	CAST(name AS VARCHAR) descripcion,
	CAST(DATE_PARSE(date_format(createddate, '%Y-%m-%d %h:%i%p'), '%Y-%m-%d %h:%i%p') AS DATE) fecha_publicacion,
	CAST(estado_de_anuncio__c AS VARCHAR) estado,
	CASE WHEN apto_discapacitado__c = true THEN '1' ELSE '0' END apto_discapacitado,
	CAST(vacantes__c AS VARCHAR) vacantes,
	CAST(modalidad_de_trabajo__c AS VARCHAR) modalidad_de_trabajo,
	CAST(edad__c AS VARCHAR) edad_minima,
	CAST(avx_edad_maxima__c AS VARCHAR) edad_maxima,
	CAST(avx_vacantes_cubiertas__c AS VARCHAR) vacantes_cubiertas,
	CAST(tipo_de_puesto__c AS VARCHAR) tipo_de_puesto,
	-- el horario de entrada y salida servirá para determinar el campo modelado Turno de Trabajo"
	CAST(horario_de_entrada__c AS VARCHAR) horario_entrada,
	CAST(horario_de_salida__c AS VARCHAR) horario_salida,
	CAST(grados_de_estudio__c AS VARCHAR) grado_de_estudio,
	-- en un futuro cuando este asociado a "Registro laboral formal" si la misma finalizo se podria obtener
	-- por la diferencia entre el inicio y fin de la relacion
	CAST(NULL AS VARCHAR) duracion_practica_formativa,
	
	-- tienen sentido los siguientes campos?
	CAST(avx_sector__c AS VARCHAR) sector_productivo,
	CAST(avx_industria__c AS VARCHAR) industria, 
	CAST(enviado_para_aprobacion__c AS VARCHAR) enviado_para_aprobacion__c,
	CAST(postulantes_contratados__c AS VARCHAR) postulantes_contratados__c,
	
	-- datos de organizacion/empresa que busca empleados
	CAST(razon_social__c AS VARCHAR) organizacion_empleadora,
	CAST(NULL AS VARCHAR) organizacion_empleadora_calle,
	CAST(NULL AS VARCHAR) organizacion_empleadora_piso,
	CAST(NULL AS VARCHAR) organizacion_empleadora_depto,
	CAST(NULL AS VARCHAR) organizacion_empleadora_cp,
	CAST(NULL AS VARCHAR) organizacion_empleadora_barrio,
	CAST(NULL AS VARCHAR) organizacion_empleadora_cuit,
	CAST(NULL AS VARCHAR) AS genero_requerido,
	CAST(NULL AS INT) AS experiencia_requerida
FROM "caba-piba-raw-zone-db"."crm_empleo_anuncio__c"

UNION

SELECT 
	'CRMEMPLEO' base_origen,
	-- se concatena una letra H al final, para indicar que se trata del id de la tabla historica
	CAST(id AS VARCHAR) || 'H' id,
 	CAST(name AS VARCHAR) descripcion,
	CAST(createddate AS DATE) fecha_publicacion,
 	CAST(estado_de_anuncio__c AS VARCHAR) estado,
 	CAST(apto_discapacitado__c AS VARCHAR),
	CAST(vacantes__c AS VARCHAR) vacantes,
	CAST(modalidad_de_trabajo__c AS VARCHAR) modalidad_de_trabajo,
	CAST(edad__c AS VARCHAR) edad_minima,
	CAST(NULL AS VARCHAR) edad_maxima,
	CAST(NULL AS VARCHAR) vacantes_cubiertas,
	CAST(tipo_de_puesto__c AS VARCHAR) tipo_de_puesto,
	-- el horario de entrada y salida servirá para determinar el campo modelado Turno de Trabajo"
	CAST(horario_de_entrada__c AS VARCHAR) horario_entrada,
	CAST(horario_de_salida__c AS VARCHAR) horario_salida,
	CAST(grados_de_estudio__c AS VARCHAR) grado_de_estudio,
	-- en un futuro cuando este asociado a "Registro laboral formal" si la misma finalizo se podria obtener
	-- por la diferencia entre el inicio y fin de la relacion
	CAST('' AS VARCHAR) duracion_practica_formativa,
	
	-- tienen sentido los siguientes campos?
	CAST(NULL AS VARCHAR) sector_productivo,
	CAST(industria__c AS VARCHAR) industria, 
	CAST(enviado_para_aprobacion__c AS VARCHAR) enviado_para_aprobacion__c,
	CAST(postulantes_contratados__c AS VARCHAR) postulantes_contratados__c,
	
	-- datos de organizacion/empresa que busca empleados
	CAST(razon_social__c AS VARCHAR) organizacion_empleadora,
	CAST(NULL AS VARCHAR) organizacion_empleadora_calle,
	CAST(NULL AS VARCHAR) organizacion_empleadora_piso,
	CAST(NULL AS VARCHAR) organizacion_empleadora_depto,
	CAST(NULL AS VARCHAR) organizacion_empleadora_cp,
	CAST(NULL AS VARCHAR) organizacion_empleadora_barrio,
	CAST(NULL AS VARCHAR) organizacion_empleadora_cuit,
	CAST(NULL AS VARCHAR) AS genero_requerido,
	CAST(NULL AS INT) AS experiencia_requerida
FROM "caba-piba-raw-zone-db"."crm_empleo_historico_anuncio__c"

UNION

SELECT 
	'CRMSL' base_origen,
	CAST(id AS VARCHAR) id,
	CAST(name AS VARCHAR) descripcion,
	CAST(DATE_PARSE(date_format(COALESCE(fecha_inicio, date_entered), '%Y-%m-%d %h:%i%p'), '%Y-%m-%d %h:%i%p') AS DATE) fecha_publicacion,
	CAST(situacion AS VARCHAR) estado,
	CAST(NULL AS VARCHAR) apto_discapacitado,
	CAST(puestos_vacantes AS VARCHAR) vacantes,
	CAST(modalidad AS VARCHAR) modalidad_de_trabajo,
	-- en algunos registros en el campo requisito dice la edad mimina y maxima
	-- hay que analizar la posibilidad de separarlo
	CAST(requisitos AS VARCHAR) edad_minima,
	CAST(requisitos AS VARCHAR) edad_maxima,

	CAST(cantidad_puestos_min - puestos_vacantes AS VARCHAR) vacantes_cubiertas,
	CAST(puesto AS VARCHAR) tipo_de_puesto,

	-- el campo horario contiene diverso texto que indica el horario de entrada y salida, 
	-- se deben analizar las posibilidades para determinar el turno de trabajo
	CAST(horarios AS VARCHAR) horario_entrada,
	CAST(horarios AS VARCHAR) horario_salida,

	-- en requisitos a veces se indica el grado de estudio
	CAST(requisitos AS VARCHAR) grado_de_estudio,
	-- en un futuro cuando este asociado a "Registro laboral formal" si la misma finalizo se podria obtener
	-- por la diferencia entre el inicio y fin de la relacion
	CAST(NULL AS VARCHAR) duracion_practica_formativa,
		
	-- tienen sentido los siguientes campos?
	-- analizar valores
	COALESCE(sector_economico, lugar) sector_productivo,
	COALESCE(sector, sector_economico) industria, 
	CAST(NULL AS VARCHAR) enviado_para_aprobacion__c,
	CAST(NULL AS VARCHAR) postulantes_contratados__c,

-- datos de organizacion/empresa que busca empleados
	CAST(empresa AS VARCHAR) organizacion_empleadora,
	CAST(lugar AS VARCHAR) organizacion_empleadora_calle,
	CAST(NULL AS VARCHAR) organizacion_empleadora_piso,
	CAST(NULL AS VARCHAR) organizacion_empleadora_depto,
	CAST(NULL AS VARCHAR) organizacion_empleadora_cp,
	CAST(NULL AS VARCHAR) organizacion_empleadora_barrio,
	CAST(NULL AS VARCHAR) organizacion_empleadora_cuit,
	CAST(NULL AS VARCHAR) AS genero_requerido,
	CAST(NULL AS INT) AS experiencia_requerida
FROM "caba-piba-raw-zone-db"."crm_sociolaboral_op_oportunidades_laborales"

UNION
SELECT 
'PORTALEMPLEO' base_origen,
CAST(jo.id AS VARCHAR) id,
CAST(jo.tasks_description AS VARCHAR) descripcion,
CAST(DATE_PARSE(date_format(COALESCE(jp.created_at, jp.published_date), '%Y-%m-%d %h:%i%p'), '%Y-%m-%d %h:%i%p') AS DATE) fecha_publicacion,
CASE WHEN jo.due_date <= NOW() THEN 'finalizada' ELSE 'en_curso' END estado,
CASE WHEN for_disabled = 0 THEN '0' ELSE '1' END apto_discapacitado,
CAST(jo.vacancy AS VARCHAR) vacantes,
CAST(m.name AS VARCHAR) modalidad_de_trabajo,

CAST(jr.age_min AS VARCHAR) edad_minima,
CAST(jr.age_max AS VARCHAR) edad_maxima,

CAST(NULL AS VARCHAR) vacantes_cubiertas,
CAST(jo.position AS VARCHAR) tipo_de_puesto,

-- mirar tambien el campo tasks_description contiene diverso texto que indica el horario de entrada y salida, 
-- se deben analizar las posibilidades para determinar el turno de trabajo
CAST(jo.checkin_time AS VARCHAR) horario_entrada,
CAST(jo.checkout_time AS VARCHAR) horario_salida,


CAST(el.value AS VARCHAR) grado_de_estudio,
-- en un futuro cuando este asociado a "Registro laboral formal" si la misma finalizo se podria obtener
-- por la diferencia entre el inicio y fin de la relacion
CAST(NULL AS VARCHAR) duracion_practica_formativa,
	
-- tienen sentido los siguientes campos?
-- analizar valores
CAST(s.name AS VARCHAR) sector_productivo,
CAST(NULL AS VARCHAR) industria, 
CAST(NULL AS VARCHAR) enviado_para_aprobacion__c,
CAST(NULL AS VARCHAR) postulantes_contratados__c,

-- datos de organizacion/empresa que busca empleados
CAST(business_name AS VARCHAR) organizacion_empleadora,
CAST(adr.street_address AS VARCHAR) organizacion_empleadora_calle,
CAST(adr.floor AS VARCHAR) organizacion_empleadora_piso,
CAST(adr.apt AS VARCHAR) organizacion_empleadora_depto,
CAST(adr.zipcode AS VARCHAR) organizacion_empleadora_cp,
CAST(adr.neighborhood AS VARCHAR) organizacion_empleadora_barrio,
CAST(org.lei_code AS VARCHAR) organizacion_empleadora_cuit	,

-- cuando gender es vacio se asume como indistinto
CASE
	WHEN UPPER(jr.gender) IN ('F', 'M') THEN UPPER(jr.gender) ELSE 'I'
END AS genero_requerido,
-- 1 si requiere experiencia, 0 en caso contrario
CAST(jr.experience_required AS  INT) as experiencia_requerida

FROM "caba-piba-raw-zone-db"."portal_empleo_job_offers" jo
LEFT JOIN "caba-piba-raw-zone-db"."portal_empleo_job_postings" jp ON (jo.id=jp.job_offer_id)
LEFT JOIN "caba-piba-raw-zone-db"."portal_empleo_organizations" org ON (org.id=jp.organization_id)
LEFT JOIN "caba-piba-raw-zone-db"."portal_empleo_addresses" adr ON (adr.id=org.addressid)
LEFT JOIN "caba-piba-raw-zone-db"."portal_empleo_mtr_job_posting_statuses" ps 
ON (jp.job_posting_status_id = ps.id and ps.description in ('Pendiente','Cerrada','Aprobado') )
LEFT JOIN "caba-piba-raw-zone-db"."portal_empleo_job_requirements" jr ON (jp.job_requirement_id=jr.id)
LEFT JOIN "caba-piba-raw-zone-db"."portal_empleo_mtr_education_level" el ON (jr.academic_level_id=el.id)
LEFT JOIN "caba-piba-raw-zone-db"."portal_empleo_mtr_working_modalities" m ON (jo.working_modality=m.id)
LEFT JOIN "caba-piba-raw-zone-db"."portal_empleo_mtr_industry_sectors" s ON (jo.sector_id=s.id)
WHERE jp.deleted = 0
),
-- 2.-- Se estandarizan los campos mas relevantes.
-- 2.1.-- Se estandarizan los campos turno_trabajo, horario_entrada y horario_salida
et AS (
SELECT
base_origen,
id,
descripcion,
fecha_publicacion,
estado,
apto_discapacitado,
vacantes,
modalidad_de_trabajo,
edad_minima,
edad_maxima,
vacantes_cubiertas,
tipo_de_puesto,
horario_entrada,
CASE
    WHEN horario_entrada LIKE '%/%' AND horario_entrada NOT IN ('Disponibilidad para realizar turnos rotativos 6:00 a 14:00 / 14:00 a 22:00 / 22:00 a 06:00 hs.','7/13 - 12/17 - 17/22') THEN regexp_replace(split_part(horario_entrada, '/', 1),'[^0-9\:?\/]+','')
END he_split1,
CASE
    WHEN horario_entrada LIKE '%/%' AND horario_entrada NOT IN ('Disponibilidad para realizar turnos rotativos 6:00 a 14:00 / 14:00 a 22:00 / 22:00 a 06:00 hs.','7/13 - 12/17 - 17/22') THEN split_part(horario_entrada, '/', 2) 
END he_split2,
CASE
    WHEN horario_entrada LIKE '% a %' AND horario_entrada NOT LIKE '%/%' THEN regexp_replace(horario_entrada,'[^0-9\:?\s?a?o?y?]+','') 
END he_limpio,
horario_salida,
CASE 
    --MAÑANA
    WHEN horario_entrada IN ('6','6:00','6:05','6:10','6:15','6:20','6:25','6:30','6:35','6:40','6:45','6:50','6:55','06','06:00','06:15','06:20','06:25','06:30','06:35','06:40','06:45','06:50','06:55','7','7:00','7:05','7:10','7:15','7:20','7:25','7:30','7:35','7:40','7:45','7:50','7:55','07','07:00','07:05','07:10','07:15','07:20','07:25','07:30','07:35','07:40','07:45','07:50','07:55','8','8:00','8:05','8:10','8:15','8:20','8:25','8:30','8:35','8:40','8:45','8:50','8:55','08','08:00','08:05','08:10','08:15','08:20','08:25','08:30','08:35','08:40','08:45','08:50','08:55','9','9:00','9:05','9:10','9:15','9:20','9:25','9:30','9:35','9:40','9:45','9:50','9:55','09','09:00','09:05','09:10','09:15','09:20','09:25','09:30','09:35','09:40','09:45','09:50','09:55','10','10:00','10:05','10:10','10:15','10:20','10:25','10:30','10:35','10:40','10:45','10:50','10:55','11','11:00','11:05','11:10','11:15','11:20','11:25','11:30','11:35','11:40','11:45','11:50','11:55','11','11:00','11:05','11:10','11:15','11:20','11:25','11:30','11:35','11:40','11:45','11:50','11:55','12','12:00','12:05','12:10','12:15','12:20','12:25','12:30','12:35','12:40','12:45','12:50','12:55','13','13:00','13:05','13:10','13:15','13:20','13:25','13:30','13:35','13:40','13:45','13:50','13:55') AND horario_salida IN ('6','6:00','6:05','6:10','6:15','6:20','6:25','6:30','6:35','6:40','6:45','6:50','6:55','06','06:00','06:15','06:20','06:25','06:30','06:35','06:40','06:45','06:50','06:55','7','7:00','7:05','7:10','7:15','7:20','7:25','7:30','7:35','7:40','7:45','7:50','7:55','07','07:00','07:05','07:10','07:15','07:20','07:25','07:30','07:35','07:40','07:45','07:50','07:55','8','8:00','8:05','8:10','8:15','8:20','8:25','8:30','8:35','8:40','8:45','8:50','8:55','08','08:00','08:05','08:10','08:15','08:20','08:25','08:30','08:35','08:40','08:45','08:50','08:55','9','9:00','9:05','9:10','9:15','9:20','9:25','9:30','9:35','9:40','9:45','9:50','9:55','09','09:00','09:05','09:10','09:15','09:20','09:25','09:30','09:35','09:40','09:45','09:50','09:55','10','10:00','10:05','10:10','10:15','10:20','10:25','10:30','10:35','10:40','10:45','10:50','10:55','11','11:00','11:05','11:10','11:15','11:20','11:25','11:30','11:35','11:40','11:45','11:50','11:55','11','11:00','11:05','11:10','11:15','11:20','11:25','11:30','11:35','11:40','11:45','11:50','11:55','12','12:00','12:05','12:10','12:15','12:20','12:25','12:30','12:35','12:40','12:45','12:50','12:55','13','13:00','13:05','13:10','13:15','13:20','13:25','13:30','13:35','13:40','13:45','13:50','13:55') THEN 'MAÑANA'
    --MAÑANA-TARDE
    WHEN horario_entrada IN ('6','6:00','6:05','6:10','6:15','6:20','6:25','6:30','6:35','6:40','6:45','6:50','6:55','06','06:00','06:15','06:20','06:25','06:30','06:35','06:40','06:45','06:50','06:55','7','7:00','7:05','7:10','7:15','7:20','7:25','7:30','7:35','7:40','7:45','7:50','7:55','07','07:00','07:05','07:10','07:15','07:20','07:25','07:30','07:35','07:40','07:45','07:50','07:55','8','8:00','8:05','8:10','8:15','8:20','8:25','8:30','8:35','8:40','8:45','8:50','8:55','08','08:00','08:05','08:10','08:15','08:20','08:25','08:30','08:35','08:40','08:45','08:50','08:55','9','9:00','9:05','9:10','9:15','9:20','9:25','9:30','9:35','9:40','9:45','9:50','9:55','09','09:00','09:05','09:10','09:15','09:20','09:25','09:30','09:35','09:40','09:45','09:50','09:55','10','10:00','10:05','10:10','10:15','10:20','10:25','10:30','10:35','10:40','10:45','10:50','10:55','11','11:00','11:05','11:10','11:15','11:20','11:25','11:30','11:35','11:40','11:45','11:50','11:55','12','12:00','12:05','12:10','12:15','12:20','12:25','12:30','12:35','12:40','12:45','12:50','12:55','13','13:00','13:05','13:10','13:15','13:20','13:25','13:30','13:35','13:40','13:45','13:50','13:55') AND horario_salida IN ('14','14:00','14:05','14:10','14:15','14:20','14:25','14:30','14:35','14:40','14:45','14:50','14:55','15','15:00','15:05','15:10','15:15','15:20','15:25','15:30','15:35','15:40','15:45','15:50','15:55','16','16:00','16:05','16:10','16:15','16:20','16:25','16:30','16:35','16:40','16:45','16:50','16:55','17','17:00','17:05','17:10','17:15','17:20','17:25','17:30','17:35','17:40','17.45','17:45','17:50','17:55','18','18:00','18:05','18:10','18:15','18:20','18:25','18:30','18:35','18:40','18:45','18:50','18:55','19','19:00','19:05','19:10','19:15','19:20','19:25','19:30','19:35','19:40','19:45','19:50','19:55','20','20:00','20:05','20:10','20:15','20:20','20:25','20:30','20:35','20:40','20:45','20:50','20:55','21','21:00','21:05','21:10','21:15','21:20','21:25','21:30','21:35','21:40','21:45','21:50','21:55','22','22:00') THEN 'MAÑANA-TARDE'
    --TARDE
    WHEN horario_entrada IN ('14','14:00','14:05','14:10','14:15','14:20','14:25','14:30','14:35','14:40','14:45','14:50','14:55','15','15:00','15:05','15:10','15:15','15:20','15:25','15:30','15:35','15:40','15:45','15:50','15:55','16','16:00','16:05','16:10','16:15','16:20','16:25','16:30','16:35','16:40','16:45','16:50','16:55','17','17:00','17:05','17:10','17:15','17:20','17:25','17:30','17:35','17:40','17:45','17:50','17:55','18','18:00','18:05','18:10','18:15','18:20','18:25','18:30','18:35','18:40','18:45','18:50','18:55','19','19:00','19:05','19:10','19:15','19:20','19:25','19:30','19:35','19:40','19:45','19:50','19:55','20','20:00','20:05','20:10','20:15','20:20','20:25','20:30','20:35','20:40','20:45','20:50','20:55','21','21:00','21:05','21:10','21:15','21:20','21:25','21:30','21:35','21:40','21:45','21:50','21:55') AND horario_salida IN ('14','14:00','14:05','14:10','14:15','14:20','14:25','14:30','14:35','14:40','14:45','14:50','14:55','15','15:00','15:05','15:10','15:15','15:20','15:25','15:30','15:35','15:40','15:45','15:50','15:55','16','16:00','16:05','16:10','16:15','16:20','16:25','16:30','16:35','16:40','16:45','16:50','16:55','17','17:00','17:05','17:10','17:15','17:20','17:25','17:30','17:35','17:40','17:45','17:50','17:55','18','18:00','18:05','18:10','18:15','18:20','18:25','18:30','18:35','18:40','18:45','18:50','18:55','19','19:00','19:05','19:10','19:15','19:20','19:25','19:30','19:35','19:40','19:45','19:50','19:55','20','20:00','20:05','20:10','20:15','20:20','20:25','20:30','20:35','20:40','20:45','20:50','20:55','21','21:00','21:05','21:10','21:15','21:20','21:25','21:30','21:35','21:40','21:45','21:50','21:55','22','22:00') THEN 'TARDE'
    --TARDE-NOCHE
    WHEN horario_entrada IN ('14','14:00','14:05','14:10','14:15','14:20','14:25','14:30','14:35','14:40','14:45','14:50','14:55','15','15:00','15:05','15:10','15:15','15:20','15:25','15:30','15:35','15:40','15:45','15:50','15:55','16','16:00','16:05','16:10','16:15','16:20','16:25','16:30','16:35','16:40','16:45','16:50','16:55','17','17:00','17:05','17:10','17:15','17:20','17:25','17:30','17:35','17:40','17:45','17:50','17:55','18','18:00','18:05','18:10','18:15','18:20','18:25','18:30','18:35','18:40','18:45','18:50','18:55','19','19:00','19:05','19:10','19:15','19:20','19:25','19:30','19:35','19:40','19:45','19:50','19:55','20','20:00','20:05','20:10','20:15','20:20','20:25','20:30','20:35','20:40','20:45','20:50','20:55','21','21:00','21:05','21:10','21:15','21:20','21:25','21:30','21:35','21:40','21:45','21:50','21:55') AND horario_salida IN ('22:05','22:10','22:15','22:30','22:35','22:40','22:45','22:50','22:55','23','23:00','23:05','23:10','23:15','23:20','23:25','23:30','23:35','23:40','23:45','23:50','23:55','24','00:00','00:05','00:10','00:15','00:20','00:25','00:30','00:35','00:40','00:45','00:50','00:55','1','1:00','1:05','1:10','1:15','1:20','1:25','1:30','1:35','1:40','1:45','1:50','1:55','01','01:00','01:05','01:10','01:15','01:20','01:25','01:30','01:35','01:40','01:45','01:50','01:55','2','2:00','2:05','2:10','2:15','2:20','2:25','2:30','2:35','2:40','2:45','2:50','2:55','02','02:00','02:05','02:10','02:15','02:20','02:25','02:30','02:35','02:40','02:45','02:50','02:55','3','3:00','3:05','3:10','3:15','3:20','3:25','3:30','3:35','3:40','3:45','3:50','3:55','03','03:00','03:05','03:10','03:15','03:20','03:25','03:30','03:35','03:40','03:45','03:50','03:55','4','4:00','4:05','4:10','4:15','4:20','4:25','4:30','4:35','4:40','4:45','4:50','4:55','04','04:00','04:05','04:10','04:15','04:20','04:25','04:30','04:35','04:40','04:45','04:50','04:55','5','5:00','5:05','5:10','5:15','5:20','5:25','5:30','5:35','5:40','5:45','5:50','5:55','05','05:00','05:05','05:10','05:15','05:20','05:25','05:30','05:35','05:40','05:45','05:50','05:55','6','6:00') THEN 'TARDE-NOCHE'
    --NOCHE
    WHEN horario_entrada IN ('22','22:00','22:05','22:10','22:15','22:30','22:35','22:40','22:45','22:50','22:55','23','23:00','23:05','23:10','23:15','23:20','23:25','23:30','23:35','23:40','23:45','23:50','23:55','24','00:00','00:05','00:10','00:15','00:20','00:25','00:30','00:35','00:40','00:45','00:50','00:55','1','1:00','1:05','1:10','1:15','1:20','1:25','1:30','1:35','1:40','1:45','1:50','1:55','01','01:00','01:05','01:10','01:15','01:20','01:25','01:30','01:35','01:40','01:45','01:50','01:55','2','2:00','2:05','2:10','2:15','2:20','2:25','2:30','2:35','2:40','2:45','2:50','2:55','02','02:00','02:05','02:10','02:15','02:20','02:25','02:30','02:35','02:40','02:45','02:50','02:55','3','3:00','3:05','3:10','3:15','3:20','3:25','3:30','3:35','3:40','3:45','3:50','3:55','03','03:00','03:05','03:10','03:15','03:20','03:25','03:30','03:35','03:40','03:45','03:50','03:55','4','4:00','4:05','4:10','4:15','4:20','4:25','4:30','4:35','4:40','4:45','4:50','4:55','04','04:00','04:05','04:10','04:15','04:20','04:25','04:30','04:35','04:40','04:45','04:50','04:55','5','5:00','5:05','5:10','5:15','5:20','5:25','5:30','5:35','5:40','5:45','5:50','5:55','05','05:00','05:05','05:10','05:15','05:20','05:25','05:30','05:35','05:40','05:45','05:50','05:55') AND horario_salida  IN ('22','22:00','22:05','22:10','22:15','22:30','22:35','22:40','22:45','22:50','22:55','23','23:00','23:05','23:10','23:15','23:20','23:25','23:30','23:35','23:40','23:45','23:50','23:55','24','00:00','00:05','00:10','00:15','00:20','00:25','00:30','00:35','00:40','00:45','00:50','00:55','1','1:00','1:05','1:10','1:15','1:20','1:25','1:30','1:35','1:40','1:45','1:50','1:55','01','01:00','01:05','01:10','01:15','01:20','01:25','01:30','01:35','01:40','01:45','01:50','01:55','2','2:00','2:05','2:10','2:15','2:20','2:25','2:30','2:35','2:40','2:45','2:50','2:55','02','02:00','02:05','02:10','02:15','02:20','02:25','02:30','02:35','02:40','02:45','02:50','02:55','3','3:00','3:05','3:10','3:15','3:20','3:25','3:30','3:35','3:40','3:45','3:50','3:55','03','03:00','03:05','03:10','03:15','03:20','03:25','03:30','03:35','03:40','03:45','03:50','03:55','4','4:00','4:05','4:10','4:15','4:20','4:25','4:30','4:35','4:40','4:45','4:50','4:55','04','04:00','04:05','04:10','04:15','04:20','04:25','04:30','04:35','04:40','04:45','04:50','04:55','5','5:00','5:05','5:10','5:15','5:20','5:25','5:30','5:35','5:40','5:45','5:50','5:55','05','05:00','05:05','05:10','05:15','05:20','05:25','05:30','05:35','05:40','05:45','05:50','05:55','6','6:00') THEN 'NOCHE'
    ELSE 'SIN TURNO ESPECIFICO'
END turno_trabajo,
grado_de_estudio,
duracion_practica_formativa,
sector_productivo,
industria,
enviado_para_aprobacion__c,
postulantes_contratados__c,
organizacion_empleadora,
organizacion_empleadora_calle,
organizacion_empleadora_piso,
organizacion_empleadora_depto,
organizacion_empleadora_cp,
organizacion_empleadora_barrio,
organizacion_empleadora_cuit,
genero_requerido,
experiencia_requerida
FROM oportunidad_laboral
),
et2 AS (
SELECT
et.base_origen,
et.id,
et.descripcion,
et.fecha_publicacion,
et.estado,
et.apto_discapacitado,
et.vacantes,
et.modalidad_de_trabajo,
et.edad_minima,
et.edad_maxima,
et.vacantes_cubiertas,
et.tipo_de_puesto,
et.horario_entrada,
et.he_split1,
et.he_split2,
CASE
    WHEN et.horario_entrada LIKE '(lunes a viernes 8:30 a 13:30 y 14:30 a 18 hs - sábados 9 a 13 hs' THEN '8:30 A 13:30 Y 14:30 A 18'
    WHEN et.horario_entrada LIKE 'Lunes a Viernes de 9 a 18 o de 8 a 17 hs. ' THEN '9 A 18 O 8 A 17'
    WHEN et.horario_entrada LIKE '9 a 13 hs y de 16 a 20 hs.' THEN '9 A 13 Y 16 A 20'
    WHEN et.horario_entrada LIKE 'De lunes a viernes de 8 a 17 o de 9 a 18 hs' THEN '8 A 17 O 9 A 18'
    WHEN et.horario_entrada LIKE 'Lunes a viernes de 8 a 12 y 13 a 17' THEN '8 A 12 Y 13 A 17'
    WHEN et.horario_entrada LIKE 'De lunes a viernes de 7:30 A 13: 30 HS' THEN '7:30 A 13:30'
    WHEN et.horario_entrada LIKE 'lun a vie 10 a 14 o 14 a 18.' THEN '10 A 14 O 14 A 18'
    WHEN et.horario_entrada LIKE '4 meses- lun a vie 4 hs 10 a 14 hs' THEN '10 A 14'
    WHEN et.horario_entrada LIKE '3 meses- lun a vie 4 hs,  13 a 17 hs' THEN '13 A 17'
    WHEN regexp_like(UPPER(et.he_limpio),'[0-9\:?]+\s?a\s?[0-9\:?]+\s?y\s?[0-9\:?]+\s?a\s?[0-9\:?]+') THEN regexp_extract(UPPER(et.he_limpio),'[0-9\:?]+\s?a\s?[0-9\:?]+\s?y\s?[0-9\:?]+\s?a\s?[0-9\:?]+')
    WHEN regexp_like(UPPER(et.he_limpio),'[0-9\:?]+\s?A\s?[0-9\:?]+\s?O\s?[0-9\:?]+\s?A\s?[0-9\:?]+') THEN regexp_extract(UPPER(et.he_limpio),'[0-9\:?]\s?A\s?[0-9\:?]+\s?O\s?[0-9\:?]+\s?A\s?[0-9\:?]+')
    WHEN regexp_like(UPPER(et.he_limpio),'[0-9\:?]+\s?\s?\s?A\s?\s?\s?[0-9\:?]+') THEN regexp_extract(UPPER(et.he_limpio),'[0-9\:?]+\s?\s?\s?A\s?\s?\s?[0-9\:?]+')
    ELSE LTRIM(replace(replace(et.he_limpio,'a',''),'o',''))
END he_limpio,
et.horario_salida,
CASE
    --TURNOS ASIGNADO EN BASE A ASSUMPTIONS
    --MAÑANA-TARDE-NOCHE
    WHEN horario_entrada IN ('13:00','19:00','20:00','21:00','1','11') AND horario_salida IN ('22:00','07:00','08:00','07:00','24','23') THEN 'MAÑANA-TARDE-NOCHE'
    WHEN (horario_entrada LIKE '12:00' AND horario_salida LIKE '23:45') OR (horario_entrada LIKE '12:00' AND horario_salida LIKE '00:00') THEN 'MAÑANA-TARDE-NOCHE'
    WHEN regexp_like(UPPER(horario_entrada),'MAÑANA') AND regexp_like(UPPER(horario_entrada),'TARDE') AND regexp_like(UPPER(horario_entrada),'NOCHE')  THEN 'MAÑANA-TARDE-NOCHE'
    --MAÑANA-TARDE
    WHEN (horario_entrada LIKE '00:00' AND horario_salida LIKE '23:45') OR (horario_entrada LIKE '00:00' AND horario_salida LIKE '23:30') OR (horario_entrada LIKE '00:15' AND horario_salida LIKE '23:45') OR (horario_entrada LIKE '00:30' AND horario_salida LIKE '23:45')  OR (horario_entrada LIKE '00:45' AND horario_salida LIKE '23:45')  OR (horario_entrada LIKE '01:00' AND horario_salida LIKE '17:00')  OR (horario_entrada LIKE '01:00' AND horario_salida LIKE '21:00')  OR (horario_entrada LIKE '01:15' AND horario_salida LIKE '21:45')  OR (horario_entrada LIKE '05:45' AND horario_salida LIKE '18:15')  OR (horario_entrada LIKE '06:00' AND horario_salida LIKE '03:00')  OR (horario_entrada LIKE '07:00' AND horario_salida LIKE '05:00')  OR (horario_entrada LIKE '07:00' AND horario_salida LIKE '03:00')  OR (horario_entrada LIKE '07:00' AND horario_salida LIKE '04:00')  OR (horario_entrada LIKE '07:00' AND horario_salida LIKE '06:00')  OR (horario_entrada LIKE '07:45' AND horario_salida LIKE '04:45')  OR (horario_entrada LIKE '08:00' AND horario_salida LIKE '04:30')  OR (horario_entrada LIKE '08:00' AND horario_salida LIKE '05:00')  OR (horario_entrada LIKE '08:15' AND horario_salida LIKE '05:45')  OR (horario_entrada LIKE '12:00' AND horario_salida LIKE '08:00')  OR (horario_entrada LIKE '08:30' AND horario_salida LIKE '05:30')  OR (horario_entrada LIKE '09:00' AND horario_salida LIKE '06:15')  OR (horario_entrada LIKE '09:00' AND horario_salida LIKE '06:00')  OR (horario_entrada LIKE '09:00' AND horario_salida LIKE '05:00')  OR (horario_entrada LIKE '09:00' AND horario_salida LIKE '04:00')  OR (horario_entrada LIKE '10:00' AND horario_salida LIKE '06:00') THEN 'MAÑANA-TARDE'
    WHEN regexp_like(UPPER(horario_entrada),'MAÑANA') AND regexp_like(UPPER(horario_entrada),'TARDE') THEN 'MAÑANA-TARDE'
    --MAÑANA
    WHEN (horario_entrada LIKE '04:00' AND horario_salida LIKE '13:00')  OR (horario_entrada LIKE '04:00' AND horario_salida LIKE '12:00')  OR (horario_entrada LIKE '05:30' AND horario_salida LIKE '12:30')  OR (horario_entrada LIKE '06:00' AND horario_salida LIKE '08:00')  OR (horario_entrada LIKE '07:00' AND horario_salida LIKE '07:00')  OR (horario_entrada LIKE '07:00' AND horario_salida LIKE '11:00')  OR (horario_entrada LIKE '07:30' AND horario_salida LIKE '11:30')  OR (horario_entrada LIKE '08:30' AND horario_salida LIKE '02:30')  OR (horario_entrada LIKE '09:00' AND horario_salida LIKE '09:00')  OR (horario_entrada LIKE '09:00' AND horario_salida LIKE '01:00')  OR (horario_entrada LIKE '09:15' AND horario_salida LIKE '09:45')  OR (horario_entrada LIKE '10:00' AND horario_salida LIKE '02:00')  OR (horario_entrada LIKE '11:00' AND horario_salida LIKE '01:30')  OR (horario_entrada LIKE '12:00' AND horario_salida LIKE '12:00') OR (horario_entrada LIKE '07:00' AND horario_salida LIKE '02:00')  OR (horario_entrada LIKE '06:15' AND horario_salida LIKE '01:00')  OR (horario_entrada LIKE '06:00' AND horario_salida LIKE '06:00')  OR (horario_entrada LIKE '4' AND horario_salida LIKE '12')  OR (horario_entrada LIKE '6' AND horario_salida LIKE '9') THEN 'MAÑANA'
    WHEN regexp_like(UPPER(horario_entrada),'MAÑANA') THEN 'MAÑANA'
    --TARDE-NOCHE
    WHEN (horario_entrada LIKE '07:00' AND horario_salida LIKE '22:30') OR (horario_entrada LIKE '08:00' AND horario_salida LIKE '03:00') OR (horario_entrada LIKE '08:00' AND horario_salida LIKE '00:00') OR (horario_entrada LIKE '09:00' AND horario_salida LIKE '00:00') OR (horario_entrada LIKE '09:30' AND horario_salida LIKE '00:00') OR (horario_entrada LIKE '16:00' AND horario_salida LIKE '11:00') OR (horario_entrada LIKE '21:00' AND horario_salida LIKE '06:00') THEN 'TARDE-NOCHE'
    WHEN regexp_like(UPPER(horario_entrada),'TARDE') AND regexp_like(UPPER(horario_entrada),'NOCHE') THEN 'TARDE-NOCHE'
    --TARDE
    WHEN (horario_entrada LIKE '03:00' AND horario_salida LIKE '21:45') OR (horario_entrada LIKE '03:30' AND horario_salida LIKE '23:00') OR (horario_entrada LIKE '06:00' AND horario_salida LIKE '22:00') OR (horario_entrada LIKE '08:00' AND horario_salida LIKE '22:00') OR (horario_entrada LIKE '14:00' AND horario_salida LIKE '06:00') OR (horario_entrada LIKE '15' AND horario_salida LIKE '19') OR (horario_entrada LIKE '15:00' AND horario_salida LIKE '19:00') OR (horario_entrada LIKE '15:00' AND horario_salida LIKE '17:00')   OR (horario_entrada LIKE '19:15' AND horario_salida LIKE '17:15') OR (horario_entrada LIKE '20:00' AND horario_salida LIKE '18:00') OR (horario_entrada LIKE '18' AND horario_salida LIKE '20.3') OR (horario_entrada LIKE '6' AND horario_salida LIKE '22') OR (horario_entrada LIKE '21:00' AND horario_salida LIKE '21:00') THEN 'TARDE'
    WHEN regexp_like(UPPER(horario_entrada),'TARDE') THEN 'TARDE'
    --NOCHE
    WHEN (horario_entrada LIKE '10:00' AND horario_salida LIKE '23:00') OR (horario_entrada LIKE '00:00' AND horario_salida LIKE '07:00') OR (horario_entrada LIKE '00:00' AND horario_salida LIKE '06:00') OR (horario_entrada LIKE '00:00' AND horario_salida LIKE '08:00') OR (horario_entrada LIKE '00:15' AND horario_salida LIKE '08:00') OR (horario_entrada LIKE '01:00' AND horario_salida LIKE '08:00') OR (horario_entrada LIKE '02:15' AND horario_salida LIKE '08:00') OR (horario_entrada LIKE '1' AND horario_salida LIKE '9') OR (horario_entrada LIKE '1' AND horario_salida LIKE '8') OR (horario_entrada LIKE '22:00' AND horario_salida LIKE '06:00') OR (horario_entrada LIKE '23:00' AND horario_salida LIKE '07:00') OR (horario_entrada LIKE '23:45' AND horario_salida LIKE '08:00') OR (horario_entrada LIKE '4' AND horario_salida LIKE '10') OR (horario_entrada LIKE '10:00' AND horario_salida LIKE '00:00') THEN 'NOCHE'
    WHEN regexp_like(UPPER(horario_entrada),'NOCHE') THEN 'NOCHE'
    --TURNOS EXTRAIDOS DE LAS OPERACIONES DE SPLIT DEL CAMPO "horario_entrada"
    --MAÑANA
    WHEN et.he_split1 IN ('6','6:00','6:05','6:10','6:15','6:20','6:25','6:30','6:35','6:40','6:45','6:50','6:55','06','06:00','06:15','06:20','06:25','06:30','06:35','06:40','06:45','06:50','06:55','7','7:00','7:05','7:10','7:15','7:20','7:25','7:30','7:35','7:40','7:45','7:50','7:55','07','07:00','07:05','07:10','07:15','07:20','07:25','07:30','07:35','07:40','07:45','07:50','07:55','8','8:00','8:05','8:10','8:15','8:20','8:25','8:30','8:35','8:40','8:45','8:50','8:55','08','08:00','08:05','08:10','08:15','08:20','08:25','08:30','08:35','08:40','08:45','08:50','08:55','9','9:00','9:05','9:10','9:15','9:20','9:25','9:30','9:35','9:40','9:45','9:50','9:55','09','09:00','09:05','09:10','09:15','09:20','09:25','09:30','09:35','09:40','09:45','09:50','09:55','10','10:00','10:05','10:10','10:15','10:20','10:25','10:30','10:35','10:40','10:45','10:50','10:55','11','11:00','11:05','11:10','11:15','11:20','11:25','11:30','11:35','11:40','11:45','11:50','11:55','11','11:00','11:05','11:10','11:15','11:20','11:25','11:30','11:35','11:40','11:45','11:50','11:55','12','12:00','12:05','12:10','12:15','12:20','12:25','12:30','12:35','12:40','12:45','12:50','12:55','13','13:00','13:05','13:10','13:15','13:20','13:25','13:30','13:35','13:40','13:45','13:50','13:55') AND et.he_split2 IN ('6','6:00','6:05','6:10','6:15','6:20','6:25','6:30','6:35','6:40','6:45','6:50','6:55','06','06:00','06:15','06:20','06:25','06:30','06:35','06:40','06:45','06:50','06:55','7','7:00','7:05','7:10','7:15','7:20','7:25','7:30','7:35','7:40','7:45','7:50','7:55','07','07:00','07:05','07:10','07:15','07:20','07:25','07:30','07:35','07:40','07:45','07:50','07:55','8','8:00','8:05','8:10','8:15','8:20','8:25','8:30','8:35','8:40','8:45','8:50','8:55','08','08:00','08:05','08:10','08:15','08:20','08:25','08:30','08:35','08:40','08:45','08:50','08:55','9','9:00','9:05','9:10','9:15','9:20','9:25','9:30','9:35','9:40','9:45','9:50','9:55','09','09:00','09:05','09:10','09:15','09:20','09:25','09:30','09:35','09:40','09:45','09:50','09:55','10','10:00','10:05','10:10','10:15','10:20','10:25','10:30','10:35','10:40','10:45','10:50','10:55','11','11:00','11:05','11:10','11:15','11:20','11:25','11:30','11:35','11:40','11:45','11:50','11:55','11','11:00','11:05','11:10','11:15','11:20','11:25','11:30','11:35','11:40','11:45','11:50','11:55','12','12:00','12:05','12:10','12:15','12:20','12:25','12:30','12:35','12:40','12:45','12:50','12:55','13','13:00','13:05','13:10','13:15','13:20','13:25','13:30','13:35','13:40','13:45','13:50','13:55') THEN 'MAÑANA'
    --MAÑANA-TARDE
    WHEN et.he_split1 IN ('6','6:00','6:05','6:10','6:15','6:20','6:25','6:30','6:35','6:40','6:45','6:50','6:55','06','06:00','06:15','06:20','06:25','06:30','06:35','06:40','06:45','06:50','06:55','7','7:00','7:05','7:10','7:15','7:20','7:25','7:30','7:35','7:40','7:45','7:50','7:55','07','07:00','07:05','07:10','07:15','07:20','07:25','07:30','07:35','07:40','07:45','07:50','07:55','8','8:00','8:05','8:10','8:15','8:20','8:25','8:30','8:35','8:40','8:45','8:50','8:55','08','08:00','08:05','08:10','08:15','08:20','08:25','08:30','08:35','08:40','08:45','08:50','08:55','9','9:00','9:05','9:10','9:15','9:20','9:25','9:30','9:35','9:40','9:45','9:50','9:55','09','09:00','09:05','09:10','09:15','09:20','09:25','09:30','09:35','09:40','09:45','09:50','09:55','10','10:00','10:05','10:10','10:15','10:20','10:25','10:30','10:35','10:40','10:45','10:50','10:55','11','11:00','11:05','11:10','11:15','11:20','11:25','11:30','11:35','11:40','11:45','11:50','11:55','12','12:00','12:05','12:10','12:15','12:20','12:25','12:30','12:35','12:40','12:45','12:50','12:55','13','13:00','13:05','13:10','13:15','13:20','13:25','13:30','13:35','13:40','13:45','13:50','13:55') AND et.he_split2 IN ('14','14:00','14:05','14:10','14:15','14:20','14:25','14:30','14:35','14:40','14:45','14:50','14:55','15','15:00','15:05','15:10','15:15','15:20','15:25','15:30','15:35','15:40','15:45','15:50','15:55','16','16:00','16:05','16:10','16:15','16:20','16:25','16:30','16:35','16:40','16:45','16:50','16:55','17','17:00','17:05','17:10','17:15','17:20','17:25','17:30','17:35','17:40','17.45','17:45','17:50','17:55','18','18:00','18:05','18:10','18:15','18:20','18:25','18:30','18:35','18:40','18:45','18:50','18:55','19','19:00','19:05','19:10','19:15','19:20','19:25','19:30','19:35','19:40','19:45','19:50','19:55','20','20:00','20:05','20:10','20:15','20:20','20:25','20:30','20:35','20:40','20:45','20:50','20:55','21','21:00','21:05','21:10','21:15','21:20','21:25','21:30','21:35','21:40','21:45','21:50','21:55','22','22:00') THEN 'MAÑANA-TARDE'
    --TARDE
    WHEN et.he_split1 IN ('14','14:00','14:05','14:10','14:15','14:20','14:25','14:30','14:35','14:40','14:45','14:50','14:55','15','15:00','15:05','15:10','15:15','15:20','15:25','15:30','15:35','15:40','15:45','15:50','15:55','16','16:00','16:05','16:10','16:15','16:20','16:25','16:30','16:35','16:40','16:45','16:50','16:55','17','17:00','17:05','17:10','17:15','17:20','17:25','17:30','17:35','17:40','17:45','17:50','17:55','18','18:00','18:05','18:10','18:15','18:20','18:25','18:30','18:35','18:40','18:45','18:50','18:55','19','19:00','19:05','19:10','19:15','19:20','19:25','19:30','19:35','19:40','19:45','19:50','19:55','20','20:00','20:05','20:10','20:15','20:20','20:25','20:30','20:35','20:40','20:45','20:50','20:55','21','21:00','21:05','21:10','21:15','21:20','21:25','21:30','21:35','21:40','21:45','21:50','21:55') AND et.he_split2 IN ('14','14:00','14:05','14:10','14:15','14:20','14:25','14:30','14:35','14:40','14:45','14:50','14:55','15','15:00','15:05','15:10','15:15','15:20','15:25','15:30','15:35','15:40','15:45','15:50','15:55','16','16:00','16:05','16:10','16:15','16:20','16:25','16:30','16:35','16:40','16:45','16:50','16:55','17','17:00','17:05','17:10','17:15','17:20','17:25','17:30','17:35','17:40','17:45','17:50','17:55','18','18:00','18:05','18:10','18:15','18:20','18:25','18:30','18:35','18:40','18:45','18:50','18:55','19','19:00','19:05','19:10','19:15','19:20','19:25','19:30','19:35','19:40','19:45','19:50','19:55','20','20:00','20:05','20:10','20:15','20:20','20:25','20:30','20:35','20:40','20:45','20:50','20:55','21','21:00','21:05','21:10','21:15','21:20','21:25','21:30','21:35','21:40','21:45','21:50','21:55','22','22:00') THEN 'TARDE'
    --TARDE-NOCHE
    WHEN et.he_split1 IN ('14','14:00','14:05','14:10','14:15','14:20','14:25','14:30','14:35','14:40','14:45','14:50','14:55','15','15:00','15:05','15:10','15:15','15:20','15:25','15:30','15:35','15:40','15:45','15:50','15:55','16','16:00','16:05','16:10','16:15','16:20','16:25','16:30','16:35','16:40','16:45','16:50','16:55','17','17:00','17:05','17:10','17:15','17:20','17:25','17:30','17:35','17:40','17:45','17:50','17:55','18','18:00','18:05','18:10','18:15','18:20','18:25','18:30','18:35','18:40','18:45','18:50','18:55','19','19:00','19:05','19:10','19:15','19:20','19:25','19:30','19:35','19:40','19:45','19:50','19:55','20','20:00','20:05','20:10','20:15','20:20','20:25','20:30','20:35','20:40','20:45','20:50','20:55','21','21:00','21:05','21:10','21:15','21:20','21:25','21:30','21:35','21:40','21:45','21:50','21:55') AND et.he_split2 IN ('22:05','22:10','22:15','22:30','22:35','22:40','22:45','22:50','22:55','23','23:00','23:05','23:10','23:15','23:20','23:25','23:30','23:35','23:40','23:45','23:50','23:55','24','00:00','00:05','00:10','00:15','00:20','00:25','00:30','00:35','00:40','00:45','00:50','00:55','1','1:00','1:05','1:10','1:15','1:20','1:25','1:30','1:35','1:40','1:45','1:50','1:55','01','01:00','01:05','01:10','01:15','01:20','01:25','01:30','01:35','01:40','01:45','01:50','01:55','2','2:00','2:05','2:10','2:15','2:20','2:25','2:30','2:35','2:40','2:45','2:50','2:55','02','02:00','02:05','02:10','02:15','02:20','02:25','02:30','02:35','02:40','02:45','02:50','02:55','3','3:00','3:05','3:10','3:15','3:20','3:25','3:30','3:35','3:40','3:45','3:50','3:55','03','03:00','03:05','03:10','03:15','03:20','03:25','03:30','03:35','03:40','03:45','03:50','03:55','4','4:00','4:05','4:10','4:15','4:20','4:25','4:30','4:35','4:40','4:45','4:50','4:55','04','04:00','04:05','04:10','04:15','04:20','04:25','04:30','04:35','04:40','04:45','04:50','04:55','5','5:00','5:05','5:10','5:15','5:20','5:25','5:30','5:35','5:40','5:45','5:50','5:55','05','05:00','05:05','05:10','05:15','05:20','05:25','05:30','05:35','05:40','05:45','05:50','05:55','6','6:00') THEN 'TARDE-NOCHE'
    --NOCHE
    WHEN et.he_split1 IN ('22','22:00','22:05','22:10','22:15','22:30','22:35','22:40','22:45','22:50','22:55','23','23:00','23:05','23:10','23:15','23:20','23:25','23:30','23:35','23:40','23:45','23:50','23:55','24','00:00','00:05','00:10','00:15','00:20','00:25','00:30','00:35','00:40','00:45','00:50','00:55','1','1:00','1:05','1:10','1:15','1:20','1:25','1:30','1:35','1:40','1:45','1:50','1:55','01','01:00','01:05','01:10','01:15','01:20','01:25','01:30','01:35','01:40','01:45','01:50','01:55','2','2:00','2:05','2:10','2:15','2:20','2:25','2:30','2:35','2:40','2:45','2:50','2:55','02','02:00','02:05','02:10','02:15','02:20','02:25','02:30','02:35','02:40','02:45','02:50','02:55','3','3:00','3:05','3:10','3:15','3:20','3:25','3:30','3:35','3:40','3:45','3:50','3:55','03','03:00','03:05','03:10','03:15','03:20','03:25','03:30','03:35','03:40','03:45','03:50','03:55','4','4:00','4:05','4:10','4:15','4:20','4:25','4:30','4:35','4:40','4:45','4:50','4:55','04','04:00','04:05','04:10','04:15','04:20','04:25','04:30','04:35','04:40','04:45','04:50','04:55','5','5:00','5:05','5:10','5:15','5:20','5:25','5:30','5:35','5:40','5:45','5:50','5:55','05','05:00','05:05','05:10','05:15','05:20','05:25','05:30','05:35','05:40','05:45','05:50','05:55') AND et.he_split2 IN ('22','22:00','22:05','22:10','22:15','22:30','22:35','22:40','22:45','22:50','22:55','23','23:00','23:05','23:10','23:15','23:20','23:25','23:30','23:35','23:40','23:45','23:50','23:55','24','00:00','00:05','00:10','00:15','00:20','00:25','00:30','00:35','00:40','00:45','00:50','00:55','1','1:00','1:05','1:10','1:15','1:20','1:25','1:30','1:35','1:40','1:45','1:50','1:55','01','01:00','01:05','01:10','01:15','01:20','01:25','01:30','01:35','01:40','01:45','01:50','01:55','2','2:00','2:05','2:10','2:15','2:20','2:25','2:30','2:35','2:40','2:45','2:50','2:55','02','02:00','02:05','02:10','02:15','02:20','02:25','02:30','02:35','02:40','02:45','02:50','02:55','3','3:00','3:05','3:10','3:15','3:20','3:25','3:30','3:35','3:40','3:45','3:50','3:55','03','03:00','03:05','03:10','03:15','03:20','03:25','03:30','03:35','03:40','03:45','03:50','03:55','4','4:00','4:05','4:10','4:15','4:20','4:25','4:30','4:35','4:40','4:45','4:50','4:55','04','04:00','04:05','04:10','04:15','04:20','04:25','04:30','04:35','04:40','04:45','04:50','04:55','5','5:00','5:05','5:10','5:15','5:20','5:25','5:30','5:35','5:40','5:45','5:50','5:55','05','05:00','05:05','05:10','05:15','05:20','05:25','05:30','05:35','05:40','05:45','05:50','05:55','6','6:00') THEN 'NOCHE'
    ELSE et.turno_trabajo
END turno_trabajo,
et.grado_de_estudio,
et.duracion_practica_formativa,
et.sector_productivo,
et.industria,
et.enviado_para_aprobacion__c,
et.postulantes_contratados__c,
et.organizacion_empleadora,
et.organizacion_empleadora_calle,
et.organizacion_empleadora_piso,
et.organizacion_empleadora_depto,
et.organizacion_empleadora_cp,
et.organizacion_empleadora_barrio,
et.organizacion_empleadora_cuit,
et.genero_requerido,
et.experiencia_requerida
FROM et
),
et3 AS (
SELECT 
et2.base_origen,
et2.id,
et2.descripcion,
et2.fecha_publicacion,
et2.estado,
et2.apto_discapacitado,
et2.vacantes,
et2.modalidad_de_trabajo,
et2.edad_minima,
et2.edad_maxima,
et2.vacantes_cubiertas,
et2.tipo_de_puesto,
et2.horario_entrada,
CASE
    WHEN et2.he_limpio LIKE '%A%' AND et2.he_limpio LIKE '%Y%' AND regexp_like(et2.he_limpio,'[0-9\:?\s?A]+\s?Y\s?[0-9\:?\s?A]+') THEN regexp_extract(et2.he_limpio,'([0-9\:?\s?A]+)(\s?Y\s?)([0-9\:?\s?A]+)',1)
    WHEN et2.he_limpio LIKE '%A%' AND et2.he_limpio LIKE '%O%' AND regexp_like(et2.he_limpio,'[0-9\:?\s?A]+\s?O\s?[0-9\:?\s?A]+') THEN regexp_extract(et2.he_limpio,'([0-9\:?\s?A]+)(\s?O\s?)([0-9\:?\s?A]+)',1)
    WHEN et2.he_limpio LIKE '%A%' AND regexp_like(et2.he_limpio,'[0-9\:?\s?A]+\s?A\s?[0-9\:?\s?A]+') THEN regexp_extract(et2.he_limpio,'([0-9\:?\s?A]+)(\s?A\s?)([0-9\:?\s?A]+)',1) 
    ELSE et2.he_limpio
END he_limpio,
CASE
    WHEN et2.he_limpio LIKE '%A%' AND et2.he_limpio LIKE '%Y%' AND regexp_like(et2.he_limpio,'[0-9\:?\s?A]+\s?Y\s?[0-9\:?\s?A]+') THEN regexp_extract(et2.he_limpio,'([0-9\:?\s?A]+)(\s?Y\s?)([0-9\:?\s?A]+)',3)
    WHEN et2.he_limpio LIKE '%A%' AND et2.he_limpio LIKE '%O%' AND regexp_like(et2.he_limpio,'[0-9\:?\s?A]+\s?O\s?[0-9\:?\s?A]+') THEN regexp_extract(et2.he_limpio,'([0-9\:?\s?A]+)(\s?O\s?)([0-9\:?\s?A]+)',3)
    WHEN et2.he_limpio LIKE '%A%' AND regexp_like(et2.he_limpio,'[0-9\:?\s?A]+\s?A\s?[0-9\:?\s?A]+') THEN regexp_extract(et2.he_limpio,'([0-9\:?\s?A]+)(\s?A\s?)([0-9\:?\s?A]+)',3) 
    ELSE et2.he_limpio
END hs_limpio,
et2.horario_salida,
et2.he_split1,
et2.he_split2,
et2.turno_trabajo,
et2.grado_de_estudio,
et2.duracion_practica_formativa,
et2.sector_productivo,
et2.industria,
et2.enviado_para_aprobacion__c,
et2.postulantes_contratados__c,
et2.organizacion_empleadora,
et2.organizacion_empleadora_calle,
et2.organizacion_empleadora_piso,
et2.organizacion_empleadora_depto,
et2.organizacion_empleadora_cp,
et2.organizacion_empleadora_barrio,
et2.organizacion_empleadora_cuit,
et2.genero_requerido,
et2.experiencia_requerida
FROM et2
),
et4 AS (
SELECT 
et3.base_origen,
et3.id,
et3.descripcion,
et3.fecha_publicacion,
et3.estado,
et3.apto_discapacitado,
et3.vacantes,
et3.modalidad_de_trabajo,
et3.edad_minima,
et3.edad_maxima,
et3.vacantes_cubiertas,
et3.tipo_de_puesto,
et3.horario_entrada AS horario_entrada_origen,
et3.he_limpio,
et3.hs_limpio,
CASE
    WHEN et3.he_limpio LIKE '4' OR et3.hs_limpio LIKE '6' THEN 'NOCHE'
    --MAÑANA
    WHEN et3.he_limpio IN ('6','6:00','6:05','6:10','6:15','6:20','6:25','6:30','6:35','6:40','6:45','6:50','6:55','06','06:00','06:15','06:20','06:25','06:30','06:35','06:40','06:45','06:50','06:55','7','7:00','7:05','7:10','7:15','7:20','7:25','7:30','7:35','7:40','7:45','7:50','7:55','07','07:00','07:05','07:10','07:15','07:20','07:25','07:30','07:35','07:40','07:45','07:50','07:55','8','8:00','8:05','8:10','8:15','8:20','8:25','8:30','8:35','8:40','8:45','8:50','8:55','08','08:00','08:05','08:10','08:15','08:20','08:25','08:30','08:35','08:40','08:45','08:50','08:55','9','9:00','9:05','9:10','9:15','9:20','9:25','9:30','9:35','9:40','9:45','9:50','9:55','09','09:00','09:05','09:10','09:15','09:20','09:25','09:30','09:35','09:40','09:45','09:50','09:55','10','10:00','10:05','10:10','10:15','10:20','10:25','10:30','10:35','10:40','10:45','10:50','10:55','11','11:00','11:05','11:10','11:15','11:20','11:25','11:30','11:35','11:40','11:45','11:50','11:55','11','11:00','11:05','11:10','11:15','11:20','11:25','11:30','11:35','11:40','11:45','11:50','11:55','12','12:00','12:05','12:10','12:15','12:20','12:25','12:30','12:35','12:40','12:45','12:50','12:55','13','13:00','13:05','13:10','13:15','13:20','13:25','13:30','13:35','13:40','13:45','13:50','13:55') OR et3.hs_limpio IN ('6','6:00','6:05','6:10','6:15','6:20','6:25','6:30','6:35','6:40','6:45','6:50','6:55','06','06:00','06:15','06:20','06:25','06:30','06:35','06:40','06:45','06:50','06:55','7','7:00','7:05','7:10','7:15','7:20','7:25','7:30','7:35','7:40','7:45','7:50','7:55','07','07:00','07:05','07:10','07:15','07:20','07:25','07:30','07:35','07:40','07:45','07:50','07:55','8','8:00','8:05','8:10','8:15','8:20','8:25','8:30','8:35','8:40','8:45','8:50','8:55','08','08:00','08:05','08:10','08:15','08:20','08:25','08:30','08:35','08:40','08:45','08:50','08:55','9','9:00','9:05','9:10','9:15','9:20','9:25','9:30','9:35','9:40','9:45','9:50','9:55','09','09:00','09:05','09:10','09:15','09:20','09:25','09:30','09:35','09:40','09:45','09:50','09:55','10','10:00','10:05','10:10','10:15','10:20','10:25','10:30','10:35','10:40','10:45','10:50','10:55','11','11:00','11:05','11:10','11:15','11:20','11:25','11:30','11:35','11:40','11:45','11:50','11:55','11','11:00','11:05','11:10','11:15','11:20','11:25','11:30','11:35','11:40','11:45','11:50','11:55','12','12:00','12:05','12:10','12:15','12:20','12:25','12:30','12:35','12:40','12:45','12:50','12:55','13','13:00','13:05','13:10','13:15','13:20','13:25','13:30','13:35','13:40','13:45','13:50','13:55') THEN 'MAÑANA'    
    --MAÑANA-TARDE
    WHEN et3.he_limpio LIKE '%A%'AND regexp_extract(et3.he_limpio,'([0-9\:?]+)(\s?A\s?)([0-9\:?]+)',1) IN ('6','6:00','6:05','6:10','6:15','6:20','6:25','6:30','6:35','6:40','6:45','6:50','6:55','06','06:00','06:15','06:20','06:25','06:30','06:35','06:40','06:45','06:50','06:55','7','7:00','7:05','7:10','7:15','7:20','7:25','7:30','7:35','7:40','7:45','7:50','7:55','07','07:00','07:05','07:10','07:15','07:20','07:25','07:30','07:35','07:40','07:45','07:50','07:55','8','8:00','8:05','8:10','8:15','8:20','8:25','8:30','8:35','8:40','8:45','8:50','8:55','08','08:00','08:05','08:10','08:15','08:20','08:25','08:30','08:35','08:40','08:45','08:50','08:55','9','9:00','9:05','9:10','9:15','9:20','9:25','9:30','9:35','9:40','9:45','9:50','9:55','09','09:00','09:05','09:10','09:15','09:20','09:25','09:30','09:35','09:40','09:45','09:50','09:55','10','10:00','10:05','10:10','10:15','10:20','10:25','10:30','10:35','10:40','10:45','10:50','10:55','11','11:00','11:05','11:10','11:15','11:20','11:25','11:30','11:35','11:40','11:45','11:50','11:55','11','11:00','11:05','11:10','11:15','11:20','11:25','11:30','11:35','11:40','11:45','11:50','11:55','12','12:00','12:05','12:10','12:15','12:20','12:25','12:30','12:35','12:40','12:45','12:50','12:55','13','13:00','13:05','13:10','13:15','13:20','13:25','13:30','13:35','13:40','13:45','13:50','13:55') AND regexp_extract(et3.he_limpio,'([0-9\:?]+)(\s?A\s?)([0-9\:?]+)',1) IN ('6','6:00','6:05','6:10','6:15','6:20','6:25','6:30','6:35','6:40','6:45','6:50','6:55','06','06:00','06:15','06:20','06:25','06:30','06:35','06:40','06:45','06:50','06:55','7','7:00','7:05','7:10','7:15','7:20','7:25','7:30','7:35','7:40','7:45','7:50','7:55','07','07:00','07:05','07:10','07:15','07:20','07:25','07:30','07:35','07:40','07:45','07:50','07:55','8','8:00','8:05','8:10','8:15','8:20','8:25','8:30','8:35','8:40','8:45','8:50','8:55','08','08:00','08:05','08:10','08:15','08:20','08:25','08:30','08:35','08:40','08:45','08:50','08:55','9','9:00','9:05','9:10','9:15','9:20','9:25','9:30','9:35','9:40','9:45','9:50','9:55','09','09:00','09:05','09:10','09:15','09:20','09:25','09:30','09:35','09:40','09:45','09:50','09:55','10','10:00','10:05','10:10','10:15','10:20','10:25','10:30','10:35','10:40','10:45','10:50','10:55','11','11:00','11:05','11:10','11:15','11:20','11:25','11:30','11:35','11:40','11:45','11:50','11:55','11','11:00','11:05','11:10','11:15','11:20','11:25','11:30','11:35','11:40','11:45','11:50','11:55','12','12:00','12:05','12:10','12:15','12:20','12:25','12:30','12:35','12:40','12:45','12:50','12:55','13','13:00','13:05','13:10','13:15','13:20','13:25','13:30','13:35','13:40','13:45','13:50','13:55') OR regexp_extract(et3.hs_limpio,'([0-9\:?]+)(\s?A\s?)([0-9\:?]+)',1) IN ('14','14:00','14:05','14:10','14:15','14:20','14:25','14:30','14:35','14:40','14:45','14:50','14:55','15','15:00','15:05','15:10','15:15','15:20','15:25','15:30','15:35','15:40','15:45','15:50','15:55','16','16:00','16:05','16:10','16:15','16:20','16:25','16:30','16:35','16:40','16:45','16:50','16:55','17','17:00','17:05','17:10','17:15','17:20','17:25','17:30','17:35','17:40','17:45','17:50','17:55','18','18:00','18:05','18:10','18:15','18:20','18:25','18:30','18:35','18:40','18:45','18:50','18:55','19','19:00','19:05','19:10','19:15','19:20','19:25','19:30','19:35','19:40','19:45','19:50','19:55','20','20:00','20:05','20:10','20:15','20:20','20:25','20:30','20:35','20:40','20:45','20:50','20:55','21','21:00','21:05','21:10','21:15','21:20','21:25','21:30','21:35','21:40','21:45','21:50','21:55') OR regexp_extract(et3.hs_limpio,'([0-9\:?]+)(\s?A\s?)([0-9\:?]+)',2) IN ('14','14:00','14:05','14:10','14:15','14:20','14:25','14:30','14:35','14:40','14:45','14:50','14:55','15','15:00','15:05','15:10','15:15','15:20','15:25','15:30','15:35','15:40','15:45','15:50','15:55','16','16:00','16:05','16:10','16:15','16:20','16:25','16:30','16:35','16:40','16:45','16:50','16:55','17','17:00','17:05','17:10','17:15','17:20','17:25','17:30','17:35','17:40','17:45','17:50','17:55','18','18:00','18:05','18:10','18:15','18:20','18:25','18:30','18:35','18:40','18:45','18:50','18:55','19','19:00','19:05','19:10','19:15','19:20','19:25','19:30','19:35','19:40','19:45','19:50','19:55','20','20:00','20:05','20:10','20:15','20:20','20:25','20:30','20:35','20:40','20:45','20:50','20:55','21','21:00','21:05','21:10','21:15','21:20','21:25','21:30','21:35','21:40','21:45','21:50','21:55','22','22:00') THEN 'MAÑANA-TARDE'
    WHEN et3.he_limpio NOT LIKE '%A%' AND et3.he_limpio IN ('6','6:00','6:05','6:10','6:15','6:20','6:25','6:30','6:35','6:40','6:45','6:50','6:55','06','06:00','06:15','06:20','06:25','06:30','06:35','06:40','06:45','06:50','06:55','7','7:00','7:05','7:10','7:15','7:20','7:25','7:30','7:35','7:40','7:45','7:50','7:55','07','07:00','07:05','07:10','07:15','07:20','07:25','07:30','07:35','07:40','07:45','07:50','07:55','8','8:00','8:05','8:10','8:15','8:20','8:25','8:30','8:35','8:40','8:45','8:50','8:55','08','08:00','08:05','08:10','08:15','08:20','08:25','08:30','08:35','08:40','08:45','08:50','08:55','9','9:00','9:05','9:10','9:15','9:20','9:25','9:30','9:35','9:40','9:45','9:50','9:55','09','09:00','09:05','09:10','09:15','09:20','09:25','09:30','09:35','09:40','09:45','09:50','09:55','10','10:00','10:05','10:10','10:15','10:20','10:25','10:30','10:35','10:40','10:45','10:50','10:55','11','11:00','11:05','11:10','11:15','11:20','11:25','11:30','11:35','11:40','11:45','11:50','11:55','12','12:00','12:05','12:10','12:15','12:20','12:25','12:30','12:35','12:40','12:45','12:50','12:55','13','13:00','13:05','13:10','13:15','13:20','13:25','13:30','13:35','13:40','13:45','13:50','13:55') OR et3.hs_limpio IN ('14','14:00','14:05','14:10','14:15','14:20','14:25','14:30','14:35','14:40','14:45','14:50','14:55','15','15:00','15:05','15:10','15:15','15:20','15:25','15:30','15:35','15:40','15:45','15:50','15:55','16','16:00','16:05','16:10','16:15','16:20','16:25','16:30','16:35','16:40','16:45','16:50','16:55','17','17:00','17:05','17:10','17:15','17:20','17:25','17:30','17:35','17:40','17.45','17:45','17:50','17:55','18','18:00','18:05','18:10','18:15','18:20','18:25','18:30','18:35','18:40','18:45','18:50','18:55','19','19:00','19:05','19:10','19:15','19:20','19:25','19:30','19:35','19:40','19:45','19:50','19:55','20','20:00','20:05','20:10','20:15','20:20','20:25','20:30','20:35','20:40','20:45','20:50','20:55','21','21:00','21:05','21:10','21:15','21:20','21:25','21:30','21:35','21:40','21:45','21:50','21:55','22','22:00') THEN 'MAÑANA-TARDE'
            --TARDE
    WHEN et3.he_limpio IN ('14','14:00','14:05','14:10','14:15','14:20','14:25','14:30','14:35','14:40','14:45','14:50','14:55','15','15:00','15:05','15:10','15:15','15:20','15:25','15:30','15:35','15:40','15:45','15:50','15:55','16','16:00','16:05','16:10','16:15','16:20','16:25','16:30','16:35','16:40','16:45','16:50','16:55','17','17:00','17:05','17:10','17:15','17:20','17:25','17:30','17:35','17:40','17:45','17:50','17:55','18','18:00','18:05','18:10','18:15','18:20','18:25','18:30','18:35','18:40','18:45','18:50','18:55','19','19:00','19:05','19:10','19:15','19:20','19:25','19:30','19:35','19:40','19:45','19:50','19:55','20','20:00','20:05','20:10','20:15','20:20','20:25','20:30','20:35','20:40','20:45','20:50','20:55','21','21:00','21:05','21:10','21:15','21:20','21:25','21:30','21:35','21:40','21:45','21:50','21:55') OR et3.hs_limpio IN ('14','14:00','14:05','14:10','14:15','14:20','14:25','14:30','14:35','14:40','14:45','14:50','14:55','15','15:00','15:05','15:10','15:15','15:20','15:25','15:30','15:35','15:40','15:45','15:50','15:55','16','16:00','16:05','16:10','16:15','16:20','16:25','16:30','16:35','16:40','16:45','16:50','16:55','17','17:00','17:05','17:10','17:15','17:20','17:25','17:30','17:35','17:40','17:45','17:50','17:55','18','18:00','18:05','18:10','18:15','18:20','18:25','18:30','18:35','18:40','18:45','18:50','18:55','19','19:00','19:05','19:10','19:15','19:20','19:25','19:30','19:35','19:40','19:45','19:50','19:55','20','20:00','20:05','20:10','20:15','20:20','20:25','20:30','20:35','20:40','20:45','20:50','20:55','21','21:00','21:05','21:10','21:15','21:20','21:25','21:30','21:35','21:40','21:45','21:50','21:55','22','22:00') THEN 'TARDE'
    --TARDE-NOCHE
    WHEN et3.he_limpio IN ('14','14:00','14:05','14:10','14:15','14:20','14:25','14:30','14:35','14:40','14:45','14:50','14:55','15','15:00','15:05','15:10','15:15','15:20','15:25','15:30','15:35','15:40','15:45','15:50','15:55','16','16:00','16:05','16:10','16:15','16:20','16:25','16:30','16:35','16:40','16:45','16:50','16:55','17','17:00','17:05','17:10','17:15','17:20','17:25','17:30','17:35','17:40','17:45','17:50','17:55','18','18:00','18:05','18:10','18:15','18:20','18:25','18:30','18:35','18:40','18:45','18:50','18:55','19','19:00','19:05','19:10','19:15','19:20','19:25','19:30','19:35','19:40','19:45','19:50','19:55','20','20:00','20:05','20:10','20:15','20:20','20:25','20:30','20:35','20:40','20:45','20:50','20:55','21','21:00','21:05','21:10','21:15','21:20','21:25','21:30','21:35','21:40','21:45','21:50','21:55') OR et3.hs_limpio IN ('22:05','22:10','22:15','22:30','22:35','22:40','22:45','22:50','22:55','23','23:00','23:05','23:10','23:15','23:20','23:25','23:30','23:35','23:40','23:45','23:50','23:55','24','00:00','00:05','00:10','00:15','00:20','00:25','00:30','00:35','00:40','00:45','00:50','00:55','1','1:00','1:05','1:10','1:15','1:20','1:25','1:30','1:35','1:40','1:45','1:50','1:55','01','01:00','01:05','01:10','01:15','01:20','01:25','01:30','01:35','01:40','01:45','01:50','01:55','2','2:00','2:05','2:10','2:15','2:20','2:25','2:30','2:35','2:40','2:45','2:50','2:55','02','02:00','02:05','02:10','02:15','02:20','02:25','02:30','02:35','02:40','02:45','02:50','02:55','3','3:00','3:05','3:10','3:15','3:20','3:25','3:30','3:35','3:40','3:45','3:50','3:55','03','03:00','03:05','03:10','03:15','03:20','03:25','03:30','03:35','03:40','03:45','03:50','03:55','4','4:00','4:05','4:10','4:15','4:20','4:25','4:30','4:35','4:40','4:45','4:50','4:55','04','04:00','04:05','04:10','04:15','04:20','04:25','04:30','04:35','04:40','04:45','04:50','04:55','5','5:00','5:05','5:10','5:15','5:20','5:25','5:30','5:35','5:40','5:45','5:50','5:55','05','05:00','05:05','05:10','05:15','05:20','05:25','05:30','05:35','05:40','05:45','05:50','05:55','6','6:00') THEN 'TARDE-NOCHE'
    --NOCHE
    WHEN et3.he_limpio IN ('22','22:00','22:05','22:10','22:15','22:30','22:35','22:40','22:45','22:50','22:55','23','23:00','23:05','23:10','23:15','23:20','23:25','23:30','23:35','23:40','23:45','23:50','23:55','24','00:00','00:05','00:10','00:15','00:20','00:25','00:30','00:35','00:40','00:45','00:50','00:55','1','1:00','1:05','1:10','1:15','1:20','1:25','1:30','1:35','1:40','1:45','1:50','1:55','01','01:00','01:05','01:10','01:15','01:20','01:25','01:30','01:35','01:40','01:45','01:50','01:55','2','2:00','2:05','2:10','2:15','2:20','2:25','2:30','2:35','2:40','2:45','2:50','2:55','02','02:00','02:05','02:10','02:15','02:20','02:25','02:30','02:35','02:40','02:45','02:50','02:55','3','3:00','3:05','3:10','3:15','3:20','3:25','3:30','3:35','3:40','3:45','3:50','3:55','03','03:00','03:05','03:10','03:15','03:20','03:25','03:30','03:35','03:40','03:45','03:50','03:55','4','4:00','4:05','4:10','4:15','4:20','4:25','4:30','4:35','4:40','4:45','4:50','4:55','04','04:00','04:05','04:10','04:15','04:20','04:25','04:30','04:35','04:40','04:45','04:50','04:55','5','5:00','5:05','5:10','5:15','5:20','5:25','5:30','5:35','5:40','5:45','5:50','5:55','05','05:00','05:05','05:10','05:15','05:20','05:25','05:30','05:35','05:40','05:45','05:50','05:55') OR et3.hs_limpio IN ('22','22:00','22:05','22:10','22:15','22:30','22:35','22:40','22:45','22:50','22:55','23','23:00','23:05','23:10','23:15','23:20','23:25','23:30','23:35','23:40','23:45','23:50','23:55','24','00:00','00:05','00:10','00:15','00:20','00:25','00:30','00:35','00:40','00:45','00:50','00:55','1','1:00','1:05','1:10','1:15','1:20','1:25','1:30','1:35','1:40','1:45','1:50','1:55','01','01:00','01:05','01:10','01:15','01:20','01:25','01:30','01:35','01:40','01:45','01:50','01:55','2','2:00','2:05','2:10','2:15','2:20','2:25','2:30','2:35','2:40','2:45','2:50','2:55','02','02:00','02:05','02:10','02:15','02:20','02:25','02:30','02:35','02:40','02:45','02:50','02:55','3','3:00','3:05','3:10','3:15','3:20','3:25','3:30','3:35','3:40','3:45','3:50','3:55','03','03:00','03:05','03:10','03:15','03:20','03:25','03:30','03:35','03:40','03:45','03:50','03:55','4','4:00','4:05','4:10','4:15','4:20','4:25','4:30','4:35','4:40','4:45','4:50','4:55','04','04:00','04:05','04:10','04:15','04:20','04:25','04:30','04:35','04:40','04:45','04:50','04:55','5','5:00','5:05','5:10','5:15','5:20','5:25','5:30','5:35','5:40','5:45','5:50','5:55','05','05:00','05:05','05:10','05:15','05:20','05:25','05:30','05:35','05:40','05:45','05:50','05:55','6','6:00') THEN 'NOCHE'
    --ARREGLOS ADICIONALES: SE ARREGLAN LOS CASOS QUE QUEDARÓN FUERA DE LA CATEGORIZACION DE TURNOS. AL SER MUY POCOS CASOS LOS MISMO SE ARREGLAN DE FORMA MANUAL
    --MAÑANA-TARDE-NOCHE
    WHEN et3.horario_entrada IN ('Disponibilidad para realizar turnos rotativos 6:00 a 14:00 / 14:00 a 22:00 / 22:00 a 06:00 hs.','Turno tarde: 12a 16 hs/ Turno noche: 19 a 23 hs') THEN 'MAÑANA-TARDE-NOCHE'
    --MAÑANA
    WHEN horario_entrada LIKE '%10.00%' AND horario_salida LIKE '%13.30%' THEN 'MAÑANA'
    --MAÑANA-TARDE
    WHEN UPPER(horario_entrada) LIKE '09:00 O 11:00' AND UPPER(horario_salida) LIKE '16:00 O 18:00' THEN 'MAÑANA-TARDE'
    WHEN et3.horario_entrada IN ('7/13 - 12/17 - 17/22','"Los turnos de trabajo son de lunes a viernes:  + turno mañana:7.00 a 13.00 + turno tarde:12.30 a 17.30 + turno noche:17.00 a 22.00hs  + sábados, domingos y feriados: 7.30 a 17.30hs"') THEN 'MAÑANA-TARDE'
    ELSE et3.turno_trabajo
END turno_trabajo,
et3.he_split1,
et3.he_split2,
CASE 
    WHEN LENGTH(et3.he_limpio) <> 0 THEN et3.he_limpio
    WHEN et3.horario_entrada LIKE 'Turno tarde: 12a 16 hs/ Turno noche: 19 a 23 hs' THEN '12 A 16 / 19 A 23'
    WHEN et3.horario_entrada LIKE 'Disponibilidad para realizar turnos rotativos 6:00 a 14:00 / 14:00 a 22:00 / 22:00 a 06:00 hs.' THEN '6:00 A 14:00 / 14:00 A 22:00 / 22:00 A 06:00'
    WHEN et3.horario_entrada LIKE '7/13 - 12/17 - 17/22' THEN '7 A 13 / 12 A 17 / 17 A 22'
    ELSE et3.horario_entrada
END horario_entrada,
et3.horario_salida AS horario_salida_origen,
CASE 
    WHEN LENGTH(et3.hs_limpio) <> 0 THEN et3.hs_limpio
    WHEN et3.horario_entrada LIKE 'Turno tarde: 12a 16 hs/ Turno noche: 19 a 23 hs' THEN '19 A 23'
    WHEN et3.horario_salida LIKE 'Disponibilidad para realizar turnos rotativos 6:00 a 14:00 / 14:00 a 22:00 / 22:00 a 06:00 hs.' THEN '6:00 A 14:00 / 14:00 A 22:00 / 22:00 A 06:00'
    WHEN et3.horario_salida LIKE '7/13 - 12/17 - 17/22' THEN '7 A 13 / 12 A 17 / 17 A 22'
    ELSE et3.horario_salida
END horario_salida,
et3.grado_de_estudio,
et3.duracion_practica_formativa,
et3.sector_productivo,
et3.industria,
et3.enviado_para_aprobacion__c,
et3.postulantes_contratados__c,
et3.organizacion_empleadora,

UPPER(TRIM(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(COALESCE(et3.organizacion_empleadora, 'OTRA EXPERIENCIA'), '[\\\\|_-]', ' '), ',|/', ' y '), '[^a-zA-Z0-9áéíóúÁÉÍÓÚñÑ ]', ''), ' +', ' '), '\+', ' '), '1/2', 'medio '), '100\%', ''), '^ ', ''), ' $'))) organizacion_empleadora_limpia,

et3.organizacion_empleadora_calle,
et3.organizacion_empleadora_piso,
et3.organizacion_empleadora_depto,
et3.organizacion_empleadora_cp,
et3.organizacion_empleadora_barrio,
et3.organizacion_empleadora_cuit,
et3.genero_requerido,
et3.experiencia_requerida
FROM et3
),
et5 AS (
SELECT
et4.base_origen,
et4.id,
et4.descripcion,
et4.fecha_publicacion,
et4.estado,
et4.apto_discapacitado,
et4.vacantes,
et4.modalidad_de_trabajo,
et4.edad_minima,
et4.edad_maxima,
et4.vacantes_cubiertas,
et4.tipo_de_puesto,
et4.horario_entrada_origen,
et4.horario_salida_origen,
CASE 
    WHEN et4.horario_entrada_origen LIKE '%Los turnos de trabajo son de lunes a viernes:  + turno mañana:7.00 a 13.00 + turno tarde:12.30 a 17.30 + turno noche:17.00 a 22.00hs  + sábados, domingos y feriados: 7.30 a 17.30hs%' THEN '7:00 A 13:00 / 12:30 A 17:30 / 17:00 A 22:00'
    WHEN et4.turno_trabajo LIKE 'SIN TURNO ESPECIFICO' THEN ''
    WHEN et4.horario_entrada LIKE ':700 ' THEN ''
    WHEN et4.horario_entrada LIKE 'Turnos de 6 hs (mañana y tarde)' THEN '' 
    WHEN regexp_like(et4.horario_entrada,'[^0-9A\:?\s?]') THEN regexp_replace(et4.horario_entrada,'[^0-9A\:?\s?\/?]','')
    ELSE et4.horario_entrada
END horario_entrada,
CASE 
    WHEN et4.horario_salida_origen LIKE '%Los turnos de trabajo son de lunes a viernes:  + turno mañana:7.00 a 13.00 + turno tarde:12.30 a 17.30 + turno noche:17.00 a 22.00hs  + sábados, domingos y feriados: 7.30 a 17.30hs%' THEN '7:00 A 13:00 / 12:30 A 17:30 / 17:00 A 22:00'
    WHEN et4.turno_trabajo LIKE 'SIN TURNO ESPECIFICO' THEN ''
    WHEN et4.horario_salida LIKE '1300' THEN ''
    WHEN et4.horario_salida LIKE 'Turnos de 6 hs (mañana y tarde)' THEN ''
    WHEN regexp_like(et4.horario_salida,'[^0-9A\:?\s?]') THEN regexp_replace(et4.horario_salida,'[^0-9A\:?\s?\/?]','')
    ELSE et4.horario_salida
END horario_salida,
et4.turno_trabajo,
et4.grado_de_estudio,
et4.duracion_practica_formativa,
et4.sector_productivo,
et4.industria,
et4.enviado_para_aprobacion__c,
et4.postulantes_contratados__c,

CASE 
	WHEN et4.organizacion_empleadora_limpia IS NULL 
	OR LENGTH(REPLACE(et4.organizacion_empleadora_limpia, ' ', ''))<3
	OR et4.organizacion_empleadora_limpia IN ('OTRAS EXPERIENCIAS','OTRAS TAREAS','OTRAS ÁREAS','OTRO','OTROS','OTROS OFICINISTAS','OTROS TRABAJOS ANTERIORES') THEN 'OTRA EXPERIENCIA' 
ELSE  et4.organizacion_empleadora_limpia END AS organizacion_empleadora,

et4.organizacion_empleadora_calle,
et4.organizacion_empleadora_piso,
et4.organizacion_empleadora_depto,
et4.organizacion_empleadora_cp,
et4.organizacion_empleadora_barrio,
et4.organizacion_empleadora_cuit,
et4.genero_requerido,
et4.experiencia_requerida
FROM et4
),
et6 AS (
SELECT
et5.base_origen,
et5.id,
et5.descripcion,
et5.fecha_publicacion,
et5.estado,
et5.apto_discapacitado,
et5.vacantes,
et5.modalidad_de_trabajo,
et5.edad_minima,
et5.edad_maxima,
et5.vacantes_cubiertas,
et5.tipo_de_puesto,
et5.horario_entrada_origen,
et5.horario_salida_origen,
et5.horario_entrada,
CASE
    WHEN et5.horario_entrada LIKE '%/%' THEN split_part(horario_entrada, '/', 1) 
    ELSE et5.horario_entrada
END he_split1,
CASE
    WHEN et5.horario_entrada LIKE '%/%' THEN split_part(horario_entrada, '/', 2)
END he_split2,
CASE
    WHEN et5.horario_entrada LIKE '%/%' THEN split_part(horario_entrada, '/', 3)
END he_split3,
et5.horario_salida,
et5.turno_trabajo,
et5.grado_de_estudio,
et5.duracion_practica_formativa,
et5.sector_productivo,
et5.industria,
et5.enviado_para_aprobacion__c,
et5.postulantes_contratados__c,
et5.organizacion_empleadora,
et5.organizacion_empleadora_calle,
et5.organizacion_empleadora_piso,
et5.organizacion_empleadora_depto,
et5.organizacion_empleadora_cp,
et5.organizacion_empleadora_barrio,
et5.organizacion_empleadora_cuit,
et5.genero_requerido,
et5.experiencia_requerida
FROM et5
),
etf AS (
SELECT
et6.base_origen,
et6.id,
et6.descripcion,
et6.fecha_publicacion,
et6.estado,
et6.apto_discapacitado,
et6.vacantes,
et6.modalidad_de_trabajo,
et6.edad_minima,
et6.edad_maxima,
et6.vacantes_cubiertas,
et6.tipo_de_puesto,
et6.horario_entrada_origen,
et6.horario_salida_origen,
CASE 
    WHEN et6.he_split1 LIKE '%A%' THEN regexp_extract(et6.he_split1,'([0-9\:?]+)\s?A\s?([0-9\:?]+)',1)
    ELSE et6.he_split1
END horario_entrada1,
CASE 
    WHEN et6.he_split2 LIKE '%A%' THEN regexp_extract(et6.he_split2,'([0-9\:?]+)\s?A\s?([0-9\:?]+)',1)
    ELSE et6.he_split2
END horario_entrada2,
CASE 
    WHEN et6.he_split3 LIKE '%A%' THEN regexp_extract(et6.he_split3,'([0-9\:?]+)\s?A\s?([0-9\:?]+)',1)
    ELSE et6.he_split3
END horario_entrada3,
CASE 
    WHEN et6.he_split1 LIKE '%A%' THEN regexp_extract(et6.he_split1,'([0-9\:?]+)\s?A\s?([0-9\:?]+)',2)
    ELSE et6.horario_salida
END horario_salida1,
CASE 
    WHEN et6.he_split2 LIKE '%A%' THEN regexp_extract(et6.he_split2,'([0-9\:?]+)\s?A\s?([0-9\:?]+)',2)
    WHEN et6.horario_salida LIKE '%A%' THEN regexp_extract(et6.horario_salida,'([0-9\:?]+)\s?A\s?([0-9\:?]+)',2)
    ELSE et6.he_split2
END horario_salida2,
CASE 
    WHEN et6.he_split3 LIKE '%A%' THEN regexp_extract(et6.he_split3,'([0-9\:?]+)\s?A\s?([0-9\:?]+)',2)
    ELSE et6.he_split3
END horario_salida3,
et6.turno_trabajo,
et6.grado_de_estudio,
et6.duracion_practica_formativa,
et6.sector_productivo,
et6.industria,
et6.enviado_para_aprobacion__c,
et6.postulantes_contratados__c,
et6.organizacion_empleadora,
et6.organizacion_empleadora_calle,
et6.organizacion_empleadora_piso,
et6.organizacion_empleadora_depto,
et6.organizacion_empleadora_cp,
et6.organizacion_empleadora_barrio,
et6.organizacion_empleadora_cuit,
et6.genero_requerido,
et6.experiencia_requerida
FROM et6
),
-- 2.2.-- Se estandarizan los campos apto_discapacitado, estado, grado_de_estudio, edad minima, edad maxima, modalidad_trabajo, sector_productivo
ecr1 AS (
SELECT
etf.base_origen,
etf.id,
etf.descripcion,
etf.fecha_publicacion,
etf.estado AS estado_origen,
CASE
    WHEN etf.estado LIKE 'Vigente' OR etf.estado LIKE 'en_curso' THEN 'ABIERTO'
    WHEN etf.estado LIKE 'cancelada'  THEN 'CANCELADO'
    WHEN etf.estado LIKE 'finalizada' THEN 'CERRADO'
    ELSE etf.estado
END estado,
etf.apto_discapacitado AS apto_discapacitado_origen,
CASE
    WHEN etf.apto_discapacitado LIKE '1' OR etf.apto_discapacitado LIKE 'true' THEN 'S'
    WHEN etf.apto_discapacitado LIKE '0' OR etf.apto_discapacitado LIKE 'false' THEN 'N'
    ELSE etf.apto_discapacitado
END apto_discapacitado,
etf.vacantes AS vacantes_origen,
REPLACE(etf.vacantes,'.0','') AS vacantes,
etf.modalidad_de_trabajo AS modalidad_de_trabajo_origen,
CASE
    WHEN regexp_like(UPPER(etf.modalidad_de_trabajo),'RELACIÓN DE DEPENDENCIA') THEN 'RELACION DE DEPENDENCIA'
    WHEN regexp_like(UPPER(etf.modalidad_de_trabajo),'PERIODO DE PRUEBA|6 MESES|POR HORA|POR CONTRATO|TEMPORAL|MONOTRIBUTO|MONOTRIBUTISTA') THEN 'CONTRATO'
    WHEN regexp_like(UPPER(etf.modalidad_de_trabajo),'PASANTÍA') THEN 'PASANTIA'
    WHEN regexp_like(UPPER(etf.modalidad_de_trabajo),'VOLUNTARIO') THEN 'AD HONOREM'
    ELSE ''
END modalidad_de_trabajo,
etf.edad_minima AS edad_minima_origen,
regexp_replace(etf.edad_minima,'[^0-9]+','') AS edad_minima,
etf.edad_maxima AS edad_maxima_origen,
regexp_replace(etf.edad_maxima,'[^0-9]+','') AS edad_maxima,
etf.vacantes_cubiertas,
etf.tipo_de_puesto,
etf.horario_entrada_origen,
etf.horario_entrada1,
etf.horario_entrada2,
etf.horario_entrada3,
etf.horario_salida_origen,
etf.horario_salida1,
etf.horario_salida2,
etf.horario_salida3,
etf.turno_trabajo,
etf.grado_de_estudio AS grado_de_estudio_origen,
CASE
    WHEN regexp_like(UPPER(etf.grado_de_estudio),'SECUNDARIO|SEC') THEN 'SECUNDARIO'
    WHEN regexp_like(UPPER(etf.grado_de_estudio),'UNIVERSITARIO|UNIVERSITARIOS|POSTGRADO|DOCTORADO|CARRERAS|ING|CS|PROFESIONAL|CIENCIAS|GRADUADO|ACADÉMICA') AND UPPER(etf.grado_de_estudio) NOT LIKE '%NINGUNO%' THEN 'UNIVERSITARIO'
    WHEN regexp_like(UPPER(etf.grado_de_estudio),'TERCIARIO|TERCIARIOS') THEN 'TERCIARIO'
    WHEN LENGTH(etf.grado_de_estudio) = 0 THEN ''
    ELSE 'OTROS'
END grado_de_estudio,
etf.duracion_practica_formativa,
etf.sector_productivo AS sector_productivo_origen,
CASE
    WHEN regexp_like(etf.sector_productivo,'Gastronomía, Hotelería y Turismo|Camareros') or regexp_like(etf.descripcion,'Camarero/a|Cocinero/a|Bachero/a') or (etf.tipo_de_puesto IS NOT NULL AND regexp_like(etf.tipo_de_puesto,'Camarero/a|Cocinero/a|Bachero/a') ) THEN 'GASTRONOMIA, HOTELERIA Y TURISMO'
    WHEN LENGTH(etf.sector_productivo) = 0 OR LENGTH(etf.industria) > 0 THEN UPPER(etf.industria)
    WHEN regexp_like(etf.sector_productivo,'Abastecimiento y Logística|Almacén / Depósito / Expedición') THEN 'ABASTECIMIENTO Y LOGISTICA'
    WHEN regexp_like(etf.sector_productivo,'Contabilidad|Secretarias y Recepción|Administración, Contabilidad y Finanzas|Gerencia y Dirección General') THEN 'ADMINISTRACION, CONTABILIDAD Y FINANZAS'
    WHEN regexp_like(etf.sector_productivo,'Recursos Humanos y Capacitación') THEN 'RECURSOS HUMANOS Y CAPACITACION'
    WHEN regexp_like(etf.sector_productivo,'Marketing y Publicidad|Ventas|Comercial, Ventas y Negocios|Comercial') THEN 'COMERCIAL, VENTAS Y NEGOCIOS'
    WHEN regexp_like(etf.sector_productivo,'Aduana y Comercio Exterior') THEN 'ADUANA Y COMERCIO EXTERIOR'
    WHEN regexp_like(etf.sector_productivo,'Atención al Cliente, Call Center y Telemarketing') THEN 'ATENCION AL CLIENTE, CALL CENTER Y TELEMARKETING'    
    WHEN regexp_like(etf.sector_productivo,'Comunicación, Relaciones Institucionales y Públicas')  THEN 'COMUNICACION, RELACIONES INSTITUCIONALES Y PUBLICAS'
    WHEN regexp_like(etf.sector_productivo,'Diseño') THEN 'DISEÑO'
    WHEN regexp_like(etf.sector_productivo,'Educación, Docencia e Investigación') THEN 'EDUCACION, DOCENCIA E INVESTIGACION'
    WHEN regexp_like(etf.sector_productivo,'Hipódromo de Palermo') THEN 'LIMPIEZA Y MANTENIMIENTO (SIN EDIFICIOS)'
    WHEN regexp_like(etf.sector_productivo,'Ingeniería Civil, Arquitectura y Construcción') THEN 'INGENIERIA CIVIL, ARQUITECTURA Y CONSTRUCCION'
    WHEN regexp_like(etf.sector_productivo,'Legales/Abogacía') THEN 'LEGALES/ABOGACIA'
    WHEN regexp_like(etf.sector_productivo,'Minería, Energía, Petróleo y Gas') THEN 'MINERIA, ENERGIA, PETROLEO, AGUA Y GAS'
    WHEN regexp_like(etf.sector_productivo,'Oficios y Otros') THEN 'OFICIOS Y OTROS'
    WHEN regexp_like(etf.sector_productivo,'Producción y Manufactura') THEN 'PRODUCCION Y MANUFACTURA (SIN TEXTIL, ELECTRONICA Y AUTOMOTRIZ)'
    WHEN regexp_like(etf.sector_productivo,'INGENIERIAS') THEN 'PRODUCCION Y MANUFACTURA (SIN TEXTIL, ELECTRONICA Y AUTOMOTRIZ)'
    WHEN regexp_like(etf.sector_productivo,'Salud, Medicina y Farmacia') THEN 'SALUD, MEDICINA, FARMACIA Y ASISTENCIA SOCIAL'
    WHEN regexp_like(etf.sector_productivo,'Administración Pública|Postas de vacunacion - CABA') THEN 'SECTOR PUBLICO'
    WHEN regexp_like(etf.sector_productivo,'Seguros') THEN 'SEGUROS'
    WHEN regexp_like(etf.sector_productivo,'Tecnología, Sistemas y Telecomunicaciones') THEN 'TECNOLOGIA, SISTEMAS Y TELECOMUNICACIONES'
    ELSE ''
END sector_productivo,
etf.industria,
etf.enviado_para_aprobacion__c,
etf.postulantes_contratados__c,
etf.organizacion_empleadora,
etf.organizacion_empleadora_calle,
etf.organizacion_empleadora_piso,
etf.organizacion_empleadora_depto,
etf.organizacion_empleadora_cp,
etf.organizacion_empleadora_barrio,
etf.organizacion_empleadora_cuit,
etf.genero_requerido,
etf.experiencia_requerida
FROM etf
),
ecr2 AS (
SELECT
ecr1.base_origen,
ecr1.id,
ecr1.descripcion,
ecr1.fecha_publicacion,
CASE
    WHEN ecr1.base_origen LIKE 'PORTALEMPLEO' THEN 'PORTAL DE EMPLEO'
    WHEN ecr1.base_origen LIKE 'CRMEMPLEO' THEN 'IMPULSO A LA INSERCIÓN LABORAL'
    WHEN ecr1.base_origen LIKE 'CRMSL' THEN 'ACTIVÁ TU POTENCIAL LABORAL'
END programa,   
ecr1.estado_origen,
ecr1.estado,
ecr1.apto_discapacitado_origen,
ecr1.apto_discapacitado,
ecr1.vacantes_origen,
CASE
    WHEN LENGTH(ecr1.vacantes) > 2 THEN ''
    ELSE ecr1.vacantes
END vacantes,
ecr1.modalidad_de_trabajo_origen,
ecr1.modalidad_de_trabajo,
ecr1.edad_minima_origen,
CASE
    WHEN ecr1.edad_minima_origen LIKE '% a %' AND  regexp_like(ecr1.edad_minima_origen,'[a-zA-ZñÑ]+\:?\s?[0-9]+\s?a\s?[0-9]+\s?[a-zA-ZñÑ]+') THEN regexp_extract(ecr1.edad_minima_origen,'([a-zA-ZñÑ]+\:?\s?)([0-9]+)(\s?a\s?)([0-9]+)(\s?[a-zA-ZñÑ]+)',2)
    WHEN ecr1.edad_minima_origen LIKE '%/%' AND regexp_like(ecr1.edad_minima_origen,'[a-zA-ZñÑ]+\:?\s?[0-9]+\s?a\s?[0-9]+\s?[a-zA-ZñÑ]+') THEN regexp_extract(ecr1.edad_minima_origen,'([a-zA-ZñÑ]+\:?\s?)([0-9]+)\/([0-9]+\s?)([a-zA-ZñÑ]+)',2)
    WHEN regexp_like(UPPER(ecr1.edad_minima_origen),'DE?\s?[0-9]+\s?A\s?[0-9]+\s?AÑOS?') THEN regexp_extract(UPPER(ecr1.edad_minima_origen),'(DE?\s?)([0-9]+)(\s?A\s?)([0-9]+)(\s?AÑOS?)',2)
    WHEN LENGTH(ecr1.edad_minima) <> 2 OR ecr1.edad_minima IN ('10','11','12','13','14','15','16','17') THEN ''
    ELSE ecr1.edad_minima
END edad_minima,
ecr1.edad_maxima_origen,
CASE
    WHEN ecr1.edad_maxima_origen LIKE '% a %' AND  regexp_like(ecr1.edad_maxima_origen,'[a-zA-ZñÑ]+\:?\s?[0-9]+\s?a\s?[0-9]+\s?[a-zA-ZñÑ]+') THEN regexp_extract(ecr1.edad_maxima_origen,'([a-zA-ZñÑ]+\:?\s?)([0-9]+)(\s?a\s?)([0-9]+)(\s?[a-zA-ZñÑ]+)',4)
    WHEN ecr1.edad_maxima_origen LIKE '%/%' AND regexp_like(ecr1.edad_maxima_origen,'[a-zA-ZñÑ]+\:?\s?[0-9]+\s?a\s?[0-9]+\s?[a-zA-ZñÑ]+') THEN regexp_extract(ecr1.edad_maxima_origen,'([a-zA-ZñÑ]+\:?\s?)([0-9]+)\/([0-9]+\s?)([a-zA-ZñÑ]+)',3)
    WHEN regexp_like(UPPER(ecr1.edad_maxima_origen),'DE?\s?[0-9]+\s?A\s?[0-9]+\s?AÑOS?') THEN regexp_extract(UPPER(ecr1.edad_maxima_origen),'(DE?\s?)([0-9]+)(\s?A\s?)([0-9]+)(\s?AÑOS?)',4)
    WHEN LENGTH(ecr1.edad_maxima) <> 2 OR ecr1.edad_maxima IN ('10','11','12','13','14','15','16','17')  THEN ''
    ELSE ecr1.edad_maxima
END edad_maxima,
ecr1.vacantes_cubiertas AS vacantes_cubiertas_origen,
REPLACE(REPLACE(ecr1.vacantes_cubiertas,'.0',''),'-','') AS vacantes_cubiertas,
ecr1.tipo_de_puesto,
ecr1.horario_entrada_origen,
ecr1.horario_entrada1,
ecr1.horario_entrada2,
ecr1.horario_entrada3,
ecr1.horario_salida_origen,
ecr1.horario_salida1,
ecr1.horario_salida2,
ecr1.horario_salida3,
ecr1.turno_trabajo,
ecr1.grado_de_estudio_origen,
ecr1.grado_de_estudio,
ecr1.duracion_practica_formativa,
ecr1.sector_productivo_origen,
REPLACE(REPLACE(ecr1.sector_productivo,'Í','I'),'Ó','O') AS sector_productivo,
ecr1.industria,
ecr1.enviado_para_aprobacion__c,
REPLACE(ecr1.postulantes_contratados__c,'.0','') AS postulantes_contratados__c,
ecr1.organizacion_empleadora,
ecr1.organizacion_empleadora_calle,
ecr1.organizacion_empleadora_piso,
ecr1.organizacion_empleadora_depto,
ecr1.organizacion_empleadora_cp,
ecr1.organizacion_empleadora_barrio,
ecr1.organizacion_empleadora_cuit,
ecr1.genero_requerido,
ecr1.experiencia_requerida
FROM ecr1
),
ecr3 AS (
SELECT
ecr2.base_origen,
ecr2.id,
ecr2.descripcion,
ecr2.fecha_publicacion,
ecr2.programa,
ecr2.estado_origen,
ecr2.estado,
ecr2.apto_discapacitado_origen,
ecr2.apto_discapacitado,
ecr2.vacantes_origen,
ecr2.vacantes,
ecr2.modalidad_de_trabajo_origen,
ecr2.modalidad_de_trabajo,
ecr2.edad_minima_origen,
CASE 
    WHEN regexp_like(ecr2.edad_minima_origen,'[0-9]+.0') THEN regexp_extract(ecr2.edad_minima_origen,'([0-9]+)(.0)',1)
    ELSE ecr2.edad_minima
END edad_minima,
ecr2.edad_maxima_origen,
CASE 
    WHEN regexp_like(ecr2.edad_maxima_origen,'[0-9]+.0') THEN regexp_extract(ecr2.edad_maxima_origen,'([0-9]+)(.0)',1)
    ELSE ecr2.edad_maxima
END edad_maxima,
ecr2.vacantes_cubiertas_origen,
ecr2.vacantes_cubiertas,
ecr2.tipo_de_puesto,
ecr2.horario_entrada_origen,
ecr2.horario_entrada1,
ecr2.horario_entrada2,
ecr2.horario_entrada3,
ecr2.horario_salida_origen,
ecr2.horario_salida1,
ecr2.horario_salida2,
ecr2.horario_salida3,
ecr2.turno_trabajo,
ecr2.grado_de_estudio_origen,
ecr2.grado_de_estudio,
ecr2.duracion_practica_formativa,
ecr2.sector_productivo_origen,
ecr2.sector_productivo,
ecr2.industria,
ecr2.enviado_para_aprobacion__c,
ecr2.postulantes_contratados__c,
ecr2.organizacion_empleadora,
ecr2.organizacion_empleadora_calle,
ecr2.organizacion_empleadora_piso,
ecr2.organizacion_empleadora_depto,
ecr2.organizacion_empleadora_cp,
ecr2.organizacion_empleadora_barrio,
ecr2.organizacion_empleadora_cuit,
ecr2.genero_requerido,
ecr2.experiencia_requerida
FROM ecr2
)
SELECT *
FROM ecr3
--</sql>--