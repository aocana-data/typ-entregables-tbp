-- 1.-- Crear tabla tmp de organizaciones
-- --<sql>--DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_tmp_organizaciones`;--</sql>--
--<sql>--
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_organizaciones" AS
--Se obtiene el "CUIT" de las organizaciones de las fuentes de registro laboral formal y oportunidades laborales
WITH rlf_op AS (
SELECT 
	  cuit_del_empleador
FROM "caba-piba-staging-zone-db"."tbp_typ_def_registro_laboral_formal"
UNION
SELECT
    ol.organizacion_empleadora_cuit AS cuit_del_empleador
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_oportunidad_laboral" ol
),
--Se agrega el campo "razon_social_new" que contiene las denominaciones sociales obtenidas fuentes oficiales. En primera instancia, se obtiene la data del Registro Nacional de Sociededades (solo se podrá obtener data de personas jurídicas).
rlf_op1 AS (
SELECT
rlf_op.cuit_del_empleador,
UPPER(rc.razon_social) AS razon_social_new
FROM rlf_op
LEFT JOIN "caba-piba-raw-zone-db"."registro_nacional_sociedades" rc ON (rc.cuit = rlf_op.cuit_del_empleador)
WHERE LENGTH(TRIM(rlf_op.cuit_del_empleador)) = 11
GROUP BY 1,2
),
--Se agrega el campo "razon_social_old" que contiene las denominaciones sociales obtenidas fuentes de CRM de oportunidades laborales. Es importante destacar que se tratan de nombres de fantasía y no de denominaciones sociales de fuentes oficiales.
rlf_op2 AS (
SELECT
rlf_op1.cuit_del_empleador,
UPPER(organizacion_empleadora) AS razon_social_old,
rlf_op1.razon_social_new,
CASE 
    WHEN SUBSTRING(rlf_op1.cuit_del_empleador,1,2) IN ('20', '23', '24', '25', '26', '27') THEN 'PF'
    WHEN SUBSTRING(rlf_op1.cuit_del_empleador,1,2) IN ('30', '33', '34') THEN 'PJ'
END tipo_persona
FROM rlf_op1
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_tmp_oportunidad_laboral" ol ON (ol.organizacion_empleadora_cuit = rlf_op1.cuit_del_empleador)
GROUP BY 1,2,3
)
--Se agregan los campos "id_organizacion","estado" y "ente_gubernamental"
SELECT
rlf_op2.cuit_del_empleador AS cuit,
rlf_op2.razon_social_old,
rlf_op2.razon_social_new,
CAST(NULL AS VARCHAR) AS estado,
CASE
    WHEN regexp_like(rlf_op2.razon_social_old ,'MINISTERIO|GOBIERNO|SUBSECRETARÍA|TSJ|GOB|AFIP|RENAPER|INTA|AGIP|PODER JUDICIAL|BANCO CENTRAL|ARBA') THEN 1
    ELSE 0
END ente_gubernamental
FROM rlf_op2
--</sql>--