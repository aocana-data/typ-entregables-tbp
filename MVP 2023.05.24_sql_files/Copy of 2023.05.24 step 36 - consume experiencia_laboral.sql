-- Crear tabla def de experiencia laboral dentro del cv
-- --<sql>--DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_def_cv_experiencia_laboral`;--</sql>--
--<sql>--
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_def_cv_experiencia_laboral" AS
WITH el AS (
SELECT 
	cv.id as curriculum_id,
	e.id_old,
	e.base_origen, 
	e.fecha_desde as fecha_inicio,
	e.fecha_hasta as fecha_fin,
	e.empresa_limpia as empresa,
	e.posicion_limpia as puesto
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_cv_experiencia_laboral" e
JOIN "caba-piba-staging-zone-db"."tbp_typ_def_curriculum" cv ON (e.id_cv_old=cv.id_old)
GROUP BY 
	cv.id,
	e.id_old,
	e.base_origen, 
	e.fecha_desde,
	e.fecha_hasta,
	e.empresa_limpia,
	e.posicion_limpia
)
SELECT 
	el.base_origen||el.id_old AS id,
	el.curriculum_id,
	el.id_old,
	el.base_origen, 
	el.fecha_inicio,
	el.fecha_fin,
	org.id_organizacion,
	el.puesto
FROM el
JOIN "caba-piba-staging-zone-db"."tbp_typ_def_organizaciones" org
ON (el.empresa = org.razon_social_old OR el.empresa = org.razon_social_new)
GROUP BY
	el.base_origen||el.id_old,
	el.curriculum_id,
	el.id_old,
	el.base_origen, 
	el.fecha_inicio,
	el.fecha_fin,
	org.id_organizacion,
	el.puesto
--</sql>--	