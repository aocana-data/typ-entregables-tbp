--1.-- Crear tabla de match de capacitaciones unificada
-- --<sql>--DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_def_capacitacion_match`;--</sql>--
--<sql>--
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_def_capacitacion_match" AS
SELECT base_origen || '-' || tipo_capacitacion || '-' || CAST(id_new AS VARCHAR) AS id,
	base_origen,
	tipo_capacitacion,
	 id_new,
	id_old
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_crmsl_match"
UNION DISTINCT
SELECT base_origen || '-' || tipo_capacitacion || '-' || CAST(id_new AS VARCHAR) AS id,
	base_origen,
	tipo_capacitacion,
	 id_new,
	id_old
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_sienfo_match"
UNION DISTINCT
SELECT base_origen || '-' || tipo_capacitacion || '-' || CAST(id_new AS VARCHAR) AS id,
	base_origen,
	tipo_capacitacion,
	 id_new,
	id_old
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_goet_match"
UNION DISTINCT
SELECT base_origen || '-' || tipo_capacitacion || '-' || CAST(id_new AS VARCHAR) AS id,
	base_origen,
	tipo_capacitacion,
	 id_new,
	id_old
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_moodle_match"
UNION DISTINCT
SELECT base_origen || '-' || tipo_capacitacion || '-' || CAST(id_new AS VARCHAR) AS id,
	base_origen,
	tipo_capacitacion,
	 id_new,
	id_old
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_siu_match";
--</sql>--

--2.--Crear tabla de maestro de capacitaciones unificada
-- --<sql>--DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_def_capacitacion`;--</sql>--
--<sql>--
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_def_capacitacion" AS
SELECT tc.id,
	tc.id_new,
	tc.base_origen,
	tc.tipo_capacitacion,
	tc.descrip_normalizada,
	tc.fecha_inicio,
	tc.fecha_fin,
	tc.estado,
	ca.capacitacion_id AS id_capacitacion_asi,
	ca.programa_id AS id_programa,
	ca.descrip_capacitacion,
	ca.tipo_formacion,
	ca.descrip_tipo_formacion,
	ca.modalidad_id AS id_modalidad,
	ca.descrip_modalidad,
	ca.tipo_capacitacion AS tipo_capacitacion_asi,
	ca.estado_capacitacion,
	ca.seguimiento_personalizado,
	ca.soporte_online,
	ca.incentivos_terminalidad,
	ca.exclusividad_participantes,
	ca.categoria_back_id AS id_categoria_back,
	ca.descrip_back,
	ca.categoria_front_id AS id_categoria_front,
	ca.descrip_front,
	ca.detalle_capacitacion,
	ca.otorga_certificado,
	ca.filtro_ingreso,
	ca.frecuencia_oferta_anual,
	ca.duracion_cantidad,
	ca.duracion_medida,
	ca.duracion_dias,
	ca.duracion_hs_reloj,
	ca.vacantes
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_capacitacion" tc
LEFT JOIN (
	SELECT ca1.*,
		cm.id
	FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_capacitacion_asi" ca1
	INNER JOIN "caba-piba-staging-zone-db"."tbp_typ_def_capacitacion_match" cm ON (
			ca1.base_origen = cm.base_origen
			AND ca1.codigo_capacitacion = cm.id_old
			)
	) AS ca ON (tc.id = ca.id);
--</sql>--

-- 3.-  Crear tabla aptitudes para cada capacitacion
-- --<sql>--DROP TABLE `caba-piba-staging-zone-db`.`tbp_typ_def_aptitudes`;--</sql>--
--<sql>--
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_def_aptitudes" AS
SELECT c.id id_capacitacion,
	   c.id_capacitacion_asi,
       c.tipo_capacitacion_asi,
       ac.aptitud_id id_aptitud,
       a.nombre descrip_aptitud
FROM "caba-piba-staging-zone-db"."tbp_typ_def_capacitacion" c
INNER JOIN "caba-piba-raw-zone-db"."api_asi_aptitud_capacitacion" ac
ON (ac.capacitacion_id = c.id_capacitacion_asi)
INNER JOIN "caba-piba-raw-zone-db"."api_asi_aptitud" a
ON (ac.aptitud_id = a.id)
--</sql>--