-- Copy of 2023.05.24 step 00 - creacion de vistas.sql 



--1. RENAPER sin duplicados
--<sql>--
CREATE OR REPLACE VIEW "caba-piba-staging-zone-db"."tbp_typ_tmp_view_ciudadanos_renaper_no_duplicates" AS

SELECT
a.dni,
a.sexo,
a.fecha_nacimiento,
a.coincidencia,
a.observaciones,
a.nombres,
a.apellidos
FROM (
SELECT cr.*,
ROW_NUMBER() OVER(
				PARTITION BY cr.dni, cr.sexo
				ORDER BY cr.coincidencia DESC
			) AS "orden_duplicado"
 FROM "caba-piba-raw-zone-db"."ciudadanos_renaper" cr

 ) a
 WHERE a.orden_duplicado=1
 --</sql>--
--2. usuarios CRSML sin duplicados
--<sql>--
  CREATE OR REPLACE VIEW "caba-piba-staging-zone-db"."tbp_typ_tmp_view_crm_sociolaboral_contacts_cstm_no_duplicates" AS
SELECT
a.id_c,
a.jjwg_maps_lng_c,
a.jjwg_maps_lat_c,
a.jjwg_maps_geocode_status_c,
a.jjwg_maps_address_c,
a.tipo_documento_c,
a.numero_documento_c,
a.movilidad_c,
a.libreta_sanitaria_c,
a.genero_c,
a.nacionalidad_c,
a.barrio_c,
a.comentarios_domicilio_c,
a.calle_c,
a.altura_c,
a.departamento_c,
a.habitacion_c,
a.edificio_c,
a.escalera_c,
a.pasillo_c,
a.parador_c,
a.monotributo_c,
a.discapacidad_c,
a.tipo_discapacidad_c,
a.certificado_discapacidad_c,
a.apoyo_trabajar_c,
a.trabaja_actualmente_c,
a.asiste_organizacion_c,
a.nombre_organizacion_c,
a.subsidio_c,
a.subsidio_nombre_c,
a.tipo_pension_c,
a.otro_nombre_c,
a.otro_apellido_c,
a.estado_civil_c,
a.pais_residencia_c,
a.pais_nacimiento_c,
a.cuil2_c,
a.sector_1_c,
a.manzana_c,
a.tira_c,
a.bloque_c,
a.casa_c,
a.piso_c,
a.registro_conducir_c,
a.relacion_telefono_c,
a.numero_rib_c,
a.id_registro_rib_c,
a.fecha_ult_modificacion_rib_c,
a.comuna_c,
a.barrio_rib_c,
a.codigo_postal_c,
a.fecha_modif_ep_c,
a.tiene_educacion_ep_c,
a.tiene_salud_ep_c,
a.tiene_trabajo_ep_c,
a.tipo_domicilio_c,
a.provincia_c,
a.localidad_c,
a.municipio_c,
a.asentamiento_c,
a.planes_activos_c,
a.tipo_monotributo_c,
a.forma_parte_interm_lab_c,
a.insert_into_dl_timestamp

FROM (
SELECT c.*,
  ROW_NUMBER() OVER(
	PARTITION BY CONCAT(
						CAST(
						COALESCE(
							CAST(numero_documento_c AS VARCHAR),
							SUBSTR(CAST(cuil2_c AS VARCHAR),3,LENGTH(CAST(cuil2_c AS VARCHAR)) -3)
							)
						AS VARCHAR)
				, c.genero_c)
			ORDER BY tipo_documento_c ASC)
    AS orden_duplicado
 FROM "caba-piba-raw-zone-db"."crm_sociolaboral_contacts_cstm" c
 ) a
 WHERE a.orden_duplicado=1
 --</sql>--
--3. usuarios GOET sin duplicados
--<sql>--
 CREATE OR REPLACE VIEW "caba-piba-staging-zone-db"."tbp_typ_tmp_view_goet_usuarios_no_duplicates" AS
SELECT
a.idusuario,
a.password,
a.idnacionalidad,
a.idtipodoc,
a.detalledocumento,
a.ndocumento,
a.sexo,
a.nombres,
a.apellidos,
a.fechanacimiento,
a.idpaisnac,
a.idprovincianac,
a.domicilio,
a.idpais,
a.idprovincia,
a.idlocalidad,
a.cpostal,
a.email,
a.emailalternativo,
a.telefono1,
a.telefono2,
a.celular,
a.fechaalta,
a.fechaupdate,
a.idprofesion,
a.idnivelestudio,
a.idgrupo,
a.datosdocente,
a.nlegajo,
a.cuil,
a.ficha,
a.idhr,
a.datosvalidados,
a.ultimoaniocursado,
a.lugartrabajo,
a.actividad,
a.puestotrabajo,
a.antiguedadpuesto,
a.ultimoempleo,
a.telefonofax,
a.numeroafiliado,
a.entidad,
a.detalleprofesion,
a.colegiojornadacompleta,
a.insert_into_dl_timestamp
FROM (
 SELECT u.* ,
  ROW_NUMBER() OVER(PARTITION BY concat(u.ndocumento, u.sexo) ORDER BY "datosvalidados" DESC, "idtipodoc" ASC)
    AS orden_duplicado
FROM "caba-piba-raw-zone-db"."goet_usuarios" u

 ) a
 WHERE a.orden_duplicado=1
 --</sql>--
--4. usuarios SIU sin duplicados
--<sql>--
 CREATE OR REPLACE VIEW "caba-piba-staging-zone-db"."tbp_typ_tmp_view_siu_toba_3_3_negocio_mdp_personas_no_duplicates" AS
  SELECT
a.persona,
a.apellido,
a.nombres,
a.apellido_elegido,
a.nombres_elegido,
a.sexo,
a.fecha_nacimiento,
a.localidad_nacimiento,
a.nacionalidad,
a.fecha_ingreso_pais,
a.pais_origen,
a.documento_principal,
a.recibe_mensajes_x_mail,
a.usuario,
a.clave,
a.fecha_vencimiento_clave,
a.autenticacion,
a.bloqueado,
a.parametro_a,
a.token,
a.email_temporal,
a.email_valido,
a.id_imagen,
a.tipo_usuario_inicial,
a.pertenece_pueblo_originario,
a.pueblo_originario,
a.pueblo_originario_otro,
a.uid_arai,
a.insert_into_dl_timestamp
FROM (
SELECT p.*,
ROW_NUMBER() OVER(
				PARTITION BY d.nro_documento, p.sexo
				ORDER BY p.clave DESC, p.persona DESC
			) AS "orden_duplicado"
 FROM "caba-piba-raw-zone-db"."siu_toba_3_3_negocio_mdp_personas" p
 JOIN "caba-piba-raw-zone-db"."siu_toba_3_3_negocio_mdp_personas_documentos" d ON d.documento = p.documento_principal
 ) a
 WHERE a.orden_duplicado=1
 --</sql>--
--5. SIENFO FICHAS sin duplicados
--<sql>--
 CREATE OR REPLACE VIEW "caba-piba-staging-zone-db"."goayvd_typ_vw_sienfo_fichas" AS
SELECT f1.id_fichas,
	f1.apenom_ord,
	f1.codigo_cf,
	f1.codigo_ct,
	f1.a_nac,
	f1.nrodoc,
	f1.fecha_inc,
    f1.baja,
    f1.fechabaja,
    f1.aprobado,
	f1.fecha,
	f1.hora,
	f1.id_centro,
	f1.bajasino,
	f1.nrocertificado,
	f1.borrar,
	f1.tipo_inc,
	f1.observ,
	f1.libro,
	f1.folio,
	f1.datosimp,
	f1.usua_act,
	f1.nro_sorteo,
	f1.control_baja,
	f1.nrogedo,
	f1.insert_into_dl_timestamp
FROM (
		SELECT ROW_NUMBER() OVER(
				PARTITION BY f.nrodoc,
				f.codigo_ct
				ORDER BY f.id_fichas DESC
			) AS "DUP",
			f.id_fichas,
			f.apenom_ord,
			f.codigo_cf,
			f.codigo_ct,
			f.a_nac,
			f.nrodoc,
			CASE
				WHEN f.fecha_inc < CAST('2003-01-01' AS DATE) THEN NULL ELSE CAST(f.fecha_inc AS DATE)
			END AS "fecha_inc",
			CASE
				WHEN f.baja IS NULL THEN 0 ELSE f.baja
			END AS "baja",
			f.fechabaja,
			CASE
				WHEN f.aprobado IS NULL THEN 0
				WHEN f.aprobado = '' THEN 0 ELSE CAST(f.aprobado AS INT)
			END AS "aprobado",
			f.fecha,
			f.hora,
			f.id_centro,
			f.bajasino,
			f.nrocertificado,
			f.borrar,
			f.tipo_inc,
			f.observ,
			f.libro,
			f.folio,
			f.datosimp,
			f.usua_act,
			f.nro_sorteo,
			f.control_baja,
			f.nrogedo,
			f.insert_into_dl_timestamp
		FROM "caba-piba-raw-zone-db"."sienfo_fichas" f
		WHERE nrodoc IS NOT NULL
			AND nrodoc != ''
	) f1
WHERE DUP = 1
--</sql>--
--6. SIENFO FICHAS PREINSCRIPCION sin duplicados
--<sql>--
CREATE OR REPLACE VIEW "caba-piba-staging-zone-db"."goayvd_typ_vw_sienfo_fichas_preinscripcion" AS
SELECT
    t.id_ficha_pre,
    t.apenom_ord,
    t.codigo_cf,
    t.codigo_ct,
    t.a_nac,
    t.nrodoc,
    t.fecha_inc,
    t.baja,
    t.fechabaja,
    t.aprobado,
    t.fecha,
    t.hora,
    t.id_centro,
    t.bajasino,
    t.nrocertificado,
    t.borrar,
    t.tipo_inc,
    t.observ,
    t.libro,
    t.folio,
    t.datosimp,
    t.usua_act,
    t.pref,
    t.insert_into_dl_timestamp
FROM (
SELECT
    ROW_NUMBER() OVER(PARTITION BY p.nrodoc, p.codigo_ct ORDER BY p.id_ficha_pre DESC) AS DUP,
    p.id_ficha_pre,
    p.apenom_ord,
    p.codigo_cf,
    p.codigo_ct,
    p.a_nac,
    p.nrodoc,
    p.fecha_inc,
    p.baja,
    p.fechabaja,
    p.aprobado,
    p.fecha,
    p.hora,
    p.id_centro,
    p.bajasino,
    p.nrocertificado,
    p.borrar,
    p.tipo_inc,
    p.observ,
    p.libro,
    p.folio,
    p.datosimp,
    p.usua_act,
    p.pref,
    p.insert_into_dl_timestamp
FROM
    "caba-piba-raw-zone-db"."sienfo_fichas_preinscripcion" p
) t
WHERE dup = 1
--</sql>--
--7. VISTA DE SIU QUE SE UTILIZA PARA CALCULO DE ESTADO DE BENEFICIARIO
--<sql>--
CREATE OR REPLACE VIEW "caba-piba-staging-zone-db"."goayvd_typ_tmp_siu_cantidad_materias_plan" AS
SELECT
  PLAN
, propuesta
, nombre
, (CASE WHEN ((PLAN = 79) AND (propuesta = 77)) THEN 15 WHEN ((PLAN = 101) AND (propuesta = 97)) THEN 15 WHEN ((PLAN = 102) AND (propuesta = 98)) THEN 15 WHEN ((PLAN = 103) AND (propuesta = 99)) THEN 15 WHEN ((PLAN = 85) AND (propuesta = 82)) THEN 24 WHEN ((PLAN = 92) AND (propuesta = 88)) THEN 22 WHEN ((PLAN = 93) AND (propuesta = 89)) THEN 22 WHEN ((PLAN = 94) AND (propuesta = 90)) THEN 22 WHEN ((PLAN = 95) AND (propuesta = 91)) THEN 22 WHEN ((PLAN = 96) AND (propuesta = 92)) THEN 22 WHEN ((PLAN = 97) AND (propuesta = 93)) THEN 22 WHEN ((PLAN = 98) AND (propuesta = 94)) THEN 22 WHEN ((PLAN = 100) AND (propuesta = 96)) THEN 22 WHEN ((PLAN = 99) AND (propuesta = 95)) THEN 22 WHEN ((PLAN = 6) AND (propuesta = 6)) THEN 15 WHEN ((PLAN = 84) AND (propuesta = 81)) THEN 33 WHEN ((PLAN = 50) AND (propuesta = 48)) THEN 31 WHEN ((PLAN = 121) AND (propuesta = 115)) THEN 20 WHEN ((PLAN = 122) AND (propuesta = 116)) THEN 20 WHEN ((PLAN = 120) AND (propuesta = 114)) THEN 20 WHEN ((PLAN = 119) AND (propuesta = 113)) THEN 20 WHEN ((PLAN = 54) AND (propuesta = 52)) THEN 26 WHEN ((PLAN = 53) AND (propuesta = 51)) THEN 27 WHEN ((PLAN = 56) AND (propuesta = 54)) THEN 27 WHEN ((PLAN = 72) AND (propuesta = 69)) THEN 27 WHEN ((PLAN = 86) AND (propuesta = 68)) THEN 27 WHEN ((PLAN = 22) AND (propuesta = 20)) THEN 21 WHEN ((PLAN = 35) AND (propuesta = 33)) THEN 31 WHEN ((PLAN = 78) AND (propuesta = 76)) THEN 22 WHEN ((PLAN = 124) AND (propuesta = 118)) THEN 22 WHEN ((PLAN = 68) AND (propuesta = 66)) THEN 22 WHEN ((PLAN = 123) AND (propuesta = 117)) THEN 22 WHEN ((PLAN = 27) AND (propuesta = 25)) THEN 29 WHEN ((PLAN = 40) AND (propuesta = 38)) THEN 26 WHEN ((PLAN = 36) AND (propuesta = 34)) THEN 35 WHEN ((PLAN = 110) AND (propuesta = 58)) THEN 32 WHEN ((PLAN = 111) AND (propuesta = 70)) THEN 32 WHEN ((PLAN = 71) AND (propuesta = 58)) THEN 32 WHEN ((PLAN = 136) AND (propuesta = 130)) THEN 23 WHEN ((PLAN = 55) AND (propuesta = 53)) THEN 23 WHEN ((PLAN = 57) AND (propuesta = 55)) THEN 23 WHEN ((PLAN = 106) AND (propuesta = 102)) THEN 23 WHEN ((PLAN = 118) AND (propuesta = 112)) THEN 33 WHEN ((PLAN = 58) AND (propuesta = 56)) THEN 33 WHEN ((PLAN = 66) AND (propuesta = 63)) THEN 30 WHEN ((PLAN = 51) AND (propuesta = 49)) THEN 30 WHEN ((PLAN = 62) AND (propuesta = 57)) THEN 30 WHEN ((PLAN = 67) AND (propuesta = 64)) THEN 30 WHEN ((PLAN = 70) AND (propuesta = 65)) THEN 30 WHEN ((PLAN = 52) AND (propuesta = 50)) THEN 30 WHEN ((PLAN = 80) AND (propuesta = 59)) THEN 30 WHEN ((PLAN = 65) AND (propuesta = 62)) THEN 30 WHEN ((PLAN = 75) AND (propuesta = 72)) THEN 22 WHEN ((PLAN = 88) AND (propuesta = 84)) THEN 22 WHEN ((PLAN = 74) AND (propuesta = 71)) THEN 22 WHEN ((PLAN = 107) AND (propuesta = 103)) THEN 23 WHEN ((PLAN = 2) AND (propuesta = 2)) THEN 14 WHEN ((PLAN = 31) AND (propuesta = 29)) THEN 28 WHEN ((PLAN = 135) AND (propuesta = 129)) THEN 19 WHEN ((PLAN = 115) AND (propuesta = 109)) THEN 19 WHEN ((PLAN = 133) AND (propuesta = 127)) THEN 19 WHEN ((PLAN = 134) AND (propuesta = 128)) THEN 19 WHEN ((PLAN = 125) AND (propuesta = 119)) THEN 27 WHEN ((PLAN = 18) AND (propuesta = 17)) THEN 27 WHEN ((PLAN = 19) AND (propuesta = 18)) THEN 27 WHEN ((PLAN = 126) AND (propuesta = 120)) THEN 27 WHEN ((PLAN = 20) AND (propuesta = 19)) THEN 30 WHEN ((PLAN = 47) AND (propuesta = 45)) THEN 27 WHEN ((PLAN = 8) AND (propuesta = 9)) THEN 12 WHEN ((PLAN = 9) AND (propuesta = 8)) THEN 13 WHEN ((PLAN = 82) AND (propuesta = 79)) THEN 23 WHEN ((PLAN = 109) AND (propuesta = 105)) THEN 23 WHEN ((PLAN = 81) AND (propuesta = 78)) THEN 23 WHEN ((PLAN = 105) AND (propuesta = 101)) THEN 23 WHEN ((PLAN = 137) AND (propuesta = 132)) THEN 23 WHEN ((PLAN = 83) AND (propuesta = 80)) THEN 23 WHEN ((PLAN = 130) AND (propuesta = 124)) THEN 23 WHEN ((PLAN = 34) AND (propuesta = 32)) THEN 29 WHEN ((PLAN = 45) AND (propuesta = 43)) THEN 28 WHEN ((PLAN = 7) AND (propuesta = 7)) THEN 15 WHEN ((PLAN = 33) AND (propuesta = 31)) THEN 24 WHEN ((PLAN = 15) AND (propuesta = 14)) THEN 37 WHEN ((PLAN = 11) AND (propuesta = 4)) THEN 28 WHEN ((PLAN = 5) AND (propuesta = 5)) THEN 18 WHEN ((PLAN = 13) AND (propuesta = 12)) THEN 28 WHEN ((PLAN = 12) AND (propuesta = 10)) THEN 28 WHEN ((PLAN = 41) AND (propuesta = 39)) THEN 41 WHEN ((PLAN = 42) AND (propuesta = 40)) THEN 34 WHEN ((PLAN = 44) AND (propuesta = 42)) THEN 29 WHEN ((PLAN = 29) AND (propuesta = 27)) THEN 25 WHEN ((PLAN = 23) AND (propuesta = 21)) THEN 34 WHEN ((PLAN = 76) AND (propuesta = 73)) THEN 31 WHEN ((PLAN = 64) AND (propuesta = 61)) THEN 31 WHEN ((PLAN = 63) AND (propuesta = 60)) THEN 31 WHEN ((PLAN = 46) AND (propuesta = 44)) THEN 32 WHEN ((PLAN = 10) AND (propuesta = 3)) THEN 15 WHEN ((PLAN = 139) AND (propuesta = 133)) THEN 18 WHEN ((PLAN = 108) AND (propuesta = 104)) THEN 18 WHEN ((PLAN = 48) AND (propuesta = 46)) THEN 30 WHEN ((PLAN = 89) AND (propuesta = 85)) THEN 35 WHEN ((PLAN = 30) AND (propuesta = 28)) THEN 41 WHEN ((PLAN = 132) AND (propuesta = 126)) THEN NULL WHEN ((PLAN = 43) AND (propuesta = 41)) THEN 34 WHEN ((PLAN = 14) AND (propuesta = 13)) THEN 34 WHEN ((PLAN = 90) AND (propuesta = 86)) THEN 24 WHEN ((PLAN = 131) AND (propuesta = 125)) THEN NULL WHEN ((PLAN = 91) AND (propuesta = 87)) THEN 24 WHEN ((PLAN = 49) AND (propuesta = 47)) THEN 31 WHEN ((PLAN = 77) AND (propuesta = 74)) THEN 33 WHEN ((PLAN = 104) AND (propuesta = 100)) THEN 33 WHEN ((PLAN = 128) AND (propuesta = 122)) THEN 33 WHEN ((PLAN = 129) AND (propuesta = 123)) THEN 33 WHEN ((PLAN = 69) AND (propuesta = 67)) THEN 33 WHEN ((PLAN = 127) AND (propuesta = 121)) THEN 34 WHEN ((PLAN = 1) AND (propuesta = 1)) THEN 3 END) cant_materias
, codigo
, version_actual
FROM
  "caba-piba-raw-zone-db"."siu_toba_3_3_negocio_sga_planes"
--</sql>--
--8. VISTA NECESARIA PARA CALCULO DE ESTADO DE BENEFICIARIO DE SIENFO
--<sql>--
CREATE OR REPLACE VIEW "caba-piba-staging-zone-db"."goayvd_typ_sienfo_vw_cantidad_materias_por_trayecto" AS
WITH carrera_29 AS (
SELECT
    29 AS carrera,
	COUNT(*) AS cant_materias_plan_estudio,
	cast(NULL AS int) AS anio_inicio,
	cast(NULL AS int) AS anio_fin
FROM
	"caba-piba-raw-zone-db".sienfo_cursos cu
LEFT JOIN "caba-piba-raw-zone-db".sienfo_areas a ON
	cu.id_area = a.id_area
LEFT JOIN "caba-piba-raw-zone-db".sienfo_carreras ca ON
	CAST(cu.id_carrera AS varchar) = CAST(ca.id_carrera AS varchar)
WHERE
	cu.id_carrera_30 = 30
	AND cu.baja = '1'
	AND cu.creditos = 1
), carrera_30 AS (
SELECT
    30 AS carrera,
	COUNT(*) AS cant_materias_plan_estudio,
	cast(NULL AS int) AS anio_inicio,
	cast(NULL AS int) AS anio_fin
FROM
	"caba-piba-raw-zone-db".sienfo_cursos cu
LEFT JOIN "caba-piba-raw-zone-db".sienfo_areas a ON
	cu.id_area = a.id_area
LEFT JOIN "caba-piba-raw-zone-db".sienfo_carreras ca ON
	CAST(cu.id_carrera AS varchar) = CAST(ca.id_carrera AS varchar)
WHERE
	cu.id_carrera_30 = 30
	AND cu.baja = '1'
	AND cu.creditos = 1
), carrera_31 AS (
SELECT
    31 AS carrera,
	COUNT(*) AS cant_materias_plan_estudio,
	cast(NULL AS int) AS anio_inicio,
	cast(NULL AS int) AS anio_fin
FROM
	"caba-piba-raw-zone-db".sienfo_cursos cu
LEFT JOIN "caba-piba-raw-zone-db".sienfo_areas a ON
	cu.id_area = a.id_area
LEFT JOIN "caba-piba-raw-zone-db".sienfo_carreras ca ON
	CAST(cu.id_carrera AS varchar) = CAST(ca.id_carrera AS varchar)
WHERE
	cu.id_carrera_31 = 31
	AND cu.baja = '1'
	AND cu.creditos = 1
), carrera_32 AS (
SELECT
    32 AS carrera,
	COUNT(*) AS cant_materias_plan_estudio,
	cast(NULL AS int) AS anio_inicio,
	cast(NULL AS int) AS anio_fin
FROM
	"caba-piba-raw-zone-db".sienfo_cursos cu
LEFT JOIN "caba-piba-raw-zone-db".sienfo_areas a ON
	cu.id_area = a.id_area
LEFT JOIN "caba-piba-raw-zone-db".sienfo_carreras ca ON
	CAST(cu.id_carrera AS varchar) = CAST(ca.id_carrera AS varchar)
WHERE
	cu.id_carrera_32 = 32
	AND cu.baja = '1'
	AND cu.creditos = 1
), carrera_1 AS (
SELECT
    1 AS carrera,
	COUNT(*) AS cant_materias_plan_estudio,
	2019 AS ano_inicio,
	cast(NULL AS int) AS anio_fin
FROM
	"caba-piba-raw-zone-db".sienfo_cursos cu
LEFT JOIN "caba-piba-raw-zone-db".sienfo_areas a ON
	cu.id_area = a.id_area
LEFT JOIN "caba-piba-raw-zone-db".sienfo_carreras ca ON
	cu.id_carrera = CAST(ca.id_carrera AS varchar)
WHERE
	cu.id_carrera_1 = 1
	AND cu.baja = '1'
	AND cu.creditos = 1
), carrera_1_old AS (
SELECT
    1 AS carrera,
	COUNT(*) -1 AS cant_materias_plan_estudio_old,
	2016 AS ano_inicio_old,
	2018 AS anio_fin_old
FROM
	"caba-piba-raw-zone-db".sienfo_cursos cu
LEFT JOIN "caba-piba-raw-zone-db".sienfo_areas a ON
	cu.id_area = a.id_area
LEFT JOIN "caba-piba-raw-zone-db".sienfo_carreras ca ON
	cu.id_carrera = CAST(ca.id_carrera AS varchar)
WHERE
	cu.id_carrera_1 = 1
	AND cu.baja = '1'
	AND cu.creditos = 1
), todas AS (
SELECT
    *
FROM
    carrera_1
UNION
SELECT
    *
FROM
    carrera_29
UNION
SELECT
    *
FROM
    carrera_30
UNION
SELECT
    *
FROM
    carrera_31
UNION
SELECT
    *
FROM
    carrera_32
)
SELECT
    todas.*,
    carrera_1_old.cant_materias_plan_estudio_old,
    carrera_1_old.ano_inicio_old,
    carrera_1_old.anio_fin_old
FROM
    todas
LEFT JOIN carrera_1_old ON carrera_1_old.carrera = todas.carrera
--</sql>--



-- Copy of 2023.05.24 step 01 - consume programa.sql 



-- --<sql>-- DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_def_programa`; --</sql>--

-- 19 programas
--<sql>--
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_def_programa" AS
SELECT p.id programa_id,
       p.ministerio_id ministerio_id,
       p.reparticion_id reparticion_id,
       p.codigo codigo_programa,
       p.nombre nombre_programa,
       p.tipo_programa,
	   CASE WHEN UPPER(p.trayectoria_educativa_bbdd) IN ('SIU','MOODLE','SIENFO','GOET') THEN UPPER(p.trayectoria_educativa_bbdd)
			WHEN UPPER(p.trayectoria_educativa_bbdd) LIKE 'CRM SOCIOLABORAL%' THEN 'CRMSL'
			ELSE UPPER(p.trayectoria_educativa_bbdd) END base_origen,
	   CASE WHEN p.integrable = 1 THEN 'S' ELSE 'N' END integrable,
	   p.duracion_estimada,
	   p.fecha_inscripcion,
	   CASE WHEN p.activo = 1 THEN 'S' ELSE 'N' END activo,
       m.nombre nombre_ministerio,
       r.nombre nombre_reparticion
FROM "caba-piba-raw-zone-db"."api_asi_programa" p
INNER JOIN "caba-piba-raw-zone-db"."api_asi_ministerio" m ON (m.id = p.ministerio_id)
LEFT JOIN "caba-piba-raw-zone-db"."api_asi_reparticion" r ON (p.ministerio_id = r.ministerio_id AND p.reparticion_id = r.id)
--</sql>--



-- Copy of 2023.05.24 step 02 - staging establecimiento.sql 



-- 1.-- Se crea tabla tbp_typ_tmp_establecimientos_1 desde los origenes GOET, MOODLE, SIENFO, CRMSL Y SIU
-- --<sql>--DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_tmp_establecimientos_1`;--</sql>--
--<sql>--
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_establecimientos_1" AS
SELECT
	'SIU' base_origen,
	CAST(est.ubicacion AS VARCHAR) codigo,
	CAST('' AS VARCHAR) cue,
	CAST(upper(est.nombre) AS VARCHAR) nombre_formateado,
	CAST(est.nombre AS VARCHAR) nombre_old,
	CAST('' AS VARCHAR) clave_tipo,
	CAST('' AS VARCHAR) tipo,
	CAST(est.calle AS VARCHAR) calle,
	CAST(est.numero AS VARCHAR) numero,
	CAST(est.localidad AS VARCHAR) clave_localidad,
	CAST(loc.nombre AS VARCHAR) localidad,
	CAST(est.codigo_postal AS VARCHAR) codigo_postal,
	CAST('' AS VARCHAR) comuna
FROM "caba-piba-raw-zone-db"."siu_toba_3_3_negocio_sga_ubicaciones" est
LEFT JOIN "caba-piba-raw-zone-db"."siu_toba_3_3_negocio_mug_localidades" loc ON (est.localidad=loc.localidad)
UNION
-- la tabla que existe de centros y sedes NO tienen datos, solo ids
SELECT
    'GOET' base_origen,
    CAST(c.idcentro AS VARCHAR) codigo,
	CAST(c.cueanexo AS VARCHAR) cue,
	CAST(UPPER(c.nombre) AS VARCHAR) nombre_formateado,
	CAST(c.nombre AS VARCHAR) nombre_old,
	CAST(c.idcentrotipo AS VARCHAR) clave_tipo,
	CAST(ct.detalle AS VARCHAR) tipo,
	-- separar nombre de calle de numero, normalizar
	CAST(c.direccion AS VARCHAR) calle,
	-- separar nombre de calle de numero, normalizar
	CAST(c.direccion AS VARCHAR) numero,
	CAST(c.idlocalidad AS VARCHAR) clave_localidad,
	CAST(l.detalle AS VARCHAR) localidad,
	CAST(l.cdpostal AS VARCHAR) codigo_postal,
	CAST(c.comuna AS VARCHAR)  comuna
FROM "caba-piba-raw-zone-db"."goet_centro_formacion" c
LEFT JOIN "caba-piba-raw-zone-db"."goet_localidades" l ON (c.idlocalidad=l.idlocalidad)
LEFT JOIN "caba-piba-raw-zone-db"."goet_centro_tipo" ct ON (c.idcentrotipo=ct.idtipocentro)
UNION
SELECT
	'SIENFO' base_origen,
    CAST(c.id_centro AS VARCHAR) codigo,
	CAST(c.cue_anexo AS VARCHAR) cue,
	CAST(UPPER(c.nom_centro) AS VARCHAR) nombre_formateado,
	CAST(c.nom_centro AS VARCHAR) nombre_old,
	CAST(c.tipo_centro AS VARCHAR) clave_tipo,
	-- verificar si hay tablas de tipos
	CAST(tc.denominacion AS VARCHAR) tipo,
	-- separar nombre de calle de numero, normalizar
	CAST(c.direccion AS VARCHAR) calle,
	-- separar nombre de calle de numero, normalizar
	CAST(c.direccion AS VARCHAR) numero,
	CAST('' AS VARCHAR) clave_localidad,
	CAST('' AS VARCHAR) localidad,
	CAST('' AS VARCHAR) codigo_postal,
	CAST('' AS VARCHAR) comuna
FROM "caba-piba-raw-zone-db"."sienfo_centros" c
LEFT JOIN "caba-piba-raw-zone-db"."sienfo_tcentro" tc ON (c.tipo_centro=tc.tipo_centro)
LEFT JOIN "caba-piba-raw-zone-db"."sienfo_cgps" cgp ON (c.id_cgp=cgp.id_cgp)
LEFT JOIN "caba-piba-raw-zone-db"."sienfo_distritos" d ON (c.id_distrito=d.id_distrito)
UNION
SELECT
	'CRMSL' base_origen,
	CASE
	WHEN sede LIKE '21-24- CEMAR' THEN cast(crc32(cast('CEMAR 21 24' AS varbinary)) AS varchar)
	WHEN sede LIKE 'Bariro Padre Mugica (Ex 31)' THEN cast(crc32(cast(UPPER('Barrio Padre Mugica (Ex 31)') AS varbinary )) AS varchar)
	WHEN sede LIKE 'Mugica' THEN cast(crc32(cast(UPPER('Barrio Padre Mugica (Ex 31)') AS varbinary )) AS varchar)
	WHEN sede LIKE 'Barrio 15' THEN cast(crc32(cast(UPPER('Villa Lugano - Barrio 15') AS varbinary )) AS varchar)
	WHEN sede LIKE 'Fraga' THEN cast(crc32(cast(UPPER('Barrio Fraga') AS varbinary )) AS varchar)
	WHEN sede LIKE 'Villa Lugano- Barrio 15' THEN cast(crc32(cast(UPPER('Villa Lugano - Barrio 15') AS varbinary )) AS varchar)
	WHEN sede LIKE 'Rodrigo Bueno' THEN cast(crc32(cast(UPPER('Barrio Rodrigo Bueno') AS varbinary )) AS varchar)
	WHEN sede LIKE 'Club Copelo' THEN cast(crc32(cast(UPPER('Club Copello') AS varbinary )) AS varchar)
	WHEN sede LIKE 'Ricchiardelli' THEN cast(crc32(cast(UPPER('Ricciardelli') AS varbinary )) AS varchar)
	ELSE cast(crc32(cast(UPPER(sede) AS varbinary))  AS varchar)
	END codigo,

	CAST('' AS VARCHAR) cue,

	CASE
	WHEN sede LIKE '21-24- CEMAR' THEN 'CEMAR 21 24'
	WHEN sede LIKE 'Bariro Padre Mugica (Ex 31)' THEN UPPER('Barrio Padre Mugica (Ex 31)')
	WHEN sede LIKE 'Mugica' THEN UPPER('Barrio Padre Mugica (Ex 31)')
	WHEN sede LIKE 'Barrio 15' THEN UPPER('Villa Lugano - Barrio 15')
	WHEN sede LIKE 'Fraga' THEN UPPER('Barrio Fraga')
	WHEN sede LIKE 'Villa Lugano- Barrio 15' THEN UPPER('Villa Lugano - Barrio 15')
	WHEN sede LIKE 'Rodrigo Bueno' THEN UPPER('Barrio Rodrigo Bueno')
	WHEN sede LIKE 'Club Copelo' THEN UPPER('Club Copello')
	WHEN sede LIKE 'Ricchiardelli' THEN UPPER('Ricciardelli')
	ELSE UPPER(sede)
	END nombre_formateado,

	CAST(sede AS VARCHAR) nombre_old,

	CAST('' AS VARCHAR) clave_tipo,
	CAST('' AS VARCHAR) tipo,
	CAST('' AS VARCHAR) calle,
	CAST('' AS VARCHAR) numero,
	CAST('' AS VARCHAR) clave_localidad,
	CAST('' AS VARCHAR) localidad,
	CAST('' AS VARCHAR) codigo_postal,

	CASE
	WHEN sede IN ('Bariro Padre Mugica (Ex 31)', 'Mugica', 'Barrio Rodrigo Bueno',
				'Rodrigo Bueno', 'Sum Rivadavia', 'Virtual')  THEN 'Comuna 1'
	WHEN sede IN ('21-24- CEMAR', 'CEMAR 21 24', 'Comedor Los Pochitos',
				'La boca', 'MDHH', 'Nido B 20', 'Zavaleta')  THEN 'Comuna 4'
	WHEN sede IN ('Barrio Charrua', 'Barrio Rivadavia II', 'Ricchiardelli',
				'Ricciardelli')  THEN 'Comuna 7'
	WHEN sede IN ('Barrio 15', 'Villa Lugano- Barrio 15', 'Barrio 20', 'Club Copelo',
				'Club Copello', 'Nido Soldati', 'NIDO Soldati', 'Piedrabuena',
				'Sabina Olmos', 'Soldati')  THEN 'Comuna 8'
	WHEN sede IN ('CildaÃ±ez', 'Lacarra', 'Mataderos')  THEN 'Comuna 9'
	WHEN sede IN ('Barrio Saavedra')  THEN 'Comuna 12'
	WHEN sede IN ('Barrio Fraga', 'Fraga')  THEN 'Comuna 15'
	END comuna

FROM "caba-piba-raw-zone-db"."crm_sociolaboral_op_oportunidades_formacion"
WHERE sede IS NOT NULL AND TRIM(sede) NOT LIKE ''
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13
-- Segun GCBA Moodle tiene como establecimiento el programa Codo a Codo dependiente del Ministerio de EducaciÃ³n
-- https://buenosaires.gob.ar/educacion/codocodo/el-programa
UNION
SELECT
'MOODLE' base_origen,
'PROGRAMACODOACODO' codigo,
'202010' cue,
'PROGRAMA CODO A CODO' nombre_formateado,
'PROGRAMA CODO A CODO' nombre_old,
'3' clave_tipo,
'Otros' tipo,
'Carlos H. Perette y Calle 10' calle,
CAST('' AS VARCHAR) numero,
'2104001' clave_localidad,
'Ciudad de Buenos Aires' localidad,
'1107' codigo_postal,
'Comuna 1' comuna
--</sql>--
-- 2.-- Se crea tabla con domicilios estandarizados
-- --<sql>--DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.tbp_typ_tmp_establecimientos_domicilios_estandarizados;--</sql>--
--<sql>--
CREATE TABLE "caba-piba-staging-zone-db".tbp_typ_tmp_establecimientos_domicilios_estandarizados AS
WITH ECN1 AS (
SELECT
base_origen,
codigo,
cue,
nombre_formateado,
nombre_old,
tipo,
clave_tipo,
clave_localidad,
calle AS nombre_calle_origen,
CASE
     --PatrÃ³n: Campos que contienen la leyenda 'calle'
    WHEN regexp_like(calle,'y calle|y Calle') THEN UPPER(CONCAT(regexp_extract(calle,'(y\s)(CALLE|calle|Calle)(\s?\d?\d?\d?)',2),' ',regexp_extract(calle,'(y\s)(CALLE|calle|Calle)(\s?\d?\d?\d?)',3)))
    WHEN regexp_like(calle,'CALLE|calle|Calle') THEN UPPER(CONCAT(regexp_extract(calle,'(CALLE|calle|Calle)(\s?\d?\d?\d?\s?[A-Za-z]+\s?\d?\d?\d?\d?)',1),' ',regexp_extract(calle,'(CALLE|calle|Calle)(\s?\d?\d?\d?\s?[A-Za-z]+\s?\d?\d?\d?\d?)',2)))
    --PatrÃ³n: Campos con solo datos de manzanas
    WHEN regexp_like(calle,'MANZANA|manzana|Manzana') THEN UPPER(CONCAT(regexp_extract(calle,'(MANZANA|manzana|Manzana)(\s[0-9]+)',1),' ',regexp_extract(calle,'(MANZANA|manzana|Manzana)(\s[0-9]+)',2)))
    --PatrÃ³n:Campos con numero + nombre (ejemplo: 24 de noviembre)
    WHEN regexp_like(calle,'[0-9]+\s\w\w\s[a-zA-Z]+') THEN UPPER(regexp_extract(calle,'[0-9]+\s\w\w\s[a-zA-Z]+'))
    --PatrÃ³n:Campos que hagan referencia a avenidas
    WHEN regexp_like(calle,'AV|AVDA|AVENIDA|Av|Avda|Avenida') THEN UPPER(regexp_extract(replace(replace(replace(calle,'Avda.','Av.'),'Avenida','Av.'),'Avda','Av.'),'(Av.?\s?[a-zA-ZÃÃ¡ÃÃ©ÃÃ­ÃÃ³ÃÃºÃÃ±ÃÃ¤ÃÃ«ÃÃ¯ÃÃ¶ÃÃ¼.?]+\s?[a-zA-ZÃÃ¡ÃÃ©ÃÃ­ÃÃ³ÃÃºÃÃ±ÃÃ¤ÃÃ«ÃÃ¯ÃÃ¶ÃÃ¼.?]+\s?[a-zA-ZÃÃ¡ÃÃ©ÃÃ­ÃÃ³ÃÃºÃÃ±ÃÃ¤ÃÃ«ÃÃ¯ÃÃ¶ÃÃ¼.?]+\s?)',1))
    ELSE UPPER(calle)
END nombre_calle_estandarizada1,
numero AS numero_calle_origen,
localidad,
codigo_postal,
comuna
FROM "caba-piba-staging-zone-db".tbp_typ_tmp_establecimientos_1
),
ECN2 AS (
SELECT
ECN1.base_origen,
ECN1.codigo,
ECN1.cue,
ECN1.nombre_formateado,
ECN1.nombre_old,
ECN1.tipo,
ECN1.clave_tipo,
ECN1.clave_localidad,
ECN1.nombre_calle_origen,
CASE
    --PatrÃ³n:Campos que hagan referencia a avenidas
    WHEN regexp_like(UPPER(ECN1.nombre_calle_origen),'[a-zA-ZÃÃ¡ÃÃ©ÃÃ­ÃÃ³ÃÃºÃÃ±ÃÃ¤ÃÃ«ÃÃ¯ÃÃ¶ÃÃ¼,?.?]+\s?[A-ZÃÃ¡ÃÃ©ÃÃ­ÃÃ³ÃÃºÃÃ±ÃÃ¤ÃÃ«ÃÃ¯ÃÃ¶ÃÃ¼,?.?]+\s?[a-zA-ZÃÃ¡ÃÃ©ÃÃ­ÃÃ³ÃÃºÃÃ±ÃÃ¤ÃÃ«ÃÃ¯ÃÃ¶ÃÃ¼,?.?]+\s?\s?AV.\s?[0-9]+') THEN UPPER(CONCAT('AV. ',regexp_extract(replace(replace(replace(UPPER(ECN1.nombre_calle_origen),'AVDA.','AV.'),'AVENIDA','Av.'),'AVDA','Av.'),'([a-zA-ZÃÃ¡ÃÃ©ÃÃ­ÃÃ³ÃÃºÃÃ±ÃÃ¤ÃÃ«ÃÃ¯ÃÃ¶ÃÃ¼,?.?]+\s?[a-zA-ZÃÃ¡ÃÃ©ÃÃ­ÃÃ³ÃÃºÃÃ±ÃÃ¤ÃÃ«ÃÃ¯ÃÃ¶ÃÃ¼,?.?]+\s?[a-zA-ZÃÃ¡ÃÃ©ÃÃ­ÃÃ³ÃÃºÃÃ±ÃÃ¤ÃÃ«ÃÃ¯ÃÃ¶ÃÃ¼,?.?]+\s?)(\s?AV.)(\s?[0-9]+)',1)))
    WHEN regexp_like(ECN1.nombre_calle_origen,'Av.?\s?[a-zA-ZÃÃ¡ÃÃ©ÃÃ­ÃÃ³ÃÃºÃÃ±ÃÃ¤ÃÃ«ÃÃ¯ÃÃ¶ÃÃ¼.?]+\s?[a-zA-ZÃÃ¡ÃÃ©ÃÃ­ÃÃ³ÃÃºÃÃ±ÃÃ¤ÃÃ«ÃÃ¯ÃÃ¶ÃÃ¼.?]+\s?[a-zA-ZÃÃ¡ÃÃ©ÃÃ­ÃÃ³ÃÃºÃÃ±ÃÃ¤ÃÃ«ÃÃ¯ÃÃ¶ÃÃ¼.?]+\s?[a-zA-ZÃÃ¡ÃÃ©ÃÃ­ÃÃ³ÃÃºÃÃ±ÃÃ¤ÃÃ«ÃÃ¯ÃÃ¶ÃÃ¼.?]+\s?[a-zA-ZÃÃ¡ÃÃ©ÃÃ­ÃÃ³ÃÃºÃÃ±ÃÃ¤ÃÃ«ÃÃ¯ÃÃ¶ÃÃ¼.?]+\s?[0-9]+') THEN UPPER(regexp_extract(replace(replace(replace(ECN1.nombre_calle_origen,'Avda.','Av.'),'Avenida','Av.'),'Avda','Av.'),'(Av.?\s?[a-zA-ZÃÃ¡ÃÃ©ÃÃ­ÃÃ³ÃÃºÃÃ±ÃÃ¤ÃÃ«ÃÃ¯ÃÃ¶ÃÃ¼.?]+\s?[a-zA-ZÃÃ¡ÃÃ©ÃÃ­ÃÃ³ÃÃºÃÃ±ÃÃ¤ÃÃ«ÃÃ¯ÃÃ¶ÃÃ¼.?]+\s?[a-zA-ZÃÃ¡ÃÃ©ÃÃ­ÃÃ³ÃÃºÃÃ±ÃÃ¤ÃÃ«ÃÃ¯ÃÃ¶ÃÃ¼.?]+\s?[a-zA-ZÃÃ¡ÃÃ©ÃÃ­ÃÃ³ÃÃºÃÃ±ÃÃ¤ÃÃ«ÃÃ¯ÃÃ¶ÃÃ¼.?]+\s?[a-zA-ZÃÃ¡ÃÃ©ÃÃ­ÃÃ³ÃÃºÃÃ±ÃÃ¤ÃÃ«ÃÃ¯ÃÃ¶ÃÃ¼.?]+)(\s?[0-9]+)',1))
    WHEN regexp_like(ECN1.nombre_calle_origen,'AV|AVDA|AVENIDA|Av|Avda|Avenida') AND ECN1.nombre_calle_origen LIKE '% y %' THEN UPPER(regexp_extract(replace(replace(replace(ECN1.nombre_calle_origen,'Avda.','Av.'),'Avenida','Av.'),'Avda','Av.'),'Av.?\s?[a-zA-ZÃÃ¡ÃÃ©ÃÃ­ÃÃ³ÃÃºÃÃ±ÃÃ¤ÃÃ«ÃÃ¯ÃÃ¶ÃÃ¼.?]+\s?[a-zA-ZÃÃ¡ÃÃ©ÃÃ­ÃÃ³ÃÃºÃÃ±ÃÃ¤ÃÃ«ÃÃ¯ÃÃ¶ÃÃ¼.?]+\s?[a-zA-ZÃÃ¡ÃÃ©ÃÃ­ÃÃ³ÃÃºÃÃ±ÃÃ¤ÃÃ«ÃÃ¯ÃÃ¶ÃÃ¼.?]+\s?[a-zA-ZÃÃ¡ÃÃ©ÃÃ­ÃÃ³ÃÃºÃÃ±ÃÃ¤ÃÃ«ÃÃ¯ÃÃ¶ÃÃ¼.?]+'))
    --PatrÃ³n: Campos con nombre (con puntos) + numero
    WHEN UPPER(ECN1.nombre_calle_origen) LIKE '%HUMBERTO%' THEN 'HUMBERTO 1Â°'
    WHEN regexp_like(ECN1.nombre_calle_estandarizada1,'[a-zA-ZÃÃ¡ÃÃ©ÃÃ­ÃÃ³ÃÃºÃÃ±ÃÃ¤ÃÃ«ÃÃ¯ÃÃ¶ÃÃ¼.?\Â°?\Âº?]+\s?[a-zA-ZÃÃ¡ÃÃ©ÃÃ­ÃÃ³ÃÃºÃÃ±ÃÃ¤ÃÃ«ÃÃ¯ÃÃ¶ÃÃ¼.?Â°?]+\s?\,?\.?\s?[a-zA-ZÃÃ¡ÃÃ©ÃÃ­ÃÃ³ÃÃºÃÃ±ÃÃ¤ÃÃ«ÃÃ¯ÃÃ¶ÃÃ¼.?\Â°?\Âº?]+\s?\,?\.?\s?[a-zA-ZÃÃ¡ÃÃ©ÃÃ­ÃÃ³ÃÃºÃÃ±ÃÃ¤ÃÃ«ÃÃ¯ÃÃ¶ÃÃ¼.?\Â°?\Âº?]+\s?[0-9]+') THEN UPPER(regexp_extract(replace(replace(replace(ECN1.nombre_calle_estandarizada1,'Avda.','Av.'),'Avenida','Av.'),'Avda','Av.'),'([a-zA-ZÃÃ¡ÃÃ©ÃÃ­ÃÃ³ÃÃºÃÃ±ÃÃ¤ÃÃ«ÃÃ¯ÃÃ¶ÃÃ¼.?Â°?]+\s?[a-zA-ZÃÃ¡ÃÃ©ÃÃ­ÃÃ³ÃÃºÃÃ±ÃÃ¤ÃÃ«ÃÃ¯ÃÃ¶ÃÃ¼.?\Â°?\Âº?]+\s?\,?\.?\s?[a-zA-ZÃÃ¡ÃÃ©ÃÃ­ÃÃ³ÃÃºÃÃ±ÃÃ¤ÃÃ«ÃÃ¯ÃÃ¶ÃÃ¼.?\Â°?\Âº?]+\s?\,?\.?\s?[a-zA-ZÃÃ¡ÃÃ©ÃÃ­ÃÃ³ÃÃºÃÃ±ÃÃ¤ÃÃ«ÃÃ¯ÃÃ¶ÃÃ¼.?\Â°?\Âº?]+)(\s?[0-9]+)',1))
    --PatrÃ³n: Campos que hagan referencia a una misma direcciones con diferente expresion
    WHEN UPPER(ECN1.nombre_calle_origen) LIKE '%PERETTE%' THEN UPPER(CONCAT('CARLOS H. PERETTE',' ',regexp_extract(UPPER(ECN1.nombre_calle_origen),'(PERETTE)(\s?Y\sCALLE\s?\d?\d?)',2)))
    ELSE ECN1.nombre_calle_estandarizada1
END nombre_calle_estandarizada2,
ECN1.numero_calle_origen,
ECN1.localidad,
ECN1.codigo_postal,
ECN1.comuna
FROM ECN1
),
ECN3 AS (
SELECT
ECN2.base_origen,
ECN2.codigo,
ECN2.cue,
ECN2.nombre_formateado,
ECN2.nombre_old,
ECN2.tipo,
ECN2.clave_tipo,
ECN2.clave_localidad,
ECN2.nombre_calle_origen,
ECN2.nombre_calle_estandarizada2,
CASE
    --PatrÃ³n: Campos que hagan referencia a una misma direcciones con diferente expresion
    WHEN ECN2.nombre_calle_estandarizada2 LIKE '%BEIRÃ%' THEN 'AV. FRANCISCO BEIRÃ'
    WHEN ECN2.nombre_calle_estandarizada2 LIKE '%RIVADAVIA%' THEN 'AV. RIVADAVIA'
    WHEN ECN2.nombre_calle_estandarizada2 LIKE '%SANTA FE%' THEN 'AV. SANTA FE'
    --Ajustes
    WHEN ECN2.nombre_calle_estandarizada2 LIKE '%NÂº%' OR ECN2.nombre_calle_estandarizada2 LIKE '%NÂ°%' THEN replace(replace(ECN2.nombre_calle_estandarizada2,'NÂº',''),'NÂ°','')
    WHEN UPPER(ECN2.nombre_calle_origen) LIKE '% Y %' AND ECN2.nombre_calle_estandarizada2 LIKE '%MÃDULO%' THEN UPPER(regexp_extract(replace(replace(replace(ECN2.nombre_calle_origen,'Avda.','Av.'),'Avenida','Av.'),'Avda','Av.'),'([a-zA-ZÃÃ¡ÃÃ©ÃÃ­ÃÃ³ÃÃºÃÃ±ÃÃ¤ÃÃ«ÃÃ¯ÃÃ¶ÃÃ¼.Â°?\s?]+)(\-[a-zA-ZÃÃ¡ÃÃ©ÃÃ­ÃÃ³ÃÃºÃÃ±ÃÃ¤ÃÃ«ÃÃ¯ÃÃ¶ÃÃ¼0-9?\,?\s?]+)',1))
    WHEN UPPER(ECN2.nombre_calle_origen) LIKE '% Y %' AND ECN2.nombre_calle_estandarizada2 LIKE '%. MZ%'  THEN UPPER(regexp_extract(replace(replace(replace(ECN2.nombre_calle_origen,'Avda.','Av.'),'Avenida','Av.'),'Avda','Av.'),'([a-zA-ZÃÃ¡ÃÃ©ÃÃ­ÃÃ³ÃÃºÃÃ±ÃÃ¤ÃÃ«ÃÃ¯ÃÃ¶ÃÃ¼.Â°?\s?]+)(\.[a-zA-ZÃÃ¡ÃÃ©ÃÃ­ÃÃ³ÃÃºÃÃ±ÃÃ¤ÃÃ«ÃÃ¯ÃÃ¶ÃÃ¼0-9?\,?\s?]+)',1))
    WHEN ECN2.nombre_calle_estandarizada2 LIKE 'LATITUD' THEN 'S/D'
    --PatrÃ³n: Campos con nombre (con doble espacio) + numero
    WHEN ECN2.nombre_calle_origen LIKE '%  %' THEN UPPER(regexp_extract(ECN2.nombre_calle_origen,'([a-zA-ZÃÃ¡ÃÃ©ÃÃ­ÃÃ³ÃÃºÃÃ±ÃÃ¤ÃÃ«ÃÃ¯ÃÃ¶ÃÃ¼.?Â°?]+\s?[a-zA-ZÃÃ¡ÃÃ©ÃÃ­ÃÃ³ÃÃºÃÃ±ÃÃ¤ÃÃ«ÃÃ¯ÃÃ¶ÃÃ¼.?\Â°?\Âº?]+)(\s?\s?[0-9]+)',1))
    ELSE ECN2.nombre_calle_estandarizada2
END nombre_calle_estandarizada3,
ECN2.numero_calle_origen,
ECN2.localidad,
ECN2.codigo_postal,
ECN2.comuna
FROM ECN2
),
-- Se realizan ajustes finales a la estandarizaciÃ³n de nombres de calles y se estandarizan los numeros de calles
ECN4 AS (
SELECT
ECN3.base_origen,
ECN3.codigo,
ECN3.cue,
ECN3.nombre_formateado,
ECN3.nombre_old,
ECN3.tipo,
ECN3.clave_tipo,
ECN3.clave_localidad,
ECN3.nombre_calle_origen,
CASE
    --PatrÃ³n: Solo numeros
    WHEN regexp_like(ECN3.nombre_calle_estandarizada3,'[^A-ZÃÃ¡ÃÃ©ÃÃ­ÃÃ³ÃÃºÃÃ±ÃÃ¤ÃÃ«ÃÃ¯ÃÃ¶ÃÃ¼.?\Â°?\Âº?\s?\,?]+') AND ECN3.nombre_calle_estandarizada3 NOT LIKE '%MANZANA%' AND ECN3.nombre_calle_estandarizada3 NOT LIKE '%HUMBERTO%' AND ECN3.nombre_calle_estandarizada3 NOT LIKE '%DE%' AND ECN3.nombre_calle_estandarizada3 NOT LIKE '%CALLE%' AND ECN3.nombre_calle_estandarizada3 NOT LIKE '%S/D%' THEN 'S/D'
    WHEN LENGTH(ECN3.nombre_calle_estandarizada3) = 0 OR ECN3.nombre_calle_estandarizada3 IS NULL THEN 'S/D'
    --Ajuste final
    WHEN ECN3.nombre_calle_estandarizada3 LIKE '%  %' THEN  replace(ECN3.nombre_calle_estandarizada3,'  ',' ')
    ELSE ECN3.nombre_calle_estandarizada3
END nombre_calle_estandarizada,
ECN3.numero_calle_origen,
CASE
    --PatrÃ³n: Campos que contengan mas de 1 altura (ejemplo 1234/5678)
    WHEN ECN3.numero_calle_origen LIKE '%/%' AND ECN3.numero_calle_origen NOT LIKE 'S/D' THEN regexp_extract(ECN3.numero_calle_origen,'([a-zA-ZÃÃ¡ÃÃ©ÃÃ­ÃÃ³ÃÃºÃÃ±ÃÃ¤ÃÃ«ÃÃ¯ÃÃ¶ÃÃ¼\Â°?\Âº?\s?\,?.?]+)([0-9]+)(\/\s?[0-9]+)',2)
    --PatrÃ³n: Campos que contengan la leyenda "CASA" o "NÂ°"
    WHEN regexp_like(UPPER(ECN3.numero_calle_origen),'CASA\s?[0-9]+') THEN regexp_extract(UPPER(ECN3.numero_calle_origen),'(CASA)(\s?[0-9]+)',2)
    WHEN regexp_like(UPPER(ECN3.numero_calle_origen),'C\s?[0-9]+') THEN regexp_extract(UPPER(ECN3.numero_calle_origen),'(C)(\s?[0-9]+)',2)
    WHEN regexp_like(UPPER(ECN3.numero_calle_origen),'NÂ°\s?[0-9]+') THEN regexp_extract(UPPER(ECN3.numero_calle_origen),'(NÂ°)(\s?[0-9]+)',2)
    --PatrÃ³n: Campos con solo datos de manzana
    WHEN UPPER(ECN3.numero_calle_origen) LIKE '%MANZANA%' THEN replace(ECN3.nombre_calle_estandarizada3,'  ',' ')
    --PatrÃ³n:Campos con numero + nombre (ejemplo: 24 de noviembre)
    WHEN regexp_like(ECN3.numero_calle_origen,'[0-9]+\s\w\w\s[a-zA-Z]+\s?[0-9]+') THEN UPPER(regexp_extract(ECN3.numero_calle_origen,'([0-9]+\s\w\w\s[a-zA-Z]+)(\s?[0-9]+)',2))
    ELSE UPPER(ECN3.numero_calle_origen)
END numero_calle_estandarizada1,
ECN3.localidad,
ECN3.codigo_postal,
ECN3.comuna
FROM ECN3
),
ECN5 AS (
SELECT
ECN4.base_origen,
ECN4.codigo,
ECN4.cue,
ECN4.nombre_formateado,
ECN4.nombre_old,
ECN4.tipo,
ECN4.clave_tipo,
ECN4.clave_localidad,
ECN4.nombre_calle_origen,
ECN4.nombre_calle_estandarizada,
ECN4.numero_calle_origen,
ECN4.numero_calle_estandarizada1,
CASE
    --PatrÃ³n:Campos que hagan referencia a avenidas
    WHEN ECN4.numero_calle_estandarizada1 LIKE '%AV. %' OR ECN4.numero_calle_estandarizada1 LIKE '%AVDA. %' THEN UPPER(regexp_extract(ECN4.numero_calle_estandarizada1,'(AV.?\s?[A-ZÃÃ¡ÃÃ©ÃÃ­ÃÃ³ÃÃºÃÃ±ÃÃ¤ÃÃ«ÃÃ¯ÃÃ¶ÃÃ¼.?]+\s?[A-ZÃÃ¡ÃÃ©ÃÃ­ÃÃ³ÃÃºÃÃ±ÃÃ¤ÃÃ«ÃÃ¯ÃÃ¶ÃÃ¼.?]+\s?[A-ZÃÃ¡ÃÃ©ÃÃ­ÃÃ³ÃÃºÃÃ±ÃÃ¤ÃÃ«ÃÃ¯ÃÃ¶ÃÃ¼.?]+\s?[A-ZÃÃ¡ÃÃ©ÃÃ­ÃÃ³ÃÃºÃÃ±ÃÃ¤ÃÃ«ÃÃ¯ÃÃ¶ÃÃ¼.?]+)(\s?\s?[0-9]+)',2))
    WHEN ECN4.nombre_calle_estandarizada LIKE '%AV. %' THEN UPPER(regexp_extract(UPPER(ECN4.numero_calle_origen),'(AV.?\s?[A-ZÃÃ¡ÃÃ©ÃÃ­ÃÃ³ÃÃºÃÃ±ÃÃ¤ÃÃ«ÃÃ¯ÃÃ¶ÃÃ¼.?]+\s?[a-zA-ZÃÃ¡ÃÃ©ÃÃ­ÃÃ³ÃÃºÃÃ±ÃÃ¤ÃÃ«ÃÃ¯ÃÃ¶ÃÃ¼.?]+\s?[A-ZÃÃ¡ÃÃ©ÃÃ­ÃÃ³ÃÃºÃÃ±ÃÃ¤ÃÃ«ÃÃ¯ÃÃ¶ÃÃ¼.?]+)(\s?[0-9]+)',2))
    ELSE ECN4.numero_calle_estandarizada1
END numero_calle_estandarizada2,
ECN4.localidad,
ECN4.codigo_postal,
ECN4.comuna
FROM ECN4
),
ECN6 AS (
SELECT
ECN5.base_origen,
ECN5.codigo,
ECN5.cue,
ECN5.nombre_formateado,
ECN5.nombre_old,
ECN5.tipo,
ECN5.clave_tipo,
ECN5.clave_localidad,
ECN5.nombre_calle_origen,
ECN5.nombre_calle_estandarizada,
ECN5.numero_calle_origen,
ECN5.numero_calle_estandarizada1,
ECN5.numero_calle_estandarizada2,
CASE
    --PatrÃ³n:Campos que hagan referencia a avenidas
    WHEN UPPER(ECN5.nombre_calle_origen) LIKE '%LATITUD%' THEN 'S/D'
    WHEN regexp_like(ECN5.numero_calle_estandarizada1,'[a-zA-ZÃÃ¡ÃÃ©ÃÃ­ÃÃ³ÃÃºÃÃ±ÃÃ¤ÃÃ«ÃÃ¯ÃÃ¶ÃÃ¼,?.?]+\s?[A-ZÃÃ¡ÃÃ©ÃÃ­ÃÃ³ÃÃºÃÃ±ÃÃ¤ÃÃ«ÃÃ¯ÃÃ¶ÃÃ¼,?.?]+\s?[a-zA-ZÃÃ¡ÃÃ©ÃÃ­ÃÃ³ÃÃºÃÃ±ÃÃ¤ÃÃ«ÃÃ¯ÃÃ¶ÃÃ¼,?.?]+\s?\s?AV.\s?[0-9]+') THEN regexp_extract(ECN5.numero_calle_estandarizada1,'([a-zA-ZÃÃ¡ÃÃ©ÃÃ­ÃÃ³ÃÃºÃÃ±ÃÃ¤ÃÃ«ÃÃ¯ÃÃ¶ÃÃ¼,?.?]+\s?[a-zA-ZÃÃ¡ÃÃ©ÃÃ­ÃÃ³ÃÃºÃÃ±ÃÃ¤ÃÃ«ÃÃ¯ÃÃ¶ÃÃ¼,?.?]+\s?[a-zA-ZÃÃ¡ÃÃ©ÃÃ­ÃÃ³ÃÃºÃÃ±ÃÃ¤ÃÃ«ÃÃ¯ÃÃ¶ÃÃ¼,?.?]+\s?)(\s?AV.)(\s?[0-9]+)',3)
    WHEN regexp_like(ECN5.numero_calle_estandarizada1,'AV.?\s?[A-ZÃÃ¡ÃÃ©ÃÃ­ÃÃ³ÃÃºÃÃ±ÃÃ¤ÃÃ«ÃÃ¯ÃÃ¶ÃÃ¼.?]+\s?[A-ZÃÃ¡ÃÃ©ÃÃ­ÃÃ³ÃÃºÃÃ±ÃÃ¤ÃÃ«ÃÃ¯ÃÃ¶ÃÃ¼.?]+\s?[A-ZÃÃ¡ÃÃ©ÃÃ­ÃÃ³ÃÃºÃÃ±ÃÃ¤ÃÃ«ÃÃ¯ÃÃ¶ÃÃ¼.?]+\s?[A-ZÃÃ¡ÃÃ©ÃÃ­ÃÃ³ÃÃºÃÃ±ÃÃ¤ÃÃ«ÃÃ¯ÃÃ¶ÃÃ¼.?]+\s?[A-ZÃÃ¡ÃÃ©ÃÃ­ÃÃ³ÃÃºÃÃ±ÃÃ¤ÃÃ«ÃÃ¯ÃÃ¶ÃÃ¼.?]+\s?[0-9]+') THEN regexp_extract(ECN5.numero_calle_estandarizada1,'(AV.?\s?[A-ZÃÃ¡ÃÃ©ÃÃ­ÃÃ³ÃÃºÃÃ±ÃÃ¤ÃÃ«ÃÃ¯ÃÃ¶ÃÃ¼.?]+\s?[A-ZÃÃ¡ÃÃ©ÃÃ­ÃÃ³ÃÃºÃÃ±ÃÃ¤ÃÃ«ÃÃ¯ÃÃ¶ÃÃ¼.?]+\s?[A-ZÃÃ¡ÃÃ©ÃÃ­ÃÃ³ÃÃºÃÃ±ÃÃ¤ÃÃ«ÃÃ¯ÃÃ¶ÃÃ¼.?]+\s?[A-ZÃÃ¡ÃÃ©ÃÃ­ÃÃ³ÃÃºÃÃ±ÃÃ¤ÃÃ«ÃÃ¯ÃÃ¶ÃÃ¼.?]+\s?[A-ZÃÃ¡ÃÃ©ÃÃ­ÃÃ³ÃÃºÃÃ±ÃÃ¤ÃÃ«ÃÃ¯ÃÃ¶ÃÃ¼.?]+)(\s?[0-9]+)',2)
    --PatrÃ³n: Campos con nombre (con puntos) + numero
    WHEN ECN5.nombre_calle_estandarizada LIKE '%HUMBERTO%' THEN regexp_extract(ECN5.numero_calle_estandarizada2,'([A-ZÃÃ¡ÃÃ©ÃÃ­ÃÃ³ÃÃºÃÃ±ÃÃ¤ÃÃ«ÃÃ¯ÃÃ¶ÃÃ¼.?]+)(\s?[0-9]+\Â°?\Âº?\s?)([0-9]+)',3)
    WHEN regexp_like(ECN5.numero_calle_estandarizada2,'[a-zA-ZÃÃ¡ÃÃ©ÃÃ­ÃÃ³ÃÃºÃÃ±ÃÃ¤ÃÃ«ÃÃ¯ÃÃ¶ÃÃ¼.?Â°?]+\s?\s?[a-zA-ZÃÃ¡ÃÃ©ÃÃ­ÃÃ³ÃÃºÃÃ±ÃÃ¤ÃÃ«ÃÃ¯ÃÃ¶ÃÃ¼.?\Â°?\Âº?]+\s?\,?\.?\s?\s?[a-zA-ZÃÃ¡ÃÃ©ÃÃ­ÃÃ³ÃÃºÃÃ±ÃÃ¤ÃÃ«ÃÃ¯ÃÃ¶ÃÃ¼.?\Â°?\Âº?]+\s?\s?[0-9]+') THEN regexp_extract(ECN5.numero_calle_estandarizada2,'([a-zA-ZÃÃ¡ÃÃ©ÃÃ­ÃÃ³ÃÃºÃÃ±ÃÃ¤ÃÃ«ÃÃ¯ÃÃ¶ÃÃ¼.?Â°?]+\s?\s?[a-zA-ZÃÃ¡ÃÃ©ÃÃ­ÃÃ³ÃÃºÃÃ±ÃÃ¤ÃÃ«ÃÃ¯ÃÃ¶ÃÃ¼.?\Â°?\Âº?]+\s?\,?\.?\s?\s?[a-zA-ZÃÃ¡ÃÃ©ÃÃ­ÃÃ³ÃÃºÃÃ±ÃÃ¤ÃÃ«ÃÃ¯ÃÃ¶ÃÃ¼.?\Â°?\Âº?]+\s?\s?)([0-9]+)',2)
    WHEN regexp_like(ECN5.numero_calle_estandarizada1,'[^A-ZÃÃ¡ÃÃ©ÃÃ­ÃÃ³ÃÃºÃÃ±ÃÃ¤ÃÃ«ÃÃ¯ÃÃ¶ÃÃ¼.?\Â°?\Âº?\s?\,?]+') AND ECN5.numero_calle_estandarizada1 NOT LIKE '%S/D%' THEN ECN5.numero_calle_estandarizada1
    WHEN LENGTH(ECN5.numero_calle_estandarizada1) > 0 AND ECN5.numero_calle_estandarizada2 IS NULL THEN ECN5.numero_calle_estandarizada1
    ELSE ECN5.numero_calle_estandarizada2
END numero_calle_estandarizada3,
ECN5.localidad,
ECN5.codigo_postal,
ECN5.comuna
FROM ECN5
),
ECN7 AS (
SELECT
ECN6.base_origen,
ECN6.codigo,
ECN6.cue,
ECN6.nombre_formateado,
ECN6.nombre_old,
ECN6.tipo,
ECN6.clave_tipo,
ECN6.clave_localidad,
ECN6.nombre_calle_origen,
ECN6.nombre_calle_estandarizada,
ECN6.numero_calle_origen,
ECN6.numero_calle_estandarizada1,
ECN6.numero_calle_estandarizada2,
ECN6.numero_calle_estandarizada3,
CASE
    --Ajustes adicionales
    WHEN ECN6.numero_calle_estandarizada3 LIKE '%Âº%' OR ECN6.numero_calle_estandarizada3 LIKE '%Â°%' THEN regexp_extract(ECN6.numero_calle_estandarizada3,'([0-9]+)(\s?\d?\Â°?\Âº?\s?)([A-ZÃÃ¡ÃÃ©ÃÃ­ÃÃ³ÃÃºÃÃ±ÃÃ¤ÃÃ«ÃÃ¯ÃÃ¶ÃÃ¼.?\s?\,?]+)',1)
    WHEN LENGTH(ECN6.numero_calle_estandarizada1) > 0 AND ECN6.numero_calle_estandarizada3 IS NULL AND ECN6.nombre_calle_estandarizada LIKE '%HUMBERTO%' THEN regexp_extract(ECN6.numero_calle_estandarizada1,'([0-9]+)(\s?\d?\Â°?\Âº?\s?)',1)
    WHEN regexp_like(ECN6.numero_calle_estandarizada3,'[^0-9\s?]+') AND ECN6.numero_calle_estandarizada3 NOT LIKE '%S/D%' THEN replace(regexp_replace(ECN6.numero_calle_estandarizada3,'[^0-9\s?]+',''),' ','')
    ELSE replace(ECN6.numero_calle_estandarizada3,' ','')
END numero_calle_estandarizada4,
ECN6.localidad,
ECN6.codigo_postal,
ECN6.comuna
FROM ECN6
),
ECN8 AS (
SELECT
ECN7.base_origen,
ECN7.codigo,
ECN7.cue,
ECN7.nombre_formateado,
ECN7.nombre_old,
ECN7.tipo,
ECN7.clave_tipo,
ECN7.clave_localidad,
ECN7.nombre_calle_origen,
ECN7.nombre_calle_estandarizada,
ECN7.numero_calle_origen,
ECN7.numero_calle_estandarizada1,
ECN7.numero_calle_estandarizada3,
ECN7.numero_calle_estandarizada4,
CASE
    --Ajutse final
    WHEN LENGTH(ECN7.numero_calle_estandarizada4) = 0 OR ECN7.numero_calle_estandarizada4 IS NULL THEN 'S/D'
    ELSE ECN7.numero_calle_estandarizada4
END numero_calle_estandarizada,
ECN7.localidad,
ECN7.codigo_postal,
ECN7.comuna
FROM ECN7
),
ECNF AS (
SELECT
ECN8.base_origen,
ECN8.codigo,
ECN8.cue,
ECN8.nombre_formateado,
ECN8.nombre_old,
ECN8.tipo,
ECN8.clave_tipo,
ECN8.clave_localidad,
ECN8.nombre_calle_origen,
ECN8.numero_calle_origen,
ECN8.nombre_calle_estandarizada,
ECN8.numero_calle_estandarizada,
ECN8.localidad,
ECN8.codigo_postal,
ECN8.comuna
FROM ECN8
GROUP BY
ECN8.base_origen,
ECN8.codigo,
ECN8.cue,
ECN8.nombre_formateado,
ECN8.nombre_old,
ECN8.tipo,
ECN8.clave_tipo,
ECN8.clave_localidad,
ECN8.nombre_calle_origen,
ECN8.numero_calle_origen,
ECN8.nombre_calle_estandarizada,
ECN8.numero_calle_estandarizada,
ECN8.localidad,
ECN8.codigo_postal,
ECN8.comuna
)
SELECT ECNF.*
FROM ECNF;
--</sql>--
-- 3.-- Se crea tabla tbp_typ_tmp_establecimientos cruzando tabla de domicilios estandarizados con
-- informacion proveniente de:
-- # https://www.argentina.gob.ar/educacion/evaluacion-e-informacion-educativa/padron-oficial-de-establecimientos-educativos
-- # https://data.educacion.gob.ar/
-- # https://data.buenosaires.gob.ar/dataset/establecimientos-educativos
-- esta tabla serÃ¡ la ultima tabla tmp antes de crear la tabla consume
-- --<sql>--DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_tmp_establecimientos`;--</sql>--
--<sql>--
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_establecimientos" AS
SELECT e.base_origen,
	e.codigo,
	CASE
	WHEN te.cue IS NOT NULL AND length(te.cue)=6 THEN te.cue
	WHEN e.cue IS NOT NULL AND length(e.cue)=6 THEN e.cue
	WHEN e.cue IS NOT NULL AND length(e.cue)=8 THEN substr(e.cue,1,6)
	END cue,
	e.nombre_formateado,
	e.nombre_old,
	-- nombre del listado oficial de establecimientos
	te.nombre AS nombre_data,
	e.tipo,
	e.clave_tipo,
	e.nombre_calle_estandarizada calle,
	e.numero_calle_estandarizada numero,
	Upper(te."domicilio_establecimiento_fuente_data_bs") dom_completo,
	-- todos los establecimientos son de CABA
	'CABA' localidad,
	e.clave_localidad,
	coalesce(te.cp, e.codigo_postal) codigo_postal,
	coalesce(te.departamento, e.comuna) comuna,
	-- SE GENERA UNA COLUMNA DE ORDEN DE DUPLICADOS, SIENDO EL MAS COINCIDENTE EL QUE TIENE EL NOMBRE DE ESTABLECIMIENTO
	-- MAS PARECIDO Y UN NUMERO DE ANEXO MENOR
	ROW_NUMBER() OVER(
		PARTITION BY(
			Concat(
				e."nombre_calle_estandarizada",
				' ',
				e."numero_calle_estandarizada"
			)
		)
		ORDER BY (
				(
					Cast(
						Greatest(
							Length(e."nombre_old"),
							Length(Upper(te."nombre"))
						) AS DOUBLE
					) - Cast(
						Levenshtein_distance(
							Upper(e."nombre_old"),
							Upper(te."nombre")
						) AS DOUBLE
					)
				) / Cast(
					Greatest(
						Length(e."nombre_old"),
						Length(Upper(te."nombre"))
					) AS DOUBLE
				)
			) DESC,
			CAST(te.anexo AS INT) ASC
	) AS "orden_duplicado"
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_establecimientos_domicilios_estandarizados" e
	-- SE HACE JOIN POR SIMILITUD DE DIRECCIONES >= 80%
	LEFT JOIN "caba-piba-raw-zone-db"."tbp_typ_tmp_data_efectores" te ON (
		(
			(
				Cast(
					Greatest(
						Length(
							Concat(
								e."nombre_calle_estandarizada",
								' ',
								e."numero_calle_estandarizada"
							)
						),
						Length(
							Upper(te."domicilio_establecimiento_fuente_data_bs")
						)
					) AS DOUBLE
				) - Cast(
					Levenshtein_distance(
						Upper(
							Concat(
								e."nombre_calle_estandarizada",
								' ',
								e."numero_calle_estandarizada"
							)
						),
						Upper(te."domicilio_establecimiento_fuente_data_bs")
					) AS DOUBLE
				)
			) / Cast(
				Greatest(
					Length(
						Concat(
							e."nombre_calle_estandarizada",
							' ',
							e."numero_calle_estandarizada"
						)
					),
					Length(
						Upper(te."domicilio_establecimiento_fuente_data_bs")
					)
				) AS DOUBLE
			)
		) >= 0.8
	)
--</sql>--



-- Copy of 2023.05.24 step 03 - consume establecimiento.sql 



-- ESTABLECIMIENTO GOET, MOODLE, SIENFO, CRMSL Y SIU
-- 1.- Crear tabla establecimientos definitiva
-- --<sql>--DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_def_establecimientos`;--</sql>--
--<sql>--
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_def_establecimientos" AS
SELECT
	row_number() OVER () AS id,
	tmp.*
FROM (
	SELECT
		base_origen,
		codigo id_old,
		cue,
		nombre_formateado nombre,
		nombre_data nombre_publicado,
		array_join(array_agg(DISTINCT(nombre_old)), ',') nombres_old,
		tipo,
		calle,
		numero,
		'CABA' localidad,
		codigo_postal,
		-- se elimina la palabra "comuna"
		-- luego se convierte a un numero entero y si es 0 se lo cambia por null
		CASE
		WHEN TRY_CAST(replace(comuna, 'Comuna ') AS int) = 0 THEN
			NULL
		ELSE
			TRY_CAST(replace(comuna, 'Comuna ') AS int)
		END comuna
	FROM
		(SELECT * FROM
		"caba-piba-staging-zone-db"."tbp_typ_tmp_establecimientos"
		WHERE orden_duplicado = 1) sin_duplicados
	GROUP BY 1,2,3,4,5,7,8,9,10,11,12
	) tmp
ORDER BY tmp.base_origen, tmp.nombre
--</sql>--



-- Copy of 2023.05.24 step 04 - staging capacitacion asi.sql 



-- 1.- Crear tabla capacitacion
-- --<sql>--DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_tmp_capacitacion_asi`;--</sql>--
--<sql>--
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_capacitacion_asi" AS
SELECT c.id capacitacion_id,
       c.codigo codigo_capacitacion,
       c.nombre descrip_capacitacion,
       c.tipo_formacion_origen_id tipo_formacion,
	   c.programa_id,
	   p.base_origen,
       tf.nombre descrip_tipo_formacion,
	   c.modalidad_id,
	   m.nombre descrip_modalidad,
	   c.categoria_back_id,
	   cb.nombre descrip_back,
	   CASE WHEN cb.tipo = 'sector_estrategico_asociado' THEN 'SECTOR PRODUCTIVO'
		    ELSE UPPER(cb.tipo) END tipo_capacitacion,
	   c.categoria_front_id,
	   cf.nombre descrip_front,
	   c.detalle detalle_capacitacion,
	   c.estado estado_capacitacion,
       cd.seguimiento_personalizado,
       cd.soporte_online,
       cd.incentivos_terminalidad,
	   cd.exclusividad_participantes,
	   cd.otorga_certificado,
	   cd.filtro_ingreso,
	   cd.frecuencia_oferta_anual,
       c.duracion_cantidad,
       c.duracion_medida,
       c.duracion_dias,
       cd.duracion_hs_reloj,
	   cd.vacantes
FROM "caba-piba-raw-zone-db"."api_asi_capacitacion" c
INNER JOIN "caba-piba-raw-zone-db"."api_asi_tipo_formacion_origen" tf
ON (tf.id = c.tipo_formacion_origen_id)
LEFT JOIN "caba-piba-raw-zone-db"."api_asi_capacitacion_data_lake" cd
ON (cd.id = c.id)
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_programa" p ON (p.programa_id = c.programa_id)
LEFT JOIN "caba-piba-raw-zone-db"."api_asi_categoria_back" cb ON (cb.id = c.categoria_back_id)
LEFT JOIN "caba-piba-raw-zone-db"."api_asi_categoria_front" cf ON (cf.id = c.categoria_front_id)
LEFT JOIN "caba-piba-raw-zone-db"."api_asi_modalidad" m ON (m.id = c.modalidad_id)
--</sql>--
-- 2.-  Crear tabla aptitudes para cada capacitacion
-- --<sql>--DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_tmp_aptitudes_asi`;--</sql>--
--<sql>--
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_aptitudes_asi" AS
SELECT c.id capacitacion_id,
       c.codigo codigo_capacitacion,
       ac.aptitud_id,
       a.nombre descrip_aptitud
FROM "caba-piba-raw-zone-db"."api_asi_capacitacion" c
INNER JOIN "caba-piba-raw-zone-db"."api_asi_aptitud_capacitacion" ac
ON (ac.capacitacion_id = c.id)
INNER JOIN "caba-piba-raw-zone-db"."api_asi_aptitud" a
ON (ac.aptitud_id = a.id)
--</sql>--



-- Copy of 2023.05.24 step 05 - staging capacitacion.sql 



-- CRM SOCIOLABORAL - CRMSL
--1.-- Crear tabla capacitaciones socio laboral
-- --<sql>--DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_tmp_crmsl_1`;--</sql>--
--<sql>--
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_crmsl_1" AS

SELECT 'CRMSL' AS base_origen,
	'CURSO' AS tipo_capacitacion,
	CAST(OF.id AS VARCHAR) AS capacitacion_id,
	TRIM(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(UPPER(CASE
																					WHEN TRIM(UPPER(OF.area)) IN (
																							'GESTIÓN COMERCIAL',
																							'LIMPIEZA Y MANTENIMIENTO',
																							'MOZO Y CAMARERA',
																							'OPERARIO CALIFICADO'
																							)
																						THEN OF.area
																					WHEN OF.name LIKE '%|%'
																						THEN split_part(OF.name, '|', 2)
																					WHEN OF.name LIKE '%/%'
																						THEN split_part(OF.name, '/', 2)
																					ELSE OF.name
																					END), CHR(160), ' '), 'Á', 'A'), 'É', 'E'), 'Í', 'I'), 'Ó', 'O'), '[ÚÜ]', 'U'), '[0-9]+', ''), 'TURNOS?|MAÑANA|NOCHE|TARDE', ''), 'LUNES|MARTES|MIERCOLES|JUEVES|VIERNES', ''), 'ENERO|FEBRERO|MARZO|ABRIL|MAYO|JUNIO|JUILO|AGOSTO|SEPTIEMBRE|OCTUBRE|NOVIEMBRE|DICIEMBRE', ''), 'BARRIO|PLAYON *DE *CHACARITA|RODRIGO *BUENO|MUGICA|RICCIARDELLI|FRAGA|MATADEROS|LACARRA|ZAVALETA|PUERTA|CHARRUA|SOLDATI', ''), 'EDICION|º|IRAM|GCBA|AKOMPANI|ARLOG|COOKMASTER', ''), '(CURSO|CAPACITACION) (EN|DE)?', ''), '[.\-]', ' '), 'AIRES ACONDICIONADOS', 'AIRE ACONDICIONADO'), '\( \)| Y *$', ''), ' +', ' ')) AS descrip_normalizada,
	OF.name AS descrip_capacitacion,
	OF.inicio AS fecha_inicio_dictado,
	OF.fin AS fecha_fin_dictado,
	CASE
		WHEN UPPER(OF.estado_curso) = 'FINALIZADA'
			THEN 0
		ELSE 1
		END AS estado,
	TRIM(UPPER(OF.area)) AS categoria
FROM "caba-piba-raw-zone-db"."crm_sociolaboral_op_oportunidades_formacion" OF;
--</sql>--

--2.-- Crear tabla maestro capacitaciones socio laboral
-- A) Si hay alguna capacitacion activa:
-- fecha_inicio => min(fecha de inicio de capacitacion activa)
-- fecha fin => null
-- B) Si NO hay alguna capacitacion activa:
-- fecha_inicio => min(fecha de inicio de capacitaciones)
-- fecha fin => min(fecha de fin de capacitaciones)
-- Si no hay valor para fecha_inicio o fecha_fin quedan en NULL

-- --<sql>--DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_tmp_crmsl_capacitaciones`;--</sql>--
--<sql>--
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_crmsl_capacitaciones" AS

SELECT ROW_NUMBER() OVER () AS id,
	c_crmsl.*
FROM (
	SELECT s1.base_origen,
		s1.tipo_capacitacion,
		s1.descrip_normalizada,
		MIN(s1.fecha_inicio_dictado) AS fecha_inicio,
		NULL AS fecha_fin,
		'ACTIVO' AS estado
	FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_crmsl_1" s1
	GROUP BY s1.base_origen,
		s1.tipo_capacitacion,
		s1.descrip_normalizada
	HAVING SUM(s1.estado) > 0

	UNION

	SELECT s1.base_origen,
		s1.tipo_capacitacion,
		s1.descrip_normalizada,
		MIN(s1.fecha_inicio_dictado) AS fecha_inicio,
		MAX(s1.fecha_fin_dictado) AS fecha_fin,
		'BAJA' AS estado
	FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_crmsl_1" s1
	GROUP BY s1.base_origen,
		s1.tipo_capacitacion,
		s1.descrip_normalizada
	HAVING SUM(s1.estado) = 0
	) c_crmsl
ORDER BY c_crmsl.descrip_normalizada;
--</sql>--

--3.-- Crear tabla match capacitaciones socio laboral
-- --<sql>--DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_tmp_crmsl_match`;--</sql>--
--<sql>--
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_crmsl_match" AS

SELECT sc.base_origen,
	sc.tipo_capacitacion,
	sc.id AS id_new,
	s1.capacitacion_id AS id_old
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_crmsl_1" s1
INNER JOIN "caba-piba-staging-zone-db"."tbp_typ_tmp_crmsl_capacitaciones" sc ON (
		sc.tipo_capacitacion = s1.tipo_capacitacion
		AND sc.descrip_normalizada = s1.descrip_normalizada
		);
--</sql>--

-- SIENFO
--4.-- Crear tabla capacitaciones sienfo
-- --<sql>--DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_tmp_sienfo_1`;--</sql>--
--<sql>--
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_sienfo_1" AS

SELECT 'SIENFO' AS base_origen,
	'CARRERA' AS tipo_capacitacion,
	-- CAST(ca.id_carrera AS VARCHAR) AS capacitacion_id,
	CAST(ca.id_carrera AS VARCHAR) || '-' || CAST(ca.id_carrera AS VARCHAR) AS capacitacion_id,
	TRIM(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(UPPER(ca.nom_carrera), CHR(160), ' '), 'Á', 'A'), 'É', 'E'), 'Í', 'I'), 'Ó', 'O'), '[ÚÜ]', 'U'), ' +', ' ')) AS descrip_normalizada,
	UPPER(ca.nom_carrera) AS descrip_capacitacion,
	MIN(date_parse(CASE
				WHEN t.fecha = '0000-00-00'
					THEN NULL
				ELSE t.fecha
				END, '%Y-%m-%d')) AS fecha_inicio_dictado,
	MAX(date_parse(CASE
				WHEN t.fecha_fin = '0000-00-00'
					THEN NULL
				ELSE t.fecha_fin
				END, '%Y-%m-%d')) AS fecha_fin_dictado,
	CASE
		WHEN SUM(CASE
					WHEN UPPER(te.nombre) = 'ACTIVO'
						THEN 1
					ELSE 0
					END) > 0
			THEN 1
		ELSE 0
		END AS estado,
	UPPER(tc.nom_categoria) AS categoria
FROM "caba-piba-raw-zone-db"."sienfo_carreras" ca
INNER JOIN "caba-piba-raw-zone-db"."sienfo_talleres" t ON (t.id_carrera = ca.id_carrera)
LEFT JOIN "caba-piba-raw-zone-db"."sienfo_testado" te ON (ca.estado = te.valor) -- REVISAR
LEFT JOIN "caba-piba-raw-zone-db"."sienfo_tcategorias" tc ON (ca.id_tcarrera = tc.id_tcategoria)
WHERE COALESCE(t.id_carrera, 0) != 0
GROUP BY CAST(ca.id_carrera AS VARCHAR),
	ca.nom_carrera,
	UPPER(tc.nom_categoria)

UNION

SELECT 'SIENFO' AS base_origen,
	'CURSO' AS tipo_capacitacion,
	CAST(COALESCE(t.id_carrera, 0) AS VARCHAR) || '-' || CAST(cu.id_curso AS VARCHAR) AS capacitacion_id,
	TRIM(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(UPPER(cu.nom_curso), CHR(160), ' '), 'Á', 'A'), 'É', 'E'), 'Í', 'I'), 'Ó', 'O'), '[ÚÜ]', 'U'), '\(ANCHORENA\)|\(ARANGUREN\)|\(ARANGUREN-JUNCAL\)|\(ARANGUREN-MANANTIALES\)|\(CORRALES\)|\(DON BOSCO\)|\(INTEGRADOR ALMAFUERTE\)|\(JUAN A. GARCIA\)|\(JUNCAL\)|\(LAMBARE\)|\(NIDO SOLDATI-CARRILLO\)|\(PRINGLES\)|\(RETIRO NORTE\)|\(RETIRO\)|\(SAN NICOLAS\)|\(SARAZA\)|\(SEIS ESQUINAS\)|HT18', ''), ' +', ' ')) AS descrip_normalizada,
	UPPER(cu.nom_curso) AS descrip_capacitacion,
	MIN(date_parse(CASE
				WHEN t.fecha = '0000-00-00'
					THEN NULL
				ELSE t.fecha
				END, '%Y-%m-%d')) AS fecha_inicio_dictado,
	MAX(date_parse(CASE
				WHEN t.fecha_fin = '0000-00-00'
					THEN NULL
				ELSE t.fecha_fin
				END, '%Y-%m-%d')) AS fecha_fin_dictado,
	CASE
		WHEN SUM(CASE
					WHEN UPPER(te.nombre) = 'ACTIVO'
						THEN 1
					ELSE 0
					END) > 0
			THEN 1
		ELSE 0
		END AS estado,
	UPPER(tc.nom_categoria)
FROM "caba-piba-raw-zone-db"."sienfo_cursos" cu
INNER JOIN "caba-piba-raw-zone-db"."sienfo_talleres" t ON (t.id_curso = cu.id_curso)
LEFT JOIN "caba-piba-raw-zone-db"."sienfo_testado" te ON (t.estado = te.valor)
LEFT JOIN "caba-piba-raw-zone-db"."sienfo_tcategorias" tc ON (tc.id_tcategoria = cu.id_tcategoria)
WHERE COALESCE(t.id_carrera, 0) = 0
GROUP BY CAST(COALESCE(t.id_carrera, 0) AS VARCHAR) || '-' || CAST(cu.id_curso AS VARCHAR),
	cu.nom_curso,
	UPPER(tc.nom_categoria);
--</sql>--

--5.-- Crear tabla maestro capacitaciones sienfo
-- A) Si hay alguna capacitacion activa:
-- fecha_inicio => min(fecha de inicio de capacitacion activa)
-- fecha fin => null
-- B) Si NO hay alguna capacitacion activa:
-- fecha_inicio => min(fecha de inicio de capacitaciones)
-- fecha fin => max(fecha de fin de capacitaciones)
-- Si no hay valor para fecha_inicio o fecha_fin quedan en NULL

-- --<sql>--DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_tmp_sienfo_capacitaciones`;--</sql>--
--<sql>--
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_sienfo_capacitaciones" AS

SELECT ROW_NUMBER() OVER () AS id,
	c_sienfo.*
FROM (
	SELECT s1.base_origen,
		s1.tipo_capacitacion,
		s1.descrip_normalizada,
		MIN(s1.fecha_inicio_dictado) AS fecha_inicio,
		NULL AS fecha_fin,
		'ACTIVO' AS estado
	FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_sienfo_1" s1
	GROUP BY s1.base_origen,
		s1.tipo_capacitacion,
		s1.descrip_normalizada
	HAVING SUM(s1.estado) > 0

	UNION

	SELECT s1.base_origen,
		s1.tipo_capacitacion,
		s1.descrip_normalizada,
		MIN(s1.fecha_inicio_dictado) AS fecha_inicio,
		MAX(s1.fecha_fin_dictado) AS fecha_fin,
		'BAJA' AS estado
	FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_sienfo_1" s1
	GROUP BY s1.base_origen,
		s1.tipo_capacitacion,
		s1.descrip_normalizada
	HAVING SUM(s1.estado) = 0
	) c_sienfo
ORDER BY c_sienfo.descrip_normalizada
--</sql>--

--6.-- Crear tabla match capacitaciones sienfo
-- --<sql>--DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_tmp_sienfo_match`;--</sql>--
--<sql>--
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_sienfo_match" AS

SELECT sc.base_origen,
	sc.tipo_capacitacion,
	sc.id AS id_new,
	s1.capacitacion_id AS id_old
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_sienfo_1" s1
INNER JOIN "caba-piba-staging-zone-db"."tbp_typ_tmp_sienfo_capacitaciones" sc ON (
		sc.tipo_capacitacion = s1.tipo_capacitacion
		AND sc.descrip_normalizada = s1.descrip_normalizada
		);
--</sql>--

-- GOET
--7.-- Crear tabla capacitaciones goet
-- --<sql>--DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_tmp_goet_1`;--</sql>--
--<sql>--
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_goet_1" AS

SELECT 'GOET' AS base_origen,
	CASE
		WHEN t.detalle IS NOT NULL
			THEN 'CARRERA'
		ELSE 'CURSO'
		END AS tipo_capacitacion,
	CASE
		WHEN t.detalle IS NOT NULL
			THEN CAST(n.idnomenclador AS VARCHAR) || '-' || CAST(t.idkeytrayecto AS VARCHAR)
		ELSE CAST(n.idnomenclador AS VARCHAR)
		END AS capacitacion_id,
	TRIM(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(UPPER(COALESCE(t.detalle, n.detalle)), CHR(160), ' '), 'Á', 'A'), 'É', 'E'), 'Í', 'I'), 'Ó', 'O'), '[ÚÜ]', 'U'), '\([0-9 ]*\)', ''), '["?¿]', ''), ' +', ' ')) AS descrip_normalizada,
	TRIM(COALESCE(t.detalle, n.detalle)) AS descrip_capacitacion,
	MIN(cc.iniciocurso) AS fecha_inicio_dictado,
	MAX(cc.fincurso) AS fecha_fin_dictado,
	CASE
		WHEN UPPER(en.detalle) = 'ACTIVO'
			THEN 1
		ELSE 0
		END AS estado,
	UPPER(COALESCE(a.detalle, f.detalle)) AS categoria
FROM
 "caba-piba-raw-zone-db"."goet_nomenclador" n
LEFT JOIN "caba-piba-raw-zone-db"."goet_centro_habilitacion_modulos" chm ON (n.idnomenclador = chm.idnomenclador)
LEFT JOIN "caba-piba-raw-zone-db"."goet_centro_codigo_curso" cc ON (cc.idctrhbmodulo = chm.idctrhbmodulo)
LEFT JOIN "caba-piba-raw-zone-db"."goet_area" a ON (a.idarea = n.idarea)
LEFT JOIN "caba-piba-raw-zone-db"."goet_familia" f ON (f.idfamilia = n.idfamilia)
LEFT JOIN "caba-piba-raw-zone-db"."goet_trayecto" t ON (t.idkeytrayecto = cc.idkeytrayecto)
LEFT JOIN "caba-piba-raw-zone-db"."goet_estado_nomenclador" en ON (en.idestadonomenclador = n.idestadonomenclador)
GROUP BY t.detalle,
	CAST(n.idnomenclador AS VARCHAR) || '-' || CAST(t.idkeytrayecto AS VARCHAR),
	CAST(n.idnomenclador AS VARCHAR),
	n.detalle,
	UPPER(en.detalle),
	a.detalle,
	f.detalle;
--</sql>--

--8.-- Crear tabla maestro capacitaciones goet
-- A) Si hay alguna capacitacion activa:
-- fecha_inicio => min(fecha de inicio de capacitacion activa)
-- fecha fin => null
-- B) Si NO hay alguna capacitacion activa:
-- fecha_inicio => min(fecha de inicio de capacitaciones)
-- fecha fin => max(fecha de fin de capacitaciones)
-- Si no hay valor para fecha_inicio o fecha_fin quedan en NULL

-- --<sql>--DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_tmp_goet_capacitaciones`;--</sql>--
--<sql>--
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_goet_capacitaciones" AS

SELECT ROW_NUMBER() OVER () AS id,
	c_goet.*
FROM (
	SELECT s1.base_origen,
		s1.tipo_capacitacion,
		s1.descrip_normalizada,
		MIN(s1.fecha_inicio_dictado) AS fecha_inicio,
		NULL AS fecha_fin,
		'ACTIVO' AS estado
	FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_goet_1" s1
	GROUP BY s1.base_origen,
		s1.tipo_capacitacion,
		s1.descrip_normalizada
	HAVING SUM(s1.estado) > 0

	UNION

	SELECT s1.base_origen,
		s1.tipo_capacitacion,
		s1.descrip_normalizada,
		MIN(s1.fecha_inicio_dictado) AS fecha_inicio,
		MAX(s1.fecha_fin_dictado) AS fecha_fin,
		'BAJA' AS estado
	FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_goet_1" s1
	GROUP BY s1.base_origen,
		s1.tipo_capacitacion,
		s1.descrip_normalizada
	HAVING SUM(s1.estado) = 0
	) c_goet
ORDER BY c_goet.descrip_normalizada
--</sql>--

--9.-- Crear tabla match capacitaciones goet
-- --<sql>--DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_tmp_goet_match`;--</sql>--
--<sql>--
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_goet_match" AS

SELECT sc.base_origen,
	sc.tipo_capacitacion,
	sc.id AS id_new,
	s1.capacitacion_id AS id_old
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_goet_1" s1
INNER JOIN "caba-piba-staging-zone-db"."tbp_typ_tmp_goet_capacitaciones" sc ON (
		sc.tipo_capacitacion = s1.tipo_capacitacion
		AND sc.descrip_normalizada = s1.descrip_normalizada
		);
--</sql>--

-- MOODLE
--10.-- Crear tabla capacitaciones moodle
-- --<sql>--DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_tmp_moodle_1`;--</sql>--
--<sql>--
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_moodle_1" AS
SELECT 'MOODLE' AS base_origen,
	-- En CAC son cursos de dos módulos
	'CURSO' AS tipo_capacitacion,
	CAST(cc.id AS VARCHAR) AS capacitacion_id,
	TRIM(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(UPPER(cc.name), CHR(160), ' '), 'Á', 'A'), 'É', 'E'), 'Í', 'I'), 'Ó', 'O'), '[ÚÜ]', 'U'), '[0-9_\-.]+', ' '), 'FORMACION PARA EMPLEABILIDAD', 'FORMACION PARA LA EMPLEABILIDAD'), ' FS ', ' FULLSTACK '), ' +', ' ')) AS descrip_normalizada,
	TRIM(cc.name) AS descrip_capacitacion,
	TRIM(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(UPPER(co.fullname), CHR(160), ' '), 'Á', 'A'), 'É', 'E'), 'Í', 'I'), 'Ó', 'O'), '[ÚÜ]', 'U'), '[0-9./_]', ' '), 'CATAMARCA|CHACO|MAR DEL PLATA|NEUQUEN|TUCUMAN', ' '), ' ENE| FEB| MAR| ABR| MAY| JUN| JUL| AGO| SEP| OCT| NOV| DIC', ' '), '\(COMISION *\)|AULA[A-Z]? *$|^CURSO:* | [A-HJ-Z]$|^CAC', ' '), '-', ' - '), ' +', ' ')) AS descrip_normalizada_modulo,
	TRIM(co.fullname) AS descrip_modulo,
	MIN(CASE
			WHEN co.startdate != 0
				THEN date_parse(date_format(from_unixtime(co.startdate), '%Y-%m-%d %h:%i%p'), '%Y-%m-%d %h:%i%p')
			END) AS fecha_inicio_dictado,
	MAX(CASE
			WHEN co.enddate != 0
				THEN date_parse(date_format(from_unixtime(co.enddate), '%Y-%m-%d %h:%i%p'), '%Y-%m-%d %h:%i%p')
			END) AS fecha_fin_dictado,
	CASE
		WHEN co.enddate = 0
			THEN 1
		ELSE 0
		END AS estado,
	CASE
		WHEN cc.idnumber LIKE 'CAC%'
			THEN 'CAC'
		WHEN cc.idnumber LIKE 'FPE%'
			THEN 'FPE'
		END AS categoria
FROM "caba-piba-raw-zone-db"."moodle_dgtedu01_mdl_course" co
INNER JOIN "caba-piba-raw-zone-db"."moodle_dgtedu01_mdl_course_categories" cc ON (co.category = cc.id)
WHERE (
		cc.idnumber LIKE 'CAC%' -- CAC
		OR cc.idnumber LIKE 'FPE%' -- Habilidades/Formación para la empleabilidad
		)
GROUP BY CAST(cc.id AS VARCHAR),
	co.fullname,
	co.enddate,
	cc.name,
	cc.idnumber;
--</sql>--

--11.-- Crear tabla maestro capacitaciones moodle
-- A) Si hay alguna capacitacion activa:
-- fecha_inicio => min(fecha de inicio de capacitacion activa)
-- fecha fin => null
-- B) Si NO hay alguna capacitacion activa:
-- fecha_inicio => min(fecha de inicio de capacitaciones)
-- fecha fin => max(fecha de fin de capacitaciones)
-- Si no hay valor para fecha_inicio o fecha_fin quedan en NULL

-- --<sql>--DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_tmp_moodle_capacitaciones`;--</sql>--
--<sql>--
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_moodle_capacitaciones" AS
SELECT ROW_NUMBER() OVER () AS id,
	c_moodle.*
FROM (
	SELECT s1.base_origen,
		s1.tipo_capacitacion,
		s1.descrip_normalizada,
		MIN(s1.fecha_inicio_dictado) AS fecha_inicio,
		NULL AS fecha_fin,
		'ACTIVO' AS estado
	FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_moodle_1" s1
	GROUP BY s1.base_origen,
		s1.tipo_capacitacion,
		s1.descrip_normalizada
	HAVING SUM(s1.estado) > 0

	UNION

	SELECT s1.base_origen,
		s1.tipo_capacitacion,
		s1.descrip_normalizada,
		MIN(s1.fecha_inicio_dictado) AS fecha_inicio,
		MAX(s1.fecha_fin_dictado) AS fecha_fin,
		'BAJA' AS estado
	FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_moodle_1" s1
	GROUP BY s1.base_origen,
		s1.tipo_capacitacion,
		s1.descrip_normalizada
	HAVING SUM(s1.estado) = 0
	) c_moodle
ORDER BY c_moodle.descrip_normalizada
--</sql>--

--12.-- Crear tabla match capacitaciones moodle
-- --<sql>--DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_tmp_moodle_match`;--</sql>--
--<sql>--
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_moodle_match" AS
SELECT sc.base_origen,
	sc.tipo_capacitacion,
	sc.id AS id_new,
	s1.capacitacion_id AS id_old
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_moodle_1" s1
INNER JOIN "caba-piba-staging-zone-db"."tbp_typ_tmp_moodle_capacitaciones" sc ON (
		sc.tipo_capacitacion = s1.tipo_capacitacion
		AND sc.descrip_normalizada = s1.descrip_normalizada
		)
GROUP BY sc.base_origen,
	sc.tipo_capacitacion,
	sc.id,
	s1.capacitacion_id;
--</sql>--

-- SIU
--13.-- Crear tabla capacitaciones siu
-- --<sql>--DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_tmp_siu_1`;--</sql>--
--<sql>--
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_siu_1" AS
	WITH siu AS (
			SELECT 'SIU' AS base_origen,
				'CARRERA' AS tipo_capacitacion,
				CAST(spl.PLAN AS VARCHAR) AS capacitacion_id,
				TRIM(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(UPPER(spl.nombre), CHR(160), ' '), 'Á', 'A'), 'É', 'E'), 'Í', 'I'), 'Ó', 'O'), '[ÚÜ]', 'U'), '[0-9\-]+|IFTS| RM |NUEVO|OK!', ''), '\( *\)', ''), ' +', ' ')) AS descrip_normalizada_plan,
				spl.nombre AS descrip_capacitacion_plan,
				TRIM(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(UPPER(spr.nombre), CHR(160), ' '), 'Á', 'A'), 'É', 'E'), 'Í', 'I'), 'Ó', 'O'), '[ÚÜ]', 'U'), '[0-9\-]+|IFTS| RM |NUEVO|OK!', ''), '\( *\)', ''), ' +', ' ')) AS descrip_normalizada_propuesta,
				spr.nombre AS descrip_capacitacion_propuesta,
				spl.fecha_entrada_vigencia AS fecha_inicio_dictado,
				spl.fecha_baja AS fecha_fin_dictado,
				CASE
					WHEN spl.estado = 'V'
						THEN 1
					ELSE 0
					END AS estado,
				CAST(NULL AS VARCHAR) AS categoria
			FROM "caba-piba-raw-zone-db"."siu_toba_3_3_negocio_sga_planes" spl
			INNER JOIN "caba-piba-raw-zone-db"."siu_toba_3_3_negocio_sga_propuestas" spr ON (spl.propuesta = spr.propuesta)
			GROUP BY CAST(spl.PLAN AS VARCHAR),
				spl.nombre,
				spr.nombre,
				spl.fecha_entrada_vigencia,
				spl.fecha_baja,
				spl.estado
			)

SELECT base_origen,
	tipo_capacitacion,
	capacitacion_id,
	CASE
		WHEN LENGTH(descrip_normalizada_plan) > 0
			THEN descrip_normalizada_plan
		ELSE descrip_normalizada_propuesta
		END AS descrip_normalizada,
	CASE
		WHEN LENGTH(descrip_normalizada_plan) > 0
			THEN descrip_capacitacion_plan
		ELSE descrip_capacitacion_propuesta
		END AS descrip_capacitacion,
	fecha_inicio_dictado,
	fecha_fin_dictado,
	estado,
	categoria
FROM siu;
--</sql>--

--14.-- Crear tabla maestro capacitaciones siu
-- A) Si hay alguna capacitacion activa:
-- fecha_inicio => min(fecha de inicio de capacitacion activa)
-- fecha fin => null
-- B) Si NO hay alguna capacitacion activa:
-- fecha_inicio => min(fecha de inicio de capacitaciones)
-- fecha fin => max(fecha de fin de capacitaciones)
-- Si no hay valor para fecha_inicio o fecha_fin quedan en NULL

-- --<sql>--DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_tmp_siu_capacitaciones`;--</sql>--
--<sql>--
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_siu_capacitaciones" AS

SELECT ROW_NUMBER() OVER () AS id,
	c_siu.*
FROM (
	SELECT s1.base_origen,
		s1.tipo_capacitacion,
		s1.descrip_normalizada,
		MIN(s1.fecha_inicio_dictado) AS fecha_inicio,
		NULL AS fecha_fin,
		'ACTIVO' AS estado
	FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_siu_1" s1
	GROUP BY s1.base_origen,
		s1.tipo_capacitacion,
		s1.descrip_normalizada
	HAVING SUM(s1.estado) > 0

	UNION

	SELECT s1.base_origen,
		s1.tipo_capacitacion,
		s1.descrip_normalizada,
		MIN(s1.fecha_inicio_dictado) AS fecha_inicio,
		MAX(s1.fecha_fin_dictado) AS fecha_fin,
		'BAJA' AS estado
	FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_siu_1" s1
	GROUP BY s1.base_origen,
		s1.tipo_capacitacion,
		s1.descrip_normalizada
	HAVING SUM(s1.estado) = 0
	) c_siu
ORDER BY c_siu.descrip_normalizada
--</sql>--

--15.-- Crear tabla match capacitaciones siu
-- --<sql>--DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_tmp_siu_match`;--</sql>--
--<sql>--
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_siu_match" AS

SELECT sc.base_origen,
	sc.tipo_capacitacion,
	sc.id AS id_new,
	s1.capacitacion_id AS id_old
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_siu_1" s1
INNER JOIN "caba-piba-staging-zone-db"."tbp_typ_tmp_siu_capacitaciones" sc ON (
		sc.tipo_capacitacion = s1.tipo_capacitacion
		AND sc.descrip_normalizada = s1.descrip_normalizada
		);
--</sql>--

-- UNIFICADAS
--16.-- Crear tabla de capacitaciones de origen unificada
-- --<sql>--DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_tmp_capacitacion`;--</sql>--
--<sql>--
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_capacitacion" AS
SELECT base_origen || '-' || tipo_capacitacion || '-' || CAST(id AS VARCHAR) id,
	id id_new,
	base_origen,
	tipo_capacitacion,
	descrip_normalizada,
	fecha_inicio,
	fecha_fin,
	estado
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_crmsl_capacitaciones"
UNION ALL
SELECT base_origen || '-' || tipo_capacitacion || '-' || CAST(id AS VARCHAR),
	id,
	base_origen,
	tipo_capacitacion,
	descrip_normalizada,
	fecha_inicio,
	fecha_fin,
	estado
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_sienfo_capacitaciones"
UNION ALL
SELECT base_origen || '-' || tipo_capacitacion || '-' || CAST(id AS VARCHAR),
	id,
	base_origen,
	tipo_capacitacion,
	descrip_normalizada,
	fecha_inicio,
	fecha_fin,
	estado
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_goet_capacitaciones"
UNION ALL
SELECT base_origen || '-' || tipo_capacitacion || '-' || CAST(id AS VARCHAR),
	id,
	base_origen,
	tipo_capacitacion,
	descrip_normalizada,
	fecha_inicio,
	fecha_fin,
	estado
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_moodle_capacitaciones"
UNION ALL
SELECT base_origen || '-' || tipo_capacitacion || '-' || CAST(id AS VARCHAR),
	id,
	base_origen,
	tipo_capacitacion,
	descrip_normalizada,
	fecha_inicio,
	fecha_fin,
	estado
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_siu_capacitaciones"
--</sql>--



-- Copy of 2023.05.24 step 06 - consume capacitacion.sql 



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
	ca.capacitacion_id AS capacitacion_id_asi,
	ca.programa_id,
	ca.descrip_capacitacion,
	ca.tipo_formacion,
	ca.descrip_tipo_formacion,
	ca.modalidad_id,
	ca.descrip_modalidad,
	ca.tipo_capacitacion AS tipo_capacitacion_asi,
	ca.estado_capacitacion,
	ca.seguimiento_personalizado,
	ca.soporte_online,
	ca.incentivos_terminalidad,
	ca.exclusividad_participantes,
	ca.categoria_back_id,
	ca.descrip_back,
	ca.categoria_front_id,
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
SELECT c.id capacitacion_id,
	   c.capacitacion_id_asi,
       c.tipo_capacitacion_asi,
       ac.aptitud_id,
       a.nombre descrip_aptitud
FROM "caba-piba-staging-zone-db"."tbp_typ_def_capacitacion" c
INNER JOIN "caba-piba-raw-zone-db"."api_asi_aptitud_capacitacion" ac
ON (ac.capacitacion_id = c.capacitacion_id_asi)
INNER JOIN "caba-piba-raw-zone-db"."api_asi_aptitud" a
ON (ac.aptitud_id = a.id)
--</sql>--



-- Copy of 2023.05.24 step 07 - staging vecinos.sql 



-- --<sql>--DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_tmp_vecino_1`;--</sql>--
-- 1- Se crea tabla tbp_typ_tmp_vecino_2 con todos los vecinos excepto los provenientes de la base origen IEL
--<sql>--
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_vecino_1" AS
SELECT 'SIU' base_origen,
       CAST(nmp.persona AS VARCHAR) cod_origen,
	   CONCAT((CASE WHEN nmtd.desc_abreviada IN ('DNI',
                                         'LC',
                                         'LE',
                                         'CI',
                                         'CUIT',
                                         'CUIL') THEN nmtd.desc_abreviada
            WHEN nmtd.desc_abreviada IN ('CM', 'CD') THEN 'CE'
            WHEN nmtd.desc_abreviada = 'PAS' THEN 'PE'
            WHEN nmtd.desc_abreviada = 'DNT' THEN 'DNI'
            WHEN nmtd.desc_abreviada IN ('CDI', 'CC') THEN 'NN'
            ELSE 'NN'
            END
            ),
		   CASE WHEN nmtd.desc_abreviada IN ('DNI',
                                         'LC',
                                         'LE',
                                         'CI',
                                         'CUIT',
                                         'CUIL') THEN REGEXP_REPLACE(UPPER(nmpd.nro_documento),'[A-Za-z]+|\.','') ELSE
           CAST(nmpd.nro_documento AS VARCHAR) END,
           nmp.sexo,
          (CASE WHEN UPPER(SUBSTR(nmn.descripcion,1,3)) = 'ARG' THEN 'ARG'
           ELSE 'NN'
           END)) broker_id_din,
	   CONCAT(RPAD((CASE WHEN nmtd.desc_abreviada IN ('DNI',
                                         'LC',
                                         'LE',
                                         'CI',
                                         'CUIT',
                                         'CUIL') THEN nmtd.desc_abreviada
            WHEN nmtd.desc_abreviada IN ('CM', 'CD') THEN 'CE'
            WHEN nmtd.desc_abreviada = 'PAS' THEN 'PE'
            WHEN nmtd.desc_abreviada = 'DNT' THEN 'DNI'
            WHEN nmtd.desc_abreviada IN ('CI', 'CDI', 'CC') THEN 'NN'
            ELSE 'NN'
            END
            ),4,' '),
           LPAD((CASE WHEN nmtd.desc_abreviada IN ('DNI',
                                         'LC',
                                         'LE',
                                         'CI',
                                         'CUIT',
                                         'CUIL') THEN REGEXP_REPLACE(UPPER(nmpd.nro_documento),'[A-Za-z]+|\.','') ELSE
           CAST(nmpd.nro_documento AS VARCHAR) END),11,'0'),
           nmp.sexo,
          (CASE WHEN UPPER(SUBSTR(nmn.descripcion,1,3)) = 'ARG' THEN 'ARG'
           ELSE 'NNN'
           END)) broker_id_est,
       CAST(nmpd.tipo_documento AS VARCHAR) tipo_documento,
       CASE WHEN nmtd.desc_abreviada IN ('DNI',
                                         'LC',
                                         'LE',
                                         'CI',
                                         'CUIT',
                                         'CUIL') THEN nmtd.desc_abreviada
            WHEN nmtd.desc_abreviada IN ('CM', 'CD') THEN 'CE'
            WHEN nmtd.desc_abreviada = 'PAS' THEN 'PE'
            WHEN nmtd.desc_abreviada = 'DNT' THEN 'DNI'
            WHEN nmtd.desc_abreviada IN ('CI', 'CDI', 'CC') THEN 'NN'
            ELSE 'NN'
            END tipo_doc_broker,
       CASE WHEN nmtd.desc_abreviada IN ('DNI',
                                         'LC',
                                         'LE',
                                         'CI',
                                         'CUIT',
                                         'CUIL') THEN REGEXP_REPLACE(UPPER(nmpd.nro_documento),'[A-Za-z]+|\.','') ELSE
           CAST(nmpd.nro_documento AS VARCHAR) END documento_broker,
       UPPER(nmp.nombres) nombre,
       UPPER(nmp.apellido) apellido,
       nmp.fecha_nacimiento,
       nmp.sexo genero_broker,
       CAST(nmp.nacionalidad AS VARCHAR) nacionalidad,
       nmn.descripcion descrip_nacionalidad,
       CASE WHEN UPPER(SUBSTR(nmn.descripcion,1,3)) = 'ARG' THEN 'ARG'
       ELSE 'NN'
       END nacionalidad_broker,
	   (CASE WHEN ((UPPER(nmp.nombres) IS NULL) OR (("length"(UPPER(nmp.nombres)) < 3) AND (NOT ("upper"(UPPER(nmp.nombres)) IN ('BO', 'GE', 'HE', 'LI', 'LU', 'QI', 'WU', 'XI', 'XU', 'YE', 'YI', 'YU')))) OR (UPPER(nmp.nombres) LIKE '%PRUEBA%')) THEN 0 ELSE 1 END) nombre_valido,
	   (CASE WHEN ((UPPER(nmp.apellido) IS NULL) OR (("length"(UPPER(nmp.apellido)) < 3) AND (NOT ("upper"(UPPER(nmp.apellido)) IN ('AL', 'AM', 'AN', 'BO', U&'B\00D3', 'CO', 'DE', 'DO', 'DU', 'FU', 'GE', 'GO', 'GU', 'HA', 'HE', 'HO', 'HU', 'IM', 'IN', 'IS', 'JI', 'JO', 'JU', 'KE', 'KI', 'KO', 'KU', 'LI', 'LO', 'LU', 'MA', 'MO', 'MU', 'NA', 'NG', 'NI', 'NO', 'OH', 'OU', 'PI', 'PO', 'PY', 'QI', 'QU', 'RA', 'RE', U&'R\00C9', 'RO', 'RU', 'SA', U&'S\00C1', 'SO', 'SU', 'TU', 'UM', 'UZ', 'WU', 'XU', 'YA', 'YE', 'YI', 'YO', 'YU')))) OR (UPPER(nmp.apellido) LIKE '%PRUEBA%')) THEN 0 ELSE 1 END) apellido_valido,
	   CAST(nmpd.nro_documento AS VARCHAR) documento_original
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_view_siu_toba_3_3_negocio_mdp_personas_no_duplicates" nmp
INNER JOIN "caba-piba-raw-zone-db"."siu_toba_3_3_negocio_sga_alumnos" nsa ON nsa.persona = nmp.persona
LEFT JOIN "caba-piba-raw-zone-db"."siu_toba_3_3_negocio_mdp_nacionalidades" nmn ON nmn.nacionalidad = nmp.nacionalidad
LEFT JOIN "caba-piba-raw-zone-db"."siu_toba_3_3_negocio_mdp_personas_documentos" nmpd ON nmpd.documento = nmp.documento_principal
LEFT JOIN "caba-piba-raw-zone-db"."siu_toba_3_3_negocio_mdp_tipo_documento" nmtd ON nmtd.tipo_documento = nmpd.tipo_documento
GROUP BY
	   CAST(nmp.persona AS VARCHAR),
	   nmtd.desc_abreviada ,
	   CAST(nmpd.nro_documento AS varchar),
	   nmp.sexo,
	   nmn.descripcion,
	   CAST(nmpd.tipo_documento AS VARCHAR),
	   UPPER(nmp.nombres),
       UPPER(nmp.apellido),
       nmp.fecha_nacimiento,
       CAST(nmp.nacionalidad AS VARCHAR),
	   CASE WHEN nmtd.desc_abreviada IN ('DNI',
									 'LC',
									 'LE',
									 'CI',
									 'CUIT',
									 'CUIL') THEN REGEXP_REPLACE(UPPER(nmpd.nro_documento),'[A-Za-z]+|\.','') ELSE
	   CAST(nmpd.nro_documento AS VARCHAR) END
UNION ALL

SELECT 'GOET' base_origen,
	    CAST(goetu.idusuario AS VARCHAR) codigo_origen,
		CONCAT((CASE WHEN (goettd.detalle IN ('DNI', 'CUIT', 'CUIL', 'LC', 'LE', 'CI')) THEN goettd.detalle
		        ELSE 'NN' END),
				CASE WHEN goettd.detalle IN ('DNI',
                                         'LC',
                                         'LE',
                                         'CI',
                                         'CUIT',
                                         'CUIL') THEN REGEXP_REPLACE(UPPER(goetu.ndocumento),'[A-Za-z]+|\.','') ELSE
					CAST(goetu.ndocumento AS VARCHAR) END,
				goetu.sexo,
				(CASE WHEN (goettd.detalle IN ('DNI', 'CUIT', 'CUIL', 'LC', 'LE', 'CI')) THEN 'ARG' ELSE 'NN' END)) broker_id_din,
	    CONCAT(RPAD(CASE WHEN (goettd.detalle IN ('DNI', 'CUIT', 'CUIL', 'LC', 'LE', 'CI')) THEN goettd.detalle
		        ELSE 'NN' END,4,' '),
				LPAD((CASE WHEN goettd.detalle IN ('DNI',
                                         'LC',
                                         'LE',
                                         'CI',
                                         'CUIT',
                                         'CUIL') THEN REGEXP_REPLACE(UPPER(goetu.ndocumento),'[A-Za-z]+|\.','') ELSE
           CAST(goetu.ndocumento AS VARCHAR) END),11,'0'),
				goetu.sexo,
				(CASE WHEN (goettd.detalle IN ('DNI', 'CUIT', 'CUIL', 'LC', 'LE', 'CI')) THEN 'ARG' ELSE 'NNN' END)) broker_id_est,
		goettd.detalle tipo_documento,
		(CASE WHEN (goettd.detalle IN ('DNI', 'CUIT', 'CUIL', 'LC', 'LE', 'CI')) THEN goettd.detalle
		        ELSE 'NN' END) tipo_doc_broker,
		CASE WHEN goettd.detalle IN ('DNI',
                                         'LC',
                                         'LE',
                                         'CI',
                                         'CUIT',
                                         'CUIL') THEN REGEXP_REPLACE(UPPER(goetu.ndocumento),'[A-Za-z]+|\.','') ELSE
           CAST(goetu.ndocumento AS VARCHAR) END documento_broker,
		UPPER(goetu.nombres) nombre,
        UPPER(goetu.apellidos) apellido,
		goetu.fechanacimiento fecha_nacimiento,
		goetu.sexo genero_broker,
		CASE WHEN (goettd.detalle IN ('DNI', 'CUIT', 'CUIL', 'LC', 'LE', 'CI')) THEN 'ARG' ELSE 'NNN' END nacionalidad,
		NULL descrip_nacionalidad,
		CASE WHEN (goettd.detalle IN ('DNI', 'CUIT', 'CUIL', 'LC', 'LE', 'CI')) THEN 'ARG' ELSE 'NNN' END nacionalidad_broker,
		(CASE WHEN ((UPPER(goetu.nombres) IS NULL) OR (("length"(UPPER(goetu.nombres)) < 3) AND (NOT ("upper"(UPPER(goetu.nombres)) IN ('BO', 'GE', 'HE', 'LI', 'LU', 'QI', 'WU', 'XI', 'XU', 'YE', 'YI', 'YU')))) OR (UPPER(goetu.nombres) LIKE '%PRUEBA%')) THEN 0 ELSE 1 END) nombre_valido,
	    (CASE WHEN ((UPPER(goetu.apellidos) IS NULL) OR (("length"(UPPER(goetu.apellidos)) < 3) AND (NOT ("upper"(UPPER(goetu.apellidos)) IN ('AL', 'AM', 'AN', 'BO', U&'B\00D3', 'CO', 'DE', 'DO', 'DU', 'FU', 'GE', 'GO', 'GU', 'HA', 'HE', 'HO', 'HU', 'IM', 'IN', 'IS', 'JI', 'JO', 'JU', 'KE', 'KI', 'KO', 'KU', 'LI', 'LO', 'LU', 'MA', 'MO', 'MU', 'NA', 'NG', 'NI', 'NO', 'OH', 'OU', 'PI', 'PO', 'PY', 'QI', 'QU', 'RA', 'RE', U&'R\00C9', 'RO', 'RU', 'SA', U&'S\00C1', 'SO', 'SU', 'TU', 'UM', 'UZ', 'WU', 'XU', 'YA', 'YE', 'YI', 'YO', 'YU')))) OR (UPPER(goetu.apellidos) LIKE '%PRUEBA%')) THEN 0 ELSE 1 END) apellido_valido,
		CAST(goetu.ndocumento AS VARCHAR) documento_original
FROM  "caba-piba-staging-zone-db"."tbp_typ_tmp_view_goet_usuarios_no_duplicates" goetu
LEFT JOIN "caba-piba-raw-zone-db"."goet_tipo_documento" goettd ON (goetu.idtipodoc = goettd.idtipodoc)
UNION ALL

SELECT 'SIENFO' base_origen,
	    CAST(a.id_alumno AS VARCHAR) codigo_origen,
		CONCAT((CASE WHEN (d.nombre = 'D.N.I.') THEN 'DNI'
				WHEN (d.nombre = 'C.I.') THEN 'CI'
				WHEN (d.nombre = 'L.E.') THEN 'LE'
				WHEN (d.nombre = 'L.C.') THEN 'LC'
				WHEN (d.nombre = 'CUIL') THEN 'CUIL'
				WHEN (d.nombre = 'CUIT') THEN 'CUIT'
				WHEN (d.nombre = 'Pasaporte') THEN 'PE'
				WHEN (d.nombre IN ('C.E. Cl', 'C.I. Py', 'C.I. Bo', 'C.I. Br', 'C.I Uy')) THEN 'CE'
				WHEN (d.nombre = 'otros') THEN 'OTRO' ELSE 'NN' END),
				CASE WHEN d.nombre IN ('D.N.I.', 'L.C.', 'L.E.', 'C.I.', 'CUIT', 'CUIL') THEN REGEXP_REPLACE(UPPER(a.nrodoc),'[A-Za-z]+|\.|\,','') ELSE
           CAST(a.nrodoc AS VARCHAR) END,
				(CASE WHEN (g.sexo = 'Masculino') THEN 'M' WHEN (g.sexo = 'Femenino') THEN 'F' ELSE 'X' END),
				(CASE WHEN (d.nombre IN ('D.N.I.', 'C.I.', 'L.E.', 'L.C.', 'CUIL', 'CUIT')) THEN 'ARG'
				 ELSE 'NN' END)) broker_id_din,
		CONCAT(RPAD(CASE WHEN (d.nombre = 'D.N.I.') THEN 'DNI'
				WHEN (d.nombre = 'C.I.') THEN 'CI'
				WHEN (d.nombre = 'L.E.') THEN 'LE'
				WHEN (d.nombre = 'L.C.') THEN 'LC'
				WHEN (d.nombre = 'CUIL') THEN 'CUIL'
				WHEN (d.nombre = 'CUIT') THEN 'CUIT'
				WHEN (d.nombre = 'Pasaporte') THEN 'PE'
				WHEN (d.nombre IN ('C.E. Cl', 'C.I. Py', 'C.I. Bo', 'C.I. Br', 'C.I Uy')) THEN 'CE'
				WHEN (d.nombre = 'otros') THEN 'OTRO' ELSE 'NN' END,4,' '),
				LPAD((CASE WHEN d.nombre IN ('D.N.I.', 'L.C.', 'L.E.', 'C.I.', 'CUIT', 'CUIL') THEN REGEXP_REPLACE(UPPER(a.nrodoc),'[A-Za-z]+|\.|\,','') ELSE
           CAST(a.nrodoc AS VARCHAR) END),11,'0'),
				(CASE WHEN (g.sexo = 'Masculino') THEN 'M' WHEN (g.sexo = 'Femenino') THEN 'F' ELSE 'X' END),
				(CASE WHEN (d.nombre IN ('D.N.I.', 'C.I.', 'L.E.', 'L.C.', 'CUIL', 'CUIT')) THEN 'ARG'
				 ELSE 'NNN' END)) broker_id_est,
		d.nombre tipo_documento,
		CASE WHEN (d.nombre = 'D.N.I.') THEN 'DNI'
				WHEN (d.nombre = 'C.I.') THEN 'CI'
				WHEN (d.nombre = 'L.E.') THEN 'LE'
				WHEN (d.nombre = 'L.C.') THEN 'LC'
				WHEN (d.nombre = 'CUIL') THEN 'CUIL'
				WHEN (d.nombre = 'CUIT') THEN 'CUIT'
				WHEN (d.nombre = 'Pasaporte') THEN 'PE'
				WHEN (d.nombre IN ('C.E. Cl', 'C.I. Py', 'C.I. Bo', 'C.I. Br', 'C.I Uy')) THEN 'CE'
				WHEN (d.nombre = 'otros') THEN 'OTRO' ELSE 'NN' END tipo_doc_broker,
		CASE WHEN d.nombre IN ('D.N.I.', 'L.C.', 'L.E.', 'C.I.', 'CUIT', 'CUIL') THEN REGEXP_REPLACE(UPPER(a.nrodoc),'[A-Za-z]+|\.|\,','') ELSE
           CAST(a.nrodoc AS VARCHAR) END documento_broker,
		UPPER(CASE WHEN (a.apenom LIKE '%,%') THEN "trim"("split_part"(apenom, ',', 2)) ELSE a.apenom END) nombre,
        UPPER((CASE WHEN (a.apenom LIKE '%,%') THEN "trim"("split_part"(apenom, ',', 1)) ELSE a.apenom END)) apellido,
		a.fechanac fecha_nacimiento,
		(CASE WHEN (g.sexo = 'Masculino') THEN 'M' WHEN (g.sexo = 'Femenino') THEN 'F' ELSE 'X' END) genero_broker,
		CAST(a.nacionalidad AS VARCHAR) nacionalidad,
		n.nombre descrip_nacionalidad,
		(CASE WHEN (d.nombre IN ('D.N.I.', 'C.I.', 'L.E.', 'L.C.', 'CUIL', 'CUIT')) THEN 'ARG'
				 ELSE 'NN' END) nacionalidad_broker,
		(CASE WHEN ((UPPER(a.apenom) IS NULL) OR (("length"(UPPER(a.apenom)) < 3) AND (NOT ("upper"(UPPER(a.apenom)) IN ('BO', 'GE', 'HE', 'LI', 'LU', 'QI', 'WU', 'XI', 'XU', 'YE', 'YI', 'YU')))) OR (UPPER(a.apenom) LIKE '%PRUEBA%')) THEN 0 ELSE 1 END) nombre_valido,
	    (CASE WHEN ((UPPER(a.apenom) IS NULL) OR (("length"(UPPER(a.apenom)) < 3) AND (NOT ("upper"(UPPER(a.apenom)) IN ('AL', 'AM', 'AN', 'BO', U&'B\00D3', 'CO', 'DE', 'DO', 'DU', 'FU', 'GE', 'GO', 'GU', 'HA', 'HE', 'HO', 'HU', 'IM', 'IN', 'IS', 'JI', 'JO', 'JU', 'KE', 'KI', 'KO', 'KU', 'LI', 'LO', 'LU', 'MA', 'MO', 'MU', 'NA', 'NG', 'NI', 'NO', 'OH', 'OU', 'PI', 'PO', 'PY', 'QI', 'QU', 'RA', 'RE', U&'R\00C9', 'RO', 'RU', 'SA', U&'S\00C1', 'SO', 'SU', 'TU', 'UM', 'UZ', 'WU', 'XU', 'YA', 'YE', 'YI', 'YO', 'YU')))) OR (UPPER(a.apenom) LIKE '%PRUEBA%')) THEN 0 ELSE 1 END) apellido_valido,
		CAST(a.nrodoc AS VARCHAR) documento_original
FROM "caba-piba-raw-zone-db"."sienfo_tdoc" d
INNER JOIN "caba-piba-raw-zone-db"."sienfo_alumnos" a ON (a.tipodoc = d.tipodoc)
LEFT JOIN "caba-piba-raw-zone-db"."sienfo_tgenero" g ON (CAST(g.id AS INT) = CAST(a.sexo AS INT))
LEFT JOIN "caba-piba-raw-zone-db"."sienfo_tnacionalidad"  n ON (a.nacionalidad = n.nacionalidad)
UNION ALL

SELECT 'CRMSL' base_origen,
	    CAST(MAX(co.id) AS VARCHAR) codigo_origen,
		CONCAT((CASE WHEN (cs.tipo_documento_c = 'dni') THEN 'DNI' ELSE 'NN' END),
			COALESCE(
					CAST(cs.numero_documento_c AS VARCHAR),
					CAST(SUBSTR(CAST(cs.cuil2_c AS VARCHAR), 3,LENGTH(CAST(cs.cuil2_c AS VARCHAR)) -3) AS VARCHAR)
				),
			(CASE
				WHEN cs.genero_c LIKE 'masculino' OR SUBSTRING(CAST(cs.cuil2_c AS VARCHAR),1,2) = '20' THEN 'M'
				WHEN cs.genero_c LIKE 'femenino' OR SUBSTRING(CAST(cs.cuil2_c AS VARCHAR),1,2) = '27' THEN 'F'
				ELSE 'X' END),
			(CASE WHEN UPPER(SUBSTR(cs.nacionalidad_c, 1, 3)) = 'ARG' THEN 'ARG' ELSE 'NN' END)
		) broker_id_din,

		CONCAT(RPAD(CASE WHEN (cs.tipo_documento_c = 'dni') THEN 'DNI' ELSE 'NN' END,4,' '),
				LPAD(COALESCE(
					CAST(cs.numero_documento_c AS VARCHAR),
					CAST(SUBSTR(CAST(cs.cuil2_c AS VARCHAR), 3,LENGTH(CAST(cs.cuil2_c AS VARCHAR)) -3) AS VARCHAR)
				),11,'0'),
				(CASE
				WHEN cs.genero_c LIKE 'masculino' OR SUBSTRING(CAST(cs.cuil2_c AS VARCHAR),1,2) = '20' THEN 'M'
				WHEN cs.genero_c LIKE 'femenino' OR SUBSTRING(CAST(cs.cuil2_c AS VARCHAR),1,2) = '27' THEN 'F'
				ELSE 'X' END),
				(CASE WHEN UPPER(SUBSTR(cs.nacionalidad_c, 1, 3)) = 'ARG' THEN 'ARG' ELSE 'NNN' END)) broker_id_est,

		cs.tipo_documento_c tipo_documento,

		CASE
			WHEN UPPER(cs.tipo_documento_c) IN ('DNI','LC','LE','CI','CUIT','CUIL') THEN UPPER(cs.tipo_documento_c)
			WHEN cs.tipo_documento_c = 'PAS' THEN 'PE' ELSE 'NN'
		END tipo_doc_broker,

		CAST(
			COALESCE(
				CAST(cs.numero_documento_c AS VARCHAR),
				SUBSTR(CAST(cs.cuil2_c AS VARCHAR),	3,LENGTH(CAST(cs.cuil2_c AS VARCHAR)) -3)
				)
		AS VARCHAR) documento_broker,

		UPPER(co.first_name) nombre,
		UPPER(co.last_name) apellido,
		co.birthdate fecha_nacimiento,

		CASE
			WHEN cs.genero_c LIKE 'masculino' OR SUBSTRING(CAST(cs.cuil2_c AS VARCHAR),1,2) = '20' THEN 'M'
			WHEN cs.genero_c LIKE 'femenino' OR SUBSTRING(CAST(cs.cuil2_c AS VARCHAR),1,2) = '27' THEN 'F'
			ELSE 'X'
		END genero_broker,

		cs.nacionalidad_c nacionalidad,
		cs.nacionalidad_c descrip_nacionalidad,

		CASE WHEN UPPER(SUBSTR(cs.nacionalidad_c, 1, 3)) = 'ARG' THEN 'ARG' ELSE 'NN' END nacionalidad_broker,

		(CASE WHEN ((UPPER(co.first_name) IS NULL) OR (("length"(UPPER(co.first_name)) < 3) AND (NOT ("upper"(UPPER(co.first_name)) IN ('BO', 'GE', 'HE', 'LI', 'LU', 'QI', 'WU', 'XI', 'XU', 'YE', 'YI', 'YU')))) OR (UPPER(co.first_name) LIKE '%PRUEBA%')) THEN 0 ELSE 1 END) nombre_valido,
	    (CASE WHEN ((UPPER(co.last_name) IS NULL) OR (("length"(UPPER(co.last_name)) < 3) AND (NOT ("upper"(UPPER(co.last_name)) IN ('AL', 'AM', 'AN', 'BO', U&'B\00D3', 'CO', 'DE', 'DO', 'DU', 'FU', 'GE', 'GO', 'GU', 'HA', 'HE', 'HO', 'HU', 'IM', 'IN', 'IS', 'JI', 'JO', 'JU', 'KE', 'KI', 'KO', 'KU', 'LI', 'LO', 'LU', 'MA', 'MO', 'MU', 'NA', 'NG', 'NI', 'NO', 'OH', 'OU', 'PI', 'PO', 'PY', 'QI', 'QU', 'RA', 'RE', U&'R\00C9', 'RO', 'RU', 'SA', U&'S\00C1', 'SO', 'SU', 'TU', 'UM', 'UZ', 'WU', 'XU', 'YA', 'YE', 'YI', 'YO', 'YU')))) OR (UPPER(co.last_name) LIKE '%PRUEBA%')) THEN 0 ELSE 1 END) apellido_valido,

		COALESCE(CAST(cs.numero_documento_c AS VARCHAR), CAST(cs.cuil2_c AS VARCHAR)) documento_original

FROM "caba-piba-raw-zone-db"."crm_sociolaboral_contacts" co
INNER JOIN "caba-piba-staging-zone-db"."tbp_typ_tmp_view_crm_sociolaboral_contacts_cstm_no_duplicates" cs ON (co.id = cs.id_c)
WHERE
(
	-- estas condiciones son para traer los vecinos con algun curso o carrera segun datos suministrados de discovery
	(co.lead_source = 'sociolaboral' OR ((co.lead_source = 'rib') AND cs.forma_parte_interm_lab_c = 'si'))
	OR
	-- esta condicion es para traer adicionalmente los vecinos que han formado parte de oportunidades laborales
	(co.id IN (SELECT DISTINCT(op_oportunidades_laborales_contactscontacts_idb) FROM "caba-piba-raw-zone-db"."crm_sociolaboral_op_oportunidades_laborales_contacts_c" WHERE deleted = FALSE))
	OR
	-- esta condicion es para traer adicionalmente los vecinos que han formado parte de entrevistas laborales
	(co.id IN (SELECT DISTINCT(id_c) FROM "caba-piba-raw-zone-db".crm_sociolaboral_contacts_cstm))
	OR
	-- esta condicion es para traer adicionalmente los vecinos que han cargado curriculum vitae
	(co.id IN (SELECT DISTINCT(per_entrevista_laboral_contactscontacts_ida) FROM "caba-piba-raw-zone-db".crm_sociolaboral_per_entrevista_laboral_contacts_c WHERE deleted = FALSE))
	OR
	-- esta condicion es para traer adicionalmente los vecinos que han cargado curriculum vitae
	(co.id IN (SELECT DISTINCT(re_experiencia_laboral_contactscontacts_ida) FROM "caba-piba-raw-zone-db".crm_sociolaboral_re_experiencia_laboral_contacts_c))
)
AND (cs.numero_documento_c IS NOT NULL OR cs.cuil2_c IS NOT NULL)
GROUP BY
		cs.tipo_documento_c,
		cs.numero_documento_c,
		cs.genero_c,
		UPPER(co.last_name),
		UPPER(co.first_name),
		co.birthdate,
		cuil2_c,
		CONCAT((CASE WHEN (cs.tipo_documento_c = 'dni') THEN 'DNI' ELSE 'NN' END),
			COALESCE(
					CAST(cs.numero_documento_c AS VARCHAR),
					CAST(SUBSTR(CAST(cs.cuil2_c AS VARCHAR), 3,LENGTH(CAST(cs.cuil2_c AS VARCHAR)) -3) AS VARCHAR)
				),
			(CASE
				WHEN cs.genero_c LIKE 'masculino' OR SUBSTRING(CAST(cs.cuil2_c AS VARCHAR),1,2) = '20' THEN 'M'
				WHEN cs.genero_c LIKE 'femenino' OR SUBSTRING(CAST(cs.cuil2_c AS VARCHAR),1,2) = '27' THEN 'F'
				ELSE 'X' END),
			(CASE WHEN UPPER(SUBSTR(cs.nacionalidad_c, 1, 3)) = 'ARG' THEN 'ARG' ELSE 'NN' END)
			),

		CONCAT(RPAD(CASE WHEN (cs.tipo_documento_c = 'dni') THEN 'DNI' ELSE 'NN' END,4,' '),
			LPAD(COALESCE(
				CAST(cs.numero_documento_c AS VARCHAR),
				CAST(SUBSTR(CAST(cs.cuil2_c AS VARCHAR), 3,LENGTH(CAST(cs.cuil2_c AS VARCHAR)) -3) AS VARCHAR)
			),11,'0'),
			(CASE
			WHEN cs.genero_c LIKE 'masculino' OR SUBSTRING(CAST(cs.cuil2_c AS VARCHAR),1,2) = '20' THEN 'M'
			WHEN cs.genero_c LIKE 'femenino' OR SUBSTRING(CAST(cs.cuil2_c AS VARCHAR),1,2) = '27' THEN 'F'
			ELSE 'X' END),
			(CASE WHEN UPPER(SUBSTR(cs.nacionalidad_c, 1, 3)) = 'ARG' THEN 'ARG' ELSE 'NNN' END)),

		cs.nacionalidad_c

UNION ALL

SELECT 'MOODLE' base_origen,
	    CAST(mu.id AS VARCHAR) codigo_origen,
		CONCAT('DNI',
				REGEXP_REPLACE(UPPER(CAST(CASE WHEN REGEXP_LIKE(mu.username, '@') THEN
					CASE WHEN REGEXP_LIKE(LOWER(mu.username), '\.ar') THEN
						split_part(split_part(mu.username, '@', 2),'.',4)
					ELSE split_part(split_part(mu.username, '@', 2),'.',3) END
					ELSE split_part(mu.username, '.', 1) END AS VARCHAR)),'[A-Za-z]+|\.',''),
                COALESCE(do.genero,'X'),
				'ARG') broker_id_din,
		CONCAT('DNI ',
				LPAD((REGEXP_REPLACE(UPPER(CAST(CASE WHEN REGEXP_LIKE(mu.username, '@') THEN
					CASE WHEN REGEXP_LIKE(LOWER(mu.username), '\.ar') THEN
						split_part(split_part(mu.username, '@', 2),'.',4)
					ELSE split_part(split_part(mu.username, '@', 2),'.',3) END
					ELSE split_part(mu.username, '.', 1) END AS VARCHAR)),'[A-Za-z]+|\.','')),11,'0'),
                COALESCE(do.genero,'X'),
				'ARG') broker_id_est,
		'DNI' tipo_documento,
		'DNI' tipo_doc_broker,
		REGEXP_REPLACE(UPPER(CAST(CASE WHEN REGEXP_LIKE(mu.username, '@') THEN
					CASE WHEN REGEXP_LIKE(LOWER(mu.username), '\.ar') THEN
						split_part(split_part(mu.username, '@', 2),'.',4)
					ELSE split_part(split_part(mu.username, '@', 2),'.',3) END
					ELSE split_part(mu.username, '.', 1) END AS VARCHAR)),'[A-Za-z]+|\.','') documento_broker,
		UPPER(mu.firstname) nombre,
		UPPER(mu.lastname) apellido,
		CAST(NULL AS DATE) fecha_nacimiento,
		COALESCE(do.genero,'X') genero_broker,
		CAST(NULL AS VARCHAR) nacionalidad,
		CAST(NULL AS VARCHAR) descrip_nacionalidad,
		'ARG' nacionalidad_broker,
		(CASE WHEN ((UPPER(mu.firstname) IS NULL) OR (("length"(UPPER(mu.firstname)) < 3) AND (NOT ("upper"(UPPER(mu.firstname)) IN ('BO', 'GE', 'HE', 'LI', 'LU', 'QI', 'WU', 'XI', 'XU', 'YE', 'YI', 'YU')))) OR (UPPER(mu.firstname) LIKE '%PRUEBA%')) THEN 0 ELSE 1 END) nombre_valido,
	    (CASE WHEN ((UPPER(mu.lastname) IS NULL) OR (("length"(UPPER(mu.lastname)) < 3) AND (NOT ("upper"(UPPER(mu.lastname)) IN ('AL', 'AM', 'AN', 'BO', U&'B\00D3', 'CO', 'DE', 'DO', 'DU', 'FU', 'GE', 'GO', 'GU', 'HA', 'HE', 'HO', 'HU', 'IM', 'IN', 'IS', 'JI', 'JO', 'JU', 'KE', 'KI', 'KO', 'KU', 'LI', 'LO', 'LU', 'MA', 'MO', 'MU', 'NA', 'NG', 'NI', 'NO', 'OH', 'OU', 'PI', 'PO', 'PY', 'QI', 'QU', 'RA', 'RE', U&'R\00C9', 'RO', 'RU', 'SA', U&'S\00C1', 'SO', 'SU', 'TU', 'UM', 'UZ', 'WU', 'XU', 'YA', 'YE', 'YI', 'YO', 'YU')))) OR (UPPER(mu.lastname) LIKE '%PRUEBA%')) THEN 0 ELSE 1 END) apellido_valido,
		CAST(CASE WHEN REGEXP_LIKE(mu.username, '@') THEN
					CASE WHEN REGEXP_LIKE(LOWER(mu.username), '\.ar') THEN
						split_part(split_part(mu.username, '@', 2),'.',4)
					ELSE split_part(split_part(mu.username, '@', 2),'.',3) END
					ELSE split_part(mu.username, '.', 1) END AS VARCHAR) documento_original
FROM "caba-piba-raw-zone-db"."moodle_dgtedu01_mdl_user" mu
LEFT JOIN "caba-piba-raw-zone-db"."moodle_dgtedu01_mdl_user_info_data" ui ON (mu.id = ui.userid AND ui.fieldid = 7)
LEFT JOIN "caba-piba-raw-zone-db"."moodle_dgtedu01_mdl_user_enrolments" mue ON (mue.userid = mu.id)
LEFT JOIN "caba-piba-raw-zone-db"."moodle_dgtedu01_mdl_enrol" me ON (me.id = mue.enrolid)
LEFT JOIN "caba-piba-raw-zone-db"."moodle_dgtedu01_mdl_course" mc ON(mc.id = me.courseid)
LEFT JOIN "caba-piba-raw-zone-db"."moodle_dgtedu01_mdl_course_categories" cc ON (mc.category = cc.id)
LEFT JOIN "caba-piba-staging-zone-db"."tbp_broker_def_broker_general" DO ON (CAST(do.documento_broker AS VARCHAR) = CAST(CASE WHEN REGEXP_LIKE(mu.username, '@') THEN
					CASE WHEN REGEXP_LIKE(LOWER(mu.username), '\.ar') THEN
						split_part(split_part(mu.username, '@', 2),'.',4)
					ELSE split_part(split_part(mu.username, '@', 2),'.',3) END
					ELSE split_part(mu.username, '.', 1) END AS VARCHAR))
WHERE (CASE WHEN REGEXP_LIKE(mu.username, '@') THEN
        CASE WHEN REGEXP_LIKE(LOWER(mu.username), '\.ar') THEN
          split_part(split_part(mu.username, '@', 2),'.',4)
        ELSE split_part(split_part(mu.username, '@', 2),'.',3) END
       ELSE split_part(mu.username, '.', 1)  END) IS NOT NULL
	   AND (cc.idnumber LIKE 'CAC%' OR cc.idnumber LIKE 'FPE%')
    -- Habilidades/Formaciï¿½n para la empleabilidad
GROUP BY mu.id,
		 mu.username,
		 ui.data,
		 mu.firstname,
		 mu.lastname,
		 do.genero

UNION ALL

SELECT 'PORTALEMPLEO' base_origen,
	    CAST(MAX(pec.id) AS VARCHAR) codigo_origen,
		CONCAT((CASE WHEN (pec.doc_type  IN ('DNI', 'LC',  'CI', 'LE', 'CUIL')) THEN pec.doc_type ELSE 'NN' END),
				CAST(pec.doc_number AS varchar),
				(CASE WHEN (pec.gender = 'M') THEN 'M' WHEN (pec.gender = 'F') THEN 'F' ELSE 'X' END),
				(CASE WHEN UPPER(SUBSTR(pec.document_nationality, 1, 3)) = 'ARG' THEN 'ARG' ELSE 'NN' END)) broker_id_din,


		CONCAT(RPAD(CASE
				WHEN (pec.doc_type  IN ('DNI', 'LC',  'CI', 'LE', 'CUIL')) THEN pec.doc_type
				WHEN (pec.doc_type  = 'PAS') THEN 'PE'
				WHEN (pec.doc_type = 'DE') THEN 'CE'
				WHEN (pec.doc_type= 'CRP') THEN 'OTRO' ELSE 'NN' END,4,' '),
				LPAD(CAST(pec.doc_number AS VARCHAR),11,'0'),
				(CASE WHEN (pec.gender = 'M') THEN 'M' WHEN (pec.gender = 'F') THEN 'F' ELSE 'X' END),
				(CASE WHEN UPPER(SUBSTR(pec.document_nationality, 1, 3)) = 'ARG' THEN 'ARG' ELSE 'NNN' END)) broker_id_est,

		pec.doc_type tipo_documento,

		CASE
			WHEN (pec.doc_type  IN ('DNI', 'LC',  'CI', 'LE', 'CUIL')) THEN pec.doc_type
			WHEN (pec.doc_type  = 'PAS') THEN 'PE'
			WHEN (pec.doc_type = 'DE') THEN 'CE'
			WHEN (pec.doc_type= 'CRP') THEN 'OTRO' ELSE 'NN' END tipo_doc_broker,

		CAST(pec.doc_number AS VARCHAR) documento_broker,
		UPPER(u.name) nombre,
		UPPER(u.lastname) apellido,
		TRY_CAST(pec.birth_date AS DATE) fecha_nacimiento,
		(CASE WHEN (pec.gender = 'M') THEN 'M' WHEN (pec.gender = 'F') THEN 'F' ELSE 'X' END) genero_broker,
		pec.nationality nacionalidad,
		pec.nationality descrip_nacionalidad,

		(CASE WHEN UPPER(SUBSTR(pec.document_nationality, 1, 3)) = 'ARG' THEN 'ARG' ELSE 'NNN' END) nacionalidad_broker,

		(CASE
			WHEN ((UPPER(u.name) IS NULL) OR (("length"(UPPER(u.name)) < 3) AND (NOT ("upper"(UPPER(u.name)) IN ('BO', 'GE', 'HE', 'LI', 'LU', 'QI', 'WU', 'XI', 'XU', 'YE', 'YI', 'YU'))))
			OR
			length(regexp_replace(UPPER(u.name),'[^a-zA-ZÃ¡Ã©Ã­Ã³ÃºÃÃÃÃÃÃ¼ÃÃ±ÃÃ Ã¨Ã¬Ã²Ã¹ÃÃÃÃÃÃ¢ÃªÃ®Ã´Ã»ÃÃÃÃÃÃ¤Ã«Ã¯Ã¶Ã¼Ã¿ÃÃÃÃÃÅ¸Ã§Ã\s\`\Â´]+', '')) = 0
			OR
			UPPER(u.name) IN ('GAMMA', 'RATABBOYPDA', 'PINCHARRATA'))
		THEN 0 ELSE 1 END) nombre_valido,

	    (CASE
			WHEN ((UPPER(u.lastname) IS NULL) OR (("length"(UPPER(u.lastname)) < 3) AND (NOT ("upper"(UPPER(u.lastname)) IN ('AL', 'AM', 'AN', 'BO', U&'B\00D3', 'CO', 'DE', 'DO', 'DU', 'FU', 'GE', 'GO', 'GU', 'HA', 'HE', 'HO', 'HU', 'IM', 'IN', 'IS', 'JI', 'JO', 'JU', 'KE', 'KI', 'KO', 'KU', 'LI', 'LO', 'LU', 'MA', 'MO', 'MU', 'NA', 'NG', 'NI', 'NO', 'OH', 'OU', 'PI', 'PO', 'PY', 'QI', 'QU', 'RA', 'RE', U&'R\00C9', 'RO', 'RU', 'SA', U&'S\00C1', 'SO', 'SU', 'TU', 'UM', 'UZ', 'WU', 'XU', 'YA', 'YE', 'YI', 'YO', 'YU'))))
			OR
			length(regexp_replace(UPPER(u.lastname),'[^a-zA-ZÃ¡Ã©Ã­Ã³ÃºÃÃÃÃÃÃ¼ÃÃ±ÃÃ Ã¨Ã¬Ã²Ã¹ÃÃÃÃÃÃ¢ÃªÃ®Ã´Ã»ÃÃÃÃÃÃ¤Ã«Ã¯Ã¶Ã¼Ã¿ÃÃÃÃÃÅ¸Ã§Ã\s\`\Â´]+', '')) = 0
			OR
			UPPER(u.lastname) LIKE '%BALLENA%'
			OR
			UPPER(u.lastname) IN ('HOLA', 'COMPUTACION', 'BBOY', 'RROOMII'))
		THEN 0 ELSE 1 END) apellido_valido,

		CAST(pec.doc_number AS VARCHAR) documento_original
FROM "caba-piba-raw-zone-db"."portal_empleo_candidates" pec
JOIN "caba-piba-raw-zone-db"."portal_empleo_users" u ON (u.id=pec.id)
WHERE
pec.doc_number!=0
AND pec.doc_number IS NOT NULL
AND pec.id IN
				(
				SELECT DISTINCT(candidate_id)
				FROM "caba-piba-raw-zone-db"."portal_empleo_job_applications"
				UNION
				SELECT DISTINCT(candidate_id)
				FROM "caba-piba-raw-zone-db"."portal_empleo_job_hirings"
				UNION
				SELECT DISTINCT(candidate_id)
				FROM "caba-piba-raw-zone-db"."portal_empleo_curriculum_vitaes"
				)
GROUP BY 1,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17

UNION ALL

SELECT 'CRMEMPLEO' base_origen,
	CAST(MAX(cea.id) AS VARCHAR) codigo_origen,
	CONCAT(
		(CASE
		WHEN (cea.tipo_de_documento__c = 'DNI') THEN 'DNI'
		WHEN (cea.tipo_de_documento__c = 'DNIEx') THEN 'CE'
		WHEN (cea.tipo_de_documento__c = 'CI' OR UPPER(cea.tipo_de_documento__c) LIKE '%IDENTIDAD%') THEN 'CI'
		WHEN (UPPER(cea.tipo_de_documento__c) LIKE '%Libreta C%') THEN 'LC'
		WHEN (cea.tipo_de_documento__c IN ('Pasaporte', 'PAS')) THEN 'PE'
		WHEN (cea.tipo_de_documento__c IN ('DNIEx', 'DNI extranjero')) THEN 'CE'
		WHEN (cea.tipo_de_documento__c IN ('DNIEx', 'DNI extranjero')) THEN 'CE'
		WHEN (cea.tipo_de_documento__c IN ('PRECARIA', 'Credencial Residencia Precaria')) THEN 'OTRO' ELSE 'NN' END),

		CAST(cea.numero_de_documento__c AS varchar),
		(CASE
		WHEN (cea.genero__c = 'MASCULINIO' OR cea.genero__c = 'Masculino' ) THEN 'M'
		WHEN (cea.genero__c = 'FEMENINA' OR cea.genero__c = 'Femenino') THEN 'F' ELSE 'X' END),

		(CASE
		WHEN cea.tipo_de_documento__c = 'DNI' OR  cea.tipo_de_documento__c = 'CI' OR UPPER(cea.tipo_de_documento__c) LIKE '%IDENTIDAD%' OR UPPER(cea.tipo_de_documento__c) LIKE '%Libreta C%'
		THEN 'ARG' ELSE 'NN' END)) broker_id_din,

	CONCAT(RPAD(CASE
			WHEN (cea.tipo_de_documento__c = 'DNI') THEN 'DNI'
			WHEN (cea.tipo_de_documento__c = 'DNIEx') THEN 'CE'
			WHEN (cea.tipo_de_documento__c = 'CI' OR UPPER(cea.tipo_de_documento__c) LIKE '%IDENTIDAD%') THEN 'CI'
			WHEN (UPPER(cea.tipo_de_documento__c) LIKE '%Libreta C%') THEN 'LC'
			WHEN (cea.tipo_de_documento__c IN ('Pasaporte', 'PAS')) THEN 'PE'
			WHEN (cea.tipo_de_documento__c IN ('DNIEx', 'DNI extranjero')) THEN 'CE'
			WHEN (cea.tipo_de_documento__c IN ('DNIEx', 'DNI extranjero')) THEN 'CE'
			WHEN (cea.tipo_de_documento__c IN ('PRECARIA', 'Credencial Residencia Precaria')) THEN 'OTRO' ELSE 'NN' END,4,' '),
			LPAD(CAST(cea.numero_de_documento__c AS VARCHAR),11,'0'),
			(CASE
			WHEN (cea.genero__c = 'MASCULINIO' OR cea.genero__c = 'Masculino' ) THEN 'M'
			WHEN (cea.genero__c = 'FEMENINA' OR cea.genero__c = 'Femenino') THEN 'F' ELSE 'X' END),
			(CASE WHEN cea.tipo_de_documento__c = 'DNI' OR  cea.tipo_de_documento__c = 'CI' OR UPPER(cea.tipo_de_documento__c) LIKE '%IDENTIDAD%' OR UPPER(cea.tipo_de_documento__c) LIKE '%Libreta C%' THEN 'ARG' ELSE 'NNN' END)) broker_id_est,

	cea.tipo_de_documento__c tipo_documento,

	CASE
		WHEN (cea.tipo_de_documento__c = 'DNI') THEN 'DNI'
		WHEN (cea.tipo_de_documento__c = 'DNIEx') THEN 'CE'
		WHEN (cea.tipo_de_documento__c = 'CI' OR UPPER(cea.tipo_de_documento__c) LIKE '%IDENTIDAD%') THEN 'CI'
		WHEN (UPPER(cea.tipo_de_documento__c) LIKE '%Libreta C%') THEN 'LC'
		WHEN (cea.tipo_de_documento__c IN ('Pasaporte', 'PAS')) THEN 'PE'
		WHEN (cea.tipo_de_documento__c IN ('DNIEx', 'DNI extranjero')) THEN 'CE'
		WHEN (cea.tipo_de_documento__c IN ('DNIEx', 'DNI extranjero')) THEN 'CE'
		WHEN (cea.tipo_de_documento__c IN ('PRECARIA', 'Credencial Residencia Precaria')) THEN 'OTRO' ELSE 'NN' END tipo_doc_broker,

	CAST(cea.numero_de_documento__c AS VARCHAR) documento_broker,
	UPPER(cea.firstname) nombre,
	UPPER(cea.lastname) apellido,
	TRY_CAST(cea.personbirthdate AS DATE) fecha_nacimiento,
	(CASE
		WHEN (cea.genero__c = 'MASCULINIO' OR cea.genero__c = 'Masculino' ) THEN 'M'
		WHEN (cea.genero__c = 'FEMENINA' OR cea.genero__c = 'Femenino') THEN 'F' ELSE 'X' END) genero_broker,
	cea.nacionalidad_pais__c nacionalidad,
	cea.nacionalidad_pais__c descrip_nacionalidad,

	(CASE WHEN cea.tipo_de_documento__c = 'DNI' OR  cea.tipo_de_documento__c = 'CI' OR UPPER(cea.tipo_de_documento__c) LIKE '%IDENTIDAD%' OR UPPER(cea.tipo_de_documento__c) LIKE '%Libreta C%' THEN 'ARG' ELSE 'NNN' END) nacionalidad_broker,

	(CASE
		WHEN ((UPPER(cea.firstname) IS NULL) OR (("length"(UPPER(cea.firstname)) < 3) AND (NOT ("upper"(UPPER(cea.firstname)) IN ('BO', 'GE', 'HE', 'LI', 'LU', 'QI', 'WU', 'XI', 'XU', 'YE', 'YI', 'YU'))))
		OR
		length(regexp_replace(UPPER(cea.firstname),'[^a-zA-ZÃ¡Ã©Ã­Ã³ÃºÃÃÃÃÃÃ¼ÃÃ±ÃÃ Ã¨Ã¬Ã²Ã¹ÃÃÃÃÃÃ¢ÃªÃ®Ã´Ã»ÃÃÃÃÃÃ¤Ã«Ã¯Ã¶Ã¼Ã¿ÃÃÃÃÃÅ¸Ã§Ã\s\`\Â´]+', '')) = 0 )
	THEN 0 ELSE 1 END) nombre_valido,

	(CASE
		WHEN ((UPPER(cea.lastname) IS NULL) OR (("length"(UPPER(cea.lastname)) < 3) AND (NOT ("upper"(UPPER(cea.lastname)) IN ('AL', 'AM', 'AN', 'BO', U&'B\00D3', 'CO', 'DE', 'DO', 'DU', 'FU', 'GE', 'GO', 'GU', 'HA', 'HE', 'HO', 'HU', 'IM', 'IN', 'IS', 'JI', 'JO', 'JU', 'KE', 'KI', 'KO', 'KU', 'LI', 'LO', 'LU', 'MA', 'MO', 'MU', 'NA', 'NG', 'NI', 'NO', 'OH', 'OU', 'PI', 'PO', 'PY', 'QI', 'QU', 'RA', 'RE', U&'R\00C9', 'RO', 'RU', 'SA', U&'S\00C1', 'SO', 'SU', 'TU', 'UM', 'UZ', 'WU', 'XU', 'YA', 'YE', 'YI', 'YO', 'YU'))))
		OR
		length(regexp_replace(UPPER(cea.lastname),'[^a-zA-ZÃ¡Ã©Ã­Ã³ÃºÃÃÃÃÃÃ¼ÃÃ±ÃÃ Ã¨Ã¬Ã²Ã¹ÃÃÃÃÃÃ¢ÃªÃ®Ã´Ã»ÃÃÃÃÃÃ¤Ã«Ã¯Ã¶Ã¼Ã¿ÃÃÃÃÃÅ¸Ã§Ã\s\`\Â´]+', '')) = 0
		OR
		UPPER(cea.lastname) LIKE 'HOLA' )
	THEN 0 ELSE 1 END) apellido_valido,

	CAST(cea.numero_de_documento__c AS VARCHAR) documento_original

FROM "caba-piba-raw-zone-db"."crm_empleo_account" cea
WHERE LENGTH(TRIM(numero_de_documento__c))>0 AND ispersonaccount = TRUE
GROUP BY 1,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
--</sql>--

-- 2- Se crea la tabla tbp_typ_tmp_vecino_2 con los vecinos de la tabla tbp_typ_tmp_vecino_1 cruzados con broker
-- --<sql>--DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_tmp_vecino_2`;--</sql>--
--<sql>--
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_vecino_2" AS
WITH tmp_vec_broker AS
(SELECT vec.broker_id_din||'-'||vec.base_origen vecino_id,
	   vec.base_origen,
	   vec.cod_origen,
	   vec.broker_id_din broker_id,
	   vec.broker_id_est,
	   vec.tipo_documento,
	   vec.tipo_doc_broker,
	   vec.documento_broker,
	   vec.nombre,
	   vec.apellido,
	   vec.fecha_nacimiento,
	   vec.genero_broker,
	   vec.nacionalidad,
	   vec.descrip_nacionalidad,
	   vec.nacionalidad_broker,
	   vec.nombre_valido,
	   vec.apellido_valido,
	   CASE WHEN bo.id = vec.broker_id_din THEN 1 ELSE 0 END broker_id_valido,
	   CASE WHEN CAST(do.documento_broker AS VARCHAR) = vec.documento_broker THEN 1 ELSE 0 END dni_valido
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_vecino_1" vec
LEFT JOIN "caba-piba-staging-zone-db"."tbp_broker_def_broker_general" bo ON (bo.id = vec.broker_id_din)
LEFT JOIN "caba-piba-staging-zone-db"."tbp_broker_def_broker_general" DO ON (CAST(do.documento_broker AS VARCHAR) = vec.documento_broker AND do.id != vec.broker_id_din)
WHERE vec.broker_id_din IS NOT NULL
GROUP BY vec.broker_id_din,
		 vec.base_origen,
		 vec.base_origen,
	     vec.cod_origen,
	     vec.broker_id_din,
	     vec.broker_id_est,
	     vec.tipo_documento,
	     vec.tipo_doc_broker,
	     vec.documento_broker,
	     vec.nombre,
	     vec.apellido,
	     vec.fecha_nacimiento,
	     vec.genero_broker,
	     vec.nacionalidad,
	     vec.descrip_nacionalidad,
	     vec.nacionalidad_broker,
	     vec.nombre_valido,
	     vec.apellido_valido,
		 bo.id,
		 CAST(do.documento_broker AS VARCHAR)),
tmp_vec_renaper AS(
	   SELECT tvb.vecino_id,
	   tvb.base_origen,
	   tvb.cod_origen,
	   tvb.broker_id,
	   tvb.broker_id_est,
	   tvb.tipo_documento,
	   tvb.tipo_doc_broker,
	   tvb.documento_broker,
	   tvb.nombre,
	   tvb.apellido,
	   UPPER(cr.apellidos) apellido_renaper,
	   UPPER(cr.nombres) nombre_renaper,
	   COALESCE(DATE_PARSE(cr.fecha_nacimiento,'%Y-%m-%d'),tvb.fecha_nacimiento) fecha_nacimiento,
	   tvb.genero_broker,
	   tvb.nacionalidad,
	   tvb.descrip_nacionalidad,
	   tvb.nacionalidad_broker,
	   tvb.nombre_valido,
	   tvb.apellido_valido,
	   tvb.broker_id_valido,
	   CASE WHEN tvb.broker_id_valido = 1 AND tvb.dni_valido = 1 THEN 0
	        WHEN tvb.broker_id_valido = 0 AND tvb.dni_valido = 1 THEN 1
			ELSE 0 END dni_valido,
	   CASE WHEN cr.coincidencia = 'si' AND tvb.broker_id_valido = 0 AND tvb.dni_valido = 0 THEN 1
		    WHEN cr.coincidencia = 'no' AND
				 UPPER(observaciones) LIKE UPPER('%la fecha de nacimiento informada por renaper%') AND
				 levenshtein_distance(UPPER(cr.apellidos),tvb.apellido) <= 0.9 AND
				 levenshtein_distance(UPPER(cr.nombres),tvb.nombre) <= 0.9 AND
				 tvb.broker_id_valido = 0 AND tvb.dni_valido = 0 THEN 1
			ELSE 0 END renaper_valido
FROM tmp_vec_broker tvb
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_tmp_view_ciudadanos_renaper_no_duplicates" cr ON (cr.dni = tvb.documento_broker AND cr.sexo = tvb.genero_broker))
SELECT tvc.vecino_id,
	   tvc.base_origen,
	   tvc.cod_origen,
	   tvc.broker_id,
	   tvc.broker_id_est,
	   tvc.tipo_documento,
	   tvc.tipo_doc_broker,
	   tvc.documento_broker,
	   tvc.nombre,
	   tvc.apellido,
	   tvc.nombre_renaper,
	   tvc.apellido_renaper,
	   tvc.fecha_nacimiento,
	   tvc.genero_broker,
	   tvc.nacionalidad,
	   tvc.descrip_nacionalidad,
	   tvc.nacionalidad_broker,
	   tvc.nombre_valido,
	   tvc.apellido_valido,
	   tvc.broker_id_valido,
	   tvc.dni_valido,
	   tvc.renaper_valido
FROM tmp_vec_renaper tvc
--</sql>--

-- 3- Se crea tabla de analisis iel cruzando con tabla tbp_typ_tmp_vecino_2
-- --<sql>--DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_tmp_analisis_iel`;--</sql>--
--<sql>--
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_analisis_iel" AS
WITH
  iel AS (
   SELECT
     (CASE WHEN (tipo_documento = U&'C\00E9dula extranjera') THEN 'CE' WHEN (tipo_documento = 'Documento Nacional de Identidad') THEN 'DNI' WHEN (tipo_documento = 'Pasaporte extranjero') THEN 'PE' ELSE 'NN' END) tipo_documento
   , nrodocumento
   , nivel
   , sede
   , curso
   , "trim"("regexp_replace"("regexp_replace"("regexp_replace"("regexp_replace"("regexp_replace"("regexp_replace"("regexp_replace"("regexp_replace"("regexp_replace"("upper"(curso), "chr"(160), ' '), U&'\00C1', 'A'), U&'\00C9', 'E'), U&'\00CD', 'I'), U&'\00D3', 'O'), U&'[\00DA\00DC]', 'U'), '\([0-9 ]*\)', ''), U&'["\201C\201D?\00BF]', ''), ' +', ' ')) descrip_normalizada
   , comision
   , inscripcion
   , estado
   , apellido
   , nombre
   , nacionalidad
   , sexo
   , fecha_nacimiento
   FROM
     "caba-piba-raw-zone-db"."iel_nsge_maqueta_v_iel_cfp"
   GROUP BY (CASE WHEN (tipo_documento = U&'C\00E9dula extranjera') THEN 'CE' WHEN (tipo_documento = 'Documento Nacional de Identidad') THEN 'DNI' WHEN (tipo_documento = 'Pasaporte extranjero') THEN 'PE' ELSE 'NN' END), nrodocumento, nivel, sede, curso, comision, inscripcion, estado, apellido, nombre, nacionalidad, sexo, fecha_nacimiento
)
SELECT
  iel.*
, vec.tipo_doc_broker
, vec.broker_id
, vec.broker_id_valido
, vec.base_origen
FROM
  (iel
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_tmp_vecino_2" vec ON ((vec.documento_broker = iel.nrodocumento) AND (vec.tipo_doc_broker = iel.tipo_documento)))
WHERE ((vec.base_origen IS NULL) OR (((vec.base_origen = 'SIU') AND (NOT (EXISTS (SELECT 1
FROM
  "caba-piba-staging-zone-db"."tbp_typ_tmp_vecino_2" vec1
WHERE ((vec1.broker_id = vec.broker_id) AND (vec1.base_origen = 'GOET'))
)))) OR (vec.base_origen = 'GOET')))
--</sql>--

-- 4- Se utilizan las tablas de los pasos 1 y 3 para crear la tabla temporal definitiva: tbp_typ_tmp_vecino
-- --<sql>--DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_tmp_vecino`;--</sql>--
--<sql>--
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_vecino" AS
WITH tmp AS
(SELECT iel.*,
       df.id,
       df.id_new,
       df.descrip_normalizada descrip_normalizada_dc,
	   df.base_origen base_origen_ok
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_analisis_iel" iel
INNER JOIN "caba-piba-staging-zone-db"."tbp_typ_def_capacitacion" df ON
levenshtein_distance(UPPER(df.descrip_normalizada),UPPER(iel.descrip_normalizada)) <= 0.9
WHERE iel.broker_id IS NULL AND df.base_origen IN ('SIU','GOET'))

SELECT tv1.* FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_vecino_1" tv1

UNION ALL

SELECT tmp.base_origen_ok base_origen,
	    CAST(gu.idusuario AS VARCHAR) codigo_origen,
		CONCAT(tmp.tipo_documento,
				CASE WHEN tmp.tipo_documento IN ('DNI',
                                         'LC',
                                         'LE',
                                         'CI',
                                         'CUIT',
                                         'CUIL') THEN REGEXP_REPLACE(UPPER(tmp.nrodocumento),'[A-Za-z]+|\.','') ELSE
					CAST(tmp.nrodocumento AS VARCHAR) END,
				CASE WHEN tmp.sexo IS NULL THEN 'X' ELSE tmp.sexo END,
				(CASE WHEN (tmp.tipo_documento IN ('DNI', 'CUIT', 'CUIL', 'LC', 'LE', 'CI')) THEN 'ARG' ELSE 'NN' END)) broker_id_din,
	    CONCAT(RPAD(tmp.tipo_documento,4,' '),
				LPAD((CASE WHEN tmp.tipo_documento IN ('DNI',
                                         'LC',
                                         'LE',
                                         'CI',
                                         'CUIT',
                                         'CUIL') THEN REGEXP_REPLACE(UPPER(tmp.nrodocumento),'[A-Za-z]+|\.','') ELSE
				CAST(tmp.nrodocumento AS VARCHAR) END),11,'0'),
				CASE WHEN tmp.sexo IS NULL THEN 'X' ELSE tmp.sexo END,
				(CASE WHEN (tmp.tipo_documento IN ('DNI', 'CUIT', 'CUIL', 'LC', 'LE', 'CI')) THEN 'ARG' ELSE 'NNN' END)) broker_id_est,
		tmp.tipo_documento tipo_documento,
		tmp.tipo_documento tipo_doc_broker,
		CASE WHEN tmp.tipo_documento IN ('DNI',
                                         'LC',
                                         'LE',
                                         'CI',
                                         'CUIT',
                                         'CUIL') THEN REGEXP_REPLACE(UPPER(tmp.nrodocumento),'[A-Za-z]+|\.','') ELSE
           CAST(tmp.nrodocumento AS VARCHAR) END documento_broker,
		UPPER(tmp.nombre) nombre,
        UPPER(tmp.apellido) apellido,
		tmp.fecha_nacimiento fecha_nacimiento,
		tmp.sexo genero_broker,
		CASE WHEN (tmp.tipo_documento IN ('DNI', 'CUIT', 'CUIL', 'LC', 'LE', 'CI')) THEN 'ARG' ELSE 'NNN' END nacionalidad,
		NULL descrip_nacionalidad,
		CASE WHEN (tmp.tipo_documento IN ('DNI', 'CUIT', 'CUIL', 'LC', 'LE', 'CI')) THEN 'ARG' ELSE 'NNN' END nacionalidad_broker,
		(CASE WHEN ((UPPER(tmp.nombre) IS NULL) OR (("length"(UPPER(tmp.nombre)) < 3) AND (NOT ("upper"(UPPER(tmp.nombre)) IN ('BO', 'GE', 'HE', 'LI', 'LU', 'QI', 'WU', 'XI', 'XU', 'YE', 'YI', 'YU')))) OR (UPPER(tmp.nombre) LIKE '%PRUEBA%')) THEN 0 ELSE 1 END) nombre_valido,
	    (CASE WHEN ((UPPER(tmp.apellido) IS NULL) OR (("length"(UPPER(tmp.apellido)) < 3) AND (NOT ("upper"(UPPER(tmp.apellido)) IN ('AL', 'AM', 'AN', 'BO', U&'B\00D3', 'CO', 'DE', 'DO', 'DU', 'FU', 'GE', 'GO', 'GU', 'HA', 'HE', 'HO', 'HU', 'IM', 'IN', 'IS', 'JI', 'JO', 'JU', 'KE', 'KI', 'KO', 'KU', 'LI', 'LO', 'LU', 'MA', 'MO', 'MU', 'NA', 'NG', 'NI', 'NO', 'OH', 'OU', 'PI', 'PO', 'PY', 'QI', 'QU', 'RA', 'RE', U&'R\00C9', 'RO', 'RU', 'SA', U&'S\00C1', 'SO', 'SU', 'TU', 'UM', 'UZ', 'WU', 'XU', 'YA', 'YE', 'YI', 'YO', 'YU')))) OR (UPPER(tmp.apellido) LIKE '%PRUEBA%')) THEN 0 ELSE 1 END) apellido_valido,
		CAST(tmp.nrodocumento AS VARCHAR) documento_original
FROM tmp
INNER JOIN "caba-piba-staging-zone-db"."tbp_typ_tmp_view_goet_usuarios_no_duplicates" gu ON (gu.ndocumento = tmp.nrodocumento)
WHERE tmp.broker_id IS NULL
GROUP BY
tmp.tipo_documento, tmp.nrodocumento, tmp.nivel, tmp.sede, tmp.curso, tmp.descrip_normalizada, tmp.comision, tmp.inscripcion, tmp.estado, tmp.apellido, tmp.nombre, tmp.nacionalidad, tmp.sexo, tmp.fecha_nacimiento, tmp.tipo_doc_broker, tmp.broker_id, tmp.broker_id_valido, tmp.base_origen,
tmp.base_origen_ok, gu.idusuario
--</sql>--



-- Copy of 2023.05.24 step 08 - consume vecinos.sql 



-- 1.- Crear la tabla definitiva de vecinos
-- --<sql>--DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_def_vecino`;--</sql>--
--<sql>--
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_def_vecino" AS
WITH tmp_vec_broker AS
(SELECT vec.broker_id_din||'-'||vec.base_origen vecino_id,
	   vec.base_origen,
	   vec.cod_origen,
	   vec.broker_id_din broker_id,
	   vec.broker_id_est,
	   vec.tipo_documento,
	   vec.tipo_doc_broker,
	   vec.documento_broker,
	   vec.nombre,
	   vec.apellido,
	   vec.fecha_nacimiento,
	   vec.genero_broker,
	   vec.nacionalidad,
	   vec.descrip_nacionalidad,
	   vec.nacionalidad_broker,
	   vec.nombre_valido,
	   vec.apellido_valido,
	   CASE WHEN bo.id = vec.broker_id_din THEN 1 ELSE 0 END broker_id_valido,
	   CASE WHEN CAST(do.documento_broker AS VARCHAR) = vec.documento_broker THEN 1 ELSE 0 END dni_valido
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_vecino" vec
LEFT JOIN "caba-piba-staging-zone-db"."tbp_broker_def_broker_general" bo ON (bo.id = vec.broker_id_din)
LEFT JOIN "caba-piba-staging-zone-db"."tbp_broker_def_broker_general" DO ON (CAST(do.documento_broker AS VARCHAR) = vec.documento_broker AND do.id != vec.broker_id_din)
WHERE vec.broker_id_din IS NOT NULL
GROUP BY vec.broker_id_din,
		 vec.base_origen,
		 vec.base_origen,
	     vec.cod_origen,
	     vec.broker_id_din,
	     vec.broker_id_est,
	     vec.tipo_documento,
	     vec.tipo_doc_broker,
	     vec.documento_broker,
	     vec.nombre,
	     vec.apellido,
	     vec.fecha_nacimiento,
	     vec.genero_broker,
	     vec.nacionalidad,
	     vec.descrip_nacionalidad,
	     vec.nacionalidad_broker,
	     vec.nombre_valido,
	     vec.apellido_valido,
		 bo.id,
		 CAST(do.documento_broker AS VARCHAR)),
tmp_vec_renaper AS(
	   SELECT tvb.vecino_id,
	   tvb.base_origen,
	   tvb.cod_origen,
	   tvb.broker_id,
	   tvb.broker_id_est,
	   tvb.tipo_documento,
	   tvb.tipo_doc_broker,
	   tvb.documento_broker,
	   tvb.nombre,
	   tvb.apellido,
	   UPPER(cr.apellidos) apellido_renaper,
	   UPPER(cr.nombres) nombre_renaper,
	   COALESCE(DATE_PARSE(cr.fecha_nacimiento,'%Y-%m-%d'),tvb.fecha_nacimiento) fecha_nacimiento,
	   tvb.genero_broker,
	   tvb.nacionalidad,
	   tvb.descrip_nacionalidad,
	   tvb.nacionalidad_broker,
	   tvb.nombre_valido,
	   tvb.apellido_valido,
	   tvb.broker_id_valido,
	   CASE WHEN tvb.broker_id_valido = 1 AND tvb.dni_valido = 1 THEN 0
	        WHEN tvb.broker_id_valido = 0 AND tvb.dni_valido = 1 THEN 1
			ELSE 0 END dni_valido,
	   CASE WHEN cr.coincidencia = 'si' AND tvb.broker_id_valido = 0 AND tvb.dni_valido = 0 THEN 1
		    WHEN cr.coincidencia = 'no' AND
				 UPPER(observaciones) LIKE UPPER('%la fecha de nacimiento informada por renaper%') AND
				 levenshtein_distance(UPPER(cr.apellidos),tvb.apellido) <= 0.9 AND
				 levenshtein_distance(UPPER(cr.nombres),tvb.nombre) <= 0.9 AND
				 tvb.broker_id_valido = 0 AND tvb.dni_valido = 0 THEN 1
			ELSE 0 END renaper_valido
FROM tmp_vec_broker tvb
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_tmp_view_ciudadanos_renaper_no_duplicates" cr ON (cr.dni = tvb.documento_broker AND cr.sexo = tvb.genero_broker))
SELECT tvc.vecino_id,
	   tvc.base_origen,
	   tvc.cod_origen,
	   tvc.broker_id,
	   tvc.broker_id_est,
	   tvc.tipo_documento,
	   tvc.tipo_doc_broker,
	   tvc.documento_broker,
	   tvc.nombre,
	   tvc.apellido,
	   tvc.nombre_renaper,
	   tvc.apellido_renaper,
	   tvc.fecha_nacimiento,
	   tvc.genero_broker,
	   tvc.nacionalidad,
	   tvc.descrip_nacionalidad,
	   tvc.nacionalidad_broker,
	   tvc.nombre_valido,
	   tvc.apellido_valido,
	   tvc.broker_id_valido,
	   tvc.dni_valido,
	   tvc.renaper_valido
FROM tmp_vec_renaper tvc
--</sql>--



-- Copy of 2023.05.24 step 09 - staging estado_beneficiario_crmsl.sql 



-- Query Estado de Beneficiario para athena
-- --<sql>--DROP TABLE `caba-piba-staging-zone-db`.`tbp_typ_tmp_estado_beneficiario_crmsl`;--</sql>--
--<sql>--
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_estado_beneficiario_crmsl" AS
-- Query Estado de Beneficiario para athena
WITH seguimientos_calculado0 AS (
SELECT
	off.id,
	ofs.op_oportunidades_formacion_se_seguimiento_1se_seguimiento_idb ,
	s.name
	 , s.date_modified
	 , dense_rank() OVER(PARTITION BY off.id , s.name ORDER BY s.date_modified DESC) AS max_date
FROM
	"caba-piba-raw-zone-db".crm_sociolaboral_op_oportunidades_formacion OFF
LEFT JOIN "caba-piba-raw-zone-db".crm_sociolaboral_op_oportunidades_formacion_se_seguimiento_1_c ofs ON
	ofs.op_oportun868armacion_ida = off.id
LEFT JOIN "caba-piba-raw-zone-db".crm_sociolaboral_se_seguimiento s ON
	s.id = ofs.op_oportunidades_formacion_se_seguimiento_1se_seguimiento_idb
	-- group by off.id, s.name
	),
seguimientos_calculado AS (
-- max_date : es usado como col de seguimiento0
-- se usa dense_rank buscando la fecha maxima
	SELECT
		sc0.id,
		sc0.op_oportunidades_formacion_se_seguimiento_1se_seguimiento_idb ,
		sc0.name
		, sc0.date_modified
		FROM seguimientos_calculado0 AS sc0
		 WHERE sc0.max_date = 1
),
resultado AS (
SELECT
	c.id,
	c.first_name,
	c.last_name,
	c.lead_source,
	cc.forma_parte_interm_lab_c,
	off.id AS id_formacion,
	off.name,
	off.inicio,
	off.fin,
	CASE
		WHEN estado_c = 'incompleto' THEN 'baja'
		-- convertir sus opciones a nuestras opciones
		WHEN estado_c = 'en_curso' THEN 'regular'
		WHEN estado_c = 'finalizado' THEN 'egresado'
		WHEN estado_c = 'nunca_inicio' THEN 'baja'
		-- abandono al inicio
		WHEN estado_c IS NOT NULL THEN estado_c
		WHEN estado_c IS NULL THEN (
	CASE
			-- when off.name is null then 'no aplica_c'
		WHEN off.name IS NOT NULL THEN (
		CASE
				WHEN off.inicio < off.fin THEN 'egresado'
				WHEN off.fin IS NULL THEN 'regular'
				WHEN /*off.inicio is null and*/
				off.fin < CURRENT_DATE THEN 'egresado'
				-- when off.inicio > off.fin then 'error_fecha_c'
			END
		 )
		END)
	END AS estado_n,
	sc.estado_c
FROM
	"caba-piba-raw-zone-db".crm_sociolaboral_contacts c
INNER JOIN "caba-piba-staging-zone-db"."tbp_typ_tmp_view_crm_sociolaboral_contacts_cstm_no_duplicates" cc ON
	cc.id_c = c.id
INNER JOIN "caba-piba-raw-zone-db".crm_sociolaboral_op_oportunidades_formacion_contacts_c ofc ON
	ofc.op_oportunidades_formacion_contactscontacts_idb = c.id
INNER JOIN "caba-piba-raw-zone-db".crm_sociolaboral_op_oportunidades_formacion OFF ON
	off.id = ofc.op_oportun1d35rmacion_ida
	-- left join crm_sociolaboral_op_oportunidades_formacion_se_seguimiento_1_c ofs ON ofs.op_oportun868armacion_ida = off.id -- acÃ¡ limitr para que solo traiga el que coincide con c.id
LEFT JOIN seguimientos_calculado SCN ON
	off.id = scn.id
	AND replace(trim(concat_ws(' ', c.first_name, c.last_name)), ' ', ' ') = replace(trim(scn.name), '  ', ' ')
LEFT JOIN "caba-piba-raw-zone-db".crm_sociolaboral_op_oportunidades_formacion_se_seguimiento_1_c ofs ON
	ofs.op_oportunidades_formacion_se_seguimiento_1se_seguimiento_idb = scn.op_oportunidades_formacion_se_seguimiento_1se_seguimiento_idb
LEFT JOIN "caba-piba-raw-zone-db".crm_sociolaboral_se_seguimiento s ON
	s.id = ofs.op_oportunidades_formacion_se_seguimiento_1se_seguimiento_idb
LEFT JOIN "caba-piba-raw-zone-db".crm_sociolaboral_se_seguimiento_cstm sc ON
	sc.id_c = s.id
WHERE
	(c.lead_source IN ('sociolaboral')
		OR ((c.lead_source IN ('rib'))
			AND forma_parte_interm_lab_c IN ('si')
		))
		)
SELECT id alumno_id_old,
	   id_formacion edicion_capacitacion_id_old,
	   UPPER(first_name) first_name,
	   UPPER(last_name) last_name,
	   name descrip_capacitacion,
	   inicio,
	   fin,
	   UPPER(estado_n) estado_beneficiario
FROM resultado
--</sql>--



-- Copy of 2023.05.24 step 10 - staging estado_beneficiario_sienfo.sql 



-- --<sql>--DROP TABLE `caba-piba-staging-zone-db`.`tbp_typ_tmp_estado_beneficiario_sienfo`;--</sql>--
--<sql>--
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_estado_beneficiario_sienfo" AS
WITH t AS (
	/*Limpieza de talleres, me quedo con los cursos validos,
	 * transformo a fechas las columnas, filtro fechas menores a 2003 o fechas de fin mayores a curdate + 1 aÃ±o
	 * merge id_duracion entre duracion de taller y la generica de cursos porque esta incompleta
	 */
	SELECT codigo_ct,
		CASE
			WHEN fecha = '0000-00-00' THEN NULL
			WHEN CAST(fecha AS DATE) < CAST('2003-01-01' AS DATE) THEN NULL ELSE CAST(fecha AS DATE)
		END AS "fecha",
		CASE
			WHEN fecha_fin = '0000-00-00' THEN NULL
			WHEN CAST(fecha_fin AS DATE) < CAST('2003-01-01' AS DATE) THEN NULL
			WHEN CAST(fecha_fin AS DATE) > DATE_ADD('year', 1, CURRENT_DATE) THEN NULL ELSE CAST(fecha_fin AS DATE)
		END AS "fecha_fin",
		CASE
			WHEN t.id_duracion IS NULL THEN c.id_duracion ELSE t.id_duracion
		END AS "id_duracion",
		t.id_curso,
		t.id_carrera,
		d.nombre AS "duracion",
		c.nom_curso
	FROM "caba-piba-raw-zone-db"."sienfo_talleres" t
		LEFT JOIN "caba-piba-raw-zone-db"."sienfo_duracion" d ON d.id_duracion = t.id_duracion
		LEFT JOIN (
			SELECT id_curso,
				max(id_duracion) AS "id_duracion",
				nom_curso
			FROM "caba-piba-raw-zone-db"."sienfo_cursos"
			GROUP BY id_curso,
				nom_curso
			HAVING max(id_duracion) > 0
		) c ON t.id_curso = c.id_curso
	WHERE codigo_ct IS NOT NULL
		AND t.id_curso IS NOT NULL
		AND t.id_curso != 0
		AND t.id_carrera IN (0,1,2,18)
),
/* Me quedo con el id_fichas con el numero mayor para casos donde se repite codigo_ct y nrodoc
 * baja como id normalizado
 * fecha de inicio de curso menor a 2003 como nulo y transformo a fecha
 * normalizo aprobado
*/
f0 AS (
SELECT
    *
FROM
(
    SELECT
        ROW_NUMBER() OVER(PARTITION BY f.nrodoc, f.codigo_ct ORDER BY f.id_fichas DESC) AS DUP,
	    f.nrodoc,
		f.codigo_ct,
		CASE
			WHEN baja IS NULL THEN 0 ELSE baja
		END AS "baja",
		f.fechabaja,
		CASE
			WHEN fecha_inc < CAST('2003-01-01' AS DATE) THEN NULL ELSE CAST(fecha_inc AS DATE)
		END AS "fecha_inc",
		CASE
			WHEN f.aprobado IS NULL THEN 0
			WHEN f.aprobado = '' THEN 0 ELSE CAST(f.aprobado AS INT)
		END AS "aprobado"
	FROM
	    "caba-piba-raw-zone-db"."sienfo_fichas" f
	WHERE nrodoc IS NOT NULL AND nrodoc != ''
) t
WHERE DUP = 1
--primer estado de beneficiario basado en los datos de la tabla original
), f01 AS (
SELECT
    f0.*,
	CASE
		WHEN f0.aprobado IN (1, 3, 5) THEN 'EGRESADO' -- GCBA agrega el id 5 el 17/1/23
		WHEN f0.fechabaja IS NOT NULL THEN 'BAJA'
		WHEN f0.baja NOT IN (14, 22, 24, 0)
		AND f0.baja IS NOT NULL THEN 'BAJA'
		WHEN f0.aprobado IN (2, 4, 6, 8) THEN 'REPROBADO'  -- GCBA quita el id 5 el 17/1/23
	END AS "estado_beneficiario"
FROM
    f0
/*
 *para completar fechas de inicio que no exISten agrupo por fichas, la minima fecha de inicio de un alumno
 */
),
f02 AS (
SELECT
    a.nrodoc,
    f01.codigo_ct,
	f01.baja,
	f01.fechabaja,
	f01.fecha_inc,
	f01.aprobado,
	f01.estado_beneficiario
FROM
    "caba-piba-raw-zone-db"."sienfo_alumnos" a
LEFT JOIN
    f01 ON a.nrodoc = f01.nrodoc
), preins_sin_ficha AS (
SELECT
    p.*,
    'PREINSCRIPTO' AS estado_beneficiario
FROM
    "caba-piba-staging-zone-db"."goayvd_typ_vw_sienfo_fichas_preinscripcion" p
LEFT JOIN f02 ON f02.nrodoc = p.nrodoc AND f02.codigo_ct = p.codigo_ct
WHERE f02.nrodoc IS NULL
), f1 AS (
SELECT
    f02.nrodoc,
    f02.codigo_ct,
	f02.baja,
	f02.fechabaja,
	f02.fecha_inc,
	f02.aprobado,
	f02.estado_beneficiario
FROM
    f02
UNION
SELECT
    pf.nrodoc,
    pf.codigo_ct,
	pf.baja,
	pf.fechabaja,
	pf.fecha_inc,
	0 AS aprobado,
	'PREINSCRIPTO' AS estado_beneficiario
FROM
    preins_sin_ficha pf
),fechas_ct AS (
	SELECT codigo_ct,
	    min(fecha_inc) AS fecha_inc_min
	FROM f1
	GROUP BY codigo_ct
),
/*
 * join de fichas y talleres
 * si la fecha de inicio del taller es nula uso la fecha de inicio del alumno
 * se invalidan fechas de fin inconsIStentes para luego ser recalculadas por duracion
 */
tf AS (
	SELECT t.codigo_ct,
		CASE
			WHEN t.fecha IS NOT NULL THEN fecha
			ELSE fct.fecha_inc_min
		END AS "fecha",
		CASE
			WHEN t.fecha IS NOT NULL
			    AND t.fecha_fin < t.fecha THEN NULL
			WHEN t.fecha_fin < fct.fecha_inc_min THEN NULL ELSE t.fecha_fin
		END AS "fecha_fin",
		t.id_duracion,
		t.id_curso,
		t.id_carrera,
		t.nom_curso,
		f1.nrodoc,
		f1.baja,
		f1.fechabaja,
		f1.fecha_inc,
		f1.aprobado,
		f1.estado_beneficiario
	FROM t
		INNER JOIN f1 ON f1.codigo_ct = t.codigo_ct
		LEFT JOIN fechas_ct fct ON fct.codigo_ct = t.codigo_ct
		/*
		 * Calcula la fecha de fin segun en campo id_duracion para fechas nulas
		 * y las previamente invalidadas por inconsIStencia, al ser fechas de fin menores que fecha de inicio
		 */
),
tf1 AS (
	SELECT tf.codigo_ct,
		tf.fecha,
		CASE
			WHEN fecha_fin IS NOT NULL THEN fecha_fin
			WHEN fecha_fin IS NULL THEN (
				CASE
					WHEN id_duracion = 1 THEN DATE_ADD('month', 9, fecha) -- anual
					WHEN id_duracion IN (2, 4) THEN DATE_ADD('month', 4, fecha) -- cuatrimestral
					WHEN id_duracion = 3 THEN DATE_ADD('month', 2, fecha) -- bimestral
				END
			)
		END AS "fecha_fin",
		tf.id_duracion,
		tf.id_curso,
		tf.id_carrera,
		tf.nom_curso,
		tf.nrodoc,
		tf.baja,
		tf.fechabaja,
		tf.fecha_inc,
		tf.aprobado,
		tf.estado_beneficiario
	FROM tf
),
tf2 AS (
	SELECT tf1.codigo_ct,
		tf1.nrodoc,
		CASE
			WHEN tf1.fecha_inc IS NULL
			AND tf1.id_duracion IS NOT NULL
			AND tf1.fecha IS NOT NULL THEN (
				CASE
					WHEN id_duracion = 1 THEN DATE_ADD('month', -9, fecha)
					WHEN id_duracion IN (2, 4) THEN DATE_ADD('month', -4, fecha)
					WHEN id_duracion = 3 THEN DATE_ADD('month', -2, fecha)
				END
			) ELSE tf1.fecha_inc
		END AS "fecha_inc",
		tf1.fecha,
		tf1.fecha_fin,
		tf1.id_duracion,
		tf1.id_curso,
		tf1.id_carrera,
		tf1.nom_curso,
		tf1.baja,
		tf1.fechabaja,
		tf1.aprobado,
		tf1.estado_beneficiario,
		CASE
			WHEN tf1.estado_beneficiario IS NOT NULL THEN tf1.estado_beneficiario
			WHEN tf1.baja = 0
    			AND tf1.fechabaja IS NULL
    			AND tf1.aprobado NOT IN (1,2,3,4,5,6,8) THEN (
    				CASE
						WHEN DATE_ADD('month', 1, tf1.fecha_fin) <= CURRENT_DATE THEN 'REPROBADO' -- GCBA cambia de 2 meses a 1 el 17/1/23
    					WHEN tf1.fecha_fin <= CURRENT_DATE THEN 'FINALIZO_CURSADA'
    					WHEN tf1.fecha_inc <= CURRENT_DATE
    					    AND tf1.fecha_fin > CURRENT_DATE THEN 'REGULAR'
    					WHEN tf1.fecha_inc > CURRENT_DATE THEN 'INSCRIPTO'
    				END
    			)
		END AS "estado_beneficiario2"
	FROM tf1
	WHERE nrodoc != ''
), carreras_al AS (
SELECT
    '' codigo_ct,
    tf2.nrodoc,
    min(tf2.fecha_inc) fecha_inc,
    min(tf2.fecha) fecha,
	max(tf2.fecha_fin) fecha_fin,
	0 id_duracion,
	0 id_curso,
	tf2.id_carrera,
	'' nom_curso,
	sc.nom_carrera,
	0 baja,
	CAST(NULL AS DATE) fechabaja,
	0 aprobado,
	'INSCRIPTO' estado_beneficiario
FROM
    tf2
LEFT JOIN "caba-piba-raw-zone-db"."sienfo_carreras" sc ON sc.id_carrera = tf2.id_carrera
WHERE tf2.id_carrera != 0
GROUP BY (tf2.id_carrera, tf2.nrodoc,sc.nom_carrera)
HAVING min(tf2.fecha) IS NOT NULL AND max(tf2.fecha_fin) IS NOT NULL
), car_cur AS ( --SOLO LA CARRERA 2 que todavia no esta definido como calcularla correctamente, el resto se calcula a continuaciÃ³n
/*
 * SELECT final, estado_beneficiario2 es el calculado con el algoritmo de fechas,
 *  estado de beneficiario se basa en las columnas de la tabla
 */
SELECT
    tf2.codigo_ct,
	tf2.nrodoc,
	tf2.id_curso,
	tf2.id_carrera,
	tf2.estado_beneficiario2 estado_beneficiario,
	tf2.nrodoc ||'-'||tf2.codigo_ct AS llave_doc_idcap, --si es curso es nrodoc y codigo_ct / si es carrera es nrodoc id_carrera
	'CURSO' tipo_formacion,
	UPPER(tf2.nom_curso) nombre_cur_car, --nombre del curso
	tf2.fecha fecha_inicio_edicion_capacitacion, -- fecha inicio del curso
	tf2.fecha_fin fecha_fin_edicion_capacitacion, -- fecha fin del curso
	tf2.fecha_inc fecha_inscipcion -- fecha inicio de la persona
	--tf2.id_duracion,
	--tf2.baja,
	--tf2.fechabaja,
	--tf2.aprobado,
	-- tf2.estado_beneficiario estado_beneficiario_orig,
FROM tf2
WHERE tf2.id_carrera = 0 --TOMO SOLO LOS QUE SON CURSOS
UNION (
SELECT
    cal.codigo_ct,
    cal.nrodoc,
    cal.id_curso,
    cal.id_carrera,
    cal.estado_beneficiario,
    cal.nrodoc ||'-'||CAST(cal.id_carrera AS VARCHAR) llave_doc_idcap, --Esta llave es para agrupar las carreras por ediciÃ³n capacitacion anual
    'CARRERA' tipo_formacion,
    UPPER(cal.nom_carrera) nombre_cur_car,
    cal.fecha fecha_inicio_edicion_capacitacion,
    cal.fecha_fin fecha_fin_edicion_capacitacion,
    cal.fecha_inc fecha_inscipcion
    --cal.id_duracion,
	--cal.baja,
	--cal.fechabaja,
	--cal.aprobado
FROM
    carreras_al cal
WHERE id_carrera = 2) --Este calculo es provisorio hasta que el area nos de la informaciÃ³n de como calcular carrera 2, el resto se calculan de otra forma
--Existen fechas de inicio del alumno anteriores a fecha de inicio del curso
--nrodoc esta muy sucio
--Los estados de beneficiario2 nulos son cuando aprobado = 9 (nuevo id, no esta en en dump) corresponde a nombre = Actualiza, observa = CETBA [NO SE QUE SIGNIFICA]
), carrera_18 AS (
SELECT
    NULL AS "codigo_ct",
    a.nrodoc,
    --a.id_alumno,
    0 AS id_curso,
    t.id_carrera,
    CASE
        WHEN count(ca.nrocertificado) > 1 AND count( DISTINCT t.id_curso) > 1 THEN 'EGRESADO'
        WHEN (ca.baja != 0 AND ca.baja IS NOT NULL) OR max(ca.fechabaja) IS NOT NULL THEN 'BAJA'
        WHEN DATE_ADD('year', 2, max(am.maxft)) < CURRENT_DATE AND (count(ca.nrocertificado) < 2 OR count( DISTINCT t.id_curso) < 2) THEN 'BAJA' -- CRITERIO A CONSULTAR 2 aÃ±os desde la finalizacion del ultimo curso de la carrera y no tiene cert
        WHEN count(ca.nrocertificado) < 2 OR count( DISTINCT t.id_curso) < 2 THEN 'REGULAR'
    END AS "estado_beneficiario",
    a.nrodoc ||'-'||CAST(t.id_carrera AS VARCHAR) llave_doc_idcap,
    'CARRERA' AS tipo_formacion,
    sc.nom_carrera AS nombre_cur_car,
    min(am.miff) AS fecha_inscipcion,
    min(am.mift) AS fecha_inicio_edicion_capacitacion,
    DATE_ADD('month', 4, max(am.maxft)) AS fecha_fin_edicion_capacitacion
    --La duraciÃ³n del ultimo curso es la fecha de inicio del curso + 4 meses
    --ca.baja,
    --max(ca.fechabaja) as fechabaja,
    --count(ca.nrocertificado) as cant_cert,
    --count( distinct t.id_curso) as cant_cur
FROM "caba-piba-raw-zone-db".sienfo_alumnos a
	INNER JOIN "caba-piba-raw-zone-db".sienfo_fichas_carreras ca ON a.nrodoc = ca.nrodoc
	INNER JOIN "caba-piba-raw-zone-db".sienfo_talleres t ON ca.id_carrera = t.id_carrera
	LEFT JOIN "caba-piba-raw-zone-db".sienfo_carreras sc ON sc.id_carrera = t.id_carrera
	LEFT JOIN (
	/* aca toma la maxima fecha de inscripciÃ³n a una cursada de la cual no se diÃ³ de baja
	no confundir con fechas de baja y aprobaciÃ³n de la carrera que provienen de sienfo_fichas_carrera*/
		SELECT
		    ff.nrodoc AS fi_nrodoc,
			MAX((cast(tt.fecha AS date))) AS maxft,
			MIN((cast(ff.fecha_inc AS date))) AS miff,
			MIN((cast(tt.fecha AS date))) AS mift
		FROM "caba-piba-raw-zone-db".sienfo_cursos cu
		LEFT JOIN "caba-piba-raw-zone-db".sienfo_talleres tt ON cu.id_curso = tt.id_curso
		LEFT JOIN "caba-piba-raw-zone-db".sienfo_fichas ff ON tt.codigo_ct = ff.codigo_ct
		WHERE cu.id_carrera = '18'
			--AND ff.baja = 0
			--AND aprobado in('1', '3','5')
			--AND cu.baja IN ('0', '1')
		GROUP BY
		    ff.nrodoc,
			cu.id_carrera
	        ) AS am ON ca.nrodoc = am.fi_nrodoc
WHERE ca.id_carrera = 18
GROUP BY
    a.nrodoc,
    a.id_alumno,
    t.id_carrera,
    ca.baja,
    sc.nom_carrera
/* existe gente que estÃ¡ como regular (2029) porque no tiene fecha de inicio ni de fin, posiblemente no hace match con el leftjoin de la subconsulta*/
), carrera_1 AS (
SELECT
    NULL AS "codigo_ct",
    fc.nrodoc,
    --al.id_alumno,
    0 AS id_curso,
    q1.id_carrera_1 AS id_carrera,
    CASE
        WHEN fc.id_carrera = 1 AND (year(maxft) BETWEEN cmt.ano_inicio_old AND cmt.anio_fin_old AND cant_mat_ap >= cmt.cant_materias_plan_estudio_old) OR (year(maxft) >=cmt.ano_inicio AND cant_mat_ap >= cmt.cant_materias_plan_estudio) AND fc.baja = 0 THEN 'EGRESADO'
        WHEN fc.fechabaja IS NOT NULL THEN 'BAJA'
    	WHEN fc.baja NOT IN (14, 22, 24, 0) AND fc.baja IS NOT NULL THEN 'BAJA'
    	ELSE 'REGULAR'
        END AS estado_beneficiario,
    fc.nrodoc ||'-'||CAST(q1.id_carrera_1 AS VARCHAR) llave_doc_idcap,
    'CARRERA' AS tipo_formacion,
    ca.nom_carrera AS nombre_cur_car,
    q1.miff AS fecha_inscipcion,
    q1.mift AS fecha_inicio_edicion_capacitacion,
    DATE_ADD('month', 6, q1.maxft) AS fecha_fin_edicion_capacitacion
    --q1.cant_mat_ap
FROM "caba-piba-raw-zone-db".sienfo_fichas_carreras fc
	LEFT JOIN "caba-piba-raw-zone-db".sienfo_alumnos al ON fc.nrodoc = al.nrodoc
	LEFT JOIN (
		SELECT fi.nrodoc,
			cu.id_carrera_1,
			COUNT(fi.nrodoc) AS cant_mat_ap,
			MAX(cast(tt.fecha AS date)) AS maxft,
			MIN((cast(fi.fecha_inc AS date))) AS miff,
			MIN((cast(tt.fecha AS date))) AS mift
		FROM "caba-piba-raw-zone-db".sienfo_cursos cu
			LEFT JOIN "caba-piba-raw-zone-db".sienfo_talleres tt ON cu.id_curso = tt.id_curso
			LEFT JOIN "caba-piba-raw-zone-db".sienfo_fichas fi ON tt.codigo_ct = fi.codigo_ct
		WHERE cu.id_carrera_1 = 1
			AND fi.baja = 0
			AND aprobado IN('1', '3','5')
			AND cu.baja IN('0', '1')
		GROUP BY fi.nrodoc,
			cu.id_carrera_1
		ORDER BY cant_mat_ap DESC
	) q1 ON al.nrodoc = q1.nrodoc
	LEFT JOIN "caba-piba-raw-zone-db".sienfo_carreras ca ON q1.id_carrera_1 = ca.id_carrera
	LEFT JOIN "caba-piba-staging-zone-db".goayvd_typ_sienfo_vw_cantidad_materias_por_trayecto cmt ON fc.id_carrera = cast(cmt.carrera AS int)
WHERE fc.id_carrera = 1
AND id_carrera_1 = 1
), carrera_29 AS (
SELECT
    NULL AS "codigo_ct",
    fc.nrodoc,
    --al.id_alumno,
    0 AS id_curso,
    q1.id_carrera_29 AS id_carrera,
    CASE
        WHEN fc.id_carrera = 29 AND (year(maxft) >=cmt.ano_inicio AND cant_mat_ap >= cmt.cant_materias_plan_estudio) AND fc.baja = 0 THEN 'EGRESADO'
        WHEN fc.fechabaja IS NOT NULL THEN 'BAJA'
    	WHEN fc.baja NOT IN (14, 22, 24, 0) AND fc.baja IS NOT NULL THEN 'BAJA'
    	ELSE 'REGULAR'
        END AS estado_beneficiario,
    fc.nrodoc ||'-'||CAST(q1.id_carrera_29 AS VARCHAR) llave_doc_idcap,
    'CARRERA' AS tipo_formacion,
    ca.nom_carrera AS nombre_cur_car,
    q1.miff AS fecha_inscipcion,
    q1.mift AS fecha_inicio_edicion_capacitacion,
    DATE_ADD('month', 6, q1.maxft) AS fecha_fin_edicion_capacitacion
    --q1.cant_mat_ap
FROM "caba-piba-raw-zone-db".sienfo_fichas_carreras fc
	LEFT JOIN "caba-piba-raw-zone-db".sienfo_alumnos al ON fc.nrodoc = al.nrodoc
	LEFT JOIN (
		SELECT fi.nrodoc,
			cu.id_carrera_29,
			COUNT(fi.nrodoc) AS cant_mat_ap,
			MAX(cast(tt.fecha AS date)) AS maxft,
			MIN((cast(fi.fecha_inc AS date))) AS miff,
			MIN((cast(tt.fecha AS date))) AS mift
		FROM "caba-piba-raw-zone-db".sienfo_cursos cu
			LEFT JOIN "caba-piba-raw-zone-db".sienfo_talleres tt ON cu.id_curso = tt.id_curso
			LEFT JOIN "caba-piba-raw-zone-db".sienfo_fichas fi ON tt.codigo_ct = fi.codigo_ct
		WHERE cu.id_carrera_29 = 29
			AND fi.baja = 0
			AND aprobado IN('1', '3','5')
			AND cu.baja IN('0', '1')
		GROUP BY fi.nrodoc,
			cu.id_carrera_29
		ORDER BY cant_mat_ap DESC
	) q1 ON al.nrodoc = q1.nrodoc
	LEFT JOIN "caba-piba-raw-zone-db".sienfo_carreras ca ON q1.id_carrera_29 = ca.id_carrera
	LEFT JOIN "caba-piba-staging-zone-db".goayvd_typ_sienfo_vw_cantidad_materias_por_trayecto cmt ON fc.id_carrera = cmt.carrera
WHERE fc.id_carrera = 29
AND id_carrera_29 = 29
), carrera_30 AS (
SELECT
    NULL AS "codigo_ct",
    fc.nrodoc,
    --al.id_alumno,
    0 AS id_curso,
    q1.id_carrera_30 AS id_carrera,
    CASE
        WHEN fc.id_carrera = 30 AND (year(maxft) >=cmt.ano_inicio AND cant_mat_ap >= cmt.cant_materias_plan_estudio) AND fc.baja = 0 THEN 'EGRESADO'
        WHEN fc.fechabaja IS NOT NULL THEN 'BAJA'
    	WHEN fc.baja NOT IN (14, 22, 24, 0) AND fc.baja IS NOT NULL THEN 'BAJA'
    	ELSE 'REGULAR'
        END AS estado_beneficiario,
    fc.nrodoc ||'-'||CAST(q1.id_carrera_30 AS VARCHAR) llave_doc_idcap,
    'CARRERA' AS tipo_formacion,
    ca.nom_carrera AS nombre_cur_car,
    q1.miff AS fecha_inscipcion,
    q1.mift AS fecha_inicio_edicion_capacitacion,
    DATE_ADD('month', 6, q1.maxft) AS fecha_fin_edicion_capacitacion
    --q1.cant_mat_ap
FROM "caba-piba-raw-zone-db".sienfo_fichas_carreras fc
	LEFT JOIN "caba-piba-raw-zone-db".sienfo_alumnos al ON fc.nrodoc = al.nrodoc
	LEFT JOIN (
		SELECT fi.nrodoc,
			cu.id_carrera_30,
			COUNT(fi.nrodoc) AS cant_mat_ap,
			MAX(cast(tt.fecha AS date)) AS maxft,
			MIN((cast(fi.fecha_inc AS date))) AS miff,
			MIN((cast(tt.fecha AS date))) AS mift
		FROM "caba-piba-raw-zone-db".sienfo_cursos cu
			LEFT JOIN "caba-piba-raw-zone-db".sienfo_talleres tt ON cu.id_curso = tt.id_curso
			LEFT JOIN "caba-piba-raw-zone-db".sienfo_fichas fi ON tt.codigo_ct = fi.codigo_ct
		WHERE cu.id_carrera_30 = 30
			AND fi.baja = 0
			AND aprobado IN('1', '3','5')
			AND cu.baja IN('0', '1')
		GROUP BY fi.nrodoc,
			cu.id_carrera_30
		ORDER BY cant_mat_ap DESC
	) q1 ON al.nrodoc = q1.nrodoc
	LEFT JOIN "caba-piba-raw-zone-db".sienfo_carreras ca ON q1.id_carrera_30 = ca.id_carrera
	LEFT JOIN "caba-piba-staging-zone-db".goayvd_typ_sienfo_vw_cantidad_materias_por_trayecto cmt ON fc.id_carrera = cmt.carrera
WHERE fc.id_carrera = 30
AND id_carrera_30 = 30
), carrera_31 AS (
SELECT
    NULL AS "codigo_ct",
    fc.nrodoc,
    --al.id_alumno,
    0 AS id_curso,
    q1.id_carrera_31 AS id_carrera,
    CASE
        WHEN fc.id_carrera = 31 AND (year(maxft) >=cmt.ano_inicio AND cant_mat_ap >= cmt.cant_materias_plan_estudio) AND fc.baja = 0 THEN 'EGRESADO'
        WHEN fc.fechabaja IS NOT NULL THEN 'BAJA'
    	WHEN fc.baja NOT IN (14, 22, 24, 0) AND fc.baja IS NOT NULL THEN 'BAJA'
    	ELSE 'REGULAR'
        END AS estado_beneficiario,
    fc.nrodoc ||'-'||CAST(q1.id_carrera_31 AS VARCHAR) llave_doc_idcap,
    'CARRERA' AS tipo_formacion,
    ca.nom_carrera AS nombre_cur_car,
    q1.miff AS fecha_inscipcion,
    q1.mift AS fecha_inicio_edicion_capacitacion,
    DATE_ADD('month', 6, q1.maxft) AS fecha_fin_edicion_capacitacion
    --q1.cant_mat_ap
FROM "caba-piba-raw-zone-db".sienfo_fichas_carreras fc
	LEFT JOIN "caba-piba-raw-zone-db".sienfo_alumnos al ON fc.nrodoc = al.nrodoc
	LEFT JOIN (
		SELECT fi.nrodoc,
			cu.id_carrera_31,
			COUNT(fi.nrodoc) AS cant_mat_ap,
			MAX(cast(tt.fecha AS date)) AS maxft,
			MIN((cast(fi.fecha_inc AS date))) AS miff,
			MIN((cast(tt.fecha AS date))) AS mift
		FROM "caba-piba-raw-zone-db".sienfo_cursos cu
			LEFT JOIN "caba-piba-raw-zone-db".sienfo_talleres tt ON cu.id_curso = tt.id_curso
			LEFT JOIN "caba-piba-raw-zone-db".sienfo_fichas fi ON tt.codigo_ct = fi.codigo_ct
		WHERE cu.id_carrera_31 = 31
			AND fi.baja = 0
			AND aprobado IN('1', '3','5')
			AND cu.baja IN('0', '1')
		GROUP BY fi.nrodoc,
			cu.id_carrera_31
		ORDER BY cant_mat_ap DESC
	) q1 ON al.nrodoc = q1.nrodoc
	LEFT JOIN "caba-piba-raw-zone-db".sienfo_carreras ca ON q1.id_carrera_31 = ca.id_carrera
	LEFT JOIN "caba-piba-staging-zone-db".goayvd_typ_sienfo_vw_cantidad_materias_por_trayecto cmt ON fc.id_carrera = cmt.carrera
WHERE fc.id_carrera = 31
AND id_carrera_31 = 31
), carrera_32 AS (
SELECT
    NULL AS "codigo_ct",
    fc.nrodoc,
    --al.id_alumno,
    0 AS id_curso,
    q1.id_carrera_32 AS id_carrera,
    CASE
        WHEN fc.id_carrera = 32 AND (year(maxft) >=cmt.ano_inicio AND cant_mat_ap >= cmt.cant_materias_plan_estudio) AND fc.baja = 0 THEN 'EGRESADO'
        WHEN fc.fechabaja IS NOT NULL THEN 'BAJA'
    	WHEN fc.baja NOT IN (14, 22, 24, 0) AND fc.baja IS NOT NULL THEN 'BAJA'
    	ELSE 'REGULAR'
        END AS estado_beneficiario,
    fc.nrodoc ||'-'||CAST(q1.id_carrera_32 AS VARCHAR) llave_doc_idcap,
    'CARRERA' AS tipo_formacion,
    ca.nom_carrera AS nombre_cur_car,
    q1.miff AS fecha_inscipcion,
    q1.mift AS fecha_inicio_edicion_capacitacion,
    DATE_ADD('month', 6, q1.maxft) AS fecha_fin_edicion_capacitacion
    --q1.cant_mat_ap
FROM "caba-piba-raw-zone-db".sienfo_fichas_carreras fc
	LEFT JOIN "caba-piba-raw-zone-db".sienfo_alumnos al ON fc.nrodoc = al.nrodoc
	LEFT JOIN (
		SELECT fi.nrodoc,
			cu.id_carrera_32,
			COUNT(fi.nrodoc) AS cant_mat_ap,
			MAX(cast(tt.fecha AS date)) AS maxft,
			MIN((cast(fi.fecha_inc AS date))) AS miff,
			MIN((cast(tt.fecha AS date))) AS mift
		FROM "caba-piba-raw-zone-db".sienfo_cursos cu
			LEFT JOIN "caba-piba-raw-zone-db".sienfo_talleres tt ON cu.id_curso = tt.id_curso
			LEFT JOIN "caba-piba-raw-zone-db".sienfo_fichas fi ON tt.codigo_ct = fi.codigo_ct
		WHERE cu.id_carrera_32 = 32
			AND fi.baja = 0
			AND aprobado IN('1', '3','5')
			AND cu.baja IN('0', '1')
		GROUP BY fi.nrodoc,
			cu.id_carrera_32
		ORDER BY cant_mat_ap DESC
	) q1 ON al.nrodoc = q1.nrodoc
	LEFT JOIN "caba-piba-raw-zone-db".sienfo_carreras ca ON q1.id_carrera_32 = ca.id_carrera
	LEFT JOIN "caba-piba-staging-zone-db".goayvd_typ_sienfo_vw_cantidad_materias_por_trayecto cmt ON fc.id_carrera = cmt.carrera
WHERE fc.id_carrera = 32
AND id_carrera_32 = 32
)
SELECT
    *
FROM
    carrera_18
UNION
SELECT
    *
FROM
    carrera_1
UNION
SELECT
    *
FROM
    carrera_29
UNION
SELECT
    *
FROM
    carrera_30
UNION
SELECT
    *
FROM
    carrera_31
UNION
SELECT
    *
FROM
    carrera_32
UNION
SELECT
    *
FROM
    car_cur
--</sql>--



-- Copy of 2023.05.24 step 11 - staging estado_beneficiario_goet.sql 



-- --<sql>--DROP TABLE `caba-piba-staging-zone-db`.`tbp_typ_tmp_estado_beneficiario_goet`;--</sql>--
--<sql>--
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_estado_beneficiario_goet" AS
WITH master AS (
	SELECT
	    ia."IdUsuario",
		ia."IdCtrCdCurso",
		u."NDocumento",
		cast(ia."FechaInscripcion" AS DATE) AS "FechaInscripcion",
		ccc."InicioCurso",
		ccc."FinCurso",
		ccc."IdKeyTrayecto",
		cast(u."IdUsuario" AS VARCHAR) || '-' || cast(ia."IdCtrCdCurso" AS varchar) AS "IdCertificadoCurso", --- ID PARA MATCH CON ALUMNOS Y CURSOS SI SOLO SI IdKeyTrayecto ES 0 (curso)
		cast(u."IdUsuario" AS VARCHAR) || '-' || cast(ccc."IdKeyTrayecto" AS varchar) AS "IdCertificadoCarrera"  --- ID PARA MATCH CON ALUMNOS Y CARRERAS SI SOLO SI IdKeyTrayecto ES !=0
		--ia."IdInscripcion",
		--ia."IdInscripcionEstado",
		--ia."IdInscripcionEstadoAL",
		--ia."NInscOnline",
		--ia."IDInscripcionOrigen",
		--ia."ESTADO"
	FROM
		"caba-piba-staging-zone-db"."tbp_typ_tmp_view_goet_usuarios_no_duplicates" u
	INNER JOIN
		"caba-piba-raw-zone-db"."goet_inscripcion_alumnos" ia ON
		u."IdUsuario" = ia."IdUsuario"
	INNER JOIN
		"caba-piba-raw-zone-db"."goet_centro_codigo_curso" ccc ON
		ia."IdCtrCdCurso" = ccc."IdCtrCdCurso"
	INNER JOIN
		"caba-piba-raw-zone-db"."goet_centro_habilitacion_modulos" chm ON
		chm."IdCtrHbModulo" = ccc."IdCtrHbModulo"
	WHERE
		1 = 1
		AND ia."IdInscripcionEstado" = 2 --InscripciÃ³n aceptada	--AND ia."IdInscripcionEstadoAL" IN (2,4) --confirmar con el area --AND ia."IDInscripcionOrigen" IN (1,2) --confirmar con el area
), cer_cur AS (
	SELECT
		t."IdCertificadoCurso",
		t."Fecha" AS "Fehca_cert",
		UPPER(ce."Detalle") AS "estado_beneficiario"
	FROM
		(  --EXISTEN CERTIFICADOS DUPLICADOS, TOMO LA ULTIMA FECHA Y ESTADO MENOR (1 = APROBADO)
		SELECT
			ca."IdCertificadoEstado",
			ca."Fecha",
			cast(ca."IdUsuario" AS varchar)||'-'||cast(ca."IdCtrCdCurso" AS varchar) AS "IdCertificadoCurso",
			row_number() OVER (PARTITION BY ca."IdUsuario", ca."IdCtrCdCurso" ORDER BY ca."Fecha" DESC, ca."IdCertificadoEstado" DESC) AS ranking
		FROM
			"caba-piba-raw-zone-db"."goet_certificado_alumnos" ca
		WHERE
			ca."IdCtrCdCurso" != 0	AND
			ca."IdCertificadoEstado" NOT IN (3,4) -- 3 = 'PENDIENTE', 4 = 'AUSENTE' CONSULTAR CON AREA, POR AHORA ENTRAN COMO SIN CERTIFICADO EN EL ALGORITMO DE FECHAS
		) t
	LEFT JOIN
		"caba-piba-raw-zone-db"."goet_certificado_estado" ce ON
		ce."IdCertificadoEstado"  = t."IdCertificadoEstado"
	WHERE
		ranking = 1
), cur AS (
	SELECT
        m."IdUsuario",
	    m."IdKeyTrayecto",
	    m."IdCtrCdCurso",
	    'CURSO' AS "tipo_capacitacion",
		m."FechaInscripcion",
		m."InicioCurso",
		m."FinCurso",
		m."IdCertificadoCurso",
		NULL AS "IdCertificadoCarrera",
		ccu."Fehca_cert",
		CASE
		    WHEN ccu."estado_beneficiario" = 'DESAPROBADO' THEN 'REPROBADO'
			WHEN ccu."estado_beneficiario" IS NULL THEN
			(
			    CASE
			        WHEN date_add('month', 24, m."FinCurso") <= CURRENT_DATE THEN 'REPROBADO' -- CONFIRMAR CON EL AREA CUANTO TIEMPO DESPUES DE FINALIZADO QUEDA LIBRE
			    	WHEN m."FinCurso" <= CURRENT_DATE THEN 'FINALIZO_CURSADA'
				    WHEN m."InicioCurso" < CURRENT_DATE AND m."FinCurso" > CURRENT_DATE THEN 'REGULAR'
				    WHEN m."InicioCurso" > CURRENT_DATE THEN 'INSCRIPTO'
			    END
			)
			ELSE ccu."estado_beneficiario"
		END AS "estado_beneficiario"
	FROM
			master m
	LEFT JOIN
			cer_cur ccu ON
			m."IdCertificadoCurso" = ccu."IdCertificadoCurso"
	WHERE
		m."IdKeyTrayecto" = 0
), cer_car AS (
	SELECT
		t."IdCertificadoCarrera",
		t."Fecha" AS "Fehca_cert",
		UPPER(ce."Detalle") AS "estado_beneficiario"
		FROM --AGRUPO Y ME QUEDO CON EL ULTIMO CERTIFICADO
			(
			SELECT
				cet."IdCertificadoEstado",
				cet."Fecha",
				cast(cet."IdUsuario" AS varchar) ||'-'|| cast(cet."IdKeyTrayecto" AS varchar) AS "IdCertificadoCarrera",
				row_number() OVER (PARTITION BY cet."IdUsuario", cet."IdKeyTrayecto" ORDER BY cet."Fecha" DESC, cet."IdCertificadoEstado" DESC) AS ranking
			FROM
				"caba-piba-raw-zone-db"."goet_certificado_trayectos" cet
			WHERE
				cet."IdKeyTrayecto" != 0 AND cet."IdCertificadoEstado" NOT IN (3,4) -- 3 = 'PENDIENTE', 4 = 'AUSENTE' CONSULTAR CON AREA, POR AHORA ENTRAN COMO SIN CERTIFICADO EN EL ALGORITMO DE FECHAS
			) t
	LEFT JOIN
			"caba-piba-raw-zone-db"."goet_certificado_estado" ce ON
			ce."IdCertificadoEstado"  = t."IdCertificadoEstado"
	WHERE
			ranking = 1
),carrera_group AS (
-- AGRUPO A LAS PERSONAS POR TRAYECTO REALIZADO
	SELECT
		min(m."FechaInscripcion") AS "FechaInscripcion",
		min(m."InicioCurso") AS "InicioCurso",
		max(m."FinCurso") AS "FinCurso",
		m."IdKeyTrayecto",
		m."IdUsuario",
		m."IdCertificadoCarrera",
		m."IdCtrCdCurso"
	FROM
		master m
	INNER JOIN --ME ASEGURO DE QUE SEAN QUIENES TIENEN UNA CORRESPONDENCIA EN TABLA DE TRAYECTOS
		"caba-piba-raw-zone-db"."goet_trayecto" t ON
		t."IdKeyTrayecto" = m."IdKeyTrayecto"
	GROUP BY
		m."IdKeyTrayecto",
		m."IdUsuario",
		m."IdCertificadoCarrera",
		m."IdCtrCdCurso"
), car1 AS ( --TOMO LAS PERSONAS AGRUPADAS Y LAS JOINEO CON LOS CERTIFICADOS
	SELECT
	    cg."IdUsuario",
	    cg."IdKeyTrayecto",
	    cg."IdCtrCdCurso" AS "IdCtrCdCurso",
	    'CARRERA' AS "tipo_capacitacion",
		cg."FechaInscripcion",
		cg."InicioCurso",
		cg."FinCurso",
		NULL AS "IdCertificadoCurso",
		cg."IdCertificadoCarrera",
		cca."Fehca_cert",
		CASE
		    WHEN cca."estado_beneficiario" = 'DESAPROBADO' THEN 'REPROBADO'
			WHEN cca."estado_beneficiario" IS NULL THEN
			(
		        CASE
			        WHEN date_add('month', 24, cg."FinCurso") <= CURRENT_DATE THEN 'REPROBADO' -- CONFIRMAR CON EL AREA CUANTO TIEMPO DESPUES DE FINALIZADO QUEDA LIBRE
			    	WHEN cg."FinCurso" <= CURRENT_DATE THEN 'FINALIZO_CURSADA'
				    WHEN cg."InicioCurso" < CURRENT_DATE AND cg."FinCurso" > CURRENT_DATE THEN 'REGULAR'
				    WHEN cg."InicioCurso" > CURRENT_DATE THEN 'INSCRIPTO'
			    END
			)
			ELSE cca."estado_beneficiario"
		END AS "estado_beneficiario"
	FROM
		carrera_group cg
	LEFT JOIN
		cer_car cca ON
		cg."IdCertificadoCarrera" = cca."IdCertificadoCarrera"
), cur_car AS (
SELECT
    *
FROM
    car1
UNION
SELECT
    *
FROM
    cur
)
SELECT
    IdUsuario,
    IdCtrCdCurso,
    tipo_capacitacion,
    min(FechaInscripcion) FechaInscripcion,
    InicioCurso,
    FinCurso,
    IdCertificadoCurso,
    IdCertificadoCarrera,
    Fehca_cert,
    estado_beneficiario
FROM
    cur_car
GROUP BY
    IdUsuario,
    IdCtrCdCurso,
    tipo_capacitacion,
    InicioCurso,
    FinCurso,
    IdCertificadoCurso,
    IdCertificadoCarrera,
    Fehca_cert,
    estado_beneficiario
--</sql>--



-- Copy of 2023.05.24 step 12 - staging estado_beneficiario_moodle.sql 



-- --<sql>--DROP TABLE `caba-piba-staging-zone-db`.`tbp_typ_tmp_estado_beneficiario_moodle`;--</sql>--
--<sql>--
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_estado_beneficiario_moodle" AS
	WITH cursos AS (
			SELECT mc.id AS id_curso,
				mc.fullname AS nombre_curso,
				CAST(from_unixtime(CAST(mc.startdate AS BIGINT)) AS DATE) AS inicio_curso,
				CASE
					WHEN mc.enddate = 0
						THEN NULL
					ELSE cast(FROM_UNIXTIME(cast(mc.enddate AS BIGINT)) AS DATE)
					END AS fin_curso,
				mc.category AS id_curso_categoria,
				mcc.id AS id_categoria,
				mcc.idnumber AS programa_curso
			FROM "caba-piba-raw-zone-db"."moodle_dgtedu01_mdl_course" mc
			LEFT JOIN "caba-piba-raw-zone-db"."moodle_dgtedu01_mdl_course_categories" mcc ON mcc.id = mc.category
			),
		inscripciones_de_usuario AS (
			SELECT mue.userid AS inscripcion_id_alumno,
				mue.enrolid AS inscripcion_id_usuario,
				cast(FROM_UNIXTIME(cast(mue.timestart AS BIGINT)) AS DATE) AS inscripcion_inicio_cursada,
				CASE
					WHEN mue.timeend = 0
						THEN cast(NULL AS date)
					END AS inscripcion_final_cursada,
				cast(FROM_UNIXTIME(cast(mue.timemodified AS BIGINT)) AS DATE) AS fecha_modificacion_inscripcion_cursada
			FROM "caba-piba-raw-zone-db"."moodle_dgtedu01_mdl_user_enrolments" mue
			),
		alumnos AS (
			SELECT mu.id AS alumno_id,
				mu.firstname AS alumno_nombre,
				mu.lastname AS alumno_apellido,
				mu.username AS alumno_dni,
				mcc.userid AS id_terminacion_alumno,
				mcc.course AS curso_terminacion_alumno,
				mcc.timecompleted AS fecha_terminacion_alumno
			FROM "caba-piba-raw-zone-db"."moodle_dgtedu01_mdl_user" mu
			LEFT JOIN "caba-piba-raw-zone-db"."moodle_dgtedu01_mdl_course_completions" mcc ON mcc.userid = mu.id
			),
		inscripcion_cursos AS (
			SELECT me.id AS inscripcion_id,
				me.courseid AS inscripcion_curso_id
			FROM "caba-piba-raw-zone-db"."moodle_dgtedu01_mdl_enrol" me
			),
		maestro AS (
			SELECT aa.alumno_id,
				aa.alumno_nombre,
				aa.alumno_apellido,
				aa.alumno_dni,
				aa.curso_terminacion_alumno,
				iu.inscripcion_inicio_cursada,
				iu.inscripcion_final_cursada,
				iu.fecha_modificacion_inscripcion_cursada,
				-- ic.*,
				c.id_curso,
				c.nombre_curso,
				c.inicio_curso,
				c.fin_curso,
				c.id_categoria,
				c.programa_curso,
				-- CALCULOS PARA DEFINIR EL ESTADO
				CASE
					-- when (inscripcion_inicio_cursada is not null and inscripcion_final_cursada is not null) then 'finalizado_c'
					WHEN inscripcion_final_cursada < fin_curso
						THEN 'baja'
					WHEN fin_curso < CURRENT_DATE
						THEN '2_finalizado'
					WHEN curso_terminacion_alumno IS NOT NULL
						THEN 'egresado'
					WHEN inscripcion_inicio_cursada IS NOT NULL
						AND inscripcion_final_cursada IS NULL
						THEN '1_regular'
					ELSE 'a definir' -- When inscripcion_inicio_cursada is not null then 'inscripto'
					END AS estado
			FROM alumnos aa
			LEFT JOIN inscripciones_de_usuario iu ON iu.inscripcion_id_alumno = aa.alumno_id
			LEFT JOIN inscripcion_cursos ic ON ic.inscripcion_id = iu.inscripcion_id_usuario
			LEFT JOIN cursos c ON c.id_curso = ic.inscripcion_curso_id
			WHERE (
					programa_curso LIKE 'CAC%' -- CAC
					OR programa_curso LIKE 'FPE%'
					) -- Habilidades/FormaciÃ³n para la empleabilidad
				-- AND alumno_id = 362 -- caso de ejemplo
				-- group by alumno_id, programa_curso
			),
		solo_cursos AS (
			SELECT m.estado,
				'curso' AS tipo_capacitacion,
				m.alumno_id,
				m.alumno_nombre,
				m.alumno_apellido,
				m.alumno_dni,
				m.curso_terminacion_alumno,
				m.inscripcion_inicio_cursada,
				m.inscripcion_final_cursada,
				m.fecha_modificacion_inscripcion_cursada,
				m.nombre_curso,
				m.inicio_curso,
				m.fin_curso,
				m.id_categoria,
				m.programa_curso,
				m.id_curso
			FROM maestro m
				/*where m.nombre_curso not in ('DW-FRONT END 2021',
		 'DW - BACK END 2021',
		 'PYTHON - BACK END 2021',
		 'PHP-BACK END 2021',
		 'JAVA-BACK END 2021',
		 'PYTHON - FRONT END 2021',
		 'PHP-FRONT END 2021',
		 'JAVA-FRONT END 2021')*/
			),
		carreras AS (
			SELECT t.estado,
				-- tipo_capacitacion,
				'carrera' AS tipo_capacitacion,
				t.alumno_id,
				t.alumno_nombre,
				t.alumno_apellido,
				t.alumno_dni,
				NULL AS curso_terminacion_alumno,
				min(t.inscripcion_inicio_cursada) AS inscripcion_inicio_cursada,
				max(t.inscripcion_final_cursada) AS inscripcion_final_cursada,
				max(t.fecha_modificacion_inscripcion_cursada) AS fecha_modificacion_inscripcion_cursada,
				t.nombre_curso,
				min(t.inicio_curso) AS inicio_curso,
				max(t.fin_curso) AS fin_curso,
				t.id_categoria,
				t.programa_curso,
				t.id_curso
			FROM (
				SELECT m.estado,
					CASE
						WHEN m.nombre_curso LIKE '%PYTHON%'
							THEN 'FULLSTACK PYTHON'
						WHEN m.nombre_curso LIKE '%JAVA%'
							THEN 'FULLSTACK JAVA'
						WHEN m.nombre_curso LIKE '%PHP%'
							THEN 'FULLSTACK PHP'
						WHEN m.nombre_curso LIKE '%DW%'
							THEN 'FULLSTACK DW'
						END AS nombre_curso,
					-- 'carrera' as tipo_capacitacion,
					m.alumno_id,
					m.alumno_nombre,
					m.alumno_apellido,
					m.alumno_dni,
					NULL AS curso_terminacion_alumno,
					m.inscripcion_inicio_cursada,
					m.inscripcion_final_cursada,
					m.fecha_modificacion_inscripcion_cursada,
					m.inicio_curso,
					m.fin_curso,
					m.id_categoria,
					m.programa_curso,
					m.id_curso,
					row_number() OVER (
						PARTITION BY alumno_id,
						m.programa_curso ORDER BY estado ASC
						) AS ranking
				FROM maestro m
				WHERE m.nombre_curso IN (
						'DW-FRONT END 2021',
						'DW - BACK END 2021',
						'PYTHON - BACK END 2021',
						'PHP-BACK END 2021',
						'JAVA-BACK END 2021',
						'PYTHON - FRONT END 2021',
						'PHP-FRONT END 2021',
						'JAVA-FRONT END 2021'
						)
				) t
			WHERE ranking = 1
			GROUP BY t.estado,
				t.alumno_id,
				t.alumno_nombre,
				t.alumno_apellido,
				t.alumno_dni,
				t.programa_curso,
				t.id_categoria,
				t.nombre_curso,
				t.id_curso
			)
SELECT resultado.*
FROM
	(SELECT a.*,
	ROW_NUMBER() OVER(
					PARTITION BY
					a.id_categoria,
					a.alumno_id
					ORDER BY a.prioridad_orden ASC
				) AS "orden_duplicado"
	FROM
	(
		SELECT UPPER(sc.tipo_capacitacion) tipo_capacitacion,
			sc.alumno_id,
			sc.alumno_nombre,
			sc.alumno_apellido,
			sc.alumno_dni,
			sc.curso_terminacion_alumno,
			sc.inscripcion_inicio_cursada,
			sc.inscripcion_final_cursada,
			sc.fecha_modificacion_inscripcion_cursada,
			sc.nombre_curso,
			sc.inicio_curso,
			sc.fin_curso,
			sc.id_categoria,
			sc.programa_curso,
			CASE
				WHEN sc.estado = '1_regular'
					THEN UPPER('regular')
				WHEN sc.estado = '2_finalizado'
					THEN UPPER('finalizado')
				ELSE UPPER(sc.estado)
				END AS estado,
			CASE
				WHEN sc.estado = '1_regular' THEN 5 -- 'REGULAR'
				WHEN sc.estado = '2_finalizado' THEN 3 -- 'FINALIZADO'
				WHEN sc.estado = 'egresado' THEN 1 -- 'EGRESADO'
				ELSE 7
			END AS prioridad_orden,
			sc.id_curso
		FROM solo_cursos sc

		UNION

		SELECT UPPER(ca.tipo_capacitacion) tipo_capacitacion,
			ca.alumno_id,
			ca.alumno_nombre,
			ca.alumno_apellido,
			ca.alumno_dni,
			ca.curso_terminacion_alumno,
			ca.inscripcion_inicio_cursada,
			ca.inscripcion_final_cursada,
			ca.fecha_modificacion_inscripcion_cursada,
			ca.nombre_curso,
			ca.inicio_curso,
			ca.fin_curso,
			ca.id_categoria,
			ca.programa_curso,
			CASE
				WHEN ca.estado = '1_regular'
					THEN UPPER('regular')
				WHEN ca.estado = '2_finalizado'
					THEN UPPER('finalizado')
				ELSE UPPER(ca.estado)
				END AS estado,
			CASE
				WHEN ca.estado = '1_regular' THEN 5 -- 'REGULAR'
				WHEN ca.estado = '2_finalizado' THEN 3 -- 'FINALIZADO'
				WHEN ca.estado = 'egresado' THEN 1 -- 'EGRESADO'
				ELSE 7
			END AS prioridad_orden,
			ca.id_curso
		FROM carreras ca) a ) resultado
WHERE resultado.orden_duplicado=1
--</sql>--



-- Copy of 2023.05.24 step 13 - staging estado_beneficiario_siu.sql 



-- --<sql>--DROP TABLE `caba-piba-staging-zone-db`.`tbp_typ_tmp_estado_beneficiario_siu`;--</sql>--
--<sql>--
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_estado_beneficiario_siu" AS
WITH preins AS (
	SELECT
		p.persona,
		p.fecha_registro AS fecha_preinscripcion,
		pp.propuesta,
		1 en_preinscripciones
	FROM
		"caba-piba-raw-zone-db".siu_preinscripcion_public_sga_preinscripcion p
	LEFT JOIN "caba-piba-raw-zone-db".siu_preinscripcion_public_sga_preinscripcion_propuestas pp ON
		p.id_preinscripcion = pp.id_preinscripcion
	WHERE
		p.persona IS NOT NULL -- Tomo solo los que tienen un id de persona, el resto NO los podemos considerar dentro de la entidad vecino
),
al AS (
	SELECT
		a.alumno,
		a.persona,
		a.propuesta,
		a.plan_version,
		a.regular, -- Regular en la carrera?
		a.calidad  -- Activo en la carrera?
	FROM
		"caba-piba-raw-zone-db"."siu_toba_3_3_negocio_sga_alumnos" a
),
--ESTA TABLA CONTIENE NOTAS DE PARCIALES + FINALES
ac AS ( --Actas detalle
	SELECT
		sad.id_acta,
		sad.instancia, -- Consultar si usar algun filtro de este campo
		sad.alumno,
		--sa.comision, -- NO TENEMOS COMISIÃN PORQUE FUERON MIGRADOS PERO NO IMPORTA PORQUE SOLO VAMOS A HACER UN COUNT
		sad.plan_version,
		sad.fecha,
		sad.fecha_vigencia,
		--- Estos campos siguientes son los que voy a filtrar en el where de la tabla siguiente para saber cuantas aprobÃ³ el alumno
		sad.nota,
		sa.origen,
		sad.resultado, -- A aprobado -R reprobado - U ausente
		sad.cond_regularidad,
		sad.estado
	FROM
		"caba-piba-raw-zone-db"."siu_toba_3_3_negocio_sga_actas_detalle" sad
	INNER JOIN "caba-piba-raw-zone-db"."siu_toba_3_3_negocio_sga_actas" sa ON sa.id_acta = sad.id_acta
),
-- Cantidad de materias aprobadas
-- PARA ESTAR APROBADO Y CERRADO YA NO DEBE TENER CONDICIÃN DE REGULARIDAD Y
-- TENER NOTA DE APROBACION (hablado con el area)
cap AS (
	SELECT
		ac.alumno,
		ac.plan_version,
		count(1) AS cantidad_materias_aprobadas
	FROM
		ac
	WHERE
		ac.estado = 'A' --Actas que NO fueron anuladas o dadas de baja
		AND ac.origen IN ('E','P')
		-- 'E' 'P' Corresponden a actas de aprobaciÃ³n de materia, no de parciales
		--'A','B' Aprobados por equivalencias en otro paÃ­s u otra provincia consultar con el area si se incluyen
		AND ac.nota IS NOT NULL -- tiene una nota
		AND ac.cond_regularidad IS NULL -- NO tiene condiciÃ³n de regularidad porque ya la aprobÃ³
		AND ac.resultado = 'A' -- EstÃ¡ aprobado
	GROUP BY ac.alumno, ac.plan_version
),
--Ultima acta del alumno para determinar regularidad
r AS (
	SELECT
		ac.alumno,
		ac.plan_version,
		max(ac.fecha) ultima_acta
	FROM
		ac
	GROUP BY
		ac.alumno,
		ac.plan_version
-- MINIMA FECHA DE INSCRIPCION A UNA MATERIA PARA DETERMINAR FECHA DE INICIO
-- MAXIMA FECHA DE INSCRIPCION PARA DETERMINAR REGULARIDAD
), min_insc_mat AS (
	SELECT
		ic.alumno,
		ic.plan_version,
		CAST(min(ic.fecha_inscripcion) AS date) AS fecha_inicio,
		CAST(max(ic.fecha_inscripcion) AS date) AS ultima_insc,
		-- desde TBP se agrega la columna ic.comision para poder joinear con la comision en la que esta inscripto
		ic.comision
	FROM
		"caba-piba-raw-zone-db"."siu_toba_3_3_negocio_sga_insc_cursada" ic
	WHERE ic.estado_preinscripcion = 'P' --Solo las inscripciones a materias aprobadas
	GROUP BY
		ic.alumno,
		ic.plan_version,
		ic.comision
), preins_ins AS (  -- UNIÃN DE LOS PREINSCRIPTOS INNER JOIN PERSONAS CON TABLA DE ALUMNOS INNER JOIN PERSONAS
	SELECT
		pe.persona,
		cast(NULL AS int) alumno,
		pin.fecha_preinscripcion,
		pin.propuesta,
		CAST (NULL AS int) plan_version,
		CASE
			WHEN en_preinscripciones = 1 THEN 'PREINSCRIPTO'
		END AS estado_beneficiario
	FROM
		"caba-piba-staging-zone-db"."tbp_typ_tmp_view_siu_toba_3_3_negocio_mdp_personas_no_duplicates" pe
	INNER JOIN
		preins pin ON pe.persona = pin.persona
UNION
	SELECT
		a.persona,
		a.alumno,
		CAST(NULL AS DATE) fecha_presincripcion,
		a.propuesta,
		a.plan_version,
		CAST(NULL AS VARCHAR) estado_beneficiario
	FROM
		al a
), preins_ins2 AS (
	SELECT
		pii.persona,
		pii.alumno,
		pii.fecha_preinscripcion,
		pii.propuesta,
		pii.plan_version,
		cap.cantidad_materias_aprobadas,
		r.ultima_acta,
		ic.fecha_inicio,
	    ic.ultima_insc,
		ic.comision,
	    pr.fecha AS fecha_baja, -- CONFIRMAR SI ES PERDIDA DE REGULARIDAD DE MATERIA O DE CARRERA (ASUMMO CARRERA PORQUE NO TIENE ID DE MATERIA)
	    CASE
	        WHEN  r.ultima_acta IS NULL AND ic.ultima_insc IS NOT NULL THEN ic.ultima_insc
    	    WHEN  r.ultima_acta IS NOT NULL AND ic.ultima_insc IS NULL THEN r.ultima_acta
	        WHEN r.ultima_acta IS NOT NULL AND ic.ultima_insc IS NOT NULL THEN (
	        CASE
    	        WHEN r.ultima_acta >= ic.ultima_insc THEN r.ultima_acta -- CONSULTAR CON EL AREA SI DETERMINO REGULARIDAD POR ULTIMA FECHA DE INSCRIPCION A MATERIA / ACTA /
    	        WHEN ic.ultima_insc > r.ultima_acta THEN ic.ultima_insc
    	   END)
	   END AS ultimo_mov,
		pii.estado_beneficiario
	FROM
		preins_ins pii
	LEFT JOIN
		cap ON
		cap.alumno = pii.alumno AND
		cap.plan_version = pii.plan_version
	LEFT JOIN
		r ON
		r.alumno = pii.alumno AND
		r.plan_version = pii.plan_version
	LEFT JOIN
	    min_insc_mat ic ON
	    ic.alumno = pii.alumno AND ic.plan_version = pii.plan_version
	LEFT JOIN
	    "caba-piba-raw-zone-db"."siu_toba_3_3_negocio_sga_perdida_regularidad" pr ON
	    pr.alumno = pii.alumno
)
SELECT resultado.*
FROM
	(SELECT a.*,
	ROW_NUMBER() OVER(
					PARTITION BY a.persona,
					a.alumno,
					a.propuesta,
					a.plan_version,
					a.broker_id
					ORDER BY a.prioridad_orden ASC
				) AS "orden_duplicado"
	FROM
	(SELECT
		pii2.persona,
		vec.broker_id,
		pii2.alumno,
		pii2.propuesta,
		pii2.plan_version,
		cmp.cant_materias,
		pii2.cantidad_materias_aprobadas,
		round((cast(pii2.cantidad_materias_aprobadas AS DOUBLE) / cmp.cant_materias)*100, 0) AS porcentaje_aprob,
		pii2.fecha_preinscripcion,
		pii2.fecha_baja,
		pii2.ultima_acta,   --ACTA DE EXAMEN APROBADO (CONSULTAR CON EL AREA SI DEBO SUMAR ACTAS REPROBADAS)
		pii2.fecha_inicio,  --PRIMERA INSCRIPCION A MATERIA
		pii2.ultima_insc,   --ULTIMA INSCRIPCION A MATERIA
		pii2.ultimo_mov,    --EVALUA SI LA ULTIMA ACTIVIDAD FUE UNA INSCRIPCION O UN ACTA DE EXAMEN APROBADO (CONSULTAR CON EL AREA SI DEBO SUMAR ACTAS REPROBADAS)
		CASE
			WHEN cmp.cant_materias <= cantidad_materias_aprobadas THEN 'EGRESADO'
			WHEN pii2.fecha_baja IS NOT NULL AND pii2.ultimo_mov IS NULL THEN 'BAJA'
			WHEN pii2.fecha_baja IS NOT NULL AND pii2.ultimo_mov IS NOT NULL AND pii2.fecha_baja > pii2.ultimo_mov THEN 'BAJA' --EXISTEN FECHAS DE ACTIVIDAD POSTERIORES A LA FECHA DE BAJA CONSULTAR CON EL AREA
			WHEN date_add('year', 2, pii2.ultimo_mov) < CURRENT_DATE THEN 'BAJA'
			WHEN date_add('year', 2, pii2.ultimo_mov) >= CURRENT_DATE THEN 'REGULAR'
			WHEN  pii2.ultimo_mov IS NULL AND pii2.fecha_preinscripcion IS NULL THEN 'INSCRIPTO'
			ELSE pii2.estado_beneficiario
		END AS estado_beneficiario,
		CASE
			WHEN cmp.cant_materias <= cantidad_materias_aprobadas THEN 1 -- EGRESADO
			WHEN pii2.fecha_baja IS NOT NULL AND pii2.ultimo_mov IS NULL THEN 4 -- 'BAJA'
			WHEN pii2.fecha_baja IS NOT NULL AND pii2.ultimo_mov IS NOT NULL AND pii2.fecha_baja > pii2.ultimo_mov THEN 4 -- 'BAJA' --EXISTEN FECHAS DE ACTIVIDAD POSTERIORES A LA FECHA DE BAJA CONSULTAR CON EL AREA
			WHEN date_add('year', 2, pii2.ultimo_mov) < CURRENT_DATE THEN 4 -- 'BAJA'
			WHEN date_add('year', 2, pii2.ultimo_mov) >= CURRENT_DATE THEN 5 -- 'REGULAR'
			WHEN  pii2.ultimo_mov IS NULL AND pii2.fecha_preinscripcion IS NULL THEN 6 -- 'INSCRIPTO'
			ELSE 7
		END AS prioridad_orden
	FROM
		preins_ins2 pii2
	LEFT JOIN "caba-piba-staging-zone-db"."goayvd_typ_tmp_siu_cantidad_materias_plan" CMP
		ON (cmp.propuesta = pii2.propuesta)
	LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_tmp_view_siu_toba_3_3_negocio_mdp_personas_no_duplicates" nmp
		ON (nmp.persona = pii2.persona)
	LEFT JOIN "caba-piba-raw-zone-db"."siu_toba_3_3_negocio_mdp_personas_documentos" nmpd
		ON (nmpd.documento = nmp.documento_principal)
	LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_vecino" vec
		ON (
			vec.base_origen = 'SIU'
			AND vec.documento_broker = CAST(nmpd.nro_documento AS VARCHAR)
			AND vec.genero_broker = nmp.sexo
			)
		) a
	) resultado
WHERE resultado.orden_duplicado=1
--</sql>--
--Ver casos fecha inicio (min(fecha insc cursada)) > fecha_acta (max(fecha de acta)) 1165 casos de 50816



-- Copy of 2023.05.24 step 14 - staging edicion capacitacion.sql 



-- 1.-- Crear EDICION CAPACITACION GOET, MOODLE, SIENFO Y CRMSL paso 1
-- --<sql>--DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_tmp_edicion_capacitacion_1`;--</sql>--
--<sql>--
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_edicion_capacitacion_1" AS
-- GOET
SELECT
		'GOET' base_origen,
		dcmatch.tipo_capacitacion,
		dcmatch.id_new capacitacion_id_new,
		CAST(dcmatch.id_old AS VARCHAR) capacitacion_id_old,
		CASE
		WHEN UPPER(t.detalle) IS NOT NULL
			THEN UPPER(t.Detalle)
		ELSE UPPER(n.Detalle)
		END descrip_capacitacion_old,

		CAST(cc.idctrcdcurso AS VARCHAR) edicion_capacitacion_id,


		-- Se utiliza las fechas de estado de beneficiario, en caso de NULL se utilizan las disponibles en la tabla goet_centro_codigo_curso
		CASE WHEN fechas.fecha_inicio IS NOT NULL
		THEN CAST(SPLIT_PART(date_format(fechas.fecha_inicio , '%Y-%m-%d %h:%i%p'), '-', 1)  AS INTEGER)
		ELSE CAST(SPLIT_PART(date_format(cc.iniciocurso, '%Y-%m-%d %h:%i%p'), '-', 1)  AS INTEGER)
		END anio_inicio,

		-- El proximo campo es el semestre de inicio de la edicion capacitacion, NO indica que la misma es semestral o anual
		-- Se utiliza las fechas de estado de beneficiario, en caso de NULL se utilizan las disponibles en la tabla goet_centro_codigo_curso
		CASE WHEN fechas.fecha_inicio IS NOT NULL
		THEN CASE WHEN fechas.fecha_inicio IS NOT NULL AND CAST(SPLIT_PART(date_format(fechas.fecha_inicio, '%Y-%m-%d %h:%i%p'), '-', 2)  AS INTEGER)  <= 6 THEN 1
					 WHEN fechas.fecha_inicio IS NOT NULL AND CAST(SPLIT_PART(date_format(fechas.fecha_inicio, '%Y-%m-%d %h:%i%p'), '-', 2)  AS INTEGER)  > 6 THEN 2
					 ELSE NULL
				END
		ELSE
			CASE WHEN cc.iniciocurso IS NOT NULL AND CAST(SPLIT_PART(date_format(cc.iniciocurso, '%Y-%m-%d %h:%i%p'), '-', 2)  AS INTEGER)  <= 6 THEN 1
				 WHEN cc.iniciocurso IS NOT NULL AND CAST(SPLIT_PART(date_format(cc.iniciocurso, '%Y-%m-%d %h:%i%p'), '-', 2)  AS INTEGER)  > 6 THEN 2
				 ELSE NULL
			END
		END semestre_inicio,

		-- Se utiliza las fechas de estado de beneficiario, en caso de NULL se utilizan las disponibles en la tabla goet_centro_codigo_curso
		CASE WHEN fechas.fecha_inicio IS NOT NULL
		THEN CAST(DATE_PARSE(date_format(fechas.fecha_inicio, '%Y-%m-%d %h:%i%p'), '%Y-%m-%d %h:%i%p') AS DATE)
		ELSE CAST(DATE_PARSE(date_format(cc.iniciocurso, '%Y-%m-%d %h:%i%p'), '%Y-%m-%d %h:%i%p') AS DATE)
		END fecha_inicio_dictado,

		-- Se utiliza las fechas de estado de beneficiario, en caso de NULL se utilizan las disponibles en la tabla goet_centro_codigo_curso
		CASE WHEN fechas.fecha_fin IS NOT NULL
		THEN CAST(DATE_PARSE(date_format(fechas.fecha_fin, '%Y-%m-%d %h:%i%p'), '%Y-%m-%d %h:%i%p') AS DATE)
		ELSE CAST(DATE_PARSE(date_format(cc.fincurso, '%Y-%m-%d %h:%i%p'), '%Y-%m-%d %h:%i%p') AS DATE)
		END fecha_fin_dictado,

		-- Se utiliza las fechas de estado de beneficiario, en caso de NULL se utilizan las disponibles en la tabla goet_centro_codigo_curso
		CASE WHEN fechas.fecha_inicio_inscripcion IS NOT NULL
		THEN CAST(DATE_PARSE(date_format(fechas.fecha_inicio_inscripcion, '%Y-%m-%d %h:%i%p'), '%Y-%m-%d %h:%i%p') AS DATE)
		ELSE CAST(DATE_PARSE(date_format(cc.inicioinscripcion, '%Y-%m-%d %h:%i%p'), '%Y-%m-%d %h:%i%p') AS DATE)
		END fecha_inicio_inscripcion,

		-- Se utiliza las fechas de estado de beneficiario, en caso de NULL se utilizan las disponibles en la tabla goet_centro_codigo_curso
		CASE WHEN fechas.fecha_fin_inscripcion IS NOT NULL
		THEN CAST(DATE_PARSE(date_format(fechas.fecha_fin_inscripcion, '%Y-%m-%d %h:%i%p'), '%Y-%m-%d %h:%i%p') AS DATE)
		ELSE CAST(DATE_PARSE(date_format(cc.cierreinscripcion, '%Y-%m-%d %h:%i%p'), '%Y-%m-%d %h:%i%p') AS DATE)
		END fecha_limite_inscripcion,

		-- se considera turno mañana si la hora de inicio es entre las 7 y 12, tarde entre 13 y 17, noche entre 18 y 24
		-- SI MENOR A 7 en GOET son datos inconsistentes, segun lo controlado en el dataleak
		CASE WHEN CAST(regexp_extract(cc.diayhorario, '(([0-9|:])\w+)') AS INTEGER) BETWEEN 7 AND 12 THEN 'Mañana'
		WHEN CAST(regexp_extract(cc.diayhorario, '(([0-9|:])\w+)') AS INTEGER) BETWEEN 13 AND 17 THEN 'Tarde'
		WHEN CAST(regexp_extract(cc.diayhorario, '(([0-9|:])\w+)') AS INTEGER) BETWEEN 18 AND 24 THEN 'Noche' ELSE NULL END turno,

		-- se convierte los dias a las siguientes posibilidades => Lunes,Martes,Miercoles,Jueves,Viernes,Sabado,Domingo
		CASE WHEN position('LU' IN UPPER(cc.diayhorario))!=0 THEN 'Lunes ' ELSE '' END ||
		CASE WHEN position('MA' IN UPPER(cc.diayhorario))!=0 THEN 'Martes ' ELSE '' END ||
		CASE WHEN position('MI' IN UPPER(cc.diayhorario))!=0 THEN 'Miercoles ' ELSE '' END ||
		CASE WHEN position('JU' IN UPPER(cc.diayhorario))!=0 THEN 'Jueves ' ELSE '' END ||
		CASE WHEN position('VI' IN UPPER(cc.diayhorario))!=0 THEN 'Viernes ' ELSE '' END ||
		CASE WHEN position('SA' IN UPPER(cc.diayhorario))!=0 THEN 'Sabado ' ELSE '' END ||
		CASE WHEN position('DO' IN UPPER(cc.diayhorario))!=0 THEN 'Domingo ' ELSE '' END dias_cursada,
		CASE
			WHEN cc.idcursoestado = 1
				THEN 'S'
			ELSE 'N'
			END inscripcion_abierta,
		CASE
			WHEN UPPER(en.Detalle) = 'ACTIVO'
				THEN 'S'
			ELSE 'N'
			END activo,

		-- Se utiliza la cantidad_inscriptos de estado de beneficiario, en caso de NULL se utiliza la disponible en la tabla goet_centro_codigo_curso
		CASE
		WHEN fechas.cantidad_inscriptos IS NOT NULL
		THEN CAST(fechas.cantidad_inscriptos AS VARCHAR)
		ELSE CAST(cc.matricula AS VARCHAR)
		END cant_inscriptos,

		CAST(cc.topematricula AS VARCHAR) vacantes,

		-- Si la modalidad no esta en la edicion se toma desde la entidad capacitacion, NULL en otro caso
		dc.modalidad_id,
		dc.descrip_modalidad,
		est.id cod_origen_establecimiento

	FROM
	"caba-piba-raw-zone-db"."goet_nomenclador" n
	LEFT JOIN "caba-piba-raw-zone-db"."goet_centro_habilitacion_modulos" chm ON n.IdNomenclador = chm.IdNomenclador
	LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_establecimientos" est ON (cast(chm.IdCentro AS varchar)=est.id_old)
	LEFT JOIN "caba-piba-raw-zone-db"."goet_centro_codigo_curso" cc ON cc.IdCtrHbModulo = chm.IdCtrHbModulo
	LEFT JOIN "caba-piba-raw-zone-db"."goet_inscripcion_alumnos" ia ON ia."IdCtrCdCurso" = cc."IdCtrCdCurso"
	LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_tmp_view_goet_usuarios_no_duplicates" u ON u."IdUsuario" = ia."IdUsuario"
	LEFT JOIN "caba-piba-raw-zone-db"."goet_trayecto" t ON (t.idkeytrayecto = cc.idkeytrayecto)
	LEFT JOIN "caba-piba-raw-zone-db"."goet_estado_nomenclador" en ON en.IdEstadoNomenclador = n.IdEstadoNomenclador
	LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_capacitacion_match" dcmatch ON (
			dcmatch.id_old = (
				CASE
					WHEN UPPER(t.detalle) IS NOT NULL
						THEN CAST(n.IdNomenclador AS VARCHAR) || '-' || CAST(t.IdKeyTrayecto AS VARCHAR)
					ELSE CAST(n.IdNomenclador AS VARCHAR)
					END
				)
			AND dcmatch.base_origen = 'GOET'
			)
	LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_capacitacion" dc ON (
			dcmatch.id_new = dc.id_new
			AND dcmatch.base_origen = dc.base_origen
			)

	LEFT JOIN (SELECT
		IdCtrCdCurso,
		CAST(MIN(InicioCurso) AS DATE) fecha_inicio,
		CAST(MAX(FinCurso) AS DATE) fecha_fin,
		CAST(MIN(FechaInscripcion) AS DATE) fecha_inicio_inscripcion,
		CAST(MAX(FechaInscripcion) AS DATE) fecha_fin_inscripcion,
		COUNT(*) cantidad_inscriptos
		FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_estado_beneficiario_goet"
		GROUP BY IdCtrCdCurso
	) fechas ON (cc."IdCtrCdCurso"=fechas.IdCtrCdCurso)
	GROUP BY
		dcmatch.tipo_capacitacion,
		dcmatch.id_new,
		dcmatch.id_old,
		UPPER(t.detalle),
		UPPER(n.Detalle),
		UPPER(en.Detalle),
		cc.idctrcdcurso,
		cc.iniciocurso,
		fechas.fecha_inicio,
		dc.fecha_inicio,
		cc.fincurso,
		fechas.fecha_fin,
		dc.fecha_fin,
		cc.inicioinscripcion,
		fechas.fecha_inicio_inscripcion,
		cc.cierreinscripcion,
		fechas.fecha_fin_inscripcion,
		cc.diayhorario,
		cc.idcursoestado,
		cc.matricula,
		fechas.cantidad_inscriptos,
		cc.topematricula,
		dc.modalidad_id,
		dc.descrip_modalidad,
		est.id
UNION
-- MOODLE
SELECT
	'MOODLE' base_origen,
	dcmatch.tipo_capacitacion,
	dcmatch.id_new capacitacion_id_new,
	CAST(cc.id AS VARCHAR) capacitacion_id_old,
	-- dado que no se utiliza en def, no se completa
	'' descrip_capacitacion_old,

	-- el id de capacitacion y de edicion capacitacion es el mismo,
	-- dado que para moodle cada edicion capacitacion es un curso/capacitacion en si mismo
	CAST(cc.id AS VARCHAR) edicion_capacitacion_id,

	-- se utiliza la fecha calculada en el script de estado de beneficiario
	-- En caso que este en null, se utiliza MIN(co.startdate) (tiene 89.87% de completitud), en otro caso queda en null
	CAST(SPLIT_PART(CASE
	WHEN fechas.fecha_inicio IS NULL
		THEN date_format(from_unixtime(MIN(TRY_CAST(co.startdate AS BIGINT))), '%Y-%m-%d %h:%i%p')
	ELSE date_format(cast(fechas.fecha_inicio AS date), '%Y-%m-%d %h:%i%p')
	END, '-', 1)  AS INTEGER) anio_inicio,

	-- El proximo campo es el semestre de inicio de la edicion capacitacion, NO indica que la misma es semestral o anual
	-- se utiliza la fecha calculada en el script de estado de beneficiario
	-- En caso que este en null, se utiliza MIN(co.startdate) (tiene 89.87% de completitud), en otro caso queda en null
	CASE
	WHEN CAST(SPLIT_PART(CASE
		WHEN fechas.fecha_inicio IS NULL
			THEN
				date_format(from_unixtime(MIN(TRY_CAST(co.startdate AS BIGINT))), '%Y-%m-%d %h:%i%p')
			ELSE
				date_format(cast(fechas.fecha_inicio AS date), '%Y-%m-%d %h:%i%p')
			END, '-', 2) AS INTEGER) <= 6
		THEN 1

	WHEN CAST(SPLIT_PART(CASE
		WHEN fechas.fecha_inicio IS NULL
			THEN
				date_format(from_unixtime(MIN(TRY_CAST(co.startdate AS BIGINT))), '%Y-%m-%d %h:%i%p')
			ELSE
				date_format(cast(fechas.fecha_inicio AS date), '%Y-%m-%d %h:%i%p')
			END, '-', 2) AS INTEGER) > 6
		THEN 2
	ELSE NULL
	END semestre_inicio,

	-- se utiliza la fecha calculada en el script de estado de beneficiario
	-- En caso que este en null, se utiliza MIN(co.startdate) (tiene 89.87% de completitud), en otro caso queda en null
	CAST(DATE_PARSE(CASE
	WHEN  fechas.fecha_inicio IS NULL
		THEN date_format(from_unixtime(MIN(TRY_CAST(co.startdate AS BIGINT))), '%Y-%m-%d %h:%i%p')
	ELSE date_format(cast(fechas.fecha_inicio AS date), '%Y-%m-%d %h:%i%p')
	END , '%Y-%m-%d %h:%i%p') AS DATE) fecha_inicio_dictado,

	-- se utiliza la fecha calculada en el script de estado de beneficiario
	-- En caso que este en null, se utiliza MAX(co.enddate) (tiene 24.90% de completitud), en otro caso queda en null
	CAST(DATE_PARSE(CASE
	WHEN  fechas.fecha_fin IS NULL
		THEN date_format(from_unixtime(MAX(TRY_CAST(co.enddate AS BIGINT))), '%Y-%m-%d %h:%i%p')
	ELSE date_format(cast(fechas.fecha_fin AS date), '%Y-%m-%d %h:%i%p')
	END , '%Y-%m-%d %h:%i%p') AS DATE) fecha_fin_dictado,

   -- no se encuentra en bbdd origen un valor para el atributo
   -- se utiliza la fecha calculada en el script de estado de beneficiario, en otro caso queda en null
	CAST(DATE_PARSE(CASE
	WHEN  fechas.fecha_inicio_inscripcion IS NULL
		THEN date_format(TRY_CAST(NULL AS DATE), '%Y-%m-%d %h:%i%p')
	ELSE date_format(cast(fechas.fecha_inicio_inscripcion AS date), '%Y-%m-%d %h:%i%p')
	END , '%Y-%m-%d %h:%i%p') AS DATE) fecha_inicio_inscripcion,

   -- no se encuentra en bbdd origen un valor para el atributo
   -- se utiliza la fecha calculada en el script de estado de beneficiario, en otro caso queda en null
   CAST(DATE_PARSE(CASE
	WHEN  fechas.fecha_fin_inscripcion IS NULL
		THEN date_format(TRY_CAST(NULL AS DATE), '%Y-%m-%d %h:%i%p')
	ELSE date_format(cast(fechas.fecha_fin_inscripcion AS date), '%Y-%m-%d %h:%i%p')
	END , '%Y-%m-%d %h:%i%p') AS DATE) fecha_limite_inscripcion,

	' ' turno,
	' ' dias_cursada,

	CASE
		WHEN sum(CASE WHEN co.visible = TRUE THEN 1 ELSE 0 END)  > 0
			THEN 'S'
		ELSE 'N'
		END inscripcion_abierta,

	CASE
		WHEN min(TRY_CAST(co.enddate AS INTEGER)) = 0
			THEN 'S'
		ELSE 'N'
		END activo,

	-- Si toma la cantidad de inscriptos desde la categoria, si es null o 0 se toma desde la tabla de estado de beneficiario,
	-- en otro caso queda null
		CASE
		WHEN cc.coursecount  = 0 OR cc.coursecount IS NULL
		THEN CAST(fechas.cantidad_inscriptos AS VARCHAR)
		ELSE CAST(cc.coursecount AS VARCHAR)
		END cant_inscriptos,

	' ' vacantes,

	-- Si la modalidad no esta en la edicion se toma desde la entidad capacitacion, NULL en otro caso
	dc.modalidad_id,
	dc.descrip_modalidad,
	est.id cod_origen_establecimiento
FROM "caba-piba-raw-zone-db"."moodle_dgtedu01_mdl_course" co
JOIN "caba-piba-raw-zone-db"."moodle_dgtedu01_mdl_course_categories" cc ON (co.category = cc.id)
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_capacitacion_match" dcmatch ON (
		dcmatch.id_old = CAST(cc.id AS VARCHAR)
		AND dcmatch.base_origen = 'MOODLE'
		)
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_capacitacion" dc ON (
		dcmatch.id_new = dc.id_new
		AND dcmatch.base_origen = dc.base_origen
		)
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_establecimientos"  est ON (dcmatch.base_origen=est.base_origen)
LEFT JOIN (SELECT
    id_categoria,
	CAST(MIN(inicio_curso) AS VARCHAR) fecha_inicio,
	CAST(MAX(fin_curso) AS VARCHAR) fecha_fin,
	CAST(MIN(inscripcion_inicio_cursada) AS VARCHAR) fecha_inicio_inscripcion,
	CAST(MAX(inscripcion_final_cursada) AS VARCHAR) fecha_fin_inscripcion,
	COUNT(*) cantidad_inscriptos
FROM
"caba-piba-staging-zone-db"."tbp_typ_tmp_estado_beneficiario_moodle"
GROUP BY
id_categoria
) fechas ON (co.category=fechas.id_categoria)
GROUP BY
	dcmatch.tipo_capacitacion,
	dcmatch.id_new,
	cc.id,
	fechas.fecha_inicio,
	fechas.fecha_fin,
	fechas.fecha_inicio_inscripcion,
	fechas.fecha_fin_inscripcion,
	cc.coursecount,
	fechas.cantidad_inscriptos,
	dc.modalidad_id,
	dc.descrip_modalidad,
	est.id

UNION
-- SIENFO CARRERA
SELECT 'SIENFO' base_origen,
	   'CARRERA' tipo_capacitacion,
       cm.id_new capacitacion_id_new,
       CAST(ca.id_carrera AS VARCHAR) capacitacion_id_old,
       ca.nom_carrera descrip_capacitacion_old,
       t.codigo_ct edicion_capacitacion_id,

		-- se utiliza la fecha calculada en el script de estado de beneficiario
		-- En caso que este en null, se utiliza la fecha de la tabla taller, en otro caso queda en null
		CAST(SPLIT_PART(CASE
		WHEN fechas.fecha_inicio IS NULL
			THEN date_format(try_cast(t.fecha AS date), '%Y-%m-%d %h:%i%p')
		ELSE date_format(cast(fechas.fecha_inicio AS date), '%Y-%m-%d %h:%i%p')
		END, '-', 1)  AS INTEGER) anio_inicio,

		-- El proximo campo es el semestre de inicio de la edicion capacitacion, NO indica que la misma es semestral o anual
		-- se utiliza la fecha calculada en el script de estado de beneficiario
		-- En caso que este en null, se utiliza la fecha de la tabla taller, en otro caso queda en null
		CASE
		WHEN CAST(SPLIT_PART(CASE
						WHEN fechas.fecha_inicio IS NULL
							THEN date_format(try_cast(t.fecha AS date), '%Y-%m-%d %h:%i%p')
						ELSE date_format(cast(fechas.fecha_inicio AS date), '%Y-%m-%d %h:%i%p')
						END, '-', 2) AS INTEGER) <= 6
			THEN 1
		WHEN CAST(SPLIT_PART(CASE
						WHEN fechas.fecha_inicio IS NULL
							THEN date_format(try_cast(t.fecha AS date), '%Y-%m-%d %h:%i%p')
						ELSE date_format(cast(fechas.fecha_inicio AS date), '%Y-%m-%d %h:%i%p')
						END, '-', 2) AS INTEGER) > 6
			THEN 2
		ELSE NULL
		END semestre_inicio,

		-- se utiliza la fecha calculada en el script de estado de beneficiario
		-- En caso que este en null, se utiliza la fecha de la tabla taller, en otro caso queda en null
		CAST(DATE_PARSE(CASE
		WHEN fechas.fecha_inicio IS NULL
			THEN date_format(try_cast(t.fecha AS date), '%Y-%m-%d %h:%i%p')
		ELSE date_format(cast(fechas.fecha_inicio AS date), '%Y-%m-%d %h:%i%p')
		END , '%Y-%m-%d %h:%i%p') AS DATE) fecha_inicio_dictado,

		-- se utiliza la fecha calculada en el script de estado de beneficiario
		-- En caso que este en null, se utiliza la fecha de la tabla taller, en otro caso queda en null
		CAST(DATE_PARSE(CASE
		WHEN  fechas.fecha_fin IS NULL
			THEN date_format(try_cast(t.fecha_fin AS date), '%Y-%m-%d %h:%i%p')
		ELSE date_format(cast(fechas.fecha_fin AS date), '%Y-%m-%d %h:%i%p')
		END , '%Y-%m-%d %h:%i%p') AS DATE) fecha_fin_dictado,

	   -- no se encuentra en bbdd origen un valor para el atributo
	   -- se utiliza la fecha calculada en el script de estado de beneficiario, en otro caso queda en null
	   	CAST(DATE_PARSE(CASE
		WHEN fechas.fecha_inicio_inscripcion IS NULL
			THEN NULL
		ELSE date_format(cast(fechas.fecha_inicio_inscripcion AS date), '%Y-%m-%d %h:%i%p')
		END , '%Y-%m-%d %h:%i%p') AS DATE) fecha_inicio_inscripcion,

	   -- no se encuentra en bbdd origen un valor para el atributo
	   -- se utiliza la fecha calculada en el script de estado de beneficiario, en otro caso queda en null
	   CAST(DATE_PARSE(CASE
		WHEN fechas.fecha_fin_inscripcion IS NULL
			THEN NULL
		ELSE date_format(cast(fechas.fecha_fin_inscripcion AS date), '%Y-%m-%d %h:%i%p')
		END , '%Y-%m-%d %h:%i%p') AS DATE) fecha_limite_inscripcion,

       CASE WHEN CAST(SUBSTR(COALESCE(t.horaini_lu, t.horaini_ma, t.horaini_mi, t.horaini_ju, t.horaini_vi, t.horaini_sa),1,2) AS INTEGER) BETWEEN 7 AND 12 THEN 'Mañana'
            WHEN CAST(SUBSTR(COALESCE(t.horaini_lu, t.horaini_ma, t.horaini_mi, t.horaini_ju, t.horaini_vi, t.horaini_sa),1,2) AS INTEGER) BETWEEN 13 AND 17 THEN 'Tarde'
			WHEN CAST(SUBSTR(COALESCE(t.horaini_lu, t.horaini_ma, t.horaini_mi, t.horaini_ju, t.horaini_vi, t.horaini_sa),1,2) AS INTEGER) BETWEEN 18 AND 24 THEN 'Noche'
			ELSE NULL END turno,
	   CASE WHEN t.lu = 1 THEN 'Lunes ' ELSE '' END ||
	   CASE WHEN t.ma = 1 THEN 'Martes ' ELSE '' END ||
	   CASE WHEN t.mi = 1 THEN 'Miercoles ' ELSE '' END ||
	   CASE WHEN t.ju = 1 THEN 'Jueves ' ELSE '' END ||
	   CASE WHEN t.vi = 1 THEN 'Viernes ' ELSE '' END ||
	   CASE WHEN t.sa = 1 THEN 'Sabado ' ELSE '' END dias_cursada,
	   CASE WHEN COALESCE(t.fecha_fin,'0000-00-00') = '0000-00-00' THEN (CASE WHEN te.nombre = 'Activo' THEN 'S' ELSE 'N' END) ELSE (CASE WHEN DATE_PARSE(CASE WHEN t.fecha_fin = '0000-00-00' THEN NULL ELSE t.fecha_fin END, '%Y-%m-%d') > CURRENT_DATE THEN 'S' ELSE 'N' END) END inscripcion_abierta,
       CASE WHEN COALESCE(t.fecha_fin,'0000-00-00') = '0000-00-00' THEN (CASE WHEN te.nombre = 'Activo' THEN 'S' ELSE 'N' END) ELSE (CASE WHEN DATE_PARSE(CASE WHEN t.fecha_fin = '0000-00-00' THEN NULL ELSE t.fecha_fin END, '%Y-%m-%d') > CURRENT_DATE THEN 'S' ELSE 'N' END) END activo,
	   CAST(t.altas_total AS VARCHAR) cant_inscriptos,
	   CAST(t.vacantes AS VARCHAR) vacantes,

		-- Si la modalidad no esta en la edicion se toma desde la entidad capacitacion, NULL en otro caso
		dc.modalidad_id,
		dc.descrip_modalidad,
		est.id cod_origen_establecimiento
FROM "caba-piba-raw-zone-db"."sienfo_carreras" ca
LEFT JOIN "caba-piba-raw-zone-db"."sienfo_talleres" t ON (t.id_carrera = ca.id_carrera)
LEFT JOIN  "caba-piba-staging-zone-db"."tbp_typ_def_establecimientos" est ON (est.id_old=CAST(t.id_centro AS VARCHAR))
LEFT JOIN "caba-piba-raw-zone-db"."sienfo_testado" te ON (t.estado = te.valor)
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_capacitacion_match" cm ON (cm.base_origen = 'SIENFO' AND cm.tipo_capacitacion = 'CARRERA'
AND cm.id_old =	CAST(ca.id_carrera AS VARCHAR) || '-' || CAST(ca.id_carrera AS VARCHAR))
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_capacitacion" dc ON (cm.id_new = dc.id_new AND cm.base_origen = dc.base_origen)
LEFT JOIN (
SELECT
    codigo_ct,
	CAST(MIN(fecha_inicio_edicion_capacitacion) AS DATE) fecha_inicio,
	CAST(MAX(fecha_fin_edicion_capacitacion) AS DATE) fecha_fin,
	CAST(MIN(fecha_inscipcion) AS DATE) fecha_inicio_inscripcion,
	CAST(MAX(fecha_inscipcion) AS DATE) fecha_fin_inscripcion,
	COUNT(*) cantidad_inscriptos
FROM
"caba-piba-staging-zone-db"."tbp_typ_tmp_estado_beneficiario_sienfo"
GROUP BY
codigo_ct
) fechas ON (t.codigo_ct=fechas.codigo_ct)
WHERE t.id_carrera != 0

UNION
-- SIENFO CURSO
SELECT 'SIENFO' base_origen,
	   'CURSO' tipo_capacitacion,
       cm.id_new capacitacion_id_new,
       CAST(cu.id_curso AS VARCHAR) capacitacion_id_old,
       cu.nom_curso descrip_capacitacion_old,
       t.codigo_ct edicion_capacitacion_id,

		-- se utiliza la fecha calculada en el script de estado de beneficiario
		-- En caso que este en null, se utiliza la fecha de la tabla taller, en otro caso queda en null
		CAST(SPLIT_PART(CASE
		WHEN fechas.fecha_inicio IS NULL
			THEN date_format(try_cast(t.fecha AS date), '%Y-%m-%d %h:%i%p')
		ELSE date_format(cast(fechas.fecha_inicio AS date), '%Y-%m-%d %h:%i%p')
		END, '-', 1)  AS INTEGER) anio_inicio,

		-- El proximo campo es el semestre de inicio de la edicion capacitacion, NO indica que la misma es semestral o anual
		-- se utiliza la fecha calculada en el script de estado de beneficiario
		-- En caso que este en null, se utiliza la fecha de la tabla taller, en otro caso queda en null
		CASE
		WHEN CAST(SPLIT_PART(CASE
						WHEN fechas.fecha_inicio IS NULL
							THEN date_format(try_cast(t.fecha AS date), '%Y-%m-%d %h:%i%p')
						ELSE date_format(cast(fechas.fecha_inicio AS date), '%Y-%m-%d %h:%i%p')
						END, '-', 2) AS INTEGER) <= 6
			THEN 1
		WHEN CAST(SPLIT_PART(CASE
						WHEN fechas.fecha_inicio IS NULL
							THEN date_format(try_cast(t.fecha AS date), '%Y-%m-%d %h:%i%p')
						ELSE date_format(cast(fechas.fecha_inicio AS date), '%Y-%m-%d %h:%i%p')
						END, '-', 2) AS INTEGER) > 6
			THEN 2
		ELSE NULL
		END semestre_inicio,

		-- se utiliza la fecha calculada en el script de estado de beneficiario
		-- En caso que este en null, se utiliza la fecha de la tabla taller, en otro caso queda en null
		CAST(DATE_PARSE(CASE
		WHEN  fechas.fecha_inicio IS NULL
			THEN date_format(try_cast(t.fecha AS date), '%Y-%m-%d %h:%i%p')
		ELSE date_format(cast(fechas.fecha_inicio AS date), '%Y-%m-%d %h:%i%p')
		END , '%Y-%m-%d %h:%i%p') AS DATE) fecha_inicio_dictado,

		-- se utiliza la fecha calculada en el script de estado de beneficiario
		-- En caso que este en null, se utiliza la fecha de la tabla taller, en otro caso queda en null
		CAST(DATE_PARSE(CASE
		WHEN  fechas.fecha_fin IS NULL
			THEN date_format(try_cast(t.fecha_fin AS date), '%Y-%m-%d %h:%i%p')
		ELSE date_format(cast(fechas.fecha_fin AS date), '%Y-%m-%d %h:%i%p')
		END , '%Y-%m-%d %h:%i%p') AS DATE) fecha_fin_dictado,

		-- se utiliza la fecha calculada en el script de estado de beneficiario, en otro caso queda en null
	   	CAST(DATE_PARSE(CASE
		WHEN  fechas.fecha_inicio_inscripcion IS NULL
			THEN NULL
		ELSE date_format(cast(fechas.fecha_inicio_inscripcion AS date), '%Y-%m-%d %h:%i%p')
		END , '%Y-%m-%d %h:%i%p') AS DATE) fecha_inicio_inscripcion,

		-- se utiliza la fecha calculada en el script de estado de beneficiario, en otro caso queda en null
	   CAST(DATE_PARSE(CASE
		WHEN  fechas.fecha_fin_inscripcion IS NULL
			THEN NULL
		ELSE date_format(cast(fechas.fecha_fin_inscripcion AS date), '%Y-%m-%d %h:%i%p')
		END , '%Y-%m-%d %h:%i%p') AS DATE) fecha_limite_inscripcion,

       CASE WHEN CAST(SUBSTR(COALESCE(t.horaini_lu, t.horaini_ma, t.horaini_mi, t.horaini_ju, t.horaini_vi, t.horaini_sa),1,2) AS INTEGER) BETWEEN 7 AND 12 THEN 'Mañana'
            WHEN CAST(SUBSTR(COALESCE(t.horaini_lu, t.horaini_ma, t.horaini_mi, t.horaini_ju, t.horaini_vi, t.horaini_sa),1,2) AS INTEGER) BETWEEN 13 AND 17 THEN 'Tarde'
			WHEN CAST(SUBSTR(COALESCE(t.horaini_lu, t.horaini_ma, t.horaini_mi, t.horaini_ju, t.horaini_vi, t.horaini_sa),1,2) AS INTEGER) BETWEEN 18 AND 24 THEN 'Noche'
			ELSE NULL END turno,
	   CASE WHEN t.lu = 1 THEN 'Lunes ' ELSE '' END ||
	   CASE WHEN t.ma = 1 THEN 'Martes ' ELSE '' END ||
	   CASE WHEN t.mi = 1 THEN 'Miercoles ' ELSE '' END ||
	   CASE WHEN t.ju = 1 THEN 'Jueves ' ELSE '' END ||
	   CASE WHEN t.vi = 1 THEN 'Viernes ' ELSE '' END ||
	   CASE WHEN t.sa = 1 THEN 'Sabado ' ELSE '' END dias_cursada,
	   CASE WHEN COALESCE(t.fecha_fin,'0000-00-00') = '0000-00-00' THEN (CASE WHEN te.nombre = 'Activo' THEN 'S' ELSE 'N' END) ELSE (CASE WHEN DATE_PARSE(CASE WHEN t.fecha_fin = '0000-00-00' THEN NULL ELSE t.fecha_fin END, '%Y-%m-%d') > CURRENT_DATE THEN 'S' ELSE 'N' END) END inscripcion_abierta,
       CASE WHEN COALESCE(t.fecha_fin,'0000-00-00') = '0000-00-00' THEN (CASE WHEN te.nombre = 'Activo' THEN 'S' ELSE 'N' END) ELSE (CASE WHEN DATE_PARSE(CASE WHEN t.fecha_fin = '0000-00-00' THEN NULL ELSE t.fecha_fin END, '%Y-%m-%d') > CURRENT_DATE THEN 'S' ELSE 'N' END) END activo,
	   CAST(t.altas_total AS VARCHAR) cant_inscriptos,
	   CAST(t.vacantes AS VARCHAR) vacantes,
		-- Si la modalidad no esta en la edicion se toma desde la entidad capacitacion, NULL en otro caso
		dc.modalidad_id,
		dc.descrip_modalidad,
       est.id cod_origen_establecimiento
FROM "caba-piba-raw-zone-db"."sienfo_cursos" cu
LEFT JOIN "caba-piba-raw-zone-db"."sienfo_talleres" t ON (t.id_curso = cu.id_curso)
LEFT JOIN  "caba-piba-staging-zone-db"."tbp_typ_def_establecimientos" est ON (est.id_old=CAST(t.id_centro AS VARCHAR))
LEFT JOIN "caba-piba-raw-zone-db"."sienfo_testado" te ON (t.estado = te.valor)
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_capacitacion_match" cm ON (cm.base_origen = 'SIENFO' AND cm.tipo_capacitacion = 'CURSO'
AND cm.id_old = CAST(COALESCE(t.id_carrera, 0) AS VARCHAR) || '-' || CAST(cu.id_curso AS VARCHAR)
)
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_capacitacion" dc ON (cm.id_new = dc.id_new AND cm.base_origen = dc.base_origen)
LEFT JOIN (
SELECT
    codigo_ct,
	CAST(MIN(fecha_inicio_edicion_capacitacion) AS DATE) fecha_inicio,
	CAST(MAX(fecha_fin_edicion_capacitacion) AS DATE) fecha_fin,
	CAST(MIN(fecha_inscipcion) AS DATE) fecha_inicio_inscripcion,
	CAST(MAX(fecha_inscipcion) AS DATE) fecha_fin_inscripcion,
	COUNT(*) cantidad_inscriptos
FROM
"caba-piba-staging-zone-db"."tbp_typ_tmp_estado_beneficiario_sienfo"
GROUP BY
codigo_ct
) fechas ON (t.codigo_ct=fechas.codigo_ct)
WHERE COALESCE(t.id_carrera,0) = 0

UNION
-- EDICIONES CAPACITACIONES CRMSL
SELECT cm.base_origen,
	   cm.tipo_capacitacion,
       cm.id_new capacitacion_id_new,
       CAST(of.id AS VARCHAR) capacitacion_id_old,
       of.name descrip_capacitacion_old,
       CAST(of.id AS VARCHAR) edicion_capacitacion_id,

		-- se utiliza la fecha calculada en el script de estado de beneficiario.
		-- en caso que este en null, se utiliza la fecha de la tabla oportunidades_formacion, en otro caso queda en null
		CAST(SPLIT_PART(CASE
		WHEN fechas.fecha_inicio = '0000-00-00'
			THEN date_format(cast(of.inicio AS date), '%Y-%m-%d %h:%i%p')
		ELSE date_format(cast(fechas.fecha_inicio AS date), '%Y-%m-%d %h:%i%p')
		END, '-', 1)  AS INTEGER) anio_inicio,


		-- El proximo campo es el semestre de inicio de la edicion capacitacion, NO indica que la misma es semestral o anual
		-- se utiliza la fecha calculada en el script de estado de beneficiario.
		-- en caso que este en null, se utiliza la fecha de la tabla oportunidades_formacion, en otro caso queda en null
		CASE
		WHEN CAST(SPLIT_PART(CASE
						WHEN fechas.fecha_inicio = '0000-00-00'
							THEN date_format(cast(of.inicio AS date), '%Y-%m-%d %h:%i%p')
						ELSE date_format(cast(fechas.fecha_inicio AS date), '%Y-%m-%d %h:%i%p')
						END, '-', 2) AS INTEGER) <= 6
			THEN 1
		WHEN CAST(SPLIT_PART(CASE
						WHEN fechas.fecha_inicio = '0000-00-00'
							THEN date_format(cast(of.inicio AS date), '%Y-%m-%d %h:%i%p')
						ELSE date_format(cast(fechas.fecha_inicio AS date), '%Y-%m-%d %h:%i%p')
						END, '-', 2) AS INTEGER) > 6
			THEN 2
		ELSE NULL
		END semestre_inicio,

		-- se utiliza la fecha calculada en el script de estado de beneficiario.
		-- en caso que este en null, se utiliza la fecha de la tabla oportunidades_formacion, en otro caso queda en null
		CAST(DATE_PARSE(CASE
		WHEN  fechas.fecha_inicio = '0000-00-00'
			THEN date_format(cast(of.inicio AS date), '%Y-%m-%d %h:%i%p')
		ELSE date_format(cast(fechas.fecha_inicio AS date), '%Y-%m-%d %h:%i%p')
		END , '%Y-%m-%d %h:%i%p') AS DATE) fecha_inicio_dictado,


		-- se utiliza la fecha calculada en el script de estado de beneficiario.
		-- en caso que este en null, se utiliza la fecha de la tabla oportunidades_formacion, en otro caso queda en null
		CAST(DATE_PARSE(CASE
		WHEN  fechas.fecha_fin = '0000-00-00'
			THEN date_format(of.fin, '%Y-%m-%d %h:%i%p')
		ELSE date_format(cast(fechas.fecha_fin AS date), '%Y-%m-%d %h:%i%p')
		END , '%Y-%m-%d %h:%i%p') AS DATE) fecha_fin_dictado,


	   -- no se encuentra en bbdd origen un valor para el atributo
	   -- se utiliza la fecha calculada en el script de estado de beneficiario, en otro caso queda en null
	   	CAST(DATE_PARSE(CASE
		WHEN  fechas.fecha_inicio_inscripcion = '0000-00-00'
			THEN NULL
		ELSE date_format(cast(fechas.fecha_inicio_inscripcion AS date), '%Y-%m-%d %h:%i%p')
		END , '%Y-%m-%d %h:%i%p') AS DATE) fecha_inicio_inscripcion,

	   -- no se encuentra en bbdd origen un valor para el atributo
	   -- se utiliza la fecha calculada en el script de estado de beneficiario, en otro caso queda en null
	   CAST(DATE_PARSE(CASE
		WHEN  fechas.fecha_fin_inscripcion = '0000-00-00'
			THEN NULL
		ELSE date_format(cast(fechas.fecha_fin_inscripcion AS date), '%Y-%m-%d %h:%i%p')
		END , '%Y-%m-%d %h:%i%p') AS DATE) fecha_limite_inscripcion,

       NULL turno,
       NULL dias_cursada,
       CASE WHEN of.fin IS NULL THEN (CASE WHEN estado_inscripcion = 'finalizada' THEN 'N' ELSE 'S' END) ELSE (CASE WHEN of.fin > CURRENT_DATE THEN 'S' ELSE 'N' END) END inscripcion_abierta,
       CASE WHEN of.fin IS NULL THEN (CASE WHEN estado_curso = 'finalizada' THEN 'N' ELSE 'S' END) ELSE (CASE WHEN of.fin > CURRENT_DATE THEN 'S' ELSE 'N' END) END activo,

	   CASE WHEN fechas.cantidad_inscriptos IS NOT NULL AND fechas.cantidad_inscriptos > 0
	   THEN CAST(fechas.cantidad_inscriptos AS VARCHAR)
	   ELSE CAST(of.inscriptos AS VARCHAR) END cant_inscriptos,

	   CAST(of.cupos AS VARCHAR) vacantes,

	   	-- Si la modalidad no esta en la edicion se toma desde la entidad capacitacion, NULL en otro caso
       CASE WHEN of.modalidad = 'virtual' THEN 2
			WHEN of.modalidad = 'semi' THEN 3
			WHEN of.modalidad = 'presencial' THEN 1
			ELSE dc.modalidad_id
			END modalidad_id,

       CASE WHEN of.modalidad = 'virtual' THEN 'Virtual'
			WHEN of.modalidad = 'semi' THEN 'Presencial y Virtual'
			WHEN of.modalidad = 'presencial' THEN 'Presencial'
			ELSE dc.descrip_modalidad
			END descrip_modalidad, -- presencial, semipresencial, virtual

       est.id cod_origen_establecimiento

FROM "caba-piba-raw-zone-db"."crm_sociolaboral_op_oportunidades_formacion" OF
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_establecimientos" est ON (of.sede IN (est.nombres_old))
LEFT JOIN "caba-piba-raw-zone-db"."crm_sociolaboral_op_oportunidades_formacion_contacts_c" ofc  ON (of.id = ofc.op_oportun1d35rmacion_ida)
LEFT JOIN "caba-piba-raw-zone-db"."crm_sociolaboral_contacts" co ON (co.id = ofc.op_oportunidades_formacion_contactscontacts_idb)
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_tmp_view_crm_sociolaboral_contacts_cstm_no_duplicates"  cs ON (co.id = cs.id_c)
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_capacitacion_match" cm ON (cm.base_origen = 'CRMSL' AND cm.tipo_capacitacion = 'CURSO' AND cm.id_old = CAST(of.id AS VARCHAR))
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_capacitacion" dc ON (cm.id_new = dc.id_new AND cm.base_origen = dc.base_origen)
LEFT JOIN (SELECT
    edicion_capacitacion_id_old,
	CAST(MIN(inicio) AS VARCHAR) fecha_inicio,
	CAST(MAX(fin) AS VARCHAR) fecha_fin,
	CAST(MIN(inicio) AS VARCHAR) fecha_inicio_inscripcion,
	CAST(MAX(inicio) AS VARCHAR) fecha_fin_inscripcion,
	COUNT(*) cantidad_inscriptos
FROM
"caba-piba-staging-zone-db"."tbp_typ_tmp_estado_beneficiario_crmsl"
GROUP BY
edicion_capacitacion_id_old) fechas ON (CAST(of.id AS VARCHAR)=fechas.edicion_capacitacion_id_old)
WHERE (co.lead_source = 'sociolaboral'
OR ((co.lead_source = 'rib') AND cs.forma_parte_interm_lab_c = 'si'))
GROUP BY
	cm.base_origen,
	cm.tipo_capacitacion,
	cm.id_new,
	of.id,
	of.name,
	of.inicio,
	dc.fecha_inicio,
	of.fin,
	dc.fecha_fin,
	fechas.fecha_inicio,
	fechas.fecha_fin,
	fechas.fecha_inicio_inscripcion,
	fechas.fecha_fin_inscripcion,
	of.estado_inscripcion,
	of.estado_curso,
	fechas.cantidad_inscriptos,
	of.inscriptos,
	of.cupos,
	of.modalidad,
	dc.modalidad_id,
	dc.descrip_modalidad,
	est.id

UNION
-- SIU
SELECT
	'SIU' base_origen,
	dcmatch.tipo_capacitacion,
	dcmatch.id_new capacitacion_id_new,
	CAST(dcmatch.id_old AS VARCHAR) capacitacion_id_old,

	-- no se completa dado que no se usa en la tabla def
	' ' descrip_capacitacion_old,

	-- la clave de edicion capacitacion se compone los identificadores del 1-plan de estudio,
	-- 2-la propuesta academica, 3-el periodo lectivo, 4-el establecimiento donde se dicta la capacitacion
	-- 5-la modalidad de la capacitacion y 6-Turno
	CAST(spl.plan AS VARCHAR) || '-' || CAST(spl.propuesta AS VARCHAR) || '-' ||
	CAST(pl.periodo_lectivo AS VARCHAR)  || '-' || CAST(est.id AS VARCHAR) || '-' ||
	CAST(dc.modalidad_id AS VARCHAR) || '-' ||
	CAST(turno_c.turno AS VARCHAR) edicion_capacitacion_id,

	CAST(SPLIT_PART(MIN(date_format(pl.fecha_inicio_dictado, '%Y-%m-%d %h:%i%p')), '-', 1)  AS INTEGER) anio_inicio,

	-- El proximo campo es el semestre de inicio de la edicion capacitacion, NO indica que la misma es semestral o anual
	CASE
		WHEN CAST(SPLIT_PART(MIN(date_format(pl.fecha_inicio_dictado, '%Y-%m-%d %h:%i%p')), '-', 2)  AS INTEGER)  <= 6 THEN 1
		WHEN CAST(SPLIT_PART(MIN(date_format(pl.fecha_inicio_dictado, '%Y-%m-%d %h:%i%p')), '-', 2)  AS INTEGER)  > 6 THEN 2
		ELSE NULL
	END semestre_inicio,

	CAST(DATE_PARSE(MIN(date_format(pl.fecha_inicio_dictado, '%Y-%m-%d %h:%i%p')), '%Y-%m-%d %h:%i%p') AS DATE) fecha_inicio_dictado,

	CAST(DATE_PARSE(MAX(date_format(pl.fecha_fin_dictado, '%Y-%m-%d %h:%i%p')), '%Y-%m-%d %h:%i%p') AS DATE) fecha_fin_dictado,

	-- se obtiene calculando la primer inscripcion en la edicion
	CAST(DATE_PARSE(date_format(inscriptos.min_fecha_inscripcion, '%Y-%m-%d %h:%i%p'), '%Y-%m-%d %h:%i%p') AS DATE) fecha_inicio_inscripcion,

	-- se obtiene calculando la ultima inscripcion en la edicion
	CAST(DATE_PARSE(date_format(inscriptos.max_fecha_inscripcion, '%Y-%m-%d %h:%i%p'), '%Y-%m-%d %h:%i%p') AS DATE) fecha_limite_inscripcion,

	-- se considera turno mañana si la hora de inicio es entre las 7 y 12, tarde entre 12 y 20, noche entre 20 y 24
	array_join(array_agg(DISTINCT(turno_c.nombre)), ',') turno,

	-- si bien la columna dias.d trae la informacion requerida, hay que agruparlo a trabes de array join y realizar lo siguiente para que los dias queden ordenados
	CASE WHEN position('LUNES' IN UPPER(array_join(array_agg(DISTINCT(dias.d)), ',')))!=0 THEN 'Lunes ' ELSE '' END ||
	CASE WHEN position('MARTES' IN UPPER(array_join(array_agg(DISTINCT(dias.d)), ',')))!=0 THEN 'Martes ' ELSE '' END ||
	CASE WHEN position('MIERCOLES' IN UPPER(array_join(array_agg(DISTINCT(dias.d)), ',')))!=0 THEN 'Miercoles ' ELSE '' END ||
	CASE WHEN position('JUEVES' IN UPPER(array_join(array_agg(DISTINCT(dias.d)), ',')))!=0 THEN 'Jueves ' ELSE '' END ||
	CASE WHEN position('VIERNES' IN UPPER(array_join(array_agg(DISTINCT(dias.d)), ',')))!=0 THEN 'Viernes ' ELSE '' END ||
	CASE WHEN position('SABADO' IN UPPER(array_join(array_agg(DISTINCT(dias.d)), ',')))!=0 THEN 'Sabado ' ELSE '' END ||
	CASE WHEN position('DOMINGO' IN UPPER(array_join(array_agg(DISTINCT(dias.d)), ',')))!=0 THEN 'Domingo ' ELSE '' END dias_cursada,

	-- Si al menos una comision esta abierta, se considera la edicion capacitacion abierta
	CASE WHEN SUM(CASE WHEN comi.inscripcion_cerrada IN ('N') THEN 1 ELSE 0 END) > 0 THEN 'S'
		ELSE 'N'
	END inscripcion_abierta,

	-- Valores posibles de la columna comi.estado: (A)ctivo, (P)endiente, (B)aja
	-- Si al menos una comision esta activa, se considera la edicion capacitacion activa
	CASE WHEN SUM(CASE WHEN comi.estado IN ('A') THEN 1 ELSE 0 END) > 0 THEN 'S'
		ELSE 'N'
	END activo,

	-- solo se consideran los alumnos inscriptos y con el estado = A (el estado (P)endiente no se tiene en cuenta)
	CAST(inscriptos.cant_alumnos AS VARCHAR) cant_inscriptos,

	CAST(cupo.cupo AS VARCHAR) AS vacantes,

	-- La modalidad se toma desde la entidad capacitacion
	dc.modalidad_id,
	dc.descrip_modalidad,

	est.id cod_origen_establecimiento

FROM "caba-piba-raw-zone-db"."siu_toba_3_3_negocio_sga_planes" spl
LEFT JOIN "caba-piba-raw-zone-db"."siu_toba_3_3_negocio_sga_propuestas" spr ON (spl.propuesta = spr.propuesta)
LEFT JOIN "caba-piba-raw-zone-db"."siu_toba_3_3_negocio_sga_comisiones_propuestas" cp ON (cp.propuesta = spr.propuesta AND cp.plan=spl.plan)
LEFT JOIN "caba-piba-raw-zone-db"."siu_toba_3_3_negocio_sga_comisiones" comi ON (comi.comision=cp.comision)
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_establecimientos" est ON (est.id_old=CAST(comi.ubicacion AS VARCHAR) AND est.base_origen = 'SIU')
LEFT JOIN "caba-piba-raw-zone-db"."siu_toba_3_3_negocio_sga_periodos_lectivos" pl ON (comi.periodo_lectivo=pl.periodo_lectivo)
LEFT JOIN "caba-piba-raw-zone-db"."siu_toba_3_3_negocio_sga_turnos_cursadas" turno_c ON (turno_c.turno=comi.turno)
LEFT JOIN
		(SELECT c.comision,  array_join(array_agg(DISTINCT(asig.dia_semana)), ',') d
		FROM "caba-piba-raw-zone-db"."siu_toba_3_3_negocio_sga_comisiones" c
		LEFT JOIN "caba-piba-raw-zone-db"."siu_toba_3_3_negocio_sga_comisiones_bh" banda_horaria ON (banda_horaria.comision=c.comision)
		LEFT JOIN "caba-piba-raw-zone-db"."siu_toba_3_3_negocio_sga_asignaciones" asig ON (asig.asignacion=banda_horaria.asignacion)
		GROUP BY c.comision) dias ON (dias.comision=comi.comision)
LEFT JOIN
		(SELECT
			spl.plan,
			spl.propuesta,
			c.periodo_lectivo,
			est.id id_est,
			c.turno,
			SUM(CAST(c.cupo AS INTEGER)) AS cupo
		FROM "caba-piba-raw-zone-db"."siu_toba_3_3_negocio_sga_comisiones" c
			JOIN "caba-piba-raw-zone-db"."siu_toba_3_3_negocio_sga_comisiones_propuestas" cp ON (c.comision=cp.comision)
			JOIN "caba-piba-raw-zone-db"."siu_toba_3_3_negocio_sga_propuestas" spr ON (cp.propuesta = spr.propuesta)
			JOIN "caba-piba-raw-zone-db"."siu_toba_3_3_negocio_sga_planes" spl ON (spl.propuesta = spr.propuesta AND cp.plan=spl.plan)
			JOIN "caba-piba-staging-zone-db"."tbp_typ_def_establecimientos" est ON (est.id_old=CAST(c.ubicacion AS VARCHAR) AND est.base_origen = 'SIU')
		    JOIN "caba-piba-raw-zone-db"."siu_toba_3_3_negocio_sga_turnos_cursadas" turno_c ON (turno_c.turno=c.turno)
		GROUP BY
			spl.plan,
			spl.propuesta,
			c.periodo_lectivo,
			c.turno,
			est.id
		) cupo ON (cupo.plan=spl.plan AND cupo.propuesta=spr.propuesta AND cupo.periodo_lectivo=pl.periodo_lectivo
		AND cupo.id_est=est.id AND cupo.turno = turno_c.turno)

LEFT JOIN
		(SELECT
		COUNT(DISTINCT cu.alumno) AS cant_alumnos,
		MIN(CAST(cu.fecha_inscripcion AS DATE)) min_fecha_inscripcion,
		MAX(CAST(cu.fecha_inscripcion AS DATE)) max_fecha_inscripcion,
		spl.plan,
		spl.propuesta,
		c.periodo_lectivo,
		est.id id_est,
		c.turno
		FROM "caba-piba-raw-zone-db"."siu_toba_3_3_negocio_sga_insc_cursada" cu
		JOIN "caba-piba-raw-zone-db"."siu_toba_3_3_negocio_sga_comisiones" c ON (cu.comision=c.comision)
		JOIN "caba-piba-raw-zone-db"."siu_toba_3_3_negocio_sga_comisiones_propuestas" cp ON (cu.comision=cp.comision)
			JOIN "caba-piba-raw-zone-db"."siu_toba_3_3_negocio_sga_propuestas" spr ON (cp.propuesta = spr.propuesta)
			JOIN "caba-piba-raw-zone-db"."siu_toba_3_3_negocio_sga_planes" spl ON (spl.propuesta = spr.propuesta AND cp.plan=spl.plan)
			JOIN "caba-piba-staging-zone-db"."tbp_typ_def_establecimientos" est ON (est.id_old=CAST(c.ubicacion AS VARCHAR) AND est.base_origen = 'SIU')
		    JOIN "caba-piba-raw-zone-db"."siu_toba_3_3_negocio_sga_turnos_cursadas" turno_c ON (turno_c.turno=c.turno)
		WHERE cu.estado LIKE 'A'
		GROUP BY
		spl.plan,
		spl.propuesta,
		c.periodo_lectivo,
		c.turno,
		est.id ) inscriptos
		ON (inscriptos.plan=spl.plan AND inscriptos.propuesta=spr.propuesta AND inscriptos.periodo_lectivo=pl.periodo_lectivo
		AND inscriptos.id_est=est.id AND inscriptos.turno = turno_c.turno)
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_capacitacion_match" dcmatch ON (dcmatch.id_old = CAST(spl.plan AS VARCHAR) AND dcmatch.base_origen = 'SIU')
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_capacitacion" dc ON (dcmatch.id_new = dc.id_new AND dcmatch.base_origen = dc.base_origen)
GROUP BY
	dcmatch.tipo_capacitacion,
	dcmatch.id_new,
	dcmatch.id_old,
	spl.plan,
	spl.propuesta,
	pl.periodo_lectivo,
	dc.modalidad_id,
	dc.descrip_modalidad,
	est.id,
	turno_c.turno,
	cupo.cupo,
	inscriptos.cant_alumnos,
	inscriptos.min_fecha_inscripcion,
	inscriptos.max_fecha_inscripcion
--</sql>--

-- 2.-- Crear EDICION CAPACITACION GOET, MOODLE, SIENFO Y CRMSL paso 2
-- se agrega un indice incremental
-- --<sql>--DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_tmp_edicion_capacitacion_2`;--</sql>--
--<sql>--
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_edicion_capacitacion_2" AS
SELECT row_number() OVER () AS id,
		ed.base_origen,
		ed.tipo_capacitacion,
		ed.capacitacion_id_new,
		ed.capacitacion_id_old,
		ed.descrip_capacitacion_old,
		ed.edicion_capacitacion_id,
		ed.anio_inicio,
		ed.semestre_inicio,
		ed.fecha_inicio_dictado,
		ed.fecha_fin_dictado,
		ed.fecha_inicio_inscripcion,
		ed.fecha_limite_inscripcion,
		ed.turno,
		ed.dias_cursada,
		ed.inscripcion_abierta,
		ed.activo,
		ed.cant_inscriptos,
		ed.vacantes,
		ed.modalidad_id,
		ed.descrip_modalidad,
		ed.cod_origen_establecimiento
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_edicion_capacitacion_1" ed
--</sql>--

-- 3.-- Crear EDICION CAPACITACION GOET, MOODLE, SIENFO Y CRMSL paso 3
-- se corrigen posibles desviasiones en fechas
-- --<sql>--DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_tmp_edicion_capacitacion`;--</sql>--
--<sql>--
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_edicion_capacitacion" AS
SELECT
	id,
	base_origen,
	tipo_capacitacion,
	capacitacion_id_new,
	capacitacion_id_old,
	descrip_capacitacion_old,
	edicion_capacitacion_id,
	anio_inicio,
	semestre_inicio,
	fecha_inicio_dictado,

	CASE
	WHEN fecha_inicio_inscripcion >= fecha_limite_inscripcion AND fecha_inicio_inscripcion >= fecha_inicio_dictado AND fecha_inicio_inscripcion >= fecha_fin_dictado
	THEN fecha_inicio_inscripcion
	WHEN fecha_limite_inscripcion >= fecha_inicio_dictado AND fecha_limite_inscripcion >= fecha_fin_dictado
	THEN fecha_limite_inscripcion
	WHEN fecha_inicio_dictado >= fecha_fin_dictado
	THEN fecha_inicio_dictado
	ELSE fecha_fin_dictado END "fecha_fin_dictado",

	CASE
	WHEN fecha_inicio_inscripcion <= fecha_limite_inscripcion AND fecha_inicio_inscripcion <= fecha_inicio_dictado AND fecha_inicio_inscripcion <= fecha_fin_dictado
	THEN fecha_inicio_inscripcion
	WHEN fecha_limite_inscripcion <= fecha_inicio_dictado AND fecha_limite_inscripcion <= fecha_fin_dictado
	THEN fecha_limite_inscripcion
	WHEN fecha_inicio_dictado <= fecha_fin_dictado
	THEN fecha_inicio_dictado
	ELSE fecha_fin_dictado END "fecha_inicio_inscripcion",

	fecha_limite_inscripcion,
	turno,
	dias_cursada,
	inscripcion_abierta,
	activo,
	cant_inscriptos,
	vacantes,
	modalidad_id,
	descrip_modalidad,
	cod_origen_establecimiento

FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_edicion_capacitacion_2"
WHERE
try_cast(SPLIT_PART(date_format(fecha_inicio_inscripcion, '%Y-%m-%d %h:%i%p'), '-', 1)  AS INTEGER) <= 2024 AND
try_cast(SPLIT_PART(date_format(fecha_inicio_inscripcion, '%Y-%m-%d %h:%i%p'), '-', 1)  AS INTEGER) >= 1997 AND
try_cast(SPLIT_PART(date_format(fecha_limite_inscripcion, '%Y-%m-%d %h:%i%p'), '-', 1)  AS INTEGER) <= 2024 AND
try_cast(SPLIT_PART(date_format(fecha_limite_inscripcion, '%Y-%m-%d %h:%i%p'), '-', 1)  AS INTEGER) >= 1997 AND
try_cast(SPLIT_PART(date_format(fecha_inicio_dictado, '%Y-%m-%d %h:%i%p'), '-', 1)  AS INTEGER) <= 2024 AND
try_cast(SPLIT_PART(date_format(fecha_inicio_dictado, '%Y-%m-%d %h:%i%p'), '-', 1)  AS INTEGER) >= 1997 AND
try_cast(SPLIT_PART(date_format(fecha_fin_dictado, '%Y-%m-%d %h:%i%p'), '-', 1)  AS INTEGER) <= 2024 AND
try_cast(SPLIT_PART(date_format(fecha_fin_dictado, '%Y-%m-%d %h:%i%p'), '-', 1)  AS INTEGER) >= 1997
UNION
SELECT t.*
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_edicion_capacitacion_2" t
LEFT JOIN (SELECT id FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_edicion_capacitacion_2"
WHERE
try_cast(SPLIT_PART(date_format(fecha_inicio_inscripcion, '%Y-%m-%d %h:%i%p'), '-', 1)  AS INTEGER) <= 2024 AND
try_cast(SPLIT_PART(date_format(fecha_inicio_inscripcion, '%Y-%m-%d %h:%i%p'), '-', 1)  AS INTEGER) >= 1997 AND
try_cast(SPLIT_PART(date_format(fecha_limite_inscripcion, '%Y-%m-%d %h:%i%p'), '-', 1)  AS INTEGER) <= 2024 AND
try_cast(SPLIT_PART(date_format(fecha_limite_inscripcion, '%Y-%m-%d %h:%i%p'), '-', 1)  AS INTEGER) >= 1997 AND
try_cast(SPLIT_PART(date_format(fecha_inicio_dictado, '%Y-%m-%d %h:%i%p'), '-', 1)  AS INTEGER) <= 2024 AND
try_cast(SPLIT_PART(date_format(fecha_inicio_dictado, '%Y-%m-%d %h:%i%p'), '-', 1)  AS INTEGER) >= 1997 AND
try_cast(SPLIT_PART(date_format(fecha_fin_dictado, '%Y-%m-%d %h:%i%p'), '-', 1)  AS INTEGER) <= 2024 AND
try_cast(SPLIT_PART(date_format(fecha_fin_dictado, '%Y-%m-%d %h:%i%p'), '-', 1)  AS INTEGER) >= 1997) a ON (a.id=t.id)
WHERE a.id IS NULL
--</sql>--



-- Copy of 2023.05.24 step 15 - consume edicion capacitacion.sql 



-- EDICION CAPACITACION GOET, MOODLE, SIENFO Y CRMSL
-- 1.- Crear tabla edicion capacitacion definitiva
-- --<sql>--DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_def_edicion_capacitacion`;--</sql>--
--<sql>--
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_def_edicion_capacitacion" AS
SELECT row_number() OVER () AS id,
       ed.base_origen,
	   ed.tipo_capacitacion,
       ed.capacitacion_id_new,
       ed.capacitacion_id_old,
       ed.edicion_capacitacion_id edicion_capacitacion_id_old,
       ed.anio_inicio,
       ed.semestre_inicio,
       ed.fecha_inicio_dictado,
       ed.fecha_fin_dictado,
	   ed.fecha_inicio_inscripcion,
       ed.fecha_limite_inscripcion,
       ed.turno,
	   ed.dias_cursada,
	   ed.inscripcion_abierta,
       ed.activo,
	   ed.cant_inscriptos,
	   ed.vacantes,
	   ed.modalidad_id,
       ed.descrip_modalidad,
       ed.cod_origen_establecimiento
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_edicion_capacitacion" ed
-- en tmp hay datos provenientes de LEFT JOIN para verificar consistencia de datos
-- los mismos se quitan de la tabla def
WHERE ed.edicion_capacitacion_id IS NOT NULL
AND ed.capacitacion_id_new IS NOT NULL
--</sql>--



-- Copy of 2023.05.24 step 16 - staging cursada.sql 



-- GOET, MOODLE, SIENFO, CRMSL, SIU
-- ENTIDAD:CURSADA
-- 1.-  Crear tabla temporal de cursadas sin IEL
-- --<sql>--DROP TABLE `caba-piba-staging-zone-db`.`tbp_typ_tmp_cursada_1`;--</sql>--
--<sql>--
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_cursada_1" AS
-- GOET

SELECT
     'GOET' base_origen,
	 CAST(gu.idusuario AS VARCHAR) identificacion_alumno,
	 CAST(gu.ndocumento AS VARCHAR) documento_broker,
	-- no esta en la BBDD, SE ASUME la fecha de inicio de la capacitacion como la fecha de preinscripcion
	date_parse(date_format(cc.iniciocurso, '%Y-%m-%d %h:%i%p'), '%Y-%m-%d %h:%i%p') fecha_preinscripcion,

	-- se utiliza la fecha de la primer asistencia del alumno a la edicion capacitacion
	-- en caso de null y que el estado de beneficiario no sea 'PREINSCRIPTO' ni 'INSCRIPTO' se utiliza
	-- el campo cc.iniciocurso, null en otro caso
	CASE WHEN eb.estado_beneficiario NOT IN ('PREINSCRIPTO', 'INSCRIPTO') AND inicio_cursada.fecha_inicio IS NULL
	THEN date_parse(date_format(cc.iniciocurso, '%Y-%m-%d %h:%i%p'), '%Y-%m-%d %h:%i%p')
	ELSE date_parse(date_format(inicio_cursada.fecha_inicio, '%Y-%m-%d %h:%i%p'), '%Y-%m-%d %h:%i%p')
	END fecha_inicio,

	-- para obtener la fecha de abondono se busca alumnos inscriptos que no tengan certificado y se establece
	-- como fecha de abandono la fecha de ultima asistencia, siempre que la edicion capacitacion haya finalizado (fecha fin menor a hoy)
	-- IMPORTANTE! hay que evaluar si la logica es correcta, porque Â¿talvez se pueden demorar en generar el certificado respectivo?
	date_parse(date_format(datos_abandono.fecha_abandono, '%Y-%m-%d %h:%i%p'), '%Y-%m-%d %h:%i%p') fecha_abandono,

	-- se utiliza la fecha fin de estado del beneficiario, en caso de null y que el caso que la persona haya abandonado la fecha de egreso es la fecha de abandono, en caso contrario
	-- es la fecha del certificado y de no existir el mismo, es la fecha de fin de curso
	-- IMPORTANTE! hay que evaluar si la logica es correcta
	CASE
		WHEN eb.FinCurso IS NOT NULL
			THEN date_parse(date_format(eb.FinCurso, '%Y-%m-%d %h:%i%p'), '%Y-%m-%d %h:%i%p')
		WHEN  eb.FinCurso IS NULL AND cc.fincurso IS NOT NULL AND datos_abandono.fecha_abandono IS NULL AND certificado.fecha IS NOT NULL
			THEN date_parse(date_format(certificado.fecha, '%Y-%m-%d %h:%i%p'), '%Y-%m-%d %h:%i%p')
		WHEN eb.FinCurso IS NULL AND cc.fincurso IS NOT NULL AND datos_abandono.fecha_abandono IS NULL AND certificado.fecha IS NULL
			THEN date_parse(date_format(cc.fincurso, '%Y-%m-%d %h:%i%p'), '%Y-%m-%d %h:%i%p')
		ELSE date_parse(date_format(datos_abandono.fecha_abandono, '%Y-%m-%d %h:%i%p'), '%Y-%m-%d %h:%i%p')

	END fecha_egreso,

	CAST(NULL AS INTEGER) porcentaje_asistencia,
	-- Se podria calcular el promedio desde la tabla calificacion_alumnos, pero no la cantidad de materias aprobadas
	-- dado que no esta contemplado en el modelo de GOET
	CAST(NULL AS VARCHAR) cant_aprobadas,

	ed.id edicion_capacitacion_id_new,
	ed.edicion_capacitacion_id_old,

	ed.capacitacion_id_new,
	ed.tipo_capacitacion,
	vec.vecino_id,
	vec.broker_id,
	eb.estado_beneficiario AS "estado_beneficiario"

FROM
"caba-piba-raw-zone-db"."goet_nomenclador"  n
INNER JOIN  "caba-piba-raw-zone-db"."goet_centro_habilitacion_modulos" chm ON n.IdNomenclador = chm.IdNomenclador
INNER JOIN "caba-piba-raw-zone-db"."goet_centro_codigo_curso" cc ON cc.IdCtrHbModulo = chm.IdCtrHbModulo
LEFT JOIN "caba-piba-raw-zone-db"."goet_trayecto" t ON (t.idkeytrayecto = cc.idkeytrayecto)
LEFT JOIN
(SELECT idctrcdcurso, idusuario FROM "caba-piba-raw-zone-db"."goet_inscripcion_alumnos"  GROUP BY idctrcdcurso, idusuario) gia  ON (cc.idctrcdcurso = gia.idctrcdcurso)
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_tmp_estado_beneficiario_goet" eb ON (cc.idctrcdcurso=eb.idctrcdcurso AND eb.idusuario=gia.idusuario)
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_tmp_view_goet_usuarios_no_duplicates" gu ON (gu.idusuario = gia.idusuario)
LEFT JOIN "caba-piba-raw-zone-db"."goet_certificado_alumnos" certificado ON (
		certificado.idctrcdcurso = cc.idctrcdcurso
		AND certificado.idusuario = gia.idusuario
		)
LEFT JOIN "caba-piba-raw-zone-db"."goet_certificado_estado" certificado_estado ON ( certificado.idcertificadoestado=certificado_estado.idcertificadoestado)
LEFT JOIN (
	SELECT min(fecha) fecha_inicio, IdUsuario, IdCtrCdCurso
	FROM "caba-piba-raw-zone-db"."goet_asistencia_alumnos"
	GROUP BY IdUsuario, IdCtrCdCurso
) inicio_cursada ON (inicio_cursada.idctrcdcurso = gia.idctrcdcurso AND inicio_cursada.IdUsuario=gia.idusuario)

LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_edicion_capacitacion" ed ON (
		ed.edicion_capacitacion_id_old = CAST(cc.idctrcdcurso AS VARCHAR)
		AND ed.base_origen = 'GOET'
		AND ed.capacitacion_id_old=(CASE WHEN UPPER(t.detalle) IS NOT NULL THEN CAST(n.IdNomenclador AS VARCHAR)||'-'||CAST(t.IdKeyTrayecto AS VARCHAR) ELSE CAST(n.IdNomenclador AS VARCHAR) END)
		)

LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_vecino" vec ON (
		vec.base_origen = 'GOET'
		AND vec.documento_broker = TRIM(gu.ndocumento )
		AND vec.genero_broker = gu.sexo
		)

LEFT JOIN (SELECT CASE
		WHEN (
				(
					cc.fincurso IS NULL
					OR date_parse(date_format(cc.fincurso, '%Y-%m-%d %h:%i%p'), '%Y-%m-%d %h:%i%p') < CURRENT_DATE
					)
				AND certi.idctrcdcurso IS NULL
				)
			THEN fin_cursada.fecha_ultima_asistencia
		ELSE
			NULL
		END fecha_abandono,
		cc.idctrcdcurso,
		gia.idusuario

	FROM
	(SELECT idctrcdcurso, idusuario FROM "caba-piba-raw-zone-db"."goet_inscripcion_alumnos"  GROUP BY idctrcdcurso, idusuario) gia
	JOIN "caba-piba-raw-zone-db"."goet_centro_codigo_curso" cc  ON (cc.idctrcdcurso = gia.idctrcdcurso)

	JOIN (
		SELECT MAX(fecha) fecha_ultima_asistencia,
			IdUsuario,
			IdCtrCdCurso
		FROM "caba-piba-raw-zone-db"."goet_asistencia_alumnos"
		GROUP BY IdUsuario,
			IdCtrCdCurso
		) fin_cursada ON (fin_cursada.idctrcdcurso = gia.idctrcdcurso
			AND fin_cursada.IdUsuario = gia.idusuario)
	LEFT JOIN "caba-piba-raw-zone-db"."goet_certificado_alumnos" certi ON (
			certi.idctrcdcurso = cc.idctrcdcurso
			AND certi.idusuario = gia.idusuario
			)
) AS datos_abandono	ON (datos_abandono.idusuario=gu.idusuario AND datos_abandono.idctrcdcurso=cc.idctrcdcurso)
GROUP BY
gu.idusuario,
gu.ndocumento,
cc.iniciocurso,
inicio_cursada.fecha_inicio,
datos_abandono.fecha_abandono,
cc.fincurso,
certificado.fecha,
eb.FinCurso,
ed.id,
ed.edicion_capacitacion_id_old,
ed.capacitacion_id_new,
ed.tipo_capacitacion,
vec.vecino_id,
vec.broker_id,
eb.estado_beneficiario

UNION
-- MOODLE
SELECT
	'MOODLE' base_origen,
	CAST(usuario.username AS VARCHAR) identificacion_alumno,
	CAST(usuario.username AS VARCHAR) documento_broker,

	date_parse(date_format(from_unixtime(uenrolments.timestart), '%Y-%m-%d %h:%i%p'), '%Y-%m-%d %h:%i%p') fecha_preinscripcion,

	-- se utiliza la fecha de la inscripcion del alumno a la edicion capacitacion para la fecha de inicio, en el caso que
	-- el estado de beneficiario no sea 'PREINSCRIPTO' ni 'INSCRIPTO', null en otro caso
	CASE WHEN eb.estado NOT IN ('PREINSCRIPTO', 'INSCRIPTO')
	THEN date_parse(date_format(from_unixtime(uenrolments.timestart), '%Y-%m-%d %h:%i%p'), '%Y-%m-%d %h:%i%p')
	ELSE NULL
	END fecha_inicio,

	CASE
		WHEN uenrolments.timeend != 0
			THEN date_parse(date_format(from_unixtime(uenrolments.timeend), '%Y-%m-%d %h:%i%p'), '%Y-%m-%d %h:%i%p')
		ELSE NULL
		END fecha_abandono,

	--Se asume que la fecha de egreso es la fecha de finalizacion del curso, si el mismo ya termino y si el alumno no abandonÃ³
	CASE
		WHEN co.enddate != 0 AND uenrolments.timeend != 0
			AND date_parse(date_format(from_unixtime(co.enddate), '%Y-%m-%d %h:%i%p'), '%Y-%m-%d %h:%i%p') <= CURRENT_DATE
			THEN date_parse(date_format(from_unixtime(co.enddate), '%Y-%m-%d %h:%i%p'), '%Y-%m-%d %h:%i%p')
		ELSE NULL
		END fecha_egreso,

	CAST(NULL AS INTEGER) porcentaje_asistencia,
	-- no se puede determinar el significado de los valores del uenrolments.STATUS, por lo que se pone estado como ' '
	CAST(NULL AS VARCHAR) cant_aprobadas,
	ed.id edicion_capacitacion_id_new,
	ed.edicion_capacitacion_id_old,
	ed.capacitacion_id_new,

	ed.tipo_capacitacion,
	vec.vecino_id,
	vec.broker_id,
	eb.estado AS "estado_beneficiario"
FROM "caba-piba-raw-zone-db"."moodle_dgtedu01_mdl_course" co
JOIN "caba-piba-raw-zone-db"."moodle_dgtedu01_mdl_course_categories" cc ON (co.category = cc.id)
LEFT JOIN "caba-piba-raw-zone-db"."moodle_dgtedu01_mdl_enrol" enrol ON (co.id = enrol.courseid)
LEFT JOIN "caba-piba-raw-zone-db"."moodle_dgtedu01_mdl_user_enrolments" uenrolments ON (uenrolments.enrolid = enrol.id)
LEFT JOIN "caba-piba-raw-zone-db"."moodle_dgtedu01_mdl_user" usuario ON (usuario.id = uenrolments.userid)
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_edicion_capacitacion" ed ON (
		ed.edicion_capacitacion_id_old = CAST(cc.id AS VARCHAR)
		AND ed.base_origen = 'MOODLE'
		)
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_vecino" vec ON (
		vec.base_origen = 'MOODLE'
		AND vec.documento_broker = REGEXP_REPLACE(UPPER(CAST(CASE WHEN REGEXP_LIKE(usuario.username, '@') THEN
					CASE WHEN REGEXP_LIKE(LOWER(usuario.username), '\.ar') THEN
						split_part(split_part(usuario.username, '@', 2),'.',4)
					ELSE split_part(split_part(usuario.username, '@', 2),'.',3) END
					ELSE split_part(usuario.username, '.', 1) END AS VARCHAR)),'[A-Za-z]+|\.','')
		)
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_tmp_estado_beneficiario_moodle" eb ON (eb.id_categoria=cc.id AND eb.alumno_id=usuario.id)
GROUP BY
	usuario.username,
	uenrolments.timestart,
	uenrolments.timeend,
	co.enddate,
	ed.id,
	ed.edicion_capacitacion_id_old,
	ed.capacitacion_id_new,
	ed.tipo_capacitacion,
	vec.vecino_id,
	vec.broker_id,
	eb.estado
UNION
-- SIENFO
SELECT 'SIENFO' base_origen,
	CAST(sf.nrodoc AS VARCHAR) identificacion_alumno,
	CAST(sf.nrodoc AS VARCHAR) documento_broker,

	-- En el caso que no se encuentre la fecha de preinscripcion se utiliza la de inscripcion, en otro caso null
	CASE WHEN sfp.fecha_inc IS NOT NULL
	THEN sfp.fecha_inc
	ELSE sf.fecha_inc
	END fecha_preinscripcion,

	-- se utiliza la fecha de la inscripcion del alumno a la edicion capacitacion para la fecha de inicio, en el caso que
	-- el estado de beneficiario no sea 'PREINSCRIPTO' ni 'INSCRIPTO', null en otro caso
	CASE WHEN ebs.estado_beneficiario NOT IN ('PREINSCRIPTO', 'INSCRIPTO')
	THEN sf.fecha_inc
	ELSE NULL
	END fecha_inicio,

	sf.fechabaja fecha_abandono,
	ed.fecha_fin_dictado fecha_egreso,
	CAST(NULL AS INTEGER) porcentaje_asistencia,
	CAST(sf.aprobado AS VARCHAR) cant_aprobadas,
	ed.id edicion_capacitacion_id_new,
	ed.edicion_capacitacion_id_old,
	ed.capacitacion_id_new,
	ed.tipo_capacitacion,
	vec.vecino_id,
	vec.broker_id,
	ebs.estado_beneficiario
FROM "caba-piba-staging-zone-db"."goayvd_typ_vw_sienfo_fichas" sf
LEFT JOIN "caba-piba-raw-zone-db"."sienfo_alumnos" a ON (sf.nrodoc = a.nrodoc)
LEFT JOIN "caba-piba-raw-zone-db"."sienfo_tgenero" g ON (CAST(g.id AS INT) = CAST(a.sexo AS INT))
LEFT JOIN "caba-piba-raw-zone-db"."sienfo_tdoc" d ON (a.tipodoc = d.tipodoc)
LEFT JOIN "caba-piba-staging-zone-db"."goayvd_typ_vw_sienfo_fichas_preinscripcion" sfp ON (sfp.codigo_ct = sf.codigo_ct AND sf.nrodoc = sfp.nrodoc)
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_edicion_capacitacion" ed ON (ed.edicion_capacitacion_id_old = sf.codigo_ct AND ed.base_origen = 'SIENFO')
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_vecino" vec ON
(vec.base_origen = 'SIENFO'
AND vec.documento_broker = CASE WHEN d.nombre IN ('D.N.I.', 'L.C.', 'L.E.', 'C.I.', 'CUIT', 'CUIL') THEN REGEXP_REPLACE(UPPER(a.nrodoc),'[A-Za-z]+|\.|\,','') ELSE
           CAST(a.nrodoc AS VARCHAR) END
AND vec.genero_broker = (CASE WHEN (g.sexo = 'Masculino') THEN 'M' WHEN (g.sexo = 'Femenino') THEN 'F' ELSE 'X' END)
)
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_tmp_estado_beneficiario_sienfo" ebs
ON (ebs.nrodoc = sf.nrodoc AND (
	(ebs.codigo_ct = ed.edicion_capacitacion_id_old AND ebs.tipo_formacion like 'CURSO' )
	OR
	(split_part(ebs.llave_doc_idcap,'-',2) = ed.capacitacion_id_old AND ebs.tipo_formacion like 'CARRERA' AND ebs.codigo_ct = ed.edicion_capacitacion_id_old))
	)
GROUP BY
	sf.nrodoc,
	sfp.fecha_inc,
	ebs.estado_beneficiario,
	sf.fecha_inc,
	sf.fechabaja,
	ed.fecha_fin_dictado,
	sf.aprobado,
	ed.id,
	ed.edicion_capacitacion_id_old,
	ed.capacitacion_id_new,
	ed.tipo_capacitacion,
	vec.vecino_id,
	vec.broker_id

UNION
-- CRMSL
SELECT 'CRMSL' base_origen,
       vec.cod_origen indentificacion_alumno,
       CAST(cs.numero_documento_c AS VARCHAR) documento_broker,
       ofc.date_modified fecha_preinscripcion,

		-- se utiliza la fecha de estado del beneficiario, en caso de null y que el estado de beneficiario no sea 'PREINSCRIPTO' ni 'INSCRIPTO' se utiliza
		-- el campo ofc.date_modified, null en otro caso
		CASE WHEN ebc.estado_beneficiario NOT IN ('PREINSCRIPTO', 'INSCRIPTO') AND ebc.inicio IS NULL
		THEN ofc.date_modified
		ELSE ebc.inicio
		END fecha_inicio,

	   -- no se encuentra valor en la base origen para este atributo
	   CAST(NULL AS DATE) fecha_abandono,

	   -- se obtiene el dato desde el script/tabla de estado del beneficiario
       ebc.fin fecha_egreso,
       CAST(sc.porcentaje_asistencia_c AS INTEGER) porcentaje_asistencia,
       CAST(NULL AS VARCHAR) cant_aprobadas,
       ed.id edicion_capacitacion_id_new,
	   ed.edicion_capacitacion_id_old,
       ed.capacitacion_id_new,
       ed.tipo_capacitacion,
       vec.vecino_id,
       vec.broker_id,
	   ebc.estado_beneficiario
FROM "caba-piba-raw-zone-db"."crm_sociolaboral_op_oportunidades_formacion_contacts_c" ofc
INNER JOIN "caba-piba-raw-zone-db"."crm_sociolaboral_contacts" co ON (co.id = ofc.op_oportunidades_formacion_contactscontacts_idb)
INNER JOIN "caba-piba-staging-zone-db"."tbp_typ_tmp_view_crm_sociolaboral_contacts_cstm_no_duplicates" cs ON (ofc.op_oportunidades_formacion_contactscontacts_idb = cs.id_c)
LEFT JOIN "caba-piba-raw-zone-db"."crm_sociolaboral_se_seguimiento_cstm" sc ON (sc.id_c = ofc.op_oportunidades_formacion_contactscontacts_idb)
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_edicion_capacitacion" ed ON (ed.edicion_capacitacion_id_old = ofc.op_oportun1d35rmacion_ida AND ed.base_origen = 'CRMSL')
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_vecino" vec
	ON (
		vec.base_origen = 'CRMSL'
		AND vec.documento_broker = CAST(cs.numero_documento_c AS VARCHAR)
		AND (CASE
				WHEN cs.genero_c LIKE 'masculino' OR SUBSTRING(CAST(cs.cuil2_c AS VARCHAR),1,2) = '20' THEN 'M'
				WHEN cs.genero_c LIKE 'femenino' OR SUBSTRING(CAST(cs.cuil2_c AS VARCHAR),1,2) = '27' THEN 'F'
				ELSE 'X' END = vec.genero_broker
			)
	)
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_tmp_estado_beneficiario_crmsl" ebc ON (ebc.edicion_capacitacion_id_old = ed.edicion_capacitacion_id_old AND ebc.alumno_id_old = vec.cod_origen )
WHERE (LOWER(co.lead_source) = 'sociolaboral'
OR ((LOWER(co.lead_source) = 'rib') AND LOWER(cs.forma_parte_interm_lab_c) = 'si'))
AND cs.numero_documento_c IS NOT NULL
GROUP BY
vec.cod_origen,
cs.numero_documento_c,
ofc.date_modified,
ebc.estado_beneficiario,
ebc.inicio,
ebc.fin,
sc.porcentaje_asistencia_c,
ed.id,
ed.edicion_capacitacion_id_old,
ed.capacitacion_id_new,
ed.tipo_capacitacion,
vec.vecino_id,
vec.broker_id

UNION
-- SIU
SELECT
	'SIU' base_origen,
	CAST(alumnos.alumno AS VARCHAR) identificacion_alumno,
	CAST(nmpd.nro_documento AS VARCHAR) documento_broker,

	-- Se obtiene el dato a partir de la tabla de estado del beneficiario
	-- en caso de null, se toma de  siu_preinscripcion_public_sga_preinscripcion
	-- en caso de null nuevamnete, se toma la fecha de inicio
	CASE WHEN eb.fecha_preinscripcion IS NOT NULL
		THEN date_parse(date_format(eb.fecha_preinscripcion, '%Y-%m-%d %h:%i%p'), '%Y-%m-%d %h:%i%p')
	WHEN P.fecha_registro IS NOT NULL
		THEN date_parse(date_format(p.fecha_registro, '%Y-%m-%d %h:%i%p'), '%Y-%m-%d %h:%i%p')
	ELSE
		date_parse(date_format(finicio.fecha_inicio, '%Y-%m-%d %h:%i%p'), '%Y-%m-%d %h:%i%p')
	END fecha_preinscripcion,

	-- se utiliza la fecha de primer asistencia a la comision y clase, en caso de null y que el estado de beneficiario no sea 'PREINSCRIPTO' ni 'INSCRIPTO' se utiliza
	-- el campo eb.fecha_preinscripcion de estado de beneficiario, null en otro caso
	CASE WHEN eb.estado_beneficiario NOT IN ('PREINSCRIPTO', 'INSCRIPTO') AND finicio.fecha_inicio IS NULL
		THEN date_parse(date_format(eb.fecha_preinscripcion, '%Y-%m-%d %h:%i%p'), '%Y-%m-%d %h:%i%p')
	WHEN eb.estado_beneficiario NOT IN ('PREINSCRIPTO')
		THEN date_parse(date_format(finicio.fecha_inicio, '%Y-%m-%d %h:%i%p'), '%Y-%m-%d %h:%i%p')
	ELSE NULL
	END fecha_inicio,

	-- Se obtiene el dato a partir de la tabla de estado del beneficiario
	-- (el script de estado de beneficiario utiliza la tabla "siu_toba_3_3_negocio_sga_perdida_regularidad")
	-- aparentemente la perdida de regularidad es por alumno y carrera y no por edicion capacitacion, por lo que si
	-- la fecha de perdida de regularidad esta entre la fecha de inicio y fin de la edicion capacitacion se considera
	-- que el alumno abandonÃ³
	CASE WHEN
			eb.fecha_baja IS NOT NULL
			AND eb.fecha_baja >= ed.fecha_inicio_dictado
			AND eb.fecha_baja <= ed.fecha_fin_dictado
		THEN date_parse(date_format(eb.fecha_baja, '%Y-%m-%d %h:%i%p'), '%Y-%m-%d %h:%i%p')
		ELSE NULL END fecha_abandono,

	date_parse(date_format(cert.fecha_egreso, '%Y-%m-%d %h:%i%p'), '%Y-%m-%d %h:%i%p') fecha_egreso,

	CAST(NULL AS INTEGER) porcentaje_asistencia,

	-- Se obtiene el dato a partir de la tabla de estado del beneficiario
	CAST(eb.cantidad_materias_aprobadas AS VARCHAR) cant_aprobadas,

	-- Cuando el estado de beneficiario es preinscripto no tiene edicion capacitacion por lo tanto es NULL la edicion_capacitacion_id_new
	CASE WHEN eb.estado_beneficiario LIKE 'PREINSCRIPTO'
	THEN NULL
	ELSE ed.id
	END edicion_capacitacion_id_new,

	-- Cuando el estado de beneficiario es preinscripto no tiene edicion capacitacion por lo tanto es NULL la edicion_capacitacion_id_old
	CASE WHEN eb.estado_beneficiario LIKE 'PREINSCRIPTO'
	THEN NULL
	ELSE ed.edicion_capacitacion_id_old
	END edicion_capacitacion_id_old,

	-- Cuando el estado de beneficiario es preinscripto el id_capacitacion se toma de la tabla capacitacion en lugar de def edicion
	CASE WHEN eb.estado_beneficiario LIKE 'PREINSCRIPTO'
	THEN dcmatch.id_new
	ELSE ed.capacitacion_id_new
	END capacitacion_id_new,

	-- Cuando el estado de beneficiario es preinscripto el id_capacitacion se toma de la tabla capacitacion en lugar de def edicion
	CASE WHEN eb.estado_beneficiario LIKE 'PREINSCRIPTO'
	THEN dcmatch.tipo_capacitacion
	ELSE ed.tipo_capacitacion
	END tipo_capacitacion,

	vec.vecino_id,
	vec.broker_id,
	eb.estado_beneficiario AS estado_beneficiario

FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_view_siu_toba_3_3_negocio_mdp_personas_no_duplicates" nmp
LEFT JOIN "caba-piba-raw-zone-db"."siu_preinscripcion_public_sga_preinscripcion" pre ON (nmp.persona = pre.persona)
LEFT JOIN "caba-piba-raw-zone-db"."siu_preinscripcion_public_sga_preinscripcion_propuestas" pre_pro ON (pre.id_preinscripcion = pre_pro.id_preinscripcion)
LEFT JOIN "caba-piba-raw-zone-db"."siu_toba_3_3_negocio_mdp_personas_documentos" nmpd ON (nmpd.documento = nmp.documento_principal)
LEFT JOIN  "caba-piba-raw-zone-db"."siu_toba_3_3_negocio_sga_alumnos" alumnos ON (alumnos.persona=nmp.persona)
LEFT JOIN "caba-piba-raw-zone-db"."siu_toba_3_3_negocio_sga_insc_cursada" insc_cur ON (insc_cur.alumno=alumnos.alumno)
LEFT JOIN "caba-piba-raw-zone-db"."siu_toba_3_3_negocio_sga_comisiones" comi ON (comi.comision=insc_cur.comision)
LEFT JOIN "caba-piba-raw-zone-db"."siu_toba_3_3_negocio_sga_comisiones_propuestas" cp ON (comi.comision = cp.comision)
LEFT JOIN "caba-piba-raw-zone-db"."siu_toba_3_3_negocio_sga_planes" spl ON (spl.propuesta = cp.propuesta)
LEFT JOIN "caba-piba-raw-zone-db"."siu_toba_3_3_negocio_sga_turnos_cursadas" turno_c ON (turno_c.turno=comi.turno)
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_capacitacion_match" dcmatch ON (dcmatch.id_old = CAST(spl.plan AS VARCHAR) AND dcmatch.base_origen = 'SIU')
LEFT JOIN "caba-piba-raw-zone-db"."siu_toba_3_3_negocio_sga_periodos_lectivos" pl ON (comi.periodo_lectivo=pl.periodo_lectivo)
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_capacitacion" dc ON (dcmatch.id_new = dc.id_new AND dcmatch.base_origen = dc.base_origen)
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_establecimientos" est ON (est.id_old=CAST(comi.ubicacion AS VARCHAR) AND est.base_origen = dc.base_origen)
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_edicion_capacitacion" ed ON
		(ed.edicion_capacitacion_id_old=
									(CAST(spl.plan AS VARCHAR) || '-' || CAST(spl.propuesta AS VARCHAR) || '-' ||
									CAST(pl.periodo_lectivo AS VARCHAR)  || '-' || CAST(est.id AS VARCHAR) || '-' ||
									CAST(dc.modalidad_id AS VARCHAR) || '-' || CAST(turno_c.turno AS VARCHAR)
									)
		AND ed.base_origen = 'SIU')

LEFT JOIN
		(SELECT ca.alumno, ca_acum.comision, MIN(cla.fecha) fecha_inicio
		FROM
		"caba-piba-raw-zone-db"."siu_toba_3_3_negocio_sga_clases_asistencia" ca
		LEFT JOIN "caba-piba-raw-zone-db"."siu_toba_3_3_negocio_sga_clases" cla ON (cla.clase = ca.clase)
		LEFT JOIN "caba-piba-raw-zone-db"."siu_toba_3_3_negocio_sga_clases_asistencia_acum" ca_acum ON (ca_acum.alumno = ca.alumno)
		GROUP BY ca.alumno, ca_acum.comision) finicio ON (finicio.comision=insc_cur.comision AND finicio.alumno=alumnos.alumno)
LEFT JOIN "caba-piba-raw-zone-db"."siu_toba_3_3_negocio_sga_certificados_otorg" cert ON (cert.alumno=alumnos.alumno AND cert.plan_version=alumnos.plan_version)
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_vecino" vec ON (
		vec.base_origen = 'SIU'
		AND vec.documento_broker = CAST(nmpd.nro_documento AS VARCHAR)
		AND vec.genero_broker = nmp.sexo
		)
LEFT JOIN "caba-piba-raw-zone-db"."siu_preinscripcion_public_sga_preinscripcion" p ON (p.persona=alumnos.persona)
LEFT JOIN "caba-piba-raw-zone-db"."siu_preinscripcion_public_sga_preinscripcion_propuestas" pp ON (p.id_preinscripcion = pp.id_preinscripcion AND pp.propuesta=alumnos.propuesta)
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_tmp_estado_beneficiario_siu" eb
	ON (
		(eb.estado_beneficiario!='PREINSCRIPTO' AND eb.broker_id=vec.broker_id
		AND eb.plan_version=alumnos.plan_version AND eb.propuesta=alumnos.propuesta)
		-- cuando el estado es preinscripto no tiene plan_version
		OR
		(eb.estado_beneficiario='PREINSCRIPTO' AND eb.broker_id=vec.broker_id AND eb.propuesta=pre_pro.propuesta)
	)
GROUP BY
	alumnos.alumno,
	nmpd.nro_documento,
	eb.fecha_preinscripcion,
	p.fecha_registro,
	finicio.fecha_inicio,
	eb.fecha_baja,
	ed.fecha_inicio_dictado,
	ed.fecha_fin_dictado,
	cert.fecha_egreso,
	eb.cantidad_materias_aprobadas,
	ed.id,
	ed.edicion_capacitacion_id_old,
	ed.capacitacion_id_new,
	ed.tipo_capacitacion,
	dcmatch.id_new,
	CAST(dcmatch.id_old AS VARCHAR),
	dcmatch.tipo_capacitacion,
	vec.vecino_id,
	vec.broker_id,
	eb.estado_beneficiario
--</sql>--

-- 2.-  Crear tabla temporal incluyendo IEL
-- --<sql>--DROP TABLE `caba-piba-staging-zone-db`.`tbp_typ_tmp_cursada`;--</sql>--
--<sql>--
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_cursada" AS
SELECT * FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_cursada_1"
-- IEL
UNION
SELECT df.base_origen,
	   iel.nrodocumento identificacion_alumno,
	   iel.nrodocumento documento_broker,
	   NULL fecha_preinscripcion,
	   NULL fecha_inicio,
	   NULL fecha_abandono,
	   NULL fecha_egreso,
	   0 porcentaje_asistencia,
	   '0' cant_aprobadas,
	   NULL edicion_capacitacion_id_new,
	   NULL edicion_capacitacion_id_old,
	   df.id_new capacitacion_id_new,
	   df.tipo_capacitacion,
	   iel.broker_id||'-'||df.base_origen vecino_id,
	   iel.broker_id,
	   'PREINSCRIPTO' estado_beneficiario
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_analisis_iel" iel
INNER JOIN "caba-piba-staging-zone-db"."tbp_typ_def_capacitacion" df ON
(levenshtein_distance(df.descrip_normalizada,iel.descrip_normalizada) <= 0.9) AND df.base_origen IN ('GOET','SIU')
WHERE NOT EXISTS (SELECT 1 FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_cursada_1" dcu
				  WHERE dcu.capacitacion_id_new = df.id_new AND
						dcu.base_origen = df.base_origen AND
						dcu.documento_broker = iel.nrodocumento)
GROUP BY
	df.base_origen,
	iel.nrodocumento,
	iel.nrodocumento,
	df.id_new,
	iel.curso,
	df.tipo_capacitacion,
	iel.broker_id||'-'||df.base_origen,
	iel.broker_id
--</sql>--



-- Copy of 2023.05.24 step 17 - consume cursada.sql 



-- CURSADA GOET, MOODLE, SIENFO, CRMSL Y SIU
-- 1.- Crear tabla cursada definitiva
-- --<sql>--DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_def_cursada`;--</sql>--
--<sql>--
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_def_cursada" AS
SELECT row_number() OVER () AS id,
       base_origen,
	   edicion_capacitacion_id_old,
	   edicion_capacitacion_id_new,
	   capacitacion_id_new,
	   identificacion_alumno identificacion_alumno_old,
       documento_broker,
	   -- si hay mas de una fecha de preinscripcion se toma la menor
       MIN(fecha_preinscripcion) fecha_preinscripcion,
	   -- si hay mas de una fecha de inicio se toma la menor
	   MIN(fecha_inicio) fecha_inicio,
	   -- si hay mas de una fecha de abandono se toma la menor
	   MIN(fecha_abandono) fecha_abandono,
	   -- si hay mas de una fecha de egreso se toma la menor
	   MIN(fecha_egreso) fecha_egreso,
	   porcentaje_asistencia,
	   -- si hay de un registro de cantidad de aprobados, porque tiene mas de un registro de inscripcion, se toma el numero mayor
       MAX(cant_aprobadas) cant_aprobadas,
       vecino_id,
       broker_id,
	   CASE
		WHEN estado_beneficiario LIKE 'FINALIZADO' THEN 'FINALIZO_CURSADA'
		WHEN estado_beneficiario LIKE 'APROBADO' THEN 'EGRESADO'
		WHEN estado_beneficiario LIKE 'NO_APLICA' THEN 'PREINSCRIPTO'
		WHEN estado_beneficiario LIKE 'BAJA' THEN 'BAJA'
		WHEN estado_beneficiario LIKE 'REGULAR' THEN 'EN_CURSO'
		ELSE estado_beneficiario
		END estado_beneficiario
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_cursada"
WHERE
vecino_id IS NOT NULL
AND broker_id IS NOT NULL
AND capacitacion_id_new IS NOT NULL
AND TRIM(documento_broker) != ''
AND
(edicion_capacitacion_id_new IS NOT NULL
OR
-- puede no existir edicion_capacitacion_id_new cuando el estado_beneficiario es 'PREINSCRIPTO'
(estado_beneficiario LIKE 'PREINSCRIPTO' AND base_origen IN ('SIU', 'GOET'))
)
GROUP BY
	base_origen,
	edicion_capacitacion_id_old,
	edicion_capacitacion_id_new,
	capacitacion_id_new,
	identificacion_alumno,
	documento_broker,
	porcentaje_asistencia,
	vecino_id,
	broker_id,
	estado_beneficiario
--</sql>--



-- Copy of 2023.05.24 step 18 - consume trayectoria_educativa.sql 



-- TRAYECTORIA EDUCATIVA GOET, MOODLE, SIENFO Y CRMSL
-- 1.- Crear tabla trayectoria educativa definitiva
-- --<sql>--DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_def_trayectoria_educativa`;--</sql>--
--<sql>--
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_def_trayectoria_educativa" AS
SELECT
	CAST(row_number() OVER () AS VARCHAR) AS cursado_id,
	bg.id broker_id,
	bg.nombre,
	bg.apellido,
	bg.tipo_doc_broker tipo_documento,
	bg.documento_broker numero_documento,
	vec.nacionalidad_broker nacionalidad,
	bg.genero,
	dc.capacitacion_id_asi codigo_curso,
	COALESCE(dc.descrip_capacitacion, dc.descrip_normalizada) nombre_curso,
	dc.detalle_capacitacion,
	p.codigo_programa,
	p.nombre_programa detalle,
	DATE_FORMAT(CAST(dcu.fecha_inicio AS DATE), '%d-%m-%Y') fecha_inicio_cursada,
	dcu.estado_beneficiario estado,
	-- Si existe mas de una fecha de egreso, se toma la menor
	DATE_FORMAT(MIN(CAST(dcu.fecha_egreso AS DATE)), '%d-%m-%Y') fecha_egreso
FROM "caba-piba-staging-zone-db"."tbp_broker_def_broker_general" bg
	INNER JOIN "caba-piba-staging-zone-db"."tbp_typ_def_vecino" vec ON (bg.id = vec.broker_id)
	LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_cursada" dcu ON (dcu.broker_id = vec.broker_id)
	LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_edicion_capacitacion" ed ON (ed.id = dcu.edicion_capacitacion_id_new AND ed.base_origen = dcu.base_origen)
	LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_capacitacion" dc ON (dc.id_new = ed.capacitacion_id_new AND dc.base_origen = ed.base_origen)
	LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_programa" p ON (p.programa_id = dc.programa_id)
WHERE
	dc.base_origen != 'MOODLE' AND
	dcu.estado_beneficiario IS NOT NULL
GROUP BY
	bg.id,
	bg.apellidos_nombres,
	bg.nombre,
	bg.apellido,
	bg.tipo_doc_broker,
	bg.documento_broker,
	vec.nacionalidad_broker,
	bg.genero,
	dc.capacitacion_id_asi,
	COALESCE(dc.descrip_capacitacion, dc.descrip_normalizada),
	dc.detalle_capacitacion,
	p.codigo_programa,
	p.nombre_programa,
	DATE_FORMAT(CAST(dcu.fecha_inicio AS DATE), '%d-%m-%Y'),
	dcu.estado_beneficiario
--</sql>--



-- Copy of 2023.05.24 step 19 - staging oportunidad_laboral.sql 



-- 1.-- Crear OPORTUNIDAD_LABORAL CRMEMPLEO, CRMSL, PORTALEMPLEO,
-- CAMPOS REQUERIDOS EN TABLA DEF SEGUN MODELO (Oferta Laboral, PrÃ¡cticas Formativas (PasantÃ­as)):
-- CÃ³digo (1+)
-- DescripciÃ³n
-- Estado => ABIERTO, CANCELADO, CERRADO
-- Apto discapacitados => S, N
-- Vacantes
-- Modalidad de Trabajo => RELACION DE DEPENDENCIA, CONTRATIO, PASANTIA, AD HONOREM
-- Edad MÃ­nima
-- Edad MÃ¡xima
-- Vacantes Cubiertas
-- Tipo de Puesto
-- Turno de Trabajo => MAÃANA, MAÃANA-TARDE, MAÃANA-TARDE-NOCHE, TARDE, TARDE-NOCHE, NOCHE
-- Grado de Estudio => SECUNDARIO, TERCIARIO, UNIVERSITARIO, OTROS
-- Duracion Practica formativa
-- Sector Productivo => ABASTECIMIENTO Y LOGISTICA, ADMINISTRACION, CONTABILIDAD Y FINANZAS,ATENCION AL CLIENTE, CALL CENTER Y TELEMARKETING,ADUANA Y COMERCIO EXTERIOR, COMERCIAL, VENTAS Y NEGOCIOS, GASTRONOMIA, HOTELERIA Y TURISMO, INGENIERIAS , LIMPIEZA Y MANTENIMIENTO (SIN EDIFICIOS), OFICIOS Y OTROS, PRODUCCION Y MANUFACTURA (SIN TEXTIL, ELECTRONICA Y AUTOMOTRIZ), SALUD, MEDICINA, FARMACIA Y ASISTENCIA SOCIAL, SECTOR PUBLICO,MINERIA, ENERGIA, PETROLEO, AGUA Y GAS
-- Nota: la tabla deberÃ¡ estar relacionada con la entidad "Registro laboral formal" si toma el empleo
-- y con la entidad "Programa"

-- --<sql>--DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_tmp_oportunidad_laboral`;--</sql>--
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
	CASE WHEN apto_discapacitado__c = TRUE THEN '1' ELSE '0' END apto_discapacitado,
	CAST(vacantes__c AS VARCHAR) vacantes,
	CAST(modalidad_de_trabajo__c AS VARCHAR) modalidad_de_trabajo,
	CAST(edad__c AS VARCHAR) edad_minima,
	CAST(avx_edad_maxima__c AS VARCHAR) edad_maxima,
	CAST(avx_vacantes_cubiertas__c AS VARCHAR) vacantes_cubiertas,
	CAST(tipo_de_puesto__c AS VARCHAR) tipo_de_puesto,
	-- el horario de entrada y salida servirÃ¡ para determinar el campo modelado Turno de Trabajo"
	CAST(horario_de_entrada__c AS VARCHAR) horario_entrada,
	CAST(horario_de_salida__c AS VARCHAR) horario_salida,
	CAST(grados_de_estudio__c AS VARCHAR) grado_de_estudio,
	-- en un futuro cuando este asociado a "Registro laboral formal" si la misma finalizo se podria obtener
	-- por la diferencia entre el inicio y fin de la relacion
	CAST('' AS VARCHAR) duracion_practica_formativa,
	
	-- tienen sentido los siguientes campos?
	CAST(avx_sector__c AS VARCHAR) sector_productivo,
	CAST(avx_industria__c AS VARCHAR) industria, 
	CAST(enviado_para_aprobacion__c AS VARCHAR) enviado_para_aprobacion__c,
	CAST(postulantes_contratados__c AS VARCHAR) postulantes_contratados__c,
	
	-- datos de organizacion/empresa que busca empleados
	CAST(razon_social__c AS VARCHAR) organizacion_empleadora,
	CAST('' AS VARCHAR) organizacion_empleadora_calle,
	CAST('' AS VARCHAR) organizacion_empleadora_piso,
	CAST('' AS VARCHAR) organizacion_empleadora_depto,
	CAST('' AS VARCHAR) organizacion_empleadora_cp,
	CAST('' AS VARCHAR) organizacion_empleadora_barrio,
	CAST('' AS VARCHAR) organizacion_empleadora_cuit	
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
	CAST('' AS VARCHAR) edad_maxima,
	CAST('' AS VARCHAR) vacantes_cubiertas,
	CAST(tipo_de_puesto__c AS VARCHAR) tipo_de_puesto,
	-- el horario de entrada y salida servirÃ¡ para determinar el campo modelado Turno de Trabajo"
	CAST(horario_de_entrada__c AS VARCHAR) horario_entrada,
	CAST(horario_de_salida__c AS VARCHAR) horario_salida,
	CAST(grados_de_estudio__c AS VARCHAR) grado_de_estudio,
	-- en un futuro cuando este asociado a "Registro laboral formal" si la misma finalizo se podria obtener
	-- por la diferencia entre el inicio y fin de la relacion
	CAST('' AS VARCHAR) duracion_practica_formativa,

	-- tienen sentido los siguientes campos?
	CAST('' AS VARCHAR) sector_productivo,
	CAST(industria__c AS VARCHAR) industria,
	CAST(enviado_para_aprobacion__c AS VARCHAR) enviado_para_aprobacion__c,
	CAST(postulantes_contratados__c AS VARCHAR) postulantes_contratados__c,

	-- datos de organizacion/empresa que busca empleados
	CAST(razon_social__c AS VARCHAR) organizacion_empleadora,
	CAST('' AS VARCHAR) organizacion_empleadora_calle,
	CAST('' AS VARCHAR) organizacion_empleadora_piso,
	CAST('' AS VARCHAR) organizacion_empleadora_depto,
	CAST('' AS VARCHAR) organizacion_empleadora_cp,
	CAST('' AS VARCHAR) organizacion_empleadora_barrio,
	CAST('' AS VARCHAR) organizacion_empleadora_cuit
FROM "caba-piba-raw-zone-db"."crm_empleo_historico_anuncio__c"

UNION

SELECT
'CRMSL' base_origen,
CAST(id AS VARCHAR) id,
CAST(name AS VARCHAR) descripcion,
CAST(DATE_PARSE(date_format(COALESCE(fecha_inicio, date_entered), '%Y-%m-%d %h:%i%p'), '%Y-%m-%d %h:%i%p') AS DATE) fecha_publicacion,
CAST(situacion AS VARCHAR) estado,
CAST('' AS VARCHAR) apto_discapacitado,
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
CAST('' AS VARCHAR) duracion_practica_formativa,

-- tienen sentido los siguientes campos?
-- analizar valores
COALESCE(sector_economico, lugar) sector_productivo,
COALESCE(sector, sector_economico) industria,
CAST('' AS VARCHAR) enviado_para_aprobacion__c,
CAST('' AS VARCHAR) postulantes_contratados__c,

-- datos de organizacion/empresa que busca empleados
	CAST(empresa AS VARCHAR) organizacion_empleadora,
	CAST(lugar AS VARCHAR) organizacion_empleadora_calle,
	CAST('' AS VARCHAR) organizacion_empleadora_piso,
	CAST('' AS VARCHAR) organizacion_empleadora_depto,
	CAST('' AS VARCHAR) organizacion_empleadora_cp,
	CAST('' AS VARCHAR) organizacion_empleadora_barrio,
CAST('' AS VARCHAR) organizacion_empleadora_cuit
FROM "caba-piba-raw-zone-db"."crm_sociolaboral_op_oportunidades_laborales"

UNION
SELECT
'PORTALEMPLEO' base_origen,
CAST(jo.id AS VARCHAR) id,
CAST(jo.position || jo.tasks_description AS VARCHAR) descripcion,
CAST(DATE_PARSE(date_format(COALESCE(jp.published_date, jp.created_at, jo.due_date), '%Y-%m-%d %h:%i%p'), '%Y-%m-%d %h:%i%p') AS DATE) fecha_publicacion,
CASE WHEN jo.due_date <= NOW() THEN 'finalizada' ELSE 'en_curso' END estado,
CASE WHEN for_disabled = 0 THEN '0' ELSE '1' END apto_discapacitado,
CAST(jo.vacancy AS VARCHAR) vacantes,
CAST(m.name AS VARCHAR) modalidad_de_trabajo,

CAST(jr.age_min AS VARCHAR) edad_minima,
CAST(jr.age_max AS VARCHAR) edad_maxima,

CAST('' AS VARCHAR) vacantes_cubiertas,
CAST(jo.position AS VARCHAR) tipo_de_puesto,

-- mirar tambien el campo tasks_description contiene diverso texto que indica el horario de entrada y salida,
-- se deben analizar las posibilidades para determinar el turno de trabajo
CAST(jo.checkin_time AS VARCHAR) horario_entrada,
CAST(jo.checkout_time AS VARCHAR) horario_salida,


CAST(el.value AS VARCHAR) grado_de_estudio,
-- en un futuro cuando este asociado a "Registro laboral formal" si la misma finalizo se podria obtener
-- por la diferencia entre el inicio y fin de la relacion
CAST('' AS VARCHAR) duracion_practica_formativa,

-- tienen sentido los siguientes campos?
-- analizar valores
CAST(s.name AS VARCHAR) sector_productivo,
CAST('' AS VARCHAR) industria,
CAST('' AS VARCHAR) enviado_para_aprobacion__c,
CAST('' AS VARCHAR) postulantes_contratados__c,

-- datos de organizacion/empresa que busca empleados
CAST(business_name AS VARCHAR) organizacion_empleadora,
CAST(adr.street_address AS VARCHAR) organizacion_empleadora_calle,
CAST(adr.floor AS VARCHAR) organizacion_empleadora_piso,
CAST(adr.apt AS VARCHAR) organizacion_empleadora_depto,
CAST(adr.zipcode AS VARCHAR) organizacion_empleadora_cp,
CAST(adr.neighborhood AS VARCHAR) organizacion_empleadora_barrio,
CAST(org.lei_code AS VARCHAR) organizacion_empleadora_cuit

FROM "caba-piba-raw-zone-db"."portal_empleo_job_offers" jo
LEFT JOIN "caba-piba-raw-zone-db"."portal_empleo_job_postings" jp ON (jo.id=jp.job_offer_id)
LEFT JOIN "caba-piba-raw-zone-db"."portal_empleo_organizations" org ON (org.id=jp.organization_id)
LEFT JOIN "caba-piba-raw-zone-db"."portal_empleo_addresses" adr ON (adr.id=org.addressid)
LEFT JOIN "caba-piba-raw-zone-db"."portal_empleo_mtr_job_posting_statuses" ps
ON (jp.job_posting_status_id = ps.id AND ps.description IN ('Pendiente','Cerrada','Aprobado') )
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
    --MAÃANA
    WHEN horario_entrada IN ('6','6:00','6:05','6:10','6:15','6:20','6:25','6:30','6:35','6:40','6:45','6:50','6:55','06','06:00','06:15','06:20','06:25','06:30','06:35','06:40','06:45','06:50','06:55','7','7:00','7:05','7:10','7:15','7:20','7:25','7:30','7:35','7:40','7:45','7:50','7:55','07','07:00','07:05','07:10','07:15','07:20','07:25','07:30','07:35','07:40','07:45','07:50','07:55','8','8:00','8:05','8:10','8:15','8:20','8:25','8:30','8:35','8:40','8:45','8:50','8:55','08','08:00','08:05','08:10','08:15','08:20','08:25','08:30','08:35','08:40','08:45','08:50','08:55','9','9:00','9:05','9:10','9:15','9:20','9:25','9:30','9:35','9:40','9:45','9:50','9:55','09','09:00','09:05','09:10','09:15','09:20','09:25','09:30','09:35','09:40','09:45','09:50','09:55','10','10:00','10:05','10:10','10:15','10:20','10:25','10:30','10:35','10:40','10:45','10:50','10:55','11','11:00','11:05','11:10','11:15','11:20','11:25','11:30','11:35','11:40','11:45','11:50','11:55','11','11:00','11:05','11:10','11:15','11:20','11:25','11:30','11:35','11:40','11:45','11:50','11:55','12','12:00','12:05','12:10','12:15','12:20','12:25','12:30','12:35','12:40','12:45','12:50','12:55','13','13:00','13:05','13:10','13:15','13:20','13:25','13:30','13:35','13:40','13:45','13:50','13:55') AND horario_salida IN ('6','6:00','6:05','6:10','6:15','6:20','6:25','6:30','6:35','6:40','6:45','6:50','6:55','06','06:00','06:15','06:20','06:25','06:30','06:35','06:40','06:45','06:50','06:55','7','7:00','7:05','7:10','7:15','7:20','7:25','7:30','7:35','7:40','7:45','7:50','7:55','07','07:00','07:05','07:10','07:15','07:20','07:25','07:30','07:35','07:40','07:45','07:50','07:55','8','8:00','8:05','8:10','8:15','8:20','8:25','8:30','8:35','8:40','8:45','8:50','8:55','08','08:00','08:05','08:10','08:15','08:20','08:25','08:30','08:35','08:40','08:45','08:50','08:55','9','9:00','9:05','9:10','9:15','9:20','9:25','9:30','9:35','9:40','9:45','9:50','9:55','09','09:00','09:05','09:10','09:15','09:20','09:25','09:30','09:35','09:40','09:45','09:50','09:55','10','10:00','10:05','10:10','10:15','10:20','10:25','10:30','10:35','10:40','10:45','10:50','10:55','11','11:00','11:05','11:10','11:15','11:20','11:25','11:30','11:35','11:40','11:45','11:50','11:55','11','11:00','11:05','11:10','11:15','11:20','11:25','11:30','11:35','11:40','11:45','11:50','11:55','12','12:00','12:05','12:10','12:15','12:20','12:25','12:30','12:35','12:40','12:45','12:50','12:55','13','13:00','13:05','13:10','13:15','13:20','13:25','13:30','13:35','13:40','13:45','13:50','13:55') THEN 'MAÃANA'
    --MAÃANA-TARDE
    WHEN horario_entrada IN ('6','6:00','6:05','6:10','6:15','6:20','6:25','6:30','6:35','6:40','6:45','6:50','6:55','06','06:00','06:15','06:20','06:25','06:30','06:35','06:40','06:45','06:50','06:55','7','7:00','7:05','7:10','7:15','7:20','7:25','7:30','7:35','7:40','7:45','7:50','7:55','07','07:00','07:05','07:10','07:15','07:20','07:25','07:30','07:35','07:40','07:45','07:50','07:55','8','8:00','8:05','8:10','8:15','8:20','8:25','8:30','8:35','8:40','8:45','8:50','8:55','08','08:00','08:05','08:10','08:15','08:20','08:25','08:30','08:35','08:40','08:45','08:50','08:55','9','9:00','9:05','9:10','9:15','9:20','9:25','9:30','9:35','9:40','9:45','9:50','9:55','09','09:00','09:05','09:10','09:15','09:20','09:25','09:30','09:35','09:40','09:45','09:50','09:55','10','10:00','10:05','10:10','10:15','10:20','10:25','10:30','10:35','10:40','10:45','10:50','10:55','11','11:00','11:05','11:10','11:15','11:20','11:25','11:30','11:35','11:40','11:45','11:50','11:55','12','12:00','12:05','12:10','12:15','12:20','12:25','12:30','12:35','12:40','12:45','12:50','12:55','13','13:00','13:05','13:10','13:15','13:20','13:25','13:30','13:35','13:40','13:45','13:50','13:55') AND horario_salida IN ('14','14:00','14:05','14:10','14:15','14:20','14:25','14:30','14:35','14:40','14:45','14:50','14:55','15','15:00','15:05','15:10','15:15','15:20','15:25','15:30','15:35','15:40','15:45','15:50','15:55','16','16:00','16:05','16:10','16:15','16:20','16:25','16:30','16:35','16:40','16:45','16:50','16:55','17','17:00','17:05','17:10','17:15','17:20','17:25','17:30','17:35','17:40','17.45','17:45','17:50','17:55','18','18:00','18:05','18:10','18:15','18:20','18:25','18:30','18:35','18:40','18:45','18:50','18:55','19','19:00','19:05','19:10','19:15','19:20','19:25','19:30','19:35','19:40','19:45','19:50','19:55','20','20:00','20:05','20:10','20:15','20:20','20:25','20:30','20:35','20:40','20:45','20:50','20:55','21','21:00','21:05','21:10','21:15','21:20','21:25','21:30','21:35','21:40','21:45','21:50','21:55','22','22:00') THEN 'MAÃANA-TARDE'
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
organizacion_empleadora_cuit
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
    WHEN et.horario_entrada LIKE '(lunes a viernes 8:30 a 13:30 y 14:30 a 18 hs - sÃ¡bados 9 a 13 hs' THEN '8:30 A 13:30 Y 14:30 A 18'
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
    --MAÃANA-TARDE-NOCHE
    WHEN horario_entrada IN ('13:00','19:00','20:00','21:00','1','11') AND horario_salida IN ('22:00','07:00','08:00','07:00','24','23') THEN 'MAÃANA-TARDE-NOCHE'
    WHEN (horario_entrada LIKE '12:00' AND horario_salida LIKE '23:45') OR (horario_entrada LIKE '12:00' AND horario_salida LIKE '00:00') THEN 'MAÃANA-TARDE-NOCHE'
    WHEN regexp_like(UPPER(horario_entrada),'MAÃANA') AND regexp_like(UPPER(horario_entrada),'TARDE') AND regexp_like(UPPER(horario_entrada),'NOCHE')  THEN 'MAÃANA-TARDE-NOCHE'
    --MAÃANA-TARDE
    WHEN (horario_entrada LIKE '00:00' AND horario_salida LIKE '23:45') OR (horario_entrada LIKE '00:00' AND horario_salida LIKE '23:30') OR (horario_entrada LIKE '00:15' AND horario_salida LIKE '23:45') OR (horario_entrada LIKE '00:30' AND horario_salida LIKE '23:45')  OR (horario_entrada LIKE '00:45' AND horario_salida LIKE '23:45')  OR (horario_entrada LIKE '01:00' AND horario_salida LIKE '17:00')  OR (horario_entrada LIKE '01:00' AND horario_salida LIKE '21:00')  OR (horario_entrada LIKE '01:15' AND horario_salida LIKE '21:45')  OR (horario_entrada LIKE '05:45' AND horario_salida LIKE '18:15')  OR (horario_entrada LIKE '06:00' AND horario_salida LIKE '03:00')  OR (horario_entrada LIKE '07:00' AND horario_salida LIKE '05:00')  OR (horario_entrada LIKE '07:00' AND horario_salida LIKE '03:00')  OR (horario_entrada LIKE '07:00' AND horario_salida LIKE '04:00')  OR (horario_entrada LIKE '07:00' AND horario_salida LIKE '06:00')  OR (horario_entrada LIKE '07:45' AND horario_salida LIKE '04:45')  OR (horario_entrada LIKE '08:00' AND horario_salida LIKE '04:30')  OR (horario_entrada LIKE '08:00' AND horario_salida LIKE '05:00')  OR (horario_entrada LIKE '08:15' AND horario_salida LIKE '05:45')  OR (horario_entrada LIKE '12:00' AND horario_salida LIKE '08:00')  OR (horario_entrada LIKE '08:30' AND horario_salida LIKE '05:30')  OR (horario_entrada LIKE '09:00' AND horario_salida LIKE '06:15')  OR (horario_entrada LIKE '09:00' AND horario_salida LIKE '06:00')  OR (horario_entrada LIKE '09:00' AND horario_salida LIKE '05:00')  OR (horario_entrada LIKE '09:00' AND horario_salida LIKE '04:00')  OR (horario_entrada LIKE '10:00' AND horario_salida LIKE '06:00') THEN 'MAÃANA-TARDE'
    WHEN regexp_like(UPPER(horario_entrada),'MAÃANA') AND regexp_like(UPPER(horario_entrada),'TARDE') THEN 'MAÃANA-TARDE'
    --MAÃANA
    WHEN (horario_entrada LIKE '04:00' AND horario_salida LIKE '13:00')  OR (horario_entrada LIKE '04:00' AND horario_salida LIKE '12:00')  OR (horario_entrada LIKE '05:30' AND horario_salida LIKE '12:30')  OR (horario_entrada LIKE '06:00' AND horario_salida LIKE '08:00')  OR (horario_entrada LIKE '07:00' AND horario_salida LIKE '07:00')  OR (horario_entrada LIKE '07:00' AND horario_salida LIKE '11:00')  OR (horario_entrada LIKE '07:30' AND horario_salida LIKE '11:30')  OR (horario_entrada LIKE '08:30' AND horario_salida LIKE '02:30')  OR (horario_entrada LIKE '09:00' AND horario_salida LIKE '09:00')  OR (horario_entrada LIKE '09:00' AND horario_salida LIKE '01:00')  OR (horario_entrada LIKE '09:15' AND horario_salida LIKE '09:45')  OR (horario_entrada LIKE '10:00' AND horario_salida LIKE '02:00')  OR (horario_entrada LIKE '11:00' AND horario_salida LIKE '01:30')  OR (horario_entrada LIKE '12:00' AND horario_salida LIKE '12:00') OR (horario_entrada LIKE '07:00' AND horario_salida LIKE '02:00')  OR (horario_entrada LIKE '06:15' AND horario_salida LIKE '01:00')  OR (horario_entrada LIKE '06:00' AND horario_salida LIKE '06:00')  OR (horario_entrada LIKE '4' AND horario_salida LIKE '12')  OR (horario_entrada LIKE '6' AND horario_salida LIKE '9') THEN 'MAÃANA'
    WHEN regexp_like(UPPER(horario_entrada),'MAÃANA') THEN 'MAÃANA'
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
    --MAÃANA
    WHEN et.he_split1 IN ('6','6:00','6:05','6:10','6:15','6:20','6:25','6:30','6:35','6:40','6:45','6:50','6:55','06','06:00','06:15','06:20','06:25','06:30','06:35','06:40','06:45','06:50','06:55','7','7:00','7:05','7:10','7:15','7:20','7:25','7:30','7:35','7:40','7:45','7:50','7:55','07','07:00','07:05','07:10','07:15','07:20','07:25','07:30','07:35','07:40','07:45','07:50','07:55','8','8:00','8:05','8:10','8:15','8:20','8:25','8:30','8:35','8:40','8:45','8:50','8:55','08','08:00','08:05','08:10','08:15','08:20','08:25','08:30','08:35','08:40','08:45','08:50','08:55','9','9:00','9:05','9:10','9:15','9:20','9:25','9:30','9:35','9:40','9:45','9:50','9:55','09','09:00','09:05','09:10','09:15','09:20','09:25','09:30','09:35','09:40','09:45','09:50','09:55','10','10:00','10:05','10:10','10:15','10:20','10:25','10:30','10:35','10:40','10:45','10:50','10:55','11','11:00','11:05','11:10','11:15','11:20','11:25','11:30','11:35','11:40','11:45','11:50','11:55','11','11:00','11:05','11:10','11:15','11:20','11:25','11:30','11:35','11:40','11:45','11:50','11:55','12','12:00','12:05','12:10','12:15','12:20','12:25','12:30','12:35','12:40','12:45','12:50','12:55','13','13:00','13:05','13:10','13:15','13:20','13:25','13:30','13:35','13:40','13:45','13:50','13:55') AND et.he_split2 IN ('6','6:00','6:05','6:10','6:15','6:20','6:25','6:30','6:35','6:40','6:45','6:50','6:55','06','06:00','06:15','06:20','06:25','06:30','06:35','06:40','06:45','06:50','06:55','7','7:00','7:05','7:10','7:15','7:20','7:25','7:30','7:35','7:40','7:45','7:50','7:55','07','07:00','07:05','07:10','07:15','07:20','07:25','07:30','07:35','07:40','07:45','07:50','07:55','8','8:00','8:05','8:10','8:15','8:20','8:25','8:30','8:35','8:40','8:45','8:50','8:55','08','08:00','08:05','08:10','08:15','08:20','08:25','08:30','08:35','08:40','08:45','08:50','08:55','9','9:00','9:05','9:10','9:15','9:20','9:25','9:30','9:35','9:40','9:45','9:50','9:55','09','09:00','09:05','09:10','09:15','09:20','09:25','09:30','09:35','09:40','09:45','09:50','09:55','10','10:00','10:05','10:10','10:15','10:20','10:25','10:30','10:35','10:40','10:45','10:50','10:55','11','11:00','11:05','11:10','11:15','11:20','11:25','11:30','11:35','11:40','11:45','11:50','11:55','11','11:00','11:05','11:10','11:15','11:20','11:25','11:30','11:35','11:40','11:45','11:50','11:55','12','12:00','12:05','12:10','12:15','12:20','12:25','12:30','12:35','12:40','12:45','12:50','12:55','13','13:00','13:05','13:10','13:15','13:20','13:25','13:30','13:35','13:40','13:45','13:50','13:55') THEN 'MAÃANA'
    --MAÃANA-TARDE
    WHEN et.he_split1 IN ('6','6:00','6:05','6:10','6:15','6:20','6:25','6:30','6:35','6:40','6:45','6:50','6:55','06','06:00','06:15','06:20','06:25','06:30','06:35','06:40','06:45','06:50','06:55','7','7:00','7:05','7:10','7:15','7:20','7:25','7:30','7:35','7:40','7:45','7:50','7:55','07','07:00','07:05','07:10','07:15','07:20','07:25','07:30','07:35','07:40','07:45','07:50','07:55','8','8:00','8:05','8:10','8:15','8:20','8:25','8:30','8:35','8:40','8:45','8:50','8:55','08','08:00','08:05','08:10','08:15','08:20','08:25','08:30','08:35','08:40','08:45','08:50','08:55','9','9:00','9:05','9:10','9:15','9:20','9:25','9:30','9:35','9:40','9:45','9:50','9:55','09','09:00','09:05','09:10','09:15','09:20','09:25','09:30','09:35','09:40','09:45','09:50','09:55','10','10:00','10:05','10:10','10:15','10:20','10:25','10:30','10:35','10:40','10:45','10:50','10:55','11','11:00','11:05','11:10','11:15','11:20','11:25','11:30','11:35','11:40','11:45','11:50','11:55','12','12:00','12:05','12:10','12:15','12:20','12:25','12:30','12:35','12:40','12:45','12:50','12:55','13','13:00','13:05','13:10','13:15','13:20','13:25','13:30','13:35','13:40','13:45','13:50','13:55') AND et.he_split2 IN ('14','14:00','14:05','14:10','14:15','14:20','14:25','14:30','14:35','14:40','14:45','14:50','14:55','15','15:00','15:05','15:10','15:15','15:20','15:25','15:30','15:35','15:40','15:45','15:50','15:55','16','16:00','16:05','16:10','16:15','16:20','16:25','16:30','16:35','16:40','16:45','16:50','16:55','17','17:00','17:05','17:10','17:15','17:20','17:25','17:30','17:35','17:40','17.45','17:45','17:50','17:55','18','18:00','18:05','18:10','18:15','18:20','18:25','18:30','18:35','18:40','18:45','18:50','18:55','19','19:00','19:05','19:10','19:15','19:20','19:25','19:30','19:35','19:40','19:45','19:50','19:55','20','20:00','20:05','20:10','20:15','20:20','20:25','20:30','20:35','20:40','20:45','20:50','20:55','21','21:00','21:05','21:10','21:15','21:20','21:25','21:30','21:35','21:40','21:45','21:50','21:55','22','22:00') THEN 'MAÃANA-TARDE'
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
et.organizacion_empleadora_cuit
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
et2.organizacion_empleadora_cuit
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
    --MAÃANA
    WHEN et3.he_limpio IN ('6','6:00','6:05','6:10','6:15','6:20','6:25','6:30','6:35','6:40','6:45','6:50','6:55','06','06:00','06:15','06:20','06:25','06:30','06:35','06:40','06:45','06:50','06:55','7','7:00','7:05','7:10','7:15','7:20','7:25','7:30','7:35','7:40','7:45','7:50','7:55','07','07:00','07:05','07:10','07:15','07:20','07:25','07:30','07:35','07:40','07:45','07:50','07:55','8','8:00','8:05','8:10','8:15','8:20','8:25','8:30','8:35','8:40','8:45','8:50','8:55','08','08:00','08:05','08:10','08:15','08:20','08:25','08:30','08:35','08:40','08:45','08:50','08:55','9','9:00','9:05','9:10','9:15','9:20','9:25','9:30','9:35','9:40','9:45','9:50','9:55','09','09:00','09:05','09:10','09:15','09:20','09:25','09:30','09:35','09:40','09:45','09:50','09:55','10','10:00','10:05','10:10','10:15','10:20','10:25','10:30','10:35','10:40','10:45','10:50','10:55','11','11:00','11:05','11:10','11:15','11:20','11:25','11:30','11:35','11:40','11:45','11:50','11:55','11','11:00','11:05','11:10','11:15','11:20','11:25','11:30','11:35','11:40','11:45','11:50','11:55','12','12:00','12:05','12:10','12:15','12:20','12:25','12:30','12:35','12:40','12:45','12:50','12:55','13','13:00','13:05','13:10','13:15','13:20','13:25','13:30','13:35','13:40','13:45','13:50','13:55') OR et3.hs_limpio IN ('6','6:00','6:05','6:10','6:15','6:20','6:25','6:30','6:35','6:40','6:45','6:50','6:55','06','06:00','06:15','06:20','06:25','06:30','06:35','06:40','06:45','06:50','06:55','7','7:00','7:05','7:10','7:15','7:20','7:25','7:30','7:35','7:40','7:45','7:50','7:55','07','07:00','07:05','07:10','07:15','07:20','07:25','07:30','07:35','07:40','07:45','07:50','07:55','8','8:00','8:05','8:10','8:15','8:20','8:25','8:30','8:35','8:40','8:45','8:50','8:55','08','08:00','08:05','08:10','08:15','08:20','08:25','08:30','08:35','08:40','08:45','08:50','08:55','9','9:00','9:05','9:10','9:15','9:20','9:25','9:30','9:35','9:40','9:45','9:50','9:55','09','09:00','09:05','09:10','09:15','09:20','09:25','09:30','09:35','09:40','09:45','09:50','09:55','10','10:00','10:05','10:10','10:15','10:20','10:25','10:30','10:35','10:40','10:45','10:50','10:55','11','11:00','11:05','11:10','11:15','11:20','11:25','11:30','11:35','11:40','11:45','11:50','11:55','11','11:00','11:05','11:10','11:15','11:20','11:25','11:30','11:35','11:40','11:45','11:50','11:55','12','12:00','12:05','12:10','12:15','12:20','12:25','12:30','12:35','12:40','12:45','12:50','12:55','13','13:00','13:05','13:10','13:15','13:20','13:25','13:30','13:35','13:40','13:45','13:50','13:55') THEN 'MAÃANA'
    --MAÃANA-TARDE
    WHEN et3.he_limpio LIKE '%A%'AND regexp_extract(et3.he_limpio,'([0-9\:?]+)(\s?A\s?)([0-9\:?]+)',1) IN ('6','6:00','6:05','6:10','6:15','6:20','6:25','6:30','6:35','6:40','6:45','6:50','6:55','06','06:00','06:15','06:20','06:25','06:30','06:35','06:40','06:45','06:50','06:55','7','7:00','7:05','7:10','7:15','7:20','7:25','7:30','7:35','7:40','7:45','7:50','7:55','07','07:00','07:05','07:10','07:15','07:20','07:25','07:30','07:35','07:40','07:45','07:50','07:55','8','8:00','8:05','8:10','8:15','8:20','8:25','8:30','8:35','8:40','8:45','8:50','8:55','08','08:00','08:05','08:10','08:15','08:20','08:25','08:30','08:35','08:40','08:45','08:50','08:55','9','9:00','9:05','9:10','9:15','9:20','9:25','9:30','9:35','9:40','9:45','9:50','9:55','09','09:00','09:05','09:10','09:15','09:20','09:25','09:30','09:35','09:40','09:45','09:50','09:55','10','10:00','10:05','10:10','10:15','10:20','10:25','10:30','10:35','10:40','10:45','10:50','10:55','11','11:00','11:05','11:10','11:15','11:20','11:25','11:30','11:35','11:40','11:45','11:50','11:55','11','11:00','11:05','11:10','11:15','11:20','11:25','11:30','11:35','11:40','11:45','11:50','11:55','12','12:00','12:05','12:10','12:15','12:20','12:25','12:30','12:35','12:40','12:45','12:50','12:55','13','13:00','13:05','13:10','13:15','13:20','13:25','13:30','13:35','13:40','13:45','13:50','13:55') AND regexp_extract(et3.he_limpio,'([0-9\:?]+)(\s?A\s?)([0-9\:?]+)',1) IN ('6','6:00','6:05','6:10','6:15','6:20','6:25','6:30','6:35','6:40','6:45','6:50','6:55','06','06:00','06:15','06:20','06:25','06:30','06:35','06:40','06:45','06:50','06:55','7','7:00','7:05','7:10','7:15','7:20','7:25','7:30','7:35','7:40','7:45','7:50','7:55','07','07:00','07:05','07:10','07:15','07:20','07:25','07:30','07:35','07:40','07:45','07:50','07:55','8','8:00','8:05','8:10','8:15','8:20','8:25','8:30','8:35','8:40','8:45','8:50','8:55','08','08:00','08:05','08:10','08:15','08:20','08:25','08:30','08:35','08:40','08:45','08:50','08:55','9','9:00','9:05','9:10','9:15','9:20','9:25','9:30','9:35','9:40','9:45','9:50','9:55','09','09:00','09:05','09:10','09:15','09:20','09:25','09:30','09:35','09:40','09:45','09:50','09:55','10','10:00','10:05','10:10','10:15','10:20','10:25','10:30','10:35','10:40','10:45','10:50','10:55','11','11:00','11:05','11:10','11:15','11:20','11:25','11:30','11:35','11:40','11:45','11:50','11:55','11','11:00','11:05','11:10','11:15','11:20','11:25','11:30','11:35','11:40','11:45','11:50','11:55','12','12:00','12:05','12:10','12:15','12:20','12:25','12:30','12:35','12:40','12:45','12:50','12:55','13','13:00','13:05','13:10','13:15','13:20','13:25','13:30','13:35','13:40','13:45','13:50','13:55') OR regexp_extract(et3.hs_limpio,'([0-9\:?]+)(\s?A\s?)([0-9\:?]+)',1) IN ('14','14:00','14:05','14:10','14:15','14:20','14:25','14:30','14:35','14:40','14:45','14:50','14:55','15','15:00','15:05','15:10','15:15','15:20','15:25','15:30','15:35','15:40','15:45','15:50','15:55','16','16:00','16:05','16:10','16:15','16:20','16:25','16:30','16:35','16:40','16:45','16:50','16:55','17','17:00','17:05','17:10','17:15','17:20','17:25','17:30','17:35','17:40','17:45','17:50','17:55','18','18:00','18:05','18:10','18:15','18:20','18:25','18:30','18:35','18:40','18:45','18:50','18:55','19','19:00','19:05','19:10','19:15','19:20','19:25','19:30','19:35','19:40','19:45','19:50','19:55','20','20:00','20:05','20:10','20:15','20:20','20:25','20:30','20:35','20:40','20:45','20:50','20:55','21','21:00','21:05','21:10','21:15','21:20','21:25','21:30','21:35','21:40','21:45','21:50','21:55') OR regexp_extract(et3.hs_limpio,'([0-9\:?]+)(\s?A\s?)([0-9\:?]+)',2) IN ('14','14:00','14:05','14:10','14:15','14:20','14:25','14:30','14:35','14:40','14:45','14:50','14:55','15','15:00','15:05','15:10','15:15','15:20','15:25','15:30','15:35','15:40','15:45','15:50','15:55','16','16:00','16:05','16:10','16:15','16:20','16:25','16:30','16:35','16:40','16:45','16:50','16:55','17','17:00','17:05','17:10','17:15','17:20','17:25','17:30','17:35','17:40','17:45','17:50','17:55','18','18:00','18:05','18:10','18:15','18:20','18:25','18:30','18:35','18:40','18:45','18:50','18:55','19','19:00','19:05','19:10','19:15','19:20','19:25','19:30','19:35','19:40','19:45','19:50','19:55','20','20:00','20:05','20:10','20:15','20:20','20:25','20:30','20:35','20:40','20:45','20:50','20:55','21','21:00','21:05','21:10','21:15','21:20','21:25','21:30','21:35','21:40','21:45','21:50','21:55','22','22:00') THEN 'MAÃANA-TARDE'
    WHEN et3.he_limpio NOT LIKE '%A%' AND et3.he_limpio IN ('6','6:00','6:05','6:10','6:15','6:20','6:25','6:30','6:35','6:40','6:45','6:50','6:55','06','06:00','06:15','06:20','06:25','06:30','06:35','06:40','06:45','06:50','06:55','7','7:00','7:05','7:10','7:15','7:20','7:25','7:30','7:35','7:40','7:45','7:50','7:55','07','07:00','07:05','07:10','07:15','07:20','07:25','07:30','07:35','07:40','07:45','07:50','07:55','8','8:00','8:05','8:10','8:15','8:20','8:25','8:30','8:35','8:40','8:45','8:50','8:55','08','08:00','08:05','08:10','08:15','08:20','08:25','08:30','08:35','08:40','08:45','08:50','08:55','9','9:00','9:05','9:10','9:15','9:20','9:25','9:30','9:35','9:40','9:45','9:50','9:55','09','09:00','09:05','09:10','09:15','09:20','09:25','09:30','09:35','09:40','09:45','09:50','09:55','10','10:00','10:05','10:10','10:15','10:20','10:25','10:30','10:35','10:40','10:45','10:50','10:55','11','11:00','11:05','11:10','11:15','11:20','11:25','11:30','11:35','11:40','11:45','11:50','11:55','12','12:00','12:05','12:10','12:15','12:20','12:25','12:30','12:35','12:40','12:45','12:50','12:55','13','13:00','13:05','13:10','13:15','13:20','13:25','13:30','13:35','13:40','13:45','13:50','13:55') OR et3.hs_limpio IN ('14','14:00','14:05','14:10','14:15','14:20','14:25','14:30','14:35','14:40','14:45','14:50','14:55','15','15:00','15:05','15:10','15:15','15:20','15:25','15:30','15:35','15:40','15:45','15:50','15:55','16','16:00','16:05','16:10','16:15','16:20','16:25','16:30','16:35','16:40','16:45','16:50','16:55','17','17:00','17:05','17:10','17:15','17:20','17:25','17:30','17:35','17:40','17.45','17:45','17:50','17:55','18','18:00','18:05','18:10','18:15','18:20','18:25','18:30','18:35','18:40','18:45','18:50','18:55','19','19:00','19:05','19:10','19:15','19:20','19:25','19:30','19:35','19:40','19:45','19:50','19:55','20','20:00','20:05','20:10','20:15','20:20','20:25','20:30','20:35','20:40','20:45','20:50','20:55','21','21:00','21:05','21:10','21:15','21:20','21:25','21:30','21:35','21:40','21:45','21:50','21:55','22','22:00') THEN 'MAÃANA-TARDE'
            --TARDE
    WHEN et3.he_limpio IN ('14','14:00','14:05','14:10','14:15','14:20','14:25','14:30','14:35','14:40','14:45','14:50','14:55','15','15:00','15:05','15:10','15:15','15:20','15:25','15:30','15:35','15:40','15:45','15:50','15:55','16','16:00','16:05','16:10','16:15','16:20','16:25','16:30','16:35','16:40','16:45','16:50','16:55','17','17:00','17:05','17:10','17:15','17:20','17:25','17:30','17:35','17:40','17:45','17:50','17:55','18','18:00','18:05','18:10','18:15','18:20','18:25','18:30','18:35','18:40','18:45','18:50','18:55','19','19:00','19:05','19:10','19:15','19:20','19:25','19:30','19:35','19:40','19:45','19:50','19:55','20','20:00','20:05','20:10','20:15','20:20','20:25','20:30','20:35','20:40','20:45','20:50','20:55','21','21:00','21:05','21:10','21:15','21:20','21:25','21:30','21:35','21:40','21:45','21:50','21:55') OR et3.hs_limpio IN ('14','14:00','14:05','14:10','14:15','14:20','14:25','14:30','14:35','14:40','14:45','14:50','14:55','15','15:00','15:05','15:10','15:15','15:20','15:25','15:30','15:35','15:40','15:45','15:50','15:55','16','16:00','16:05','16:10','16:15','16:20','16:25','16:30','16:35','16:40','16:45','16:50','16:55','17','17:00','17:05','17:10','17:15','17:20','17:25','17:30','17:35','17:40','17:45','17:50','17:55','18','18:00','18:05','18:10','18:15','18:20','18:25','18:30','18:35','18:40','18:45','18:50','18:55','19','19:00','19:05','19:10','19:15','19:20','19:25','19:30','19:35','19:40','19:45','19:50','19:55','20','20:00','20:05','20:10','20:15','20:20','20:25','20:30','20:35','20:40','20:45','20:50','20:55','21','21:00','21:05','21:10','21:15','21:20','21:25','21:30','21:35','21:40','21:45','21:50','21:55','22','22:00') THEN 'TARDE'
    --TARDE-NOCHE
    WHEN et3.he_limpio IN ('14','14:00','14:05','14:10','14:15','14:20','14:25','14:30','14:35','14:40','14:45','14:50','14:55','15','15:00','15:05','15:10','15:15','15:20','15:25','15:30','15:35','15:40','15:45','15:50','15:55','16','16:00','16:05','16:10','16:15','16:20','16:25','16:30','16:35','16:40','16:45','16:50','16:55','17','17:00','17:05','17:10','17:15','17:20','17:25','17:30','17:35','17:40','17:45','17:50','17:55','18','18:00','18:05','18:10','18:15','18:20','18:25','18:30','18:35','18:40','18:45','18:50','18:55','19','19:00','19:05','19:10','19:15','19:20','19:25','19:30','19:35','19:40','19:45','19:50','19:55','20','20:00','20:05','20:10','20:15','20:20','20:25','20:30','20:35','20:40','20:45','20:50','20:55','21','21:00','21:05','21:10','21:15','21:20','21:25','21:30','21:35','21:40','21:45','21:50','21:55') OR et3.hs_limpio IN ('22:05','22:10','22:15','22:30','22:35','22:40','22:45','22:50','22:55','23','23:00','23:05','23:10','23:15','23:20','23:25','23:30','23:35','23:40','23:45','23:50','23:55','24','00:00','00:05','00:10','00:15','00:20','00:25','00:30','00:35','00:40','00:45','00:50','00:55','1','1:00','1:05','1:10','1:15','1:20','1:25','1:30','1:35','1:40','1:45','1:50','1:55','01','01:00','01:05','01:10','01:15','01:20','01:25','01:30','01:35','01:40','01:45','01:50','01:55','2','2:00','2:05','2:10','2:15','2:20','2:25','2:30','2:35','2:40','2:45','2:50','2:55','02','02:00','02:05','02:10','02:15','02:20','02:25','02:30','02:35','02:40','02:45','02:50','02:55','3','3:00','3:05','3:10','3:15','3:20','3:25','3:30','3:35','3:40','3:45','3:50','3:55','03','03:00','03:05','03:10','03:15','03:20','03:25','03:30','03:35','03:40','03:45','03:50','03:55','4','4:00','4:05','4:10','4:15','4:20','4:25','4:30','4:35','4:40','4:45','4:50','4:55','04','04:00','04:05','04:10','04:15','04:20','04:25','04:30','04:35','04:40','04:45','04:50','04:55','5','5:00','5:05','5:10','5:15','5:20','5:25','5:30','5:35','5:40','5:45','5:50','5:55','05','05:00','05:05','05:10','05:15','05:20','05:25','05:30','05:35','05:40','05:45','05:50','05:55','6','6:00') THEN 'TARDE-NOCHE'
    --NOCHE
    WHEN et3.he_limpio IN ('22','22:00','22:05','22:10','22:15','22:30','22:35','22:40','22:45','22:50','22:55','23','23:00','23:05','23:10','23:15','23:20','23:25','23:30','23:35','23:40','23:45','23:50','23:55','24','00:00','00:05','00:10','00:15','00:20','00:25','00:30','00:35','00:40','00:45','00:50','00:55','1','1:00','1:05','1:10','1:15','1:20','1:25','1:30','1:35','1:40','1:45','1:50','1:55','01','01:00','01:05','01:10','01:15','01:20','01:25','01:30','01:35','01:40','01:45','01:50','01:55','2','2:00','2:05','2:10','2:15','2:20','2:25','2:30','2:35','2:40','2:45','2:50','2:55','02','02:00','02:05','02:10','02:15','02:20','02:25','02:30','02:35','02:40','02:45','02:50','02:55','3','3:00','3:05','3:10','3:15','3:20','3:25','3:30','3:35','3:40','3:45','3:50','3:55','03','03:00','03:05','03:10','03:15','03:20','03:25','03:30','03:35','03:40','03:45','03:50','03:55','4','4:00','4:05','4:10','4:15','4:20','4:25','4:30','4:35','4:40','4:45','4:50','4:55','04','04:00','04:05','04:10','04:15','04:20','04:25','04:30','04:35','04:40','04:45','04:50','04:55','5','5:00','5:05','5:10','5:15','5:20','5:25','5:30','5:35','5:40','5:45','5:50','5:55','05','05:00','05:05','05:10','05:15','05:20','05:25','05:30','05:35','05:40','05:45','05:50','05:55') OR et3.hs_limpio IN ('22','22:00','22:05','22:10','22:15','22:30','22:35','22:40','22:45','22:50','22:55','23','23:00','23:05','23:10','23:15','23:20','23:25','23:30','23:35','23:40','23:45','23:50','23:55','24','00:00','00:05','00:10','00:15','00:20','00:25','00:30','00:35','00:40','00:45','00:50','00:55','1','1:00','1:05','1:10','1:15','1:20','1:25','1:30','1:35','1:40','1:45','1:50','1:55','01','01:00','01:05','01:10','01:15','01:20','01:25','01:30','01:35','01:40','01:45','01:50','01:55','2','2:00','2:05','2:10','2:15','2:20','2:25','2:30','2:35','2:40','2:45','2:50','2:55','02','02:00','02:05','02:10','02:15','02:20','02:25','02:30','02:35','02:40','02:45','02:50','02:55','3','3:00','3:05','3:10','3:15','3:20','3:25','3:30','3:35','3:40','3:45','3:50','3:55','03','03:00','03:05','03:10','03:15','03:20','03:25','03:30','03:35','03:40','03:45','03:50','03:55','4','4:00','4:05','4:10','4:15','4:20','4:25','4:30','4:35','4:40','4:45','4:50','4:55','04','04:00','04:05','04:10','04:15','04:20','04:25','04:30','04:35','04:40','04:45','04:50','04:55','5','5:00','5:05','5:10','5:15','5:20','5:25','5:30','5:35','5:40','5:45','5:50','5:55','05','05:00','05:05','05:10','05:15','05:20','05:25','05:30','05:35','05:40','05:45','05:50','05:55','6','6:00') THEN 'NOCHE'
    --ARREGLOS ADICIONALES: SE ARREGLAN LOS CASOS QUE QUEDARÃN FUERA DE LA CATEGORIZACION DE TURNOS. AL SER MUY POCOS CASOS LOS MISMO SE ARREGLAN DE FORMA MANUAL
    --MAÃANA-TARDE-NOCHE
    WHEN et3.horario_entrada IN ('Disponibilidad para realizar turnos rotativos 6:00 a 14:00 / 14:00 a 22:00 / 22:00 a 06:00 hs.','Turno tarde: 12a 16 hs/ Turno noche: 19 a 23 hs') THEN 'MAÃANA-TARDE-NOCHE'
    --MAÃANA
    WHEN horario_entrada LIKE '%10.00%' AND horario_salida LIKE '%13.30%' THEN 'MAÃANA'
    --MAÃANA-TARDE
    WHEN UPPER(horario_entrada) LIKE '09:00 O 11:00' AND UPPER(horario_salida) LIKE '16:00 O 18:00' THEN 'MAÃANA-TARDE'
    WHEN et3.horario_entrada IN ('7/13 - 12/17 - 17/22','"Los turnos de trabajo son de lunes a viernes:  + turno maÃ±ana:7.00 a 13.00 + turno tarde:12.30 a 17.30 + turno noche:17.00 a 22.00hs  + sÃ¡bados, domingos y feriados: 7.30 a 17.30hs"') THEN 'MAÃANA-TARDE'
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
et3.organizacion_empleadora_calle,
et3.organizacion_empleadora_piso,
et3.organizacion_empleadora_depto,
et3.organizacion_empleadora_cp,
et3.organizacion_empleadora_barrio,
et3.organizacion_empleadora_cuit
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
    WHEN et4.horario_entrada_origen LIKE '%Los turnos de trabajo son de lunes a viernes:  + turno maÃ±ana:7.00 a 13.00 + turno tarde:12.30 a 17.30 + turno noche:17.00 a 22.00hs  + sÃ¡bados, domingos y feriados: 7.30 a 17.30hs%' THEN '7:00 A 13:00 / 12:30 A 17:30 / 17:00 A 22:00'
    WHEN et4.turno_trabajo LIKE 'SIN TURNO ESPECIFICO' THEN ''
    WHEN et4.horario_entrada LIKE ':700 ' THEN ''
    WHEN et4.horario_entrada LIKE 'Turnos de 6 hs (maÃ±ana y tarde)' THEN ''
    WHEN regexp_like(et4.horario_entrada,'[^0-9A\:?\s?]') THEN regexp_replace(et4.horario_entrada,'[^0-9A\:?\s?\/?]','')
    ELSE et4.horario_entrada
END horario_entrada,
CASE
    WHEN et4.horario_salida_origen LIKE '%Los turnos de trabajo son de lunes a viernes:  + turno maÃ±ana:7.00 a 13.00 + turno tarde:12.30 a 17.30 + turno noche:17.00 a 22.00hs  + sÃ¡bados, domingos y feriados: 7.30 a 17.30hs%' THEN '7:00 A 13:00 / 12:30 A 17:30 / 17:00 A 22:00'
    WHEN et4.turno_trabajo LIKE 'SIN TURNO ESPECIFICO' THEN ''
    WHEN et4.horario_salida LIKE '1300' THEN ''
    WHEN et4.horario_salida LIKE 'Turnos de 6 hs (maÃ±ana y tarde)' THEN ''
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
et4.organizacion_empleadora,
et4.organizacion_empleadora_calle,
et4.organizacion_empleadora_piso,
et4.organizacion_empleadora_depto,
et4.organizacion_empleadora_cp,
et4.organizacion_empleadora_barrio,
et4.organizacion_empleadora_cuit
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
et5.organizacion_empleadora_cuit
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
et6.organizacion_empleadora_cuit
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
    WHEN regexp_like(UPPER(etf.modalidad_de_trabajo),'RELACIÃN DE DEPENDENCIA') THEN 'RELACION DE DEPENDENCIA'
    WHEN regexp_like(UPPER(etf.modalidad_de_trabajo),'PERIODO DE PRUEBA|6 MESES|POR HORA|POR CONTRATO|TEMPORAL|MONOTRIBUTO|MONOTRIBUTISTA') THEN 'CONTRATO'
    WHEN regexp_like(UPPER(etf.modalidad_de_trabajo),'PASANTÃA') THEN 'PASANTIA'
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
    WHEN regexp_like(UPPER(etf.grado_de_estudio),'UNIVERSITARIO|UNIVERSITARIOS|POSTGRADO|DOCTORADO|CARRERAS|ING|CS|PROFESIONAL|CIENCIAS|GRADUADO|ACADÃMICA') AND UPPER(etf.grado_de_estudio) NOT LIKE '%NINGUNO%' THEN 'UNIVERSITARIO'
    WHEN regexp_like(UPPER(etf.grado_de_estudio),'TERCIARIO|TERCIARIOS') THEN 'TERCIARIO'
    WHEN LENGTH(etf.grado_de_estudio) = 0 THEN ''
    ELSE 'OTROS'
END grado_de_estudio,
etf.duracion_practica_formativa,
etf.sector_productivo AS sector_productivo_origen,
CASE
    WHEN regexp_like(etf.sector_productivo,'GastronomÃ­a, HotelerÃ­a y Turismo|Camareros') OR regexp_like(etf.descripcion,'Camarero/a|Cocinero/a|Bachero/a') THEN 'GASTRONOMIA, HOTELERIA Y TURISMO'
    WHEN LENGTH(etf.sector_productivo) = 0 OR LENGTH(etf.industria) > 0 THEN UPPER(etf.industria)
    WHEN regexp_like(etf.sector_productivo,'Abastecimiento y LogÃ­stica|AlmacÃ©n / DepÃ³sito / ExpediciÃ³n') THEN 'ABASTECIMIENTO Y LOGISTICA'
    WHEN regexp_like(etf.sector_productivo,'Contabilidad|Secretarias y RecepciÃ³n|AdministraciÃ³n, Contabilidad y Finanzas|Gerencia y DirecciÃ³n General') THEN 'ADMINISTRACION, CONTABILIDAD Y FINANZAS'
    WHEN regexp_like(etf.sector_productivo,'Recursos Humanos y CapacitaciÃ³n') THEN 'RECURSOS HUMANOS Y CAPACITACION'
    WHEN regexp_like(etf.sector_productivo,'Marketing y Publicidad|Ventas|Comercial, Ventas y Negocios|Comercial') THEN 'COMERCIAL, VENTAS Y NEGOCIOS'
    WHEN regexp_like(etf.sector_productivo,'Aduana y Comercio Exterior') THEN 'ADUANA Y COMERCIO EXTERIOR'
    WHEN regexp_like(etf.sector_productivo,'AtenciÃ³n al Cliente, Call Center y Telemarketing') THEN 'ATENCION AL CLIENTE, CALL CENTER Y TELEMARKETING'
    WHEN regexp_like(etf.sector_productivo,'ComunicaciÃ³n, Relaciones Institucionales y PÃºblicas')  THEN 'COMUNICACION, RELACIONES INSTITUCIONALES Y PUBLICAS'
    WHEN regexp_like(etf.sector_productivo,'DiseÃ±o') THEN 'DISEÃO'
    WHEN regexp_like(etf.sector_productivo,'EducaciÃ³n, Docencia e InvestigaciÃ³n') THEN 'EDUCACION, DOCENCIA E INVESTIGACION'
    WHEN regexp_like(etf.sector_productivo,'HipÃ³dromo de Palermo') THEN 'LIMPIEZA Y MANTENIMIENTO (SIN EDIFICIOS)'
    WHEN regexp_like(etf.sector_productivo,'IngenierÃ­a Civil, Arquitectura y ConstrucciÃ³n') THEN 'INGENIERIA CIVIL, ARQUITECTURA Y CONSTRUCCION'
    WHEN regexp_like(etf.sector_productivo,'Legales/AbogacÃ­a') THEN 'LEGALES/ABOGACIA'
    WHEN regexp_like(etf.sector_productivo,'MinerÃ­a, EnergÃ­a, PetrÃ³leo y Gas') THEN 'MINERIA, ENERGIA, PETROLEO, AGUA Y GAS'
    WHEN regexp_like(etf.sector_productivo,'Oficios y Otros') THEN 'OFICIOS Y OTROS'
    WHEN regexp_like(etf.sector_productivo,'ProducciÃ³n y Manufactura') THEN 'PRODUCCION Y MANUFACTURA (SIN TEXTIL, ELECTRONICA Y AUTOMOTRIZ)'
    WHEN regexp_like(etf.sector_productivo,'INGENIERIAS') THEN 'PRODUCCION Y MANUFACTURA (SIN TEXTIL, ELECTRONICA Y AUTOMOTRIZ)'
    WHEN regexp_like(etf.sector_productivo,'Salud, Medicina y Farmacia') THEN 'SALUD, MEDICINA, FARMACIA Y ASISTENCIA SOCIAL'
    WHEN regexp_like(etf.sector_productivo,'AdministraciÃ³n PÃºblica|Postas de vacunacion - CABA') THEN 'SECTOR PUBLICO'
    WHEN regexp_like(etf.sector_productivo,'Seguros') THEN 'SEGUROS'
    WHEN regexp_like(etf.sector_productivo,'TecnologÃ­a, Sistemas y Telecomunicaciones') THEN 'TECNOLOGIA, SISTEMAS Y TELECOMUNICACIONES'
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
etf.organizacion_empleadora_cuit
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
    WHEN ecr1.base_origen LIKE 'CRMEMPLEO' THEN 'IMPULSO A LA INSERCION LABORAL'
    WHEN ecr1.base_origen LIKE 'CRMSL' THEN 'ACTIVA TU POTENCIAL LABORAL'
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
    WHEN ecr1.edad_minima_origen LIKE '% a %' AND  regexp_like(ecr1.edad_minima_origen,'[a-zA-ZÃ±Ã]+\:?\s?[0-9]+\s?a\s?[0-9]+\s?[a-zA-ZÃ±Ã]+') THEN regexp_extract(ecr1.edad_minima_origen,'([a-zA-ZÃ±Ã]+\:?\s?)([0-9]+)(\s?a\s?)([0-9]+)(\s?[a-zA-ZÃ±Ã]+)',2)
    WHEN ecr1.edad_minima_origen LIKE '%/%' AND regexp_like(ecr1.edad_minima_origen,'[a-zA-ZÃ±Ã]+\:?\s?[0-9]+\s?a\s?[0-9]+\s?[a-zA-ZÃ±Ã]+') THEN regexp_extract(ecr1.edad_minima_origen,'([a-zA-ZÃ±Ã]+\:?\s?)([0-9]+)\/([0-9]+\s?)([a-zA-ZÃ±Ã]+)',2)
    WHEN regexp_like(UPPER(ecr1.edad_minima_origen),'DE?\s?[0-9]+\s?A\s?[0-9]+\s?AÃOS?') THEN regexp_extract(UPPER(ecr1.edad_minima_origen),'(DE?\s?)([0-9]+)(\s?A\s?)([0-9]+)(\s?AÃOS?)',2)
    WHEN LENGTH(ecr1.edad_minima) <> 2 OR ecr1.edad_minima IN ('10','11','12','13','14','15','16','17') THEN ''
    ELSE ecr1.edad_minima
END edad_minima,
ecr1.edad_maxima_origen,
CASE
    WHEN ecr1.edad_maxima_origen LIKE '% a %' AND  regexp_like(ecr1.edad_maxima_origen,'[a-zA-ZÃ±Ã]+\:?\s?[0-9]+\s?a\s?[0-9]+\s?[a-zA-ZÃ±Ã]+') THEN regexp_extract(ecr1.edad_maxima_origen,'([a-zA-ZÃ±Ã]+\:?\s?)([0-9]+)(\s?a\s?)([0-9]+)(\s?[a-zA-ZÃ±Ã]+)',4)
    WHEN ecr1.edad_maxima_origen LIKE '%/%' AND regexp_like(ecr1.edad_maxima_origen,'[a-zA-ZÃ±Ã]+\:?\s?[0-9]+\s?a\s?[0-9]+\s?[a-zA-ZÃ±Ã]+') THEN regexp_extract(ecr1.edad_maxima_origen,'([a-zA-ZÃ±Ã]+\:?\s?)([0-9]+)\/([0-9]+\s?)([a-zA-ZÃ±Ã]+)',3)
    WHEN regexp_like(UPPER(ecr1.edad_maxima_origen),'DE?\s?[0-9]+\s?A\s?[0-9]+\s?AÃOS?') THEN regexp_extract(UPPER(ecr1.edad_maxima_origen),'(DE?\s?)([0-9]+)(\s?A\s?)([0-9]+)(\s?AÃOS?)',4)
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
REPLACE(REPLACE(ecr1.sector_productivo,'Ã','I'),'Ã','O') AS sector_productivo,
ecr1.industria,
ecr1.enviado_para_aprobacion__c,
REPLACE(ecr1.postulantes_contratados__c,'.0','') AS postulantes_contratados__c,
ecr1.organizacion_empleadora,
ecr1.organizacion_empleadora_calle,
ecr1.organizacion_empleadora_piso,
ecr1.organizacion_empleadora_depto,
ecr1.organizacion_empleadora_cp,
ecr1.organizacion_empleadora_barrio,
ecr1.organizacion_empleadora_cuit
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
ecr2.organizacion_empleadora_cuit
FROM ecr2
)
SELECT *
FROM ecr3
--</sql>--



-- Copy of 2023.05.24 step 20 - consume oportunidad_laboral.sql 



-- 1.-- Crear la tabla def OPORTUNIDAD_LABORAL CRMEMPLEO, CRMSL, PORTALEMPLEO,
-- CAMPOS REQUERIDOS EN TABLA DEF SEGUN MODELO (Oferta Laboral, PrÃ¡cticas Formativas (PasantÃ­as)):
-- CÃ³digo (1+)
-- DescripciÃ³n
-- Estado => ABIERTO, CANCELADO, CERRADO
-- Apto discapacitados => S, N
-- Vacantes
-- Modalidad de Trabajo => RELACION DE DEPENDENCIA, CONTRATIO, PASANTIA, AD HONOREM
-- Edad MÃ­nima
-- Edad MÃ¡xima
-- Vacantes Cubiertas
-- Tipo de Puesto
-- Turno de Trabajo => MAÃANA, MAÃANA-TARDE, MAÃANA-TARDE-NOCHE, TARDE, TARDE-NOCHE, NOCHE
-- Grado de Estudio => SECUNDARIO, TERCIARIO, UNIVERSITARIO, OTROS
-- Duracion Practica formativa
-- Sector Productivo => ABASTECIMIENTO Y LOGISTICA, ADMINISTRACION, CONTABILIDAD Y FINANZAS,ATENCION AL CLIENTE, CALL CENTER Y TELEMARKETING,ADUANA Y COMERCIO EXTERIOR, COMERCIAL, VENTAS Y NEGOCIOS, GASTRONOMIA, HOTELERIA Y TURISMO, INGENIERIAS , LIMPIEZA Y MANTENIMIENTO (SIN EDIFICIOS), OFICIOS Y OTROS, PRODUCCION Y MANUFACTURA (SIN TEXTIL, ELECTRONICA Y AUTOMOTRIZ), SALUD, MEDICINA, FARMACIA Y ASISTENCIA SOCIAL, SECTOR PUBLICO,MINERIA, ENERGIA, PETROLEO, AGUA Y GAS
-- Nota: la tabla deberÃ¡ estar relacionada con la entidad "Registro laboral formal" si tomo el empleo
-- y con la entidad "Programa"

-- --<sql>--DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_def_oportunidad_laboral`;--</sql>--
--<sql>--
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_def_oportunidad_laboral" AS
SELECT
	row_number() OVER () AS oportunidad_laboral_id,
	base_origen,
	id oportunidad_laboral_id_old,
	descripcion,
	fecha_publicacion,
	programa,
	estado,
	apto_discapacitado,
	vacantes,
	modalidad_de_trabajo,
	edad_minima,
	edad_maxima,
	vacantes_cubiertas,
	tipo_de_puesto,
	turno_trabajo,
	grado_de_estudio,
	duracion_practica_formativa,
	sector_productivo
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_oportunidad_laboral"
GROUP BY
	base_origen,
	id,
	descripcion,
	fecha_publicacion,
	programa,
	estado,
	apto_discapacitado,
	vacantes,
	modalidad_de_trabajo,
	edad_minima,
	edad_maxima,
	vacantes_cubiertas,
	tipo_de_puesto,
	turno_trabajo,
	grado_de_estudio,
	duracion_practica_formativa,
	sector_productivo
--</sql>--



-- Copy of 2023.05.24 step 21 - staging registro_laboral_formal.sql 



-- 1.-- Crear REGISTRO LABORAL SIN CRUCE AGIP/AFIP
-- --<sql>--DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_tmp_registro_laboral_formal_1`;--</sql>--
--<sql>--
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_registro_laboral_formal_1" AS
-- CRMSL
SELECT
	vec.broker_id,
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
		WHERE cc.deleted = FALSE
	) crmsl_candidatos
	JOIN "caba-piba-staging-zone-db"."tbp_typ_def_vecino" vec ON (
		vec.genero_broker = crmsl_candidatos.genero_broker
		AND vec.documento_broker = crmsl_candidatos.documento_broker
		AND vec.tipo_doc_broker = crmsl_candidatos.tipo_doc_broker
		AND vec.base_origen = 'CRMSL'
	)
WHERE crmsl_candidatos.id_candidato IS NOT NULL
GROUP BY
vec.broker_id,
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
	vec.broker_id,
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
			-- Campo doc_type : A fin de homogeneizar criterios, se adpotarÃ¡ la tipologÃ­a utilizada para tipos de documentos en las tablas tbp_broker_def_broker_general y tbp_typ_def_vecino
			--Tipos del campo doc_type (origen)
			--DE: Documento extranjero
			--CRP: Cdelua de Residencia Precaria
			--CI: CÃ©dula de Identidad de Capital Federal
			--CUIL: Clave Ãºnica de IdentificaciÃ³n Laboral. Los casos que figuran con tipo de documento CUIL poseen una longitud de numeros de documento que es menor a los 11 digito. Es por ello que se asume este campo se encuentra mal catalogado y que en realidad es el tipo de documento DNI
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
vec.broker_id,
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
--Campo descripcion_modalidad_contratacion. Existen casos con mas de una descripciÃ³n para un mismo codigo de modalidad de contrataciÃ³n. Los mismos provienen de la tabla "afip_agip_tipo_contratacion". Se optÃ³ por elegÃ­r una Ãºnica descripciÃ³n basada estrictamente en la descripciÃ³n que figura en la tabla de modalidades de contrataciÃ³n proveniente de la web de AFIP
-- --<sql>--DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_tmp_registro_laboral_formal_afip_agip_ab`;--</sql>--
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
			OR (LENGTH(TRIM(ab.fecha_inicio_de_relacion_laboral)) > 0 AND try_cast(ab.fecha_inicio_de_relacion_laboral AS date) IS NULL)
			THEN CAST(NULL AS DATE)
			ELSE try_cast(ab.fecha_inicio_de_relacion_laboral AS date)
		END fecha_inicio_de_relacion_laboral,

		-- se convierte la fecha a date, si las fechas de inicio o fin son inconsistentes quedan con el valor NULL
		CASE
			WHEN ab.fecha_fin_de_relacion_laboral IN ('9999-12-31','999-12-31','3202-03-09','7202-08-17','4202-02-23','2109-02-25')
			OR (LENGTH(TRIM(ab.fecha_inicio_de_relacion_laboral)) > 0 AND try_cast(ab.fecha_inicio_de_relacion_laboral AS date) IS NULL)
			OR (LENGTH(TRIM(ab.fecha_fin_de_relacion_laboral)) > 0 AND try_cast(ab.fecha_fin_de_relacion_laboral AS date) IS NULL)
			THEN CAST(NULL AS DATE)
			ELSE try_cast(ab.fecha_fin_de_relacion_laboral AS date)
		END fecha_fin_de_relacion_laboral,

		TRY_CAST(ab.codigo_modalidad_de_contratato AS INT) AS codigo_modalidad_contratacion,
		--Campo descripcion_modalidad_contratacion: Existen casos con mas de una descripciÃ³n para un mismo codigo de modalidad de contrataciÃ³n. Los mismos provienen de la tabla "afip_agip_tipo_contratacion". Se optÃ³ por elegÃ­r una Ãºnica descripciÃ³n basada estrictamente en la descripciÃ³n que figura en la tabla de modalidades de contrataciÃ³n proveniente de la web de AFIP
		CASE
			WHEN TRY_CAST(ab.codigo_modalidad_de_contratato AS INT) = 0 THEN 'Contrato Modalidad Promovida. ReducciÃ³n 0%'
			WHEN TRY_CAST(ab.codigo_modalidad_de_contratato AS INT) = 10 THEN 'PasantÃ­as Ley NÂ° 25.165 Decreto NÂ° 340/92'
			WHEN TRY_CAST(ab.codigo_modalidad_de_contratato AS INT) = 27 THEN 'PasantÃ­as Decreto NÂ° 1.227/01' ELSE tc."descripciÃ³n"
		END descripcion_modalidad_contratacion,
		ab.codigo_de_puesto_desempeniado,
		--Campo descripcion_de_puesto_desempeniado: Se asume este campo como input para obtener el cargo de un empleado.
		pd.descripcion AS descripcion_de_puesto_desempeniado,
		ab.codigo_de_movimiento,
		cm."descripciÃ³n" AS descripcion_movimiento,
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
		LEFT JOIN "caba-piba-raw-zone-db".afip_agip_codigos_movimientos cm ON (ab.codigo_de_movimiento = cm."cÃ³digo")
		LEFT JOIN "caba-piba-raw-zone-db".afip_agip_tipo_contratacion tc ON (
			TRY_CAST(ab.codigo_modalidad_de_contratato AS INT) = TRY_CAST(tc."cÃ³digo" AS INT)
		)
		LEFT JOIN "caba-piba-raw-zone-db".afip_agip_situacion_bajas sb ON (
			TRY_CAST(ab.codigo_de_situacion_de_baja AS INT) = TRY_CAST(sb.codigo AS INT)
		)
		LEFT JOIN "caba-piba-staging-zone-db".tbp_typ_tmp_puesto_desempeniado_afip pd ON (ab.codigo_de_puesto_desempeniado = pd.codigo)
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
		--Campo descripcion_modalidad_contratacion: Existen casos que cuentan con este campo nulo. Se puede inferir que aquellos valores que no tengan descripciÃ³n corresponde a la introducciÃ³n de un cÃ³digo de modalidad de contratacion errÃ³neo o con un cÃ³digo que no figura o no se actualizado en la tablas paramÃ©tricas de AFIP AGIP
		aa_ab.descripcion_modalidad_contratacion,
		--Para el campo modalidad_contratacion, las opciones se reducen a cuatro: "RELACIÃN DE DEPENDENCIA", "CONTRATO", "PASANTÃA" y "AD HONÃREM". Para poder realizar esta selecciÃ³n, se utiliza como input el campo "descripcion_modalidad_contratacion", tomando en cuenta las categorÃ­as establecidas por la tabla de AFIP/AGIP ALTAS y BAJAS que se muestran en la tabla paramÃ©trica afip_agip_tipo_contratacion
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
		--Campo descripcion_puesto_desempenado: Existen casos que cuentan con este campo nulo. Se puede inferir que aquellos valores que no tengan descripciÃ³n corresponde a la introducciÃ³n de un cÃ³digo de puesto desempeniado errÃ³neo o con un cÃ³digo que no figura o no se actualizado en la tablas paramÃ©tricas de AFIP AGIP
		aa_ab.descripcion_de_puesto_desempeniado,
		aa_ab.codigo_de_movimiento,
		aa_ab.descripcion_movimiento,
		aa_ab.fecha_de_movimiento AS fecha_de_movimiento_origen,
		aa_ab.codigo_de_actividad,
		--Campo descripction_actividad: Existen casos que cuentan con este campo nulo. Se puede inferir que aquellas actividades que no tengan descripciÃ³n corresponde a la introducciÃ³n de un cÃ³digo de actividad errÃ³neo o con un cÃ³digo que no figura o no se actualizado en la tablas paramÃ©tricas de AFIP AGIP
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
		--fecha_de_movimiento: A fin de evitar duplicidad de casos, se toma el Ãºltimo movimiento registrado en funciÃ³n del numero de documento del empleado y su genero y el CUIT del empleador
		ROW_NUMBER() OVER(
			PARTITION BY abf1.numero_documento,
			abf1.cuit_del_empleador,
			abf1.genero
			ORDER BY abf1.fecha_de_movimiento_origen DESC
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
-- --<sql>--DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_tmp_registro_laboral_formal`;--</sql>--
--<sql>--
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_registro_laboral_formal" AS
--fecha_inicio_de_relacion_laboral: Dado que no hasta el dÃ­a de la fecha no se cuenta con fuentes completas o fidedignas para determinar si un caso efectivamente participo de un proceso de alguna oportunidad laboral y fue contratado y dado de alta en AFIP (CUIT de organizaciones, fecha de contrato laboral) se establece como condicion que la fecha de inicio de la relaciÃ³n laboral no supere los 6 meses de la fecha de aplicaciÃ³n a una vacante
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
-- --<sql>--DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_tmp_registro_laboral_formal_sin_ol`;--</sql>--
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
vec.broker_id,
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
-- --<sql>--DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_tmp_registro_laboral_formal_completa`;--</sql>--
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
		lf.descripcion_de_puesto_desempeniado AS cargo,
		ol.oportunidad_laboral_id,
		lf.broker_id,
		lf.cuit_del_empleador,
		lf.fecha_inicio_de_relacion_laboral AS fecha_inicio,
		lf.fecha_fin_de_relacion_laboral AS fecha_fin,
		lf.modalidad_contratacion AS modalidad_de_trabajo,
		lf.remuneracion_bruta AS remuneracion_moneda_corriente,
		CAST(NULL AS DECIMAL) AS remuneracion_moneda_constante
	FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_registro_laboral_formal"  lf
	LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_oportunidad_laboral"  ol
		ON (lf.base_origen=ol.base_origen AND lf.oportunidad_laboral_id_old=ol.oportunidad_laboral_id_old)
	GROUP BY
		lf.base_origen,
		lf.id_candidato,
		lf.descripcion_de_puesto_desempeniado,
		ol.oportunidad_laboral_id,
		lf.broker_id,
		lf.cuit_del_empleador,
		lf.fecha_inicio_de_relacion_laboral,
		lf.fecha_fin_de_relacion_laboral,
		lf.modalidad_contratacion,
		lf.remuneracion_bruta

	UNION

	SELECT
		'' AS base_origen,
		rf_sin_oportunidad.id_candidato AS registro_laboral_formal_id_old,
		rf_sin_oportunidad.descripcion_de_puesto_desempeniado AS cargo,
		NULL oportunidad_laboral_id,
		rf_sin_oportunidad.broker_id,
		rf_sin_oportunidad.cuit_del_empleador,
		rf_sin_oportunidad.fecha_inicio_de_relacion_laboral AS fecha_inicio,
		rf_sin_oportunidad.fecha_fin_de_relacion_laboral AS fecha_fin,
		rf_sin_oportunidad.modalidad_contratacion AS modalidad_de_trabajo,
		rf_sin_oportunidad.remuneracion_bruta AS remuneracion_moneda_corriente,
		CAST(NULL AS DECIMAL) AS remuneracion_moneda_constante
	FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_registro_laboral_formal_sin_ol"  rf_sin_oportunidad
	GROUP BY
		rf_sin_oportunidad.id_candidato,
		rf_sin_oportunidad.descripcion_de_puesto_desempeniado,
		rf_sin_oportunidad.broker_id,
		rf_sin_oportunidad.cuit_del_empleador,
		rf_sin_oportunidad.fecha_inicio_de_relacion_laboral,
		rf_sin_oportunidad.fecha_fin_de_relacion_laboral,
		rf_sin_oportunidad.modalidad_contratacion,
		rf_sin_oportunidad.remuneracion_bruta
) registros
--</sql>--



-- Copy of 2023.05.24 step 22 - consume sector_productivo.sql 



-- 1.-- Crear tabla def de Sector_Productivo
-- --<sql>--DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_def_sector_productivo`;--</sql>--
--<sql>--
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_def_sector_productivo" AS
--CLAE
WITH clae AS (
SELECT
cod_actividad_afip_o_naes AS codigo_clae,
--se crea el campo sector productivo
CASE
    WHEN TRY_CAST(cod_actividad_afip_o_naes AS INT) IN (530010,492230,492290,492221,492229,492250,492190,492170,492280,491200,502200,501200,492240,493200,493110,493120,524290,524190,523020,522099,524210,524110,522092,523090,521030,521020,521010,530090,523039,801010,524230,351201,492210,492160,492150,492180,492140,492110,492130,491120,491110,502101,501100,524130,492120,309100,301100,301200,522091,524120,309900,302000,524220) THEN 'ABASTECIMIENTO Y LOGISTICA'
    WHEN TRY_CAST(cod_actividad_afip_o_naes AS INT) IN (649100,661131,823000,643009,661999,661910,821100,661992,829100,649910,702099,702092,702091,949990,661920,692000,649290,649220,661991,662010,643001,649999,821900,663000,641943,641941,641942,641920,641910,641930,661121,661111,949910,941100,941200,931010,942000,661930,642000,649991,829900,649210,949930,829200) THEN 'ADMINISTRACION, CONTABILIDAD Y FINANZAS'
    WHEN TRY_CAST(cod_actividad_afip_o_naes AS INT) IN (523019,523031,523032,523011) THEN 'ADUANA Y COMERCIO EXTERIOR'
    WHEN TRY_CAST(cod_actividad_afip_o_naes AS INT) IN (524390,524310,303000,511000,524320,524330,512000) THEN 'AERONAUTICA'
    WHEN TRY_CAST(cod_actividad_afip_o_naes AS INT) IN (462110,462132,461014,13,16292,14910,17010,14930,14990,14810,14300,14121,14113,14430,14440,14221,14211,14410,14420,14520,14510,11501,11111,11329,12510,11129,11119,12800,11911,12200,12320,12319,12490,12420,12410,12600,11291,11331,11341,11342,12311,11121,11299,11310,11130,11912,11509,12590,11211,11400,11321,11112,12709,12121,12110,12701,12900,11990,14920,14115,32000,21030,22010,22020,81100,89300,51000,52000,89200,16210,14114,461031,461032,31200,31110,31120,21010,14820,14710,14610,14620,13020,14720,13019,13013,13011,13012,31130,21020,522020,522010,16190,17020,31300,16299,16130,16220,16120,16230,16119,939010,16140,16150,24020,24010) THEN 'AGRICULTURA, GANADERIA, CAZA Y SERVICIOS CONEXOS'
    WHEN TRY_CAST(cod_actividad_afip_o_naes AS INT) IN (822000) THEN 'ATENCION AL CLIENTE, CALL CENTER Y TELEMARKETING'
    WHEN TRY_CAST(cod_actividad_afip_o_naes AS INT) IN (281100,221901,221110,452700,452101,452800,454020,452990,221120,293011,452401,452220,452210,452600,452500,452300,452910,292000,293090,291000) THEN 'AUTOMOTRIZ'
    WHEN TRY_CAST(cod_actividad_afip_o_naes AS INT) IN (465500,464910,466310,466932,463153,463170,464632,464950,466330,464620,464223,466391,464149,464112,466931,464420,464999,464631,464410,466399,466360,463129,463152,463219,463212,463220,464940,463154,464130,463121,462131,463160,463300,466110,466129,465220,466350,464212,464501,464222,465930,464502,465210,465100,463112,464991,463191,464330,469010,464930,462201,464211,464113,465920,464920,462190,462209,464122,469090,466200,465690,465610,464610,465400,465910,465990,465340,465320,465360,465310,465390,465350,463151,464221,466370,453100,463130,464141,466340,464121,464129,463199,464320,466320,464310,466920,466940,466910,466939,466990,463111,464119,464340,463159,462120,464142,464114,464111,464150,463211,461092,461094,461099,461039,461011,461040,461013,461021,461022,461093,461095,461091,461019,461029,461012,463180,463140,475300,476200,475210,478010,477830,474020,476320,475440,475230,475430,477290,477420,477210,477410,477490,475490,475250,475190,477890,453220,472200,477430,472172,477230,477220,472130,475420,473000,475120,475260,453210,476120,476310,474010,472112,477440,472160,477460,475110,472140,477140,477130,477330,476400,476110,477820,475220,475290,477450,475410,477810,477480,477840,472171,476130,475270,453291,453292,472150,475240,477150,477190,472190,477320,472120,477310,472111,478090,477470,477110,472300,477120,471900,471110,471190,471130,471120,479900,479109,479101,451110,451210,454010,451190,451290,465330,771210,771290,774000,771220,773010,771110,771190,773030,773020,772091,773040,772099,773090,772010,771110) THEN 'COMERCIAL, VENTAS Y NEGOCIOS'
    WHEN TRY_CAST(cod_actividad_afip_o_naes AS INT) IN (741000) THEN 'DISEÃO'
    WHEN TRY_CAST(cod_actividad_afip_o_naes AS INT) IN (749009,853100,854960,854920,854950,854910,854940,851020,854930,852100,852200,853201,8,853300,721030,722020,722010,721090,855000,854990,851010) THEN 'EDUCACION, DOCENCIA E INVESTIGACION'
    WHEN TRY_CAST(cod_actividad_afip_o_naes AS INT) IN (920009,900021,591200,591300,931020,591120,900011,591110,931030,182000,900030,910900,910100,939090,900091,592000,910200,920001,939030,939020,931090,931041,931042,900040) THEN 'ENTRETENIMIENTO, ESPECTACULOS Y ACTIVIDADES CULTURALES'
    WHEN TRY_CAST(cod_actividad_afip_o_naes AS INT) IN (791909,561013,552000,551022,551023,551021,551010,562099,561014,561019,551090,791901,791200,791100,561012,561011,561030,562091,562010,561020,561040) THEN 'GASTRONOMIA, HOTELERIA Y TURISMO'
    WHEN TRY_CAST(cod_actividad_afip_o_naes AS INT) IN (432110,431210,439990,432920,439100,433030,429090,410021,410011,421000,429010,422200,431100,432910,432990,332000,711009,711001,433090,433010,422100,431220,432200,433020,439910) THEN 'INGENIERIA CIVIL, ARQUITECTURA Y CONSTRUCCION'
    WHEN TRY_CAST(cod_actividad_afip_o_naes AS INT) IN (432190) THEN 'INGENIERIAS'
    WHEN TRY_CAST(cod_actividad_afip_o_naes AS INT) IN (691002,691001) THEN 'LEGALES/ABOGACIA'
    WHEN TRY_CAST(cod_actividad_afip_o_naes AS INT) IN (331290,331210,331900,16113,16112,813000,812020,331101,331400,331301,331220,949920,812010,811000,812090) THEN 'LIMPIEZA Y MANTENIMIENTO'
    WHEN TRY_CAST(cod_actividad_afip_o_naes AS INT) IN (732000,731001,731009) THEN 'MARKETING Y PUBLICIDAD'
    WHEN TRY_CAST(cod_actividad_afip_o_naes AS INT) IN (351310,351320,89900,81300,62000,72910,71000,72990,89110,89120,72100,61000,192000,466121,351130,351190,351110,351120,91000,99000,711002,353001,81400,360010,360020,352020,352010) THEN 'MINERIA, ENERGIA, PETROLEO, AGUA Y GAS'
    WHEN TRY_CAST(cod_actividad_afip_o_naes AS INT) IN (801090,9,181109,7,433040,952910,952990,952920,952300,742000,749002,749003,749001,16291,12,11,181200) THEN 'OFICIOS Y OTROS'
    WHEN TRY_CAST(cod_actividad_afip_o_naes AS INT) IN (639100,581300,581900,602100,601000,181101,639900,602320,581100,602310,602200,602900,581200) THEN 'PRENSA Y MEDIOS DE COMUNICACIÃN'
    WHEN TRY_CAST(cod_actividad_afip_o_naes AS INT) IN (110100,239421,108000,242010,110420,110492,107301,239422,239410,110300,120091,107991,101030,107110,110491,239591,104020,107410,107420,102002,107999,107309,107129,120099,105090,105020,110290,107930,105030,107121,103012,107912,110411,81200,221909,281900,201300,201210,252000,239593,239399,259302,323001,162902,170910,170990,239310,162901,309200,321020,201220,281201,259992,251101,329030,170202,259309,281400,202907,201140,281301,202320,201120,231010,259910,222010,329040,329020,202906,203000,201110,162100,281500,322001,202312,321011,324000,239201,329010,282300,282500,282901,281600,281700,282909,201130,201409,201180,201190,210010,210020,239510,310020,310010,282200,239391,321012,170201,170102,170101,202200,202311,239209,239100,162903,210090,162909,231090,259999,259993,251102,239900,222090,242090,202908,162300,201401,239202,110412,310030,251200,259991,282110,162202,241009,231020,259100,243100,243200,329090,241001,109000,204000,952200,104013,104011,101040,104012,102001,103091,103099,103030,103011,105010,101020,101099,101012,101011,101040,101091,106131,106120,106110,106139,106200,107200,107930,107911,107920,107992,110211,110212,202101,282120,282130,161001,161002,103020,162201,282400,239592,259200,251300,239600,210030,107500,191000,259301,282600,120010,102003,101013) THEN 'PRODUCCION Y MANUFACTURA (SIN TEXTIL, ELECTRONICA Y AUTOMOTRIZ)'
    WHEN TRY_CAST(cod_actividad_afip_o_naes AS INT) IN (780000) THEN 'RECURSOS HUMANOS Y CAPACITACION'
    WHEN TRY_CAST(cod_actividad_afip_o_naes AS INT) IN (721020,712000,863300,862110,931050,870100,862130,864000,702010,861020,861010,862120,863110,863190,863120,869010,862200,869090,870990,750000,870210,870910,870220,863200,870920,880000,970000) THEN 'SALUD, MEDICINA, FARMACIA Y ASISTENCIA SOCIAL'
    WHEN TRY_CAST(cod_actividad_afip_o_naes AS INT) IN (641100,842400,10,841900,842100,842200,910300,843000,16111,842500,841100,842300,841300,841200) THEN 'SECTOR PUBLICO'
    WHEN TRY_CAST(cod_actividad_afip_o_naes AS INT) IN (653000,652000,662090,651210,662020,651120,651220,651130,651320,651110,651310) THEN 'SEGUROS'
    WHEN TRY_CAST(cod_actividad_afip_o_naes AS INT) IN (390000,381100,381200,382010,382020,370000) THEN 'SERVICIOS DE MANEJO DE RESIDUOS Y DE REMEDIACION'
    WHEN TRY_CAST(cod_actividad_afip_o_naes AS INT) IN (949200,949100,990000) THEN 'SERVICIOS DE ORGANIZACIONES POLITICAS Y RELIGIOSAS'
    WHEN TRY_CAST(cod_actividad_afip_o_naes AS INT) IN (681020,681010,682099,681099,681098,682091,682010) THEN 'SERVICIOS INMOBILIARIOS Y ALQUILER DE BIENES MUEBLES E INTANGIBLES'
    WHEN TRY_CAST(cod_actividad_afip_o_naes AS INT) IN (960910,960201,960202,960990,960101,960300,960102) THEN 'SERVICIOS PERSONALES (SECTOR A INCORPORAR EN TAXONOMIA DE TYP) POSICION 96 DE LA LETRA S (FUENTE: CLAE AFIP)'
    WHEN TRY_CAST(cod_actividad_afip_o_naes AS INT) IN (279000,262000,264000,263000,721010,631200,952100,951200,951100,620200,620100,620300,620900,611010,614010,801020,619000,613000,614090,611090,612000,711003,631190,631120,631110,271020,271010,266010,266090,265101,265200,267002,265102,268000,261000,267001,266010,266090,272000,274000,275091,275020,273110,275099,275092,273190,275010 ) THEN 'TECNOLOGIA, SISTEMAS Y TELECOMUNICACIONES'
    WHEN TRY_CAST(cod_actividad_afip_o_naes AS INT) IN (131300,141130,141140,141120,141110,141191,139209,139204,131132,131131,131139,143010,143020,139900,131202,131209,131201,149000,142000,131120,131110,139400,139202,139203,139201,139100,139300,151100,141191,141199,141201,141202,152040,152011,152031,152021,151200) THEN 'TEXTIL'
END sector_productivo
FROM "caba-piba-raw-zone-db"."tbp_typ_tmp_nomenclador_actividades_economicas"
WHERE (cod_actividad_afip_o_naes IS NOT NULL OR LENGTH(TRIM(TRY_CAST(cod_actividad_afip_o_naes AS VARCHAR))) <> 0)
GROUP BY 1,2
),
--portal_empleo_mtr_industry_sectors
inds AS (
SELECT
isec.code AS codigo_mtr,
CASE
    WHEN TRY_CAST(isec.code AS INT) IN (1143) THEN 'ABASTECIMIENTO Y LOGISTICA'
    WHEN TRY_CAST(isec.code AS INT) IN (558,1773,701) THEN 'ADMINISTRACION, CONTABILIDAD Y FINANZAS'
    WHEN TRY_CAST(isec.code AS INT) IN (2723) THEN 'ADUANA Y COMERCIO EXTERIOR'
    WHEN TRY_CAST(isec.code AS INT) IN (1174) THEN 'ATENCION AL CLIENTE, CALL CENTER Y TELEMARKETING'
    WHEN TRY_CAST(isec.code AS INT) IN (2888) THEN 'COMERCIAL, VENTAS Y NEGOCIOS'
    WHEN TRY_CAST(isec.code AS INT) IN (16) THEN 'DISEÃO'
    WHEN TRY_CAST(isec.code AS INT) IN (18) THEN 'EDUCACION, DOCENCIA E INVESTIGACION'
    WHEN TRY_CAST(isec.code AS INT) IN (25) THEN  'GASTRONOMIA, HOTELERIA Y TURISMO'
    WHEN TRY_CAST(isec.code AS INT) IN (4) THEN 'INGENIERIA CIVIL, ARQUITECTURA Y CONSTRUCCION'
    WHEN TRY_CAST(isec.code AS INT) IN (648) THEN 'INGENIERIAS'
    WHEN TRY_CAST(isec.code AS INT) IN (601) THEN 'LEGALES/ABOGACIA'
    WHEN TRY_CAST(isec.code AS INT) IN (45) THEN 'MARKETING Y PUBLICIDAD'
    WHEN TRY_CAST(isec.code AS INT) IN (19) THEN 'MINERIA, ENERGIA, PETROLEO, AGUA Y GAS'
    WHEN TRY_CAST(isec.code AS INT) IN (1108) THEN 'OFICIOS Y OTROS'
    WHEN TRY_CAST(isec.code AS INT) IN (734) THEN 'PRODUCCION Y MANUFACTURA (SIN TEXTIL, ELECTRONICA Y AUTOMOTRIZ)'
    WHEN TRY_CAST(isec.code AS INT) IN (590) THEN 'RECURSOS HUMANOS Y CAPACITACION'
    WHEN TRY_CAST(isec.code AS INT) IN (48) THEN 'SALUD, MEDICINA, FARMACIA Y ASISTENCIA SOCIAL'
    WHEN TRY_CAST(isec.code AS INT) IN (2889,2890,2891) THEN 'SECTOR PUBLICO'
    WHEN TRY_CAST(isec.code AS INT) IN (49) THEN 'SEGUROS'
    WHEN TRY_CAST(isec.code AS INT) IN (32) THEN 'TECNOLOGIA, SISTEMAS Y TELECOMUNICACIONES'
END sector_productivo
FROM "caba-piba-raw-zone-db"."portal_empleo_mtr_industry_sectors" isec
),
sp AS (
SELECT
sector_productivo
FROM clae
WHERE sector_productivo IS NOT NULL
GROUP BY sector_productivo
ORDER BY sector_productivo
)
SELECT
row_number() OVER () AS id_sector_productivo,
sector_productivo
FROM sp
--</sql>--



-- Copy of 2023.05.24 step 23 - consume_registro_laboral_formal.sql 



-- 1.-- Crear la tabla definitiva de REGISTRO LABORAL FORMAL
-- --<sql>--DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_def_registro_laboral_formal`;--</sql>--
--<sql>--
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_def_registro_laboral_formal" AS
SELECT
	MAX(registro_laboral_formal_id) AS registro_laboral_formal_id,
	cargo,
	broker_id,
	cuit_del_empleador,
	fecha_inicio,
	fecha_fin,
	modalidad_de_trabajo,
	remuneracion_moneda_corriente,
	remuneracion_moneda_constante
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_registro_laboral_formal_completa"
GROUP BY
	cargo,
	broker_id,
	cuit_del_empleador,
	fecha_inicio,
	fecha_fin,
	modalidad_de_trabajo,
	remuneracion_moneda_corriente,
	remuneracion_moneda_constante
--</sql>--

-- 2.-- Crear la tabla definitiva N-N de OPORTUNIDAD LABORAL - REGISTRO LABORAL FORMAL
-- --<sql>--DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_def_oportunidad_laboral_registro_laboral_formal`;--</sql>--
--<sql>--
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_def_oportunidad_laboral_registro_laboral_formal" AS
SELECT
	lfc.registro_laboral_formal_id,
	lfc.registro_laboral_formal_id_old,
	lfc.base_origen AS base_origen_registro_laboral_formal,
	lfc.oportunidad_laboral_id
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_registro_laboral_formal_completa" lfc
JOIN "caba-piba-staging-zone-db"."tbp_typ_def_registro_laboral_formal" r ON (r.registro_laboral_formal_id=lfc.registro_laboral_formal_id)
WHERE lfc.base_origen IS NOT NULL AND lfc.oportunidad_laboral_id IS NOT NULL
GROUP BY
	lfc.registro_laboral_formal_id,
	lfc.registro_laboral_formal_id_old,
	lfc.base_origen,
	lfc.oportunidad_laboral_id
--<sql>--



-- Copy of 2023.05.24 step 24 - staging entrevista.sql 



-- 1.-- Crear ENTREVISTA LABORAL
-- --<sql>--DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_tmp_entrevista`; --</sql>--
--<sql>--
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_entrevista" AS
-- PORTALEMPLEO
SELECT vec.base_origen,
	vec.vecino_id,
	ol.oportunidad_laboral_id,
	CAST(NULL AS VARCHAR) AS "tipo_entrevista",
	-- es la fecha de alta del registro, no hay una fecha de entrevista
	CAST(jac.created_at AS DATE) AS "fecha_entrevista",
	-- es la fecha de postulacion del candidato, no hay una fecha de notificacion
	CAST(ja.application_date AS DATE) AS "fecha_notificacion",
	-- 1 si fue contratado, 0 en caso contrario
	ja.hired AS "consiguio_trabajo",
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
	LEFT JOIN  "caba-piba-staging-zone-db"."tbp_typ_def_oportunidad_laboral" ol ON (ol.oportunidad_laboral_id_old=CAST(jp.id AS VARCHAR) AND ol.base_origen= 'PORTALEMPLEO')
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
		WHEN e.tipo_de_entrevista__c LIKE 'TelefÃ³nica' THEN 'Virtual Individual'
		WHEN e.tipo_de_entrevista__c LIKE 'Presencial Individual' THEN 'Presencial Individual'
		ELSE ''
	END AS "tipo_entrevista",
	CAST(e.fecha_de_entrevista__c AS DATE) AS "fecha_entrevista",
	-- es la fecha de creacion del registro, no hay una fecha de notificacion
	CAST(e.createddate AS DATE) AS "fecha_notificacion",
	-- 1 si fue contratado, 0 en caso contrario
	CASE WHEN p.estado__c LIKE 'Contratado' THEN 1 ELSE 0 END  AS "consiguio_trabajo",
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
	LEFT JOIN  "caba-piba-staging-zone-db"."tbp_typ_def_oportunidad_laboral" ol ON (ol.oportunidad_laboral_id_old=CAST(anu.id AS VARCHAR) AND ol.base_origen= 'CRMEMPLEO')
	WHERE e.isdeleted = FALSE
GROUP BY
	vec.base_origen,
	vec.vecino_id,
	ol.oportunidad_laboral_id,
	CASE
		WHEN e.tipo_de_entrevista__c LIKE 'TelefÃ³nica' THEN 'Virtual Individual'
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
		WHEN e.tipo_de_entrevista__c LIKE 'TelefÃ³nica' THEN 'Virtual Individual'
		WHEN e.tipo_de_entrevista__c LIKE 'Presencial Individual' THEN 'Presencial Individual'
		WHEN e.tipo_de_entrevista__c LIKE 'Presencial Grupal' THEN 'Presencial Grupal'
		ELSE ''
	END AS "tipo_entrevista",
	CAST(e.fecha_de_entrevista__c AS DATE) AS "fecha_entrevista",
	-- se deje en NULL porque no existe fecha de notificacion y la fecha de creacion es inexacta
	CAST(NULL AS DATE) AS "fecha_notificacion",
	-- 1 si fue contratado, 0 en caso contrario
	CASE WHEN p.estado__c LIKE 'Contratado' THEN 1 ELSE 0 END  AS "consiguio_trabajo",
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
	LEFT JOIN  "caba-piba-staging-zone-db"."tbp_typ_def_oportunidad_laboral" ol ON (ol.oportunidad_laboral_id_old=(CAST(anu.id AS VARCHAR) || 'H') AND ol.base_origen= 'CRMEMPLEO')
	WHERE e.isdeleted = 'false'
GROUP BY
	vec.base_origen,
	vec.vecino_id,
	ol.oportunidad_laboral_id,
	CASE
		WHEN e.tipo_de_entrevista__c LIKE 'TelefÃ³nica' THEN 'Virtual Individual'
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
			WHERE deleted = FALSE
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



-- Copy of 2023.05.24 step 25 - consume entrevista.sql 



-- 1.-- Crear la tabla def de entrevista
-- --<sql>--DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_def_entrevista`;--</sql>--
--<sql>--
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_def_entrevista" AS
SELECT
row_number() OVER () AS id_entrevista,
vecino_id,
oportunidad_laboral_id,
tipo_entrevista,
fecha_notificacion,
fecha_entrevista,
consiguio_trabajo,
estado_entrevista
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_entrevista"
--</sql>--



-- Copy of 2023.05.24 step 26 - staging organizaciones.sql 



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
--Se agrega el campo "razon_social_new" que contiene las denominaciones sociales obtenidas fuentes oficiales. En primera instancia, se obtiene la data del Registro Nacional de Sociededades (solo se podrÃ¡ obtener data de personas jurÃ­dicas).
rlf_op1 AS (
SELECT
rlf_op.cuit_del_empleador,
UPPER(rc.razon_social) AS razon_social_new
FROM rlf_op
LEFT JOIN "caba-piba-raw-zone-db"."registro_nacional_sociedades" rc ON (rc.cuit = rlf_op.cuit_del_empleador)
WHERE LENGTH(TRIM(rlf_op.cuit_del_empleador)) = 11
GROUP BY 1,2
),
--Se agrega el campo "razon_social_old" que contiene las denominaciones sociales obtenidas fuentes de CRM de oportunidades laborales. Es importante destacar que se tratan de nombres de fantasÃ­a y no de denominaciones sociales de fuentes oficiales.
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
    WHEN regexp_like(rlf_op2.razon_social_old ,'MINISTERIO|GOBIERNO|SUBSECRETARÃA|TSJ|GOB|AFIP|RENAPER|INTA|AGIP|PODER JUDICIAL|BANCO CENTRAL|ARBA') THEN 1
    ELSE 0
END ente_gubernamental
FROM rlf_op2
--</sql>--



-- Copy of 2023.05.24 step 27 - staging experiencia_laboral.sql 



-- Crear tabla tmp de experiencia laboral dentro del cv
-- --<sql>--DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_tmp_cv_experiencia_laboral_1`;--</sql>--
--<sql>--
 CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_cv_experiencia_laboral_1" AS
	SELECT
		'PORTALEMPLEO' base_origen,
		CAST(j.id AS VARCHAR) AS id_old,
		CAST(j.curriculum_id AS VARCHAR) AS id_cv_old,
		TRY_CAST(j.start_date AS DATE) AS fecha_desde,
		TRY_CAST(j.end_date AS DATE) AS fecha_hasta,
		CAST(j.company AS VARCHAR) AS empresa,
		CAST(j.task_description AS VARCHAR) AS descripcion_empleo,
		CAST(j.position AS VARCHAR) AS posicion,
		CASE WHEN j.end_date IS NULL THEN '0' ELSE '1' END trabajo_actual,
		CAST(cv.candidate_id AS VARCHAR) AS id_candidato
	FROM "caba-piba-raw-zone-db"."portal_empleo_jobs" j
		JOIN "caba-piba-raw-zone-db"."portal_empleo_curriculum_vitaes" cv ON (j.curriculum_id=cv.id)
		JOIN  "caba-piba-raw-zone-db"."portal_empleo_candidates" c ON (c.id=cv.candidate_id)
	GROUP BY
		CAST(j.id AS VARCHAR),
		CAST(j.curriculum_id AS VARCHAR),
		TRY_CAST(j.start_date AS DATE),
		TRY_CAST(j.end_date AS DATE),
		CAST(j.company AS VARCHAR),
		CAST(j.task_description AS VARCHAR),
		CAST(j.position AS VARCHAR),
		CASE WHEN j.end_date IS NULL THEN '0' ELSE '1' END,
		CAST(cv.candidate_id AS VARCHAR)
	UNION
	SELECT
		'CRMEMPLEO' base_origen,
		CAST(el.id AS VARCHAR) AS id_old,
		CAST(e.id AS VARCHAR) AS id_cv_old,
		TRY_CAST(el.fecha_inicio__c AS DATE),
		TRY_CAST(el.fecha_fin__c AS DATE),
		CAST(el.empresa__c AS VARCHAR),
		CAST(el.descripcion_de_tareas__c AS VARCHAR),
		CAST(el.puesto__c AS VARCHAR),
		CASE WHEN CAST(el.trabajo_actual__c AS VARCHAR) = 'false' THEN '0' ELSE '1' END,
		CAST(el.postulante__c AS VARCHAR)
	FROM "caba-piba-raw-zone-db"."crm_empleo_experiencia_laboral__c" el
	INNER JOIN "caba-piba-raw-zone-db"."crm_empleo_entrevista__c" e ON (
		el.postulante__c = e.postulante__c
		AND e.dni__c IS NOT NULL
	)
	GROUP BY
		CAST(el.id AS VARCHAR),
		CAST(e.id AS VARCHAR),
		TRY_CAST(el.fecha_inicio__c AS DATE),
		TRY_CAST(el.fecha_fin__c AS DATE),
		CAST(el.empresa__c AS VARCHAR),
		CAST(el.descripcion_de_tareas__c AS VARCHAR),
		CAST(el.puesto__c AS VARCHAR),
		CASE WHEN CAST(el.trabajo_actual__c AS VARCHAR) = 'false' THEN '0' ELSE '1' END,
		CAST(el.postulante__c AS VARCHAR)
	UNION
	SELECT
		'CRMSL' base_origen,
		CAST(ecc.id AS VARCHAR) AS id_old,
		CAST(ecc.id AS VARCHAR) AS id_cv_old,
		TRY_CAST(ecc.fecha_inicio_trabajo AS DATE),
		TRY_CAST(ecc.fecha_fin AS DATE),
		CAST(ecc.nombre_empresa AS VARCHAR),
		CAST(ecc.description AS VARCHAR),
		CAST(ecc.puesto AS VARCHAR),
		CASE WHEN CAST(ecc.trabaja_actualmente AS VARCHAR) = 'false' THEN '0' ELSE '1' END,
		CAST(cc.id_c  AS VARCHAR)
	FROM "caba-piba-raw-zone-db"."crm_sociolaboral_re_experiencia_laboral" ecc
	JOIN  "caba-piba-raw-zone-db"."crm_sociolaboral_re_experiencia_laboral_contacts_c" c ON (c.re_experiencia_laboral_contactsre_experiencia_laboral_idb = ecc.id)
	JOIN  "caba-piba-raw-zone-db"."crm_sociolaboral_contacts_cstm" cc ON (cc.id_c = c.re_experiencia_laboral_contactscontacts_ida)
	GROUP BY
		CAST(ecc.id AS VARCHAR),
		TRY_CAST(ecc.fecha_inicio_trabajo AS DATE),
		TRY_CAST(ecc.fecha_fin AS DATE),
		CAST(ecc.nombre_empresa AS VARCHAR),
		CAST(ecc.description AS VARCHAR),
		CAST(ecc.puesto AS VARCHAR),
		CASE WHEN CAST(ecc.trabaja_actualmente AS VARCHAR) = 'false' THEN '0' ELSE '1' END,
				CAST(cc.id_c  AS VARCHAR)
--</sql>--

-- --<sql>--DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_tmp_cv_experiencia_laboral`;--</sql>--
--<sql>--
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_cv_experiencia_laboral" AS
WITH aux AS (
SELECT
base_origen,
id_old,
id_cv_old,
fecha_desde,
fecha_hasta,
empresa,
UPPER(TRIM(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(COALESCE("empresa", 'OTRA EXPERIENCIA'), '[\\\\|_-]', ' '), ',|/', ' y '), '[^a-zA-Z0-9Ã¡Ã©Ã­Ã³ÃºÃÃÃÃÃÃ±Ã ]', ''), ' +', ' '), '\+', ' '), '1/2', 'medio '), '100\%', ''), '^ ', ''), ' $'))) empresa_limpia,
descripcion_empleo,
posicion,
UPPER(TRIM(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE("posicion", '[\\\\|_-]', ' '), ',|/', ' y '), '[^a-zA-Z0-9Ã¡Ã©Ã­Ã³ÃºÃÃÃÃÃÃ±Ã ]', ''), ' +', ' '), '\+', ' '), '1/2', 'medio '), '3Âº', '3ER '), '^ ', ''), ' $'))) posicion_limpia,
trabajo_actual,
id_candidato
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_cv_experiencia_laboral_1" )
SELECT
	base_origen,
	id_old,
	id_cv_old,
	CASE
	WHEN YEAR(fecha_desde)<1940 OR YEAR(fecha_desde)>(YEAR(CURRENT_DATE)+1)
		THEN CAST(NULL AS DATE) ELSE fecha_desde END AS fecha_desde,
	CASE
		WHEN YEAR(fecha_hasta)<1940 OR YEAR(fecha_hasta)>(YEAR(CURRENT_DATE)+1)  OR fecha_hasta<fecha_desde
		THEN CAST(NULL AS DATE) ELSE fecha_hasta END AS fecha_hasta,
	UPPER(empresa) AS empresa,
	CASE
		WHEN empresa_limpia IS NULL
		OR LENGTH(REPLACE(empresa_limpia, ' ', ''))<3
		OR empresa_limpia IN ('OTRAS EXPERIENCIAS','OTRAS TAREAS','OTRAS ÃREAS','OTRO','OTROS','OTROS OFICINISTAS','OTROS TRABAJOS ANTERIORES') THEN 'OTRA EXPERIENCIA'
	ELSE  empresa_limpia END AS empresa_limpia,
	UPPER(descripcion_empleo) AS descripcion_empleo,
	UPPER(posicion) AS posicion,
	CASE WHEN TRY_CAST(REPLACE(REPLACE(aux.posicion_limpia, ' ', ''), 'Y', '') AS BIGINT) IS NOT NULL THEN ''
	WHEN LENGTH(REPLACE(aux.posicion_limpia, ' ', '')) < 4
	AND REPLACE(aux.posicion_limpia, ' ', '') NOT IN ('DJ','DBA','DEV','CEO','CMO', 'BAR', 'ADM','CIO','JTP','KFC','PM','PMO','QA')
	THEN ''
	WHEN REPLACE(aux.posicion_limpia, ' ', '') IN ('NULL','AAAAA','AABB')  THEN ''
	WHEN aux.posicion_limpia LIKE '0PERADOR INTERNOATENCION A CLIENTE' THEN 'OPERADOR INTERNOATENCION A CLIENTE'
	WHEN aux.posicion_limpia LIKE '0PERARIO' THEN 'OPERARIO'
	WHEN aux.posicion_limpia LIKE '1 ANALISTA 2 CONSULTOR' THEN 'ANALISTA Y CONSULTOR'
	WHEN aux.posicion_limpia LIKE 'AAALISTA SR POR ANALISTA SR' THEN 'ANALISTA'
	WHEN aux.posicion_limpia LIKE 'AAALISTA' THEN 'ANALISTA'
	WHEN aux.posicion_limpia LIKE 'AADMINISTRACION' THEN 'ADMINISTRACION'
	WHEN aux.posicion_limpia LIKE 'AADMINISTRATIVA SUPERIOR' THEN 'ADMINISTRATIVA SUPERIOR'
	WHEN aux.posicion_limpia LIKE 'AALISTA COMERCIAL' THEN 'ANALISTA COMERCIAL'
	WHEN aux.posicion_limpia LIKE 'AANLISTA CUENTAS A PAGAR' THEN 'ANALISTA CUENTAS A PAGAR'
	WHEN aux.posicion_limpia LIKE 'AASISTENTE DE MARKETING' THEN 'ASISTENTE DE MARKETING'
	WHEN aux.posicion_limpia LIKE 'AATENCION AL CLIENTE' THEN 'ATENCION AL CLIENTE'
	WHEN aux.posicion_limpia LIKE 'AAUDITOR INSPECTOR' THEN 'AUDITOR INSPECTOR'
	WHEN aux.posicion_limpia LIKE 'AAYUDANTE DE COCINA Y CAMARERA' THEN 'AYUDANTE DE COCINA Y CAMARERA'
	WHEN aux.posicion_limpia IN ('OTROS', 'OTRAS ÃREAS', 'OTRAS EXPERIENCIAS', 'OTRAS TAREAS', 'OTROS TRABAJOS ANTERIORES')  THEN 'OTRO'
    ELSE aux.posicion_limpia END AS posicion_limpia,
	trabajo_actual,
	id_candidato,

	CASE
	WHEN empresa_limpia IS NULL
		OR LENGTH(REPLACE(empresa_limpia, ' ', ''))<3
		OR empresa_limpia IN ('OTRAS EXPERIENCIAS','OTRAS TAREAS','OTRAS ÃREAS','OTRO','OTROS','OTROS OFICINISTAS','OTROS TRABAJOS ANTERIORES')
		OR empresa_limpia IN ('01 Y 08 Y 2006', '01 Y 2008', '0KM', '100 BANCO', '1127811047', '16 12', '1810', '1816', '1EN1', '2 A SA', '2 AS', '2000', '20006', '2015', '2015 2019', '2019', '2080', '214', '2211', '225', '24CON', '257', '3080', '314', '365', '451', '6720', '747', 'A 1500', 'A 53', 'A COAST AUDIOVISUAL FREELANCE', 'A24', 'ABOGADA EN FORMA INDEPENDIENTE', 'ABOGADA FREE LANCE', 'ABOGADA INDEPENDIENTE', 'ABOGADA INDEPENDIENTE Y DOCENTE UBA', 'ABOGADA PARTICULAR', 'ABOGADO ASOCIADO INDEPENDIENTE', 'ABOGADO FREELANCER', 'ABOGADO INDEPENDIENTE', 'ABOGADO PARTICULAR', 'ABOGADOS INDEPENDIENTES', 'ABRIL GOMEZ FOTOGRAFÃA INDEPENDIENTE', 'ACOMPAÃAMIENTO DOMICILIARIO', 'ACOMPAÃANTE NO TERAPÃUTICO PARTICULAR', 'ACTIVIDAD EN FORMA INDEPENDIENTE', 'ACTIVIDAD FREE LANCE', 'ACTIVIDAD INDEPENDIENTE', 'ACTIVIDAD POR CUENTA PROPIA', 'ACTIVIDAD PROFESIONAL INDEPENDIENTE', 'ACTIVIDADES FREELANCE', 'ACTIVIDADES PARTICULARES DE LA PROFESION', 'ADM CEN DE BARRIOS MILITARES DE BUENOS AIRES', 'ADMINISTRATIVA FREELANCE', 'ADMINISTRATIVO FREE LANCE', 'AE PHONE INDEPENDIENTE', 'AGENCIA DOS PUNTOS DIARIO EL SOL FREELANCE', 'AGRONE SECXTOR32 FREELANCE DISEÃADOR SEMISENIOR', 'AGUSTIN HIGINIO FREELANCE', 'AKIROS FREELANCE TÃCNICO EN ELECTRÃNICA', 'ALBAÃIL', 'ALBAÃILERIA', 'ALBAÃILERÃA', 'ALEGA SOCIEDAD DE POLICÃA PARTICULAR', 'ALMACEN DE BARRIO', 'ALMACEN DE BARRIO FAMILIAR', 'ALMACEN DE BARRIO MANYULA', 'ALMACENES PARTICULARES', 'ALMACÃN DE BARRIO', 'ALMACÃN PARTICULAR', 'ALMACÃS Y KIOSCO DE BARRIO', 'AMA DE CASA Y NIÃERA', 'AMBULANCIA PARTICULAR', 'ANDERSCH INGENIERIA SRL FREELANCE', 'ANGEL PARTICULAR', 'APOYO ESCOLAR PARTICULAR', 'AQUITECTO EN PARTICULAR', 'ARBITRO INDEPENDIENTE', 'ARGENCHINO INDEPENDIENTE', 'ARGENTINIAN EMPANADAS PROYECTO INDEPENDIENTE', 'ARMADO Y REPARACIÃN DE PC INDEPENDIENTE', 'ARMD SRL INTERNACION DOMICILIARIA', 'ARQ PARTICULAR', 'ARQ UITECTO INDEPENDIENTE', 'ARQTA SILVIA NAYA FREELANCE', 'ARQTO GABRIEL OMAR BARUDI FREELANCE', 'ARQUITECTA FREELANCE', 'ARQUITECTA INDEPENDIENTE', 'ARQUITECTA JR FREELANCE', 'ARQUITECTA PARTICULAR', 'ARQUITECTA PROYECTISTA FREELANCE', 'ARQUITECTO EN INDEPENDIENTE', 'ARQUITECTO EN LIBRE EJERCICIO FREELANCE', 'ARQUITECTO FREE LANCE', 'ARQUITECTO FREELANCE', 'ARQUITECTO FREELANCER', 'ARQUITECTO INDEPENDIENTE', 'ARQUITECTO INDEPENDIENTE HOMEOFFICE', 'ARQUITECTO INDEPENDIENTE ORLANDO DÃVILA', 'ARQUITECTURA FREELANCE', 'ARQUITECTURA FREELANCER', 'ARREGLO DE PC POR CUENTA PROPIA', 'ARTEREX PROYECTO INDEPENDIENTE', 'ARTISTA INDEPENDIENTE', 'ASEGURADORA INDEPENDIENTE', 'ASESOR INDEPENDIENTE', 'ASESOR TÃCNICO CONSULTOR FREELANCE', 'ASESORA INDEPENDIENTE', 'ASESORÃA INDEPENDIENTE', 'ASISTENCIA DE CASAS PARTICULARES', 'ASISTENCIA DOMICILIARIA', 'ASISTENCIA DOMICILIARIA PARTICULAR', 'ASISTENCIA PRIVADA ODONTOLOGICA', 'ASISTENCIA PRIVADA ODONTOLÃGICA', 'ASISTENTE CONTABLE PARTICULAR', 'ASOCIADO INDEPENDIENTE DE HERBALIFE', 'AT Y PARTICULAR', 'ATENCION DOMICILIARIA', 'ATENCION DOMICILIARIO', 'ATENCION PARTICULAR', 'ATENCIÃN AL CLIENTE FREELANCE', 'ATENCIÃN DOMICILIARIA ADULTOS MAYOREA', 'ATENCIÃN MÃDICA DOMICILIARIA', 'ATENCIÃN PARTICULAR', 'AUTOCADISTA INDEPENDIENTE', 'AUTOGESTIVO Y INDEPENDIENTE', 'AUTONOMA FREELANCE', 'AUTONOMO FREE LANCE', 'AUTONOMO INDEPENDIENTE', 'AUTÃNOMA Y POR CUENTA PROPIA', 'AUTÃNOMO FREELANCE', 'AUXILIAR DE CASA PARTICULARES', 'AUXILIAR DE CASAS PARTICULARES', 'AYUDANTE DE CONTADOR FREELANCE', 'AYUDAR SALUD INTERNACIÃN DOMICILIARIA', 'BAJO CONTRATISTA E INDEPENDIENTE', 'BANKHOUSE SA Y NEGOCIOS INMOBILIARIOS FREELANCE', 'BAR INDEPENDIENTE', 'BARBERO POR MI CUENTA', 'BARMANAGER FREELANCE', 'BARRIO CERRADO', 'BARRIO CERRADO LA HERRADURA PINAMAR', 'BARRIO CERRADO SOBRE RUTA 2', 'BARRIO CHINO', 'BARRIO LA BOCA', 'BARRIO LA PODEROSA', 'BARRIO NORTE', 'BARRIO NUEVO ACOSTA', 'BARRIO NUEVO BARRIO TRICOLOR', 'BARRIO PAMI PALMIRO VALONI', 'BARRIO PAMPA AMBA', 'BARRIO PARQUE FUTBOL CLUB SA', 'BARRIO PRIVADO CANNING', 'BARRIO PRIVADO CORTIJO', 'BARRIO PRIVADO SOL LELOIR', 'BARRIO PRIVADO TERRALAGOS', 'BARRIO QUINTA FERRE Y PROV CORRIENTES', 'BARRIO SANTA MARIA NORTE', 'BARRIO SEPTIEMBRE SA', 'BARRIO UNIVRSITARIO', 'BAZAR Y REGALERÃA BARRIO CHINO', 'BIBLIOTECA PARTICULAR', 'BILINGUAL FREELANCE', 'BOLIVIA HOSPITAL Y PARTICULAR', 'BORDER CREATIVA FREELANCE', 'BRANDING Y MARKETING FREELANCE', 'CADETE EN SERVICIOS DE CADETERÃA PARTICULAR', 'CADETE FREELANCE EN CADETE CON MOTO', 'CADETE INDEPENDIENTE EN MOTO', 'CADISTA 2D FREELANCE', 'CADISTA FREE LANCE', 'CADISTA FREELANCE', 'CADISTA INDEPENDIENTE', 'CAFETERIA DE BARRIO', 'CAMPEONATO ALTO NONO', 'CAMPEONATO MUNDIAL DE MOTOCICLISMO MOTO GP', 'CAPACITACIÃN FREELANCE', 'CARNICERÃA', 'CARPINTERIA', 'CARPINTERÃA FREELANCE', 'CASA DE FAMILIA NIÃERA', 'CASA DE FAMILIA PARTICULAR', 'CASA DE PARTICULAR', 'CASA PARTICULAR', 'CASA PARTICULAR ACASUSSO SRA MARIA VIRGINIA', 'CASA PARTICULAR BARRIO SAN BENITO BENAVIDEZ', 'CASA PARTICULAR EN PARAGUAY', 'CASA PARTICULAR FAMILIAR', 'CASA PARTICULAR FLORIDA', 'CASA PARTICULAR GRAND BOURG', 'CASA PARTICULAR LOS POLVORINES', 'CASA PARTICULAR MONSEÃOR MIGUEL DE ANDREA', 'CASA PARTICULAR SAN MIGUEL Y BUENOS AIRES', 'CASA PARTICULAR TALAR DE PACHECO', 'CASA PARTICULAR TORTUGUITAS', 'CASA PARTICULAR VICTORIA SAN ISIDRO', 'CASA PARTICULAR Y LAPRIDA', 'CASA PARTICULARES', 'CASA PARTICULARES Y CLINICAS EH HOSPITALES', 'CASAS DE FAMILIA Y EDIFICIOS Y OFICINAS', 'CASAS DE FAMILIA Y OFICINA', 'CASAS DE FAMILIA Y OFICINAS', 'CASAS PARTICULAR', 'CASAS PARTICULARES', 'CASAS PARTICULARES BOSQUES', 'CASAS PARTICULARES FAMILIARES', 'CASAS PARTICULARES Y CAFÃ BAR ZURICH', 'CASAS PARTICULARES Y DEPARTAMENTOS', 'CASAS PARTICULARES Y EVENTOS', 'CASAS Y OFICINAS', 'CASES PARTICULARES', 'CASO PARTICULAR', 'CASO PARTICULAR EN DOMICILIO', 'CATERING PARTICULAR', 'CEA Y PARTICULAR', 'CENTRO DE DÃA CISAM Y CONSULTORIO PARTICULAR', 'CENTRO DE ESTUDIANTE DE INGENIERÃA', 'CENTRO DE ESTUDIANTES', 'CENTRO DE ESTUDIANTES CBC', 'CENTRO DE ESTUDIANTES DE ARTES VISUALES', 'CENTRO DE ESTUDIANTES DE CIENCIAS ECONÃMICAS', 'CENTRO DE ESTUDIANTES DE CIENCIAS MÃDICAS', 'CENTRO DE ESTUDIANTES DE CIENCIAS SOCIALES CECSO', 'CENTRO DE ESTUDIANTES DE CIENCIAS SOCIALES UBA', 'CENTRO DE ESTUDIANTES DE CS POL Y RRII UCA', 'CENTRO DE ESTUDIANTES DE EXACTAS', 'CENTRO DE ESTUDIANTES DE INGENIERIA ELECTRONICA', 'CENTRO DE ESTUDIANTES DE INGENIERÃA ELECTRÃNICA', 'CENTRO DE ESTUDIANTES DE LA UNA', 'CENTRO DE ESTUDIANTES DE LA UNQUI', 'CENTRO DE ESTUDIANTES DE PSICOLOGÃA', 'CENTRO DE ESTUDIANTES DE UNIVERSIDAD', 'CENTRO DE ESTUDIANTES DE VETERINARIA', 'CENTRO DE ESTUDIANTES EN UNQ', 'CENTRO DE ESTUDIANTES EOS27', 'CENTRO DE ESTUDIANTES FACULTAD DE INGENIERÃA', 'CENTRO DE ESTUDIANTES PSICOLOGIA CEP', 'CENTRO DE ESTUDIANTES UCA DERECHO ROSARIO', 'CENTRO DE ESTUDIANTES UNQ', 'CENTRO EDUCATIVO INDEPENDIENTE', 'CENTRO ESTUDIANTIL FADU', 'CENTRO ESTUDIANTIL NO OFICIAL', 'CENTRO PSICOPESPACIOS Y DE FORMA INDEPENDIENTE', 'CERRAJERÃA', 'CERVECERIA', 'CERVECERÃA', 'CHRISTIAN VELEZ FREELANCE', 'CHURRERÃA', 'CINE INDEPENDIENTE MON AMOUR', 'CLASE PARTICULARES', 'CLASES COMO PROFESOR PARTICULAR', 'CLASES DE APOYO INDEPENDIENTE', 'CLASES PARTICULAR DE INGLES', 'CLASES PARTICULARE DE MUSICA', 'CLASES PARTICULARES', 'CLASES PARTICULARES DE APOYO ESCOLAR', 'CLASES PARTICULARES DE INGLES', 'CLASES PARTICULARES DE INGLÃS', 'CLASES PARTICULARES DE INGLÃS FREELANCE', 'CLASES PARTICULARES DE LENGUA Y LITERATURA', 'CLASES PARTICULARES DE MANERA INDEPENDIENTE', 'CLASES PARTICULARES DE MATEMÃTICA', 'CLASES PARTICULARES DE MÃSICA', 'CLASES PARTICULARES DE PIANO', 'CLASES PARTICULARES DE PLASTICA', 'CLASES PARTICULARES DE QUÃMICA', 'CLASES PARTICULARES DIBUJO Y PINTURA', 'CLASES PARTICULARES EN MI DOMICILIO', 'CLASES PARTICULARES INDEPENDIENTE', 'CLASES PARTICULARES INGLÃS', 'CLASES PARTICULARES MATEMÃTICAS', 'CLASES PARTICULARES SECUNDARIO Y UNIVERSITARIO', 'CLASES PARTICULARES TODOS LOS NIVELES', 'CLASES PARTICULARES Y DE APOYO ESCOLAR', 'CLASES PARTICULARES Y GRUPALES', 'CLIENTE PARTICULAR', 'CLIENTES PARTICULARES', 'CLINICA MEDICA PARTICULAR', 'CLUB DE BARRIO', 'COARQ FREELANCE', 'COCINA PROPIA', 'CODESAR Y PARTICULAR', 'COLEGIO PARTICULAR MIXTO IBEMA', 'COLEGIO PARTICULAR TÃCNICO CUISSINE', 'COMERCIANTE INDEPENDIENTE', 'COMERCIO DE BARRIO', 'COMERCIO ELECTRÃNICO FREE LANCE', 'COMERCIO INDEPENDIENTE', 'COMERCIO PARTICULAR', 'COMITENTE PARTICULAR MDP', 'COMMUNITY MANAGER FREE LANCE', 'COMMUNITY MANAGER FREELANCE', 'COMO ANTES', 'COMPAÃIA 360 Y INDEPENDIENTE', 'COMPAÃÃA INDEPENDIENTE CASTADIVA', 'COMPUTACIÃN INDEPENDIENTE', 'COMUNICADORA FREELANCE', 'CONFECCIÃN DE ROPA INDEPENDIENTE', 'CONGRESOS MÃDICOS FREELANCE', 'CONSTRUCCION INDEPENDIENTE', 'CONSTRUCCIONES INDEPENDIENTES', 'CONSTRUCTOR INDEPENDIENTE', 'CONSTRUCTOR INDEPENDIENTE GERMÃN CHICO UBALDE', 'CONSTRUCTOR Y PROYECTISTA INDEPENDIENTE', 'CONSTRUCTORA DIPCA CASA PROPIA BIENES Y RAICES', 'CONSTRUCTORA INDEPENDIENTE', 'CONSULTOR ECONOMÃCO INDEPENDIENTE', 'CONSULTOR EMPRENDEDOR PROFESIONAL INDEPENDIENTE', 'CONSULTOR FREELANCE', 'CONSULTOR INDEPENDIENTE', 'CONSULTOR IT FREELANCE', 'CONSULTOR IT INDEPENDIENTE', 'CONSULTOR PARTICULAR', 'CONSULTOR PROFESIONAL INDEPENDIENTE DE SISTEMAS', 'CONSULTOR TI FREELANCE', 'CONSULTOR Y FREELANCER EN COMUNICACIONES Y MEDIOS', 'CONSULTORA DE RRHH INDEPENDIENTE', 'CONSULTORA FREELANCE', 'CONSULTORA INDEPENDIENTE', 'CONSULTORA PRIVADA', 'CONSULTORA PRIVADA DE FINANZAS', 'CONSULTORA PROPIA', 'CONSULTORAS PRIVADAS', 'CONSULTORES AMBIENTALES INDEPENDIENTES', 'CONSULTORES INDEPENDIENTES', 'CONSULTORIA FREE LANCE', 'CONSULTORIA INDEPENDIENTE', 'CONSULTORIA INDEPENDIENTE FREELANCE', 'CONSULTORIA IT INDEPENDIENTE', 'CONSULTORIO CLÃNICA PRIVADA', 'CONSULTORIO CLÃNICA PRIVADA FIBROCEMENTO', 'CONSULTORIO DE MEDICINA PRIVADA', 'CONSULTORIO MEDICO PARTICULAR', 'CONSULTORIO MÃDICA PARTICULAR', 'CONSULTORIO MÃDICO INDEPENDIENTE', 'CONSULTORIO MÃDICO PARTICULAR', 'CONSULTORIO MÃDICO PARTICULAR DR LUIS TROMBETTA', 'CONSULTORIO MÃDICO PARTICULAR DR ROMERO', 'CONSULTORIO MÃDICO PARTICULAR PAMI', 'CONSULTORIO MÃDICO TOCO GINECOLÃGICO PARTICULAR', 'CONSULTORIO ODONTOLGICO PARTICULAR', 'CONSULTORIO ODONTOLOGICO PARTICULAR', 'CONSULTORIO ODONTOLÃGICO PARTICULAR', 'CONSULTORIO PARTICULAR', 'CONSULTORIO PARTICULAR BELLA VISTA', 'CONSULTORIO PARTICULAR DE GINECOLOGÃA', 'CONSULTORIO PARTICULAR DE ODONTOLOGÃA', 'CONSULTORIO PARTICULAR DE PSIQUIATRIA', 'CONSULTORIO PARTICULAR DOCOTR PATOCCI', 'CONSULTORIO PARTICULAR DOCTOR CARRANO', 'CONSULTORIO PARTICULAR DR ARIEL RANTZ', 'CONSULTORIO PARTICULAR DR CASSAGNET ENRIQUE', 'CONSULTORIO PARTICULAR DR RUBÃN MORA', 'CONSULTORIO PARTICULAR DR TARNOVSKY', 'CONSULTORIO PARTICULAR DRA ANA GARAY', 'CONSULTORIO PARTICULAR DRA DIANA VACCA', 'CONSULTORIO PARTICULAR DRA MALDONADO', 'CONSULTORIO PARTICULAR DRA MASCARINI ANALIA', 'CONSULTORIO PARTICULAR ELDA MARTA PALACIOS', 'CONSULTORIO PARTICULAR GINECOLÃGICO', 'CONSULTORIO PARTICULAR INDEPENDIENTE', 'CONSULTORIO PARTICULAR KINESIOLOGIA', 'CONSULTORIO PARTICULAR LIC MÃNICA RUSSO', 'CONSULTORIO PARTICULAR NUTRICIÃN', 'CONSULTORIO PARTICULAR ODONTOLOGICO', 'CONSULTORIO PARTICULAR PSICOLOGICO', 'CONSULTORIO PEDIÃTRICO PARTICULAR', 'CONSULTORIO PEDRIATRICO PARTICULAR DRA UNGARO', 'CONSULTORIO PROFESIONAL INDEPENDIENTE', 'CONSULTORIO PSICOLOGICO PARTICULAR', 'CONSULTORIO Y CLÃNICA PRIVADA', 'CONSULTORIOS BARRIO NORTE', 'CONSULTORÃA FREE LANCE', 'CONSULTORÃA INDEPENDIENTE', 'CONSULTORÃA INDEPENDIENTE EN REDES SOCIALES', 'CONSULTORÃA PARTICULAR', 'CONSULTORÃA PRIVADA DE ENERGÃA', 'CONSUTORIO MÃDICO PARTICULAR', 'CONTADOR INDEPENDIENTE', 'CONTADOR INDEPENDIENTE Y MAURICIO ACOSTA', 'CONTADOR PUBLICO INDEPENDIENTE', 'CONTADOR PÃBLICO INDEPENDIENTE', 'CONTADORA INDEPENDIENTE', 'CONTADORA PARTICULAR', 'CONTADORA PUBLICA INDEPENDIENTE', 'CONTADORA PÃBLICA INDEPENDIENTE', 'CONTADORES INDEPENDIENTES', 'CONTADURÃA PRIVADA', 'CONTRATISTA INDEPENDIENTE', 'CONTRATISTA PARTICULAR', 'CONTRATISTACUENTA PROPIA', 'CONTRATO INDEPENDIENTE', 'CONVENIO ENTRE FADU Y GCBA BARRIO 31', 'CONVENIO GBA FADU Y VTV BARRIO 31', 'COOPERATIVA PROPIA', 'COORDINADA POR LIC PARTICULAR', 'CORMI CUENTA', 'CORRECTORA LITERARIA FREELANCE', 'CORREDOR DE MAT ELÃCTRICOS PARTICULAR', 'CORREDOR INMOBILIARIO INDEPENDIENTE', 'CORREDOR TEXTIL INDEPENDIENTE', 'CORRIENTE VILLERA INDEPENDIENTE', 'CORTOMETRAJE INDEPENDIENTE', 'COSULTORIO MÃDICO PARTICULAR', 'CREADOR AUDIOVISUAL INDEPENDIENTE', 'CROSS INFORMATICA FREELANCE', 'CUENTA PROPIA', 'CUENTA PROPIA CON AMIGOS', 'CUENTA PROPIA CONTADOR', 'CUENTAPROPIA', 'CUETA PROPIA', 'CUIDADO', 'CUIDADO A DOMICILIO', 'CUIDADO AL ADULTO MAYOR', 'CUIDADO DE ABUELOS', 'CUIDADO DE ADULTO MAYOR', 'CUIDADO DE ADULTOS MAYORES', 'CUIDADO DE ANCIANO', 'CUIDADO DE ANCIANOS', 'CUIDADO DE INFANTES', 'CUIDADO DE MENORES', 'CUIDADO DE NIÃO', 'CUIDADO DE NIÃOS', 'CUIDADO DE NIÃOS DE MANERA PARTICULAR', 'CUIDADO DE NIÃOS INDEPENDIENTE', 'CUIDADO DE NIÃOS NIÃERA PEDAGOGICA', 'CUIDADO DE NIÃOS VARRIAL', 'CUIDADO DE NIÃOS Y LIMPIEZA GENERAL DEL HOGAR', 'CUIDADO DE NIÃXS', 'CUIDADO DE PACIENTES A DOMICILIO', 'CUIDADO DE PERSONA', 'CUIDADO DE PERSONA MAYOR', 'CUIDADO DE PERSONA MAYOR EN HOGAR', 'CUIDADO DE PERSONAS', 'CUIDADO DE PERSONAS DE LA TERCERA EDAD', 'CUIDADO DE PERSONAS ESPECIALES', 'CUIDADO DE PERSONAS MAYORES', 'CUIDADO DE PERSONAS NIÃOS', 'CUIDADO DE PERSONAS Y LIMPIEZA', 'CUIDADO DEL ADULTO Y EL ANCIANO PRACTICAS', 'CUIDADO DOMICILIARIO', 'CUIDADO DOMICILIARIOS', 'CUIDADO EN CASA PARTICULAR', 'CUIDADO EN DOMICILIO', 'CUIDADO EN DOMICILIO PARTICULAR', 'CUIDADO INFANTIL', 'CUIDADO TERAPEUTICO', 'CUIDADO Y ACOMPAÃAMIENTO', 'CUIDADO Y ACOMPAÃAMIENTO DE ADULTOS MAYORES', 'CUIDADO Y ACOMPAÃANTE DE ADULTO MAYOR', 'CUIDADO Y ASISTENCIA DE PERSONAS', 'CUIDADO Y ATENCION DE PERSONA MAYOR DE EDAD', 'CUIDADO Y CADETERIA PARA PERSONA MAYOR', 'CUIDADODEADULTOMAYOR', 'CUIDADOR', 'CUIDADOR DE PERSONAS NIÃERA', 'CUIDADOR DOMICILIARIO', 'CUIDADOR DOMICILIARIO POR CUENTA PROPIA', 'CUIDADOR PARTICULAR DOMICIIARIO', 'CUIDADOR TRABAJO AUTÃNOMO', 'CUIDADOR Y A DE NIÃOS', 'CUIDADORA', 'CUIDADORA A DOMICILIO', 'CUIDADORA ADULTOS MAYORES PARTICULAR', 'CUIDADORA ASISTENCIAL', 'CUIDADORA DE ADULTO MAYOR', 'CUIDADORA DE ADULTO MAYOR Y MENORES DE EDAD', 'CUIDADORA DE ADULTOS', 'CUIDADORA DE CHICOS', 'CUIDADORA DE MAYORES DE EDAD', 'CUIDADORA DE NENES Y ADULTOS', 'CUIDADORA DE NIÃA ESPECIAL AUTISTA', 'CUIDADORA DE NIÃOS', 'CUIDADORA DE PACIENTES A DOMICILIO', 'CUIDADORA DE PERROS', 'CUIDADORA DE PERSOMA MAYORES', 'CUIDADORA DE PERSONA', 'CUIDADORA DE PERSONA MAYOR', 'CUIDADORA DE PERSONAS', 'CUIDADORA DOMICILIARIA', 'CUIDADORA DOMICILIO', 'CUIDADORA ELIAS BUCAY JORGE BUCAY HIJO', 'CUIDADORA GERIATRICA PARTICULAR', 'CUIDADORA INDEPENDIENTE', 'CUIDADORA PARTICULAR', 'CUIDADORA PERSONAL CASA DE FAMILIA', 'CUIDADORAS', 'CUIDADORES DE ADULTO MAYOR', 'CUIDADORES Y AUXILIARES', 'CUIDADOS A DOMICILIO', 'CUIDADOS DE ADULTOS MAYOR', 'CUIDADOS DE ADULTOS MAYORES', 'CUIDADOS DE MENORES', 'CUIDADOS DE NIÃOS', 'CUIDADOS DE SALUD DOMICILIARIOS', 'CUIDADOS DEL ADULTO MAYOR', 'CUIDADOS DOMICILIARIO', 'CUIDADOS DOMICILIARIOS', 'CUIDADOS DOMICILIARIOS BETA', 'CUIDADOS DOMICILIARIOS PALIATIVOS', 'CUIDADOS DOMICILIARIOS SRL', 'CUIDADOS DOMICILIRIARIOS', 'CUIDADOS DOMICIRIARIOS', 'CUIDADOS PARTICULARES A PERSONAS ADULTAS', 'CUIDAR A UNA PERSONA MAYOR', 'CUIDAR PERSONAS MAYORES', 'CUPERTINO FREELANCE', 'CURSO INDEPENDIENTE DE FOTOGRAFÃA', 'CURSOS PARTICULARES', 'CÃTEDRA SAGGESE PARTICULAR UNA VISUALES', 'CÃNSULTORIO PARTICULAR', 'DAVK EMPRENDIMIENTO INDEPENDIENTE', 'DE CASAS Y OFICINAS', 'DE FORMA INDEPENDIENTE', 'DE FORMA PARTICULAR', 'DE MANERA INDEPENDIENTE', 'DE TODO UN POCO', 'DEE DEE DESIGN PROYECTO INDEPENDIENTE', 'DEISUR DUEÃO Y PERSONA QUE LACREO EMPRESA', 'DEL ESTADO', 'DEPARTAMENTO PARTICULAR', 'DEPENDENCIA PROPIA', 'DESARROLLADOR FREELANCE', 'DESARROLLADOR FREELANCER', 'DESARROLLADOR INDEPENDIENTE', 'DESARROLLO FREELANCE', 'DESARROLLO INDEPENDIENTE', 'DESARROLLO PERSONAL', 'DESARROLLO PERSONAL CONSULTORES DP Y CA', 'DESCONOCIDA Y PROPIA', 'DESDE 1981 A 2010', 'DESEMPEÃO INDEPENDIENTE', 'DESPENSA INDEPENDIENTE', 'DESPENSA Y FIAMBRERIA', 'DESPENSA Y FIAMBRERÃA', 'DESPENSA Y JUGUETERIA', 'DEVELOPER FREE LANCER', 'DG DISEÃO MARCA PERSONAL', 'DI ROSSA INDUMENTARIA FEMENINA MARCA PROPIA', 'DIBUJANTE FREE LANCE', 'DIBUJANTE FREELANCE', 'DIBUJANTE PARTICULAR', 'DIBUJANTE Y RENDERISTA FREELANCE', 'DIBUJANTE Y RENDERISTA INDEPENDIENTE', 'DIBUJO TECNICO FREELANCER', 'DICTADO DE CLASES PARTICULARES DE INGLÃS', 'DIRECCIÃN DE PERSONAL EJECUTIVO REGIONAL VZLA', 'DISEÃADOR DE INTERIORES FREELANCE', 'DISEÃADOR FREELANCE', 'DISEÃADOR GRAFICO FREELANCE', 'DISEÃADOR GRÃFICO FREELANCE', 'DISEÃADOR GRÃFICO FREELANCER', 'DISEÃADOR GRÃFICO INDEPENDIENTE', 'DISEÃADOR GRÃFICO SR INDEPENDIENTE', 'DISEÃADOR INDEPENDIENTE', 'DISEÃADOR MULTIMEDIAL FREELANCE', 'DISEÃADOR WEB FREELANCE', 'DISEÃADORA DE INTERIORES INDEPENDIENTE', 'DISEÃADORA FREELANCE', 'DISEÃADORA GRAFICA INDEPENDIENTE', 'DISEÃADORA GRÃFICA EN DISEÃO FREELANCE', 'DISEÃADORA GRÃFICA FREELANCE', 'DISEÃADORA INDEPENDIENTE', 'DISEÃADORA Y Y DIR DE ARTE FREELANCE', 'DISEÃO DE INTERIORES INDEPENDIENTE', 'DISEÃO DE VIVIENDA PARTICULAR', 'DISEÃO FREE LANCE', 'DISEÃO FREELANCE', 'DISEÃO GRAFICO FREELANCE', 'DISEÃO GRÃFICO E ILUSTRACIÃN FREELANCE', 'DISEÃO GRÃFICO FREELANCE', 'DISEÃO INDEPENDIENTE', 'DISEÃO INTERIOR FREELANCE', 'DISEÃO WEB FREELANCE', 'DISEÃORA GRÃFICA FREELANCE', 'DISEÃOS GRÃFICOS INDEPENDIENTES FREELANCE', 'DISTINTAS EMPRESAS DOMICILIARIAS', 'DISTRIBUIDORA INDEPENDIENTE', 'DIVERSAS EMPRESAS TEXTILES Y UNIPERSONAL', 'DOCENCIA EN OFICINA DE EMPLEOS', 'DOCENTE PARTICULAR', 'DOCUMENTACIÃN DE OBRA FREE LANCE', 'DOMICILIARIA', 'DOMICILIARIO', 'DOMICILIARIO Y INSTITUCIONAL', 'DOMICILIO PARTICULAR', 'DOMICILIO PERSONAL', 'DOMICILIO PERSONAL Y', 'DOÃA ÃRSULA SELECCIÃN DE PERSONAL DOMÃSTICO', 'DP PERSONAL DE EVENTOS', 'DSIEÃADORA FREELANCE', 'DURANTE', 'EDICIÃN FREELANCE', 'EDIFICIO PARTICULAR', 'EDUCACIÃN PARTICULAR', 'EJERCICIO INDEPENDIENTE', 'EJERCICIO INDEPENDIENTE DE LA PROFESION', 'EJERCICIO INDEPENDIENTE DE LA PROFESIÃN', 'EJERCICIO INDEPENDIENTEMENTE DE LA PROFESIÃN', 'EJERCICIO PARTICULAR', 'EJERCICIO PROFESIONAL INDEPENDIENTE', 'EKSPRESA FREELANCE', 'EL MERCADO EN TU BARRIO', 'ELECTRICISTA DOMICILIARIO', 'ELECTRICISTA INDEPENDIENTE', 'ELECTRICISTA OFICIAL CUENTA PROPIA', 'ELECTRICISTA PARTICULAR', 'ELECTRICISTA POR CUENTA PROPIA', 'ELECTRISISTA PARTICULAR', 'ELENCO DE TEATRO INDEPENDIENTE', 'ELLA', 'ELLA Y YO', 'ELÃCTRICISTA PARTICULAR', 'EMERGENCIAS DOMICILIARIAS', 'EMMESOL SERVICIOS DOMICILIARIOS', 'EMPLEADA DE CASAS PARTICULARES', 'EMPLEADA DE LIMPIEZA Y NIÃERA', 'EMPLEADA DOMESTICA PARTICULAR', 'EMPLEADA DOMESTICA Y NIÃERA', 'EMPLEADA DOMESTICADOMICILIO PARTICULAR', 'EMPLEADA DOMÃSTICA DE CASAS PARTICULARES', 'EMPLEADA PARTICULAR', 'EMPLEADA POR CUENTA PROPIA EN PARTICULAR', 'EMPLEADO DE MENSAJERIA', 'EMPLEADO EN CUENTA PROPIA', 'EMPLEADO INDEPENDIENTE', 'EMPLEADOR INDEPENDIENTE', 'EMPLEADOR PARTICULAR', 'EMPLEADORA PARTICULAR', 'EMPLEDA PARTICULAR', 'EMPLEO DE FORMA INDEPENDIENTE', 'EMPLEO EN CUENTA PROPIA', 'EMPLEO FREE LANCE', 'EMPLEO FREELANCE', 'EMPLEO INDEPENDIENTE', 'EMPREDIMIENTO DE LENCERIA PROPIO', 'EMPREDIMIENTO PERSONAL', 'EMPRENDEDOR INDEPENDIENTE', 'EMPRENDEDOR PARTICULAR', 'EMPRENDIEMINTO PERSONAL', 'EMPRENDIENDO DE INDUMENTARIA FEMENIA', 'EMPRENDIMIENTO BARRIAL', 'EMPRENDIMIENTO DE ESTAMPERÃA Y EN BORDADO', 'EMPRENDIMIENTO DE INDUMENTARIA', 'EMPRENDIMIENTO DE INDUMENTARIA LOLA JUNCO', 'EMPRENDIMIENTO DE LENCERIA', 'EMPRENDIMIENTO DE MARROQUINERÃA', 'EMPRENDIMIENTO DE PASTELERIA', 'EMPRENDIMIENTO DE PASTELERÃA', 'EMPRENDIMIENTO DE REPOSTERÃA', 'EMPRENDIMIENTO DE VENTA DE INDUMENTARIA', 'EMPRENDIMIENTO EN FORMA INDEPENDIENTE', 'EMPRENDIMIENTO FAMILIAR DE INDUMENTARIA', 'EMPRENDIMIENTO FREELANCE', 'EMPRENDIMIENTO INDEPENDIENTE', 'EMPRENDIMIENTO INDEPENDIENTE DE COSMÃTICA NATURAL', 'EMPRENDIMIENTO INDEPENDIENTE ECOMERCE', 'EMPRENDIMIENTO INDUMENTARIA', 'EMPRENDIMIENTO INDUMENTARIA MULTIMARCAS FEMENINA', 'EMPRENDIMIENTO PARTICULAR', 'EMPRENDIMIENTO PASTELERIA', 'EMPRENDIMIENTO PERSONAL', 'EMPRENDIMIENTO PERSONAL BELS DESIGN', 'EMPRENDIMIENTO PERSONAL BENEDITA ACCESORIOS', 'EMPRENDIMIENTO PERSONAL CARNICERÃA Y FIAMBRERIA', 'EMPRENDIMIENTO PERSONAL COMERCIO', 'EMPRENDIMIENTO PERSONAL CULINARIO', 'EMPRENDIMIENTO PERSONAL DE ASESORÃA DE IMAGEN', 'EMPRENDIMIENTO PERSONAL DE COMIDA', 'EMPRENDIMIENTO PERSONAL DE DISEÃO GRÃFICO', 'EMPRENDIMIENTO PERSONAL ECOMMERCE', 'EMPRENDIMIENTO PERSONAL EL MUNDO DE NANANAN', 'EMPRENDIMIENTO PERSONAL GESTORÃA DEL AUTOMOTOR', 'EMPRENDIMIENTO PERSONAL HANA MÃNDEZ', 'EMPRENDIMIENTO PERSONAL ROTISERÃA', 'EMPRENDIMIENTO PERSONAL RUBRO TELEFONÃA MÃVIL', 'EMPRENDIMIENTO PERSONAL TIA FLOR INDUMENTARIA', 'EMPRENDIMIENTOS PERSONAL', 'EMPRESA DE INTERNACION DOMICILIARIA', 'EMPRESA DE INTERNACIÃN DOMICILIARIA', 'EMPRESA DE SEGURIDAD PRIVADA', 'EMPRESA INDEPENDIENTE', 'EMPRESA INDEPENDIENTE VENTA DE PRODUCTOS NATURALES', 'EMPRESA PARTICULAR', 'EMPRESA PERSONAL', 'EMPRESA PRIVADA', 'EMPRESA PRIVADA DE INTERNACIÃN DOMICILIARIA', 'EMPRESA PRIVADA DE PASAJEROS', 'EMPRESA PROPIA', 'EMPRESA PROPIA AUTONOMA', 'EMPRESA PROPIA DE PASTELERÃA', 'EMPRESA UNIPERSONAL', 'EMPRESAPROPIA', 'EMPRESAS DE CUIDADOS DOMICILIARIOS', 'EMPRESAS DE ENFERMERÃA DOMICILIARIA', 'EMPRESAS DE INTERNACION DOMICILIARIA', 'EMPRESAS DOMICILIARIA EN INTERNACIÃN', 'EMPRESAS VARIAS', 'EMPRESAS Y OFICINAS EN CABA', 'EN CASA SERVICIO DE ASISTENCIA DOMICILIARIA', 'EN CASAS PARTICULARES', 'EN FORMA INDEPENDIENTE', 'EN FORMA PARTICULAR', 'EN HOGARES PARTICULARES', 'EN LOCAL DE RECUERDOS EN EL BARRIO LA BOCA', 'EN UN CALL CENTER DEL BARRIO', 'EN UN LOCAL DE CAFETERÃA DE SUS PADRES', 'ENCASA CUIDADOS DOMICILIARIOS', 'ENENFERMERA DOMICILIARIA AUTÃNOMA', 'ENFERMERA DOMICILIARIA', 'ENFERMERA PARTICULAR', 'ENFERMERIA', 'ENFERMERIA AUTONOMA', 'ENFERMERIA DOMICILIARIA', 'ENFERMERIA MODERNA', 'ENFERMERIA MOFERNA', 'ENFERMERÃA', 'ENFERMERÃA DOMICILIARIA', 'ENFERMERÃA Y CUIDADOR', 'ENNEASTUDIO INDEPENDIENTE', 'ENPERSONA', 'ENSEÃANZA PARTICULAR', 'ENSEÃANZA PARTICULAR DE INGLÃS', 'ENSEÃANZA PERSONALIZADA', 'ENTREGAS PERSONALES SRL CORREO PRIVADO', 'ENTRENADOR PERSONAL', 'ENTRENAMIENTO PERSONALIZADOS', 'ENTRENAMIENTOS PERSONALIZADOS', 'ENTRENAMIENTOS PERSONALIZADOS ARRETTINO', 'EQUIPO DE ABOGADOS INDEPENDIENTE', 'ERAS', 'ES 22 DE 19', 'ES24', 'ESCRITORA FREELANCE', 'ESCUELA PARTICULAR INCORPORADA N 1115 SAN JOSE', 'ESCUELA PARTICULAR JOSÃ MARÃA LUIS MORA', 'ESCUELA PRIVADA', 'ESPACIO INDIGO Y EMPRENDIMIENTO PERSONAL', 'ESTADO', 'ESTAMPERIA', 'ESTUDIO ARQUITECTURA INDEPENDIENTE', 'ESTUDIO CONTABLE INDEPENDIENTE', 'ESTUDIO CONTABLE PARTICULAR', 'ESTUDIO DE ARQUITECTURA INDEPENDIENTE', 'ESTUDIO DE DISEÃO FREE LANCE', 'ESTUDIO DE INGENIERIA', 'ESTUDIO DE INGENIERÃA', 'ESTUDIO DFE ARQUITECTURA PERSONAL', 'ESTUDIO EDGARDO VILLAFAÃE FREE LANCE', 'ESTUDIO INDEPENDIENTE', 'ESTUDIO JURIDICO INDEPENDIENTE', 'ESTUDIO JURIDICO PARTICULAR', 'ESTUDIO JURÃDICO PARTICULAR', 'ESTUDIO PARTICULAR', 'ESTUDIO PARTICULAR DRALUCERO', 'ESTUDIOS JURIDICOS PARTICULARES', 'EU PRODUCTORA INDEPENDIENTE', 'EVENTOS PARTICULARES', 'FABRICA DE TEXTIL INDEPENDIENTE', 'FACTURACION MEDICA PARTICULAR', 'FAENA PROPIA', 'FAENA PROPIA SAS', 'FAMILIA PARTICULAR', 'FAMILIAR INDEPENDIENTE', 'FAMILIAS PARTICULAR', 'FAMILIAS PARTICULARES', 'FEDERACIÃN DE ESTUDIANTES DE CIENCIAS POLÃTICAS', 'FEDERACIÃN DE ESTUDIANTES DE DERECHO DE VENEZUELA', 'FEDERACIÃN DE ESTUDIANTES UCA', 'FEDERAL CERVECERIA INDEPENDIENTE', 'FEFYM COMITÃ INDEPENDIENTE DE ÃTICA', 'FERIANTE', 'FERIANTE TODO BLANCO', 'FERIAS', 'FERIAS AMERICANAS', 'FERIAS ARTESANALES', 'FERIAS COMERCIALES', 'FERIAS DE LA CIUDAD', 'FERIAS DE LA CUIDAD', 'FERRETERIA', 'FERRETERIA PROPIA', 'FERRETERÃA', 'FESTIVAL DE CINE INDEPENDIENTE DE EL PALOMAR', 'FESTIVAL DE CINE LIMA INDEPENDIENTE', 'FIAMBRERIA', 'FIAMBRERRIA', 'FIAMBRERÃA', 'FIVERR FREELANCE PAGE WWWFIVERRCOM', 'FLETE POR CUENTA PROPIA', 'FLETES PARTICULARES', 'FLORENCIA SAPUCCAI PARTICULAR', 'FM INDEPENDIENTE', 'FORMA INDEPENDIENTE', 'FOTOCOPIADORA CENTRO DE ESTUDIANTES DE DERECHO', 'FOTOCOPIADORA COLEGIO NORMAL 1', 'FOTOCOPIADORA DE LA FACULTAD DE CS MÃDICAS UNLP', 'FOTOCOPIADORA DE LA UNIVERSIDAD DE BUENOS AIRES', 'FOTOCOPIADORA EN ESCUELAS PRIMARIAS Y SECUNDARIAS', 'FOTOCOPIADORA FACULTAD DE DERECHO', 'FOTOCOPIADORA FULL TIME', 'FOTOCOPIADORA FYL UBA', 'FOTOCOPIADORA UBA', 'FOTOCOPIADORA UNIVERSIDAD UMET', 'FOTOCOPIADORA UNLP', 'FOTOCOPIADORA Y LIBRERIA EL ESTUDIANTE', 'FOTOGRAFA FREELANCE', 'FOTOGRAFA INDEPENDIENTE', 'FOTOGRAFIA FREELANCE', 'FOTOGRAFIA INDEPENDIENTE', 'FOTOGRAFO FREELANCE', 'FOTOGRAFO INDEPENDIENTE', 'FOTOGRAFO Y DISEÃADOR MULTIMEDIA FREELANCE', 'FOTOGRAFÃA FREE LANCE', 'FOTOGRAFÃA FREELANCE', 'FOTOGRAFÃA INDEPENDIENTE', 'FOTOGRAFÃA PROFESIONAL FREELANCE', 'FOTÃGRAFA FREELANCE', 'FOTÃGRAFA FREELANCER', 'FOTÃGRAFA INDEPENDIENTE', 'FOTÃGRAFIA INDEPENDIENTE', 'FOTÃGRAFO FREE LANCE', 'FOTÃGRAFO FREELANCE', 'FOTÃGRAFO FREELANCER', 'FOTÃGRAFO INDEPENDIENTE', 'FOTÃGRAFO Y REALIZADOR AUDIOVISUAL FREELANCE', 'FREE LANCE', 'FREE LANCE CONGRESOS', 'FREE LANCE ESTUDIO3PORCIENTO', 'FREE LANCE INDEPENDIENTE', 'FREE LANCE PLANOS EN AUTOCAD', 'FREE LANCE PROYECTO SOCIAL', 'FREE LANCE SECTOR DISEÃO GRÃFICO', 'FREE LANCE TIENDA PROPIA', 'FREE LANCE TRABAJADORA POR CUENTA PROPIA', 'FREE LANCER', 'FREE LANCER WEB', 'FREELANCE', 'FREELANCE A PEDIDO', 'FREELANCE ARQUITECTO INDEPENDIENTE', 'FREELANCE ARQUITECTURA', 'FREELANCE COMUNICACIONES CORPORATIVAS', 'FREELANCE CONSULTOR', 'FREELANCE DESIGNER', 'FREELANCE DEVELOPER', 'FREELANCE DISEÃO', 'FREELANCE DISEÃO GRÃFICO', 'FREELANCE DISEÃO Y COMUNICACIÃN', 'FREELANCE EMPRENDEDOR DISEÃO', 'FREELANCE EN MERCADOLIBRE', 'FREELANCE EN PUBLICIDAD Y DISEÃO', 'FREELANCE ESTILIST ROX', 'FREELANCE ESTUDIO PROPIO', 'FREELANCE ESTUDIOS ARQ TAVOLARO', 'FREELANCE INDEPENDIENTE', 'FREELANCE JUNIOR EN DISEÃO Y COMUNICACIN', 'FREELANCE LACICLA', 'FREELANCE PARA CARTELLONE', 'FREELANCE PARA DIVERSAS PRODUCTORAS AUDIOVISUALES', 'FREELANCE PARA ESTUDIOS GOTIKA', 'FREELANCE PARA UTE ILUBAIRES APCO LIHUE', 'FREELANCE PARTICULARES Y FABRICAS Y COMERCIOS', 'FREELANCE PHOTOGRAPHER', 'FREELANCE PROPIO', 'FREELANCE PROYECTO RETOR', 'FREELANCE PUBLICIDAD', 'FREELANCE PUERTO RICO', 'FREELANCE RRHH Y RRLL', 'FREELANCE TELETRABAJO', 'FREELANCE Y ALCALDÃA DE JAMUNDI', 'FREELANCE Y EMPRENDEDORA', 'FREELANCE Y INDEPENDIENTE', 'FREELANCE Y REMOTO', 'FREELANCE Y SECTOR PRIVADO', 'FREELANCE Y SELF EMPLOYED', 'FREELANCER', 'FREELANCER DOCENTE UNIVERSITARIO VENEZUELA', 'FREELANCER EN MARKETING DIGITAL', 'FREELANCER PÃGINAS WEB', 'FREELANCER Y INDEPENDIENTE', 'FREELANCERCOM', 'FREELANCERS', 'FRESH MARKETING EMPRENDIMIENTO INDEPENDIENTE', 'FRUTERIA Y VERDULERIA', 'FULL PR FREELANCE', 'GABINETE PARTICULAR', 'GALERIA DE ARTE', 'GALERIA DE ROPA', 'GALERÃA', 'GAME SERVERS ARGENTINA FREELANCE', 'GENESIS FREELANCE', 'GESTIONES ADUANERAS FREE LANCE', 'GESTIÃN INDEPENDIENTE', 'GESTOR FREELANCE', 'GESTOR PARTICULAR', 'GESTORA INDEPENDIENTE', 'GESTORIA FREELANCE', 'GESTORIA INDEPENDIENTE', 'GESTORIA PARTICULAR', 'GESTORIA Y CONSULTORIA INDEPENDIENTE', 'GESTORÃA', 'GESTORÃA DEL AUTOMOTOR OFICINA PROPIA', 'GESTORÃA INDEPENDIENTE', 'GESTORÃA MUNICIPAL FREELANCE', 'GMG CLASES PARTICULARES', 'GOMERIA', 'GRUPO ESE ARQTA LUISA ENTENZA FREELANCE', 'GUIA DE TURISMO FREE LANCE', 'GUÃA DE TURISMO FREE LANCE', 'HAMBURGUESERÃA', 'HAMBURGUSERIA', 'HAS', 'HELADERIAS', 'HELADERÃA', 'HELADERÃA ARTESANAL', 'HELADERÃA Y CAFETERIA', 'HELADERÃA Y CAFETERÃA', 'HERNÃNDEZ CARLOS CONTRATO PARTICULAR', 'HERRERÃA INDEPENDIENTE', 'HOGAR GERIATRICO', 'HOGAR PARTICULAR', 'HOGAR VILLALOBOSCASA PARTICULAR', 'HOGARES PARTICULARES', 'HOLY RAINBOW MARCA PROPIA', 'HONORABLE CONCEJO DELIBERANTE FREELANCE', 'HOSPITAL FIORITO Y CONSULTORIO PARTICULAR', 'HOSPITAL Y CASAS PARTICULARES', 'HOSPITALES Y HOGARES Y PARTICULARES Y OTROS', 'ILUSTRADORA FREELANCE', 'IMPRENTA INDEPENDIENTE', 'IMPRESIÃN 3D INDEPENDIENTE', 'INDEPENDIENTE', 'INDEPENDIENTE ARQ MARLY TORRES', 'INDEPENDIENTE AUTÃNOMO', 'INDEPENDIENTE BIRNA', 'INDEPENDIENTE CA', 'INDEPENDIENTE CENTRO DE MANICURIA', 'INDEPENDIENTE DE MERLO', 'INDEPENDIENTE EMPRENDEDOR', 'INDEPENDIENTE EMPRENDIMIENTO GASTRONÃMICO', 'INDEPENDIENTE EN TALLERES DE CHAPA Y PINTURA', 'INDEPENDIENTE EN TIENDA DE MODA', 'INDEPENDIENTE FREE LANCE', 'INDEPENDIENTE FREELANCE', 'INDEPENDIENTE KATERSTUDIO', 'INDEPENDIENTE LIBRE EJERCICIO', 'INDEPENDIENTE MMO', 'INDEPENDIENTE MONOTRIBUTISTA', 'INDEPENDIENTE MONOTRIBUTO ACTIVO', 'INDEPENDIENTE MYSTERY SHOPPER', 'INDEPENDIENTE PELUQUERIA CENTRO SPA', 'INDEPENDIENTE VENEZUELA', 'INDEPENDIENTE X2', 'INDEPENDIENTE Y CENTROS CULTURALES', 'INDEPENDIENTE Y EMPRESAS', 'INDEPENDIENTE Y EMPRESAS Y CONTINUA', 'INDEPENDIENTE Y FREE LACNCE', 'INDEPENDIENTE Y FREE LANCE', 'INDEPENDIENTE Y FREE LANCER', 'INDEPENDIENTE Y FREELANCE', 'INDEPENDIENTE Y FREELANCER', 'INDEPENDIENTE Y HOLOS CAPITAL', 'INDEPENDIENTE Y VENEZUELA', 'INDEPENDIENTEANTEPROYECTO PLAZA SECA', 'INDEPENDIENTEFREELANCE', 'INDEPENDIENTEMENTE', 'INDEPENDIENTEOFICINAS DE AEROTERRA', 'INDEPENDIENTEREMODELACIÃNPALERMO', 'INDEPENDIENTES', 'INDEPENDIENTEVIVIENDA UNIFAMILIAR', 'INDUMENTARIA', 'INDUMENTARIA INDEPENDIENTE', 'INDUMENTARIA PROPIA FEMENINA', 'INDUMENTARIAS', 'INDUSTRIAL', 'INFORMATICA VARIAS', 'INGENIERO CIVIL INDEPENDIENTE', 'INGENIERO FREELANCE CARACAS VENEZUELA', 'INGENIERO INDEPENDIENTE', 'INGLÃS PARTICULAR', 'INMIBILIARIA', 'INMOBILIARIA', 'INMOBILIARIA INDEPENDIENTE', 'INMOBILIARIAS Y OFICINAS', 'INMOBILIRIAS', 'INMOVILIARIA', 'INSTALACION DE MATERIAL PUBLICITARIO', 'INSTALADOR INDEPENDIENTE DE CCTV', 'INSTITO ZABALA PARTICULAR', 'INSTITUTO DE CLASES PARTICULARES', 'INSTITUTO EDITORIAL', 'INSTITUTO PARTICULAR', 'INSTITUTO PARTICULAR DE INGLÃS', 'INSTITUTO PARTICULAR DE INGLÃS PATRICIA CODUTTI', 'INSTITUTO PRIVADO DE INGLÃS PARTICULAR', 'INSTITUTOS DE ENSEÃANZA Y FREELANCE', 'INSTITUTOS PRIVADOS Y POR CUENTA PROPIA', 'INSTRUCTOR DE INGLÃS PARTICULAR', 'INTERNACION DOMICILARIA', 'INTERNACION DOMICILIARIA', 'INTERNACIONES DOMICILIARIAS', 'INTERNACIÃN DOMICILIARIA', 'INTERNACIÃN DOMICILIARIA Y GOBIERNO DE LA CIUDAD', 'INTERNACIÃN DOMIXILIARIA', 'INTERPRETE FREELANCER', 'JARDINERIA EN CASAS PARTICULARES', 'JARDINERIA INDEPENDIENTE', 'JARDINERÃA POR CUENTA PROPIA', 'JOYERIA', 'JOYERÃA', 'JUEGUETERÃA', 'JUGETERIA', 'JUGUETERIA', 'KALINKA FREELANCE', 'KARINA MILEWICZ FREELANCE', 'KINESIOLOGA PARTICULAR', 'KIOSCO BARRIAL', 'KIOSCO CAFETERIA', 'KIOSCO LIBRERIA', 'KIOSCO Y FERIA', 'KIOSCO Y LIBRERIA', 'KIOSCO Y VERDULERIA EN CASA PARTICULAR', 'KIOSKO FLORISTERÃA', 'KIOSKO JUGUETERÃA', 'KIOSKO LIBRERIA', 'KIOSKO LIBRERÃA', 'KRIF CONSULTORIO PARTICULAR', 'LA 100', 'LA PARTICULAR DE VIRGINIO', 'LA PROPIA', 'LABOR INDEPENDIENTE', 'LAS 4 A', 'LAVANDERIA', 'LAVANDERÃA', 'LENCERIA', 'LENCERÃA', 'LIBRE EJERCICIO PROFESIONAL FREELANCE', 'LIBRERÃA', 'LIBRERÃA Y JUGUETERÃA', 'LIBRERÃA Y KIOSCO', 'LIC EN NUTRICION PARTICULAR', 'LIMPIEZA CASA PARTICULAR', 'LIMPIEZA DE CASA PARTICULAR', 'LIMPIEZA DE CASAS PARTICULAR', 'LIMPIEZA DE CASAS PARTICULARES', 'LIMPIEZA DE OFICINA', 'LIMPIEZA DE OFICINAS', 'LIMPIEZA DE OFICINAS TUCUMÃN 540', 'LIMPIEZA DOMICILIARIA', 'LIMPIEZA DOMICILIARIA Y DAMA DE COMPAÃÃA', 'LIMPIEZA EN CASA PARTICULARES', 'LIMPIEZA EN CASA PARTICULARES DIFERENTES LUGARES', 'LIMPIEZA EN CASAS DE FAMILIA Y OFICINAS', 'LIMPIEZA EN CASAS PARTICULARES', 'LIMPIEZA EN LOCALES Y OFICINA', 'LIMPIEZA EN OFICINA', 'LIMPIEZA EN OFICINAS', 'LIMPIEZA EN OFICINAS Y EDIFICIOS', 'LIMPIEZA PARTICULAR', 'LIMPIEZA POR CUENTA PROPIA', 'LIMPIEZA Y CASA PARTICULAR', 'LIMPIEZA Y NIÃERA', 'LIMPIEZAS DE OFICINAS', 'LISTA ROJA CLUB ATLÃTICO INDEPENDIENTE', 'LITIGO EN FORMA INDEPENDIENTE', 'LOCAL DE BARRIO', 'LOCAL DE INDUMENTARIA', 'LOCAL DE INDUMENTARIA DE MUJER', 'LOCAL DE INDUMENTARIA FAMILIAR', 'LOCAL DE INDUMENTARIA FEMENINA', 'LOCAL DE INDUMENTARIA GENERAL', 'LOCAL DE INDUMENTARIA INFANTIL', 'LOCAL DE INDUMENTARIA MASCULINA', 'LOCAL DE INDUMENTARIA UNISEX', 'LOCAL DE INDUMENTARIA Y TEXTIL', 'LOCAL DE INDUMMENTARIA', 'LOCAL DE LENCERIA', 'LOCAL DE LENCERIA CORDOBA CPAITAL', 'LOCAL DE LENCERIA EMPRENDIMIENTO PROPIO', 'LOCAL DE LENCERÃA', 'LOCAL DE PIZZERÃA Y ROTICERÃA', 'LOCAL DE PRODUCCIÃN DE INDUMENTARIA', 'LOCAL DE REGALERIA E INDUMENTARIA FEMENINA', 'LOCAL DE ROPA EN GALERÃA TRES ELEFANTES', 'LOCAL DE ROPA INDEPENDIENTE', 'LOCAL DE ROPA INDUMENTARIA FEMENINA', 'LOCAL DE ROPA Y JUGUETERÃA Y BAZAR', 'LOCAL DE VENTA DE LENCERÃA', 'LOCAL ELSA DE ROPA Y ARREGLOS DE INDUMENTARIA', 'LOCAL EN GALERIA', 'LOCAL INDUMENTARIA', 'LOCAL INDUMENTARIA FEMENINA', 'LOCAL LENCERÃA', 'LOCAL MARIA', 'LOCAL PARTICULAR', 'LOCAL PARTICULAR DE INDUMENTARIA FEMENINA', 'LOCAL PERFUMERIA Y LIMPIEZA', 'LOCALES COMERIALES', 'LOS TESOROS DE LUDIVINA PROYECTO INDEPENDIENTE', 'LOTERIA', 'LOTERÃA', 'LÃDICA MENTE FREE LANCE', 'MAESTRA DE APOYO ESCOLAR INDEPENDIENTE SENIOR', 'MAESTRA PARTICULAR', 'MAESTRO DE CLASES PARTICULARES AUTÃNOMO', 'MAESTRO MAYOR DE OBRA INDEPENDIENTE', 'MAESTRO MAYOR DE OBRAS INDEPENDIENTE', 'MANDATARIO FREELANCE', 'MANERA INDEPENDIENTE', 'MANTENIMIENTO EN CASAS PARTICULARES Y EDIFICIOS', 'MANTENIMIENTO INDEPENDIENTE', 'MAQUILLADORA INDEPENDIENTE', 'MAQUILLADORA PROFESIONAL FREELANCE', 'MARCA PROPIA', 'MARCAS INDEPENDIENTES', 'MARIA', 'MARIANISTA', 'MARIANO', 'MARIAS', 'MARROQUINERIA', 'MARROQUINERÃA', 'MARTINA FIERRO PROYECTO INDEPENDIENTE', 'MARÃA S', 'MARÃAS', 'MASAJISTA INDEPENDIENTE', 'MASAJISTA PARTICULAR', 'MASAJISTA PARTICULAR FREELANCE', 'MASSALIN PARTICULARES', 'MASSALIN PARTICULARES SA', 'MASSALIN PARTICULARES SRL', 'MATERIA', 'MATERIAL', 'MATERIALES DE LA CONSTRUCCIÃN', 'MATERIALES PARA LA CONSTRUCCIÃN', 'MATRICERIA', 'MAXI FRUIT VERDULERÃA', 'MAXI KIOSCO LIBRERÃA DANIEL', 'MAXI QUIOSCO Y LIBRERÃA', 'MAXIKIOSCO HELADERÃA', 'MAXIKIOSCO LA HISTORIA', 'MAXIKIOSCO LIBRERIA', 'MAXIKIOSCO LIBRERÃA', 'MAXIKIOSCO MARIANI', 'MAXIKIOSCO MIRIAM', 'MAXIKIOSCO Y CAFETERIA', 'MAXIKIOSCO Y LIBRERIA', 'MAXIKIOSCO Y LIBRERIA LA VIA', 'MAXIKIOSCO Y PERFUMERIA', 'MAXIKIOSCO Y TABAQUERIA PRIMERA JUNTA', 'MAXIKIOSKO MARIANA', 'MAXIKIOSKO Y LIBRERIA', 'MAXIQUIOSCO ADRIAN', 'MAXIQUIOSCO LIBRERIA', 'MAYORISTA DE ARTÃCULOS DE FERRETERÃA LUQUE', 'MAYORISTA DE ARTÃCULOS DE LIBRERÃA Y ARTÃSTICA', 'MAYORISTA DE INDUMENTARIA FABRICANTES ARGENTINOS', 'MAYORISTA LAS MARIAS', 'MAYORISTA MARIANA', 'MAYORISTA MARIANITA', 'MEDICA LEGISTA PRIVADA', 'MEDICA PRIVADA', 'MEDICINA DOMICILIARIA', 'MEDICINA ESTETICA LASER', 'MEDICINA ESTÃTICA', 'MEDICINA ESTÃTICA LÃSER', 'MEDICINA ESTÃTICA MATISSE DE TANIA SILVA', 'MEDICO INDEPENDIENTE', 'MEDICO PARTICULAR', 'MEDICOS PARTICULARES', 'MENAJERIA', 'MENSAJERIA EMPRESARIAL', 'MENSAJERIA EN MOTO', 'MENSAJERÃA DE MOTOS', 'MENSAJERÃA ENCOMIENDAS EN CORREO PRIVADO', 'MERCADO PARTICULAR', 'MERCERIA', 'MERCERIA Y BAZAR', 'MERCERÃA', 'MI PROPIA EMPRESA', 'MICRO EMPRENDIMIENTO DE REPOSTERÃA Y PANADERÃA', 'MICRO EMPRENDIMIENTO FREELANCE', 'MICRO EMPRENDIMIENTO INDEPENDIENTE', 'MICROEMPRENDIMIENTO DE INDUMENTARIA', 'MICROEMPRENDIMIENTO DE JOYERÃA', 'MICROEMPRENDIMIENTO PROPIO INDUMENTARIA', 'MINERIA', 'MODALIDAD FREELANCE', 'MODALIDAD INDEPENDIENTE', 'MODALIDAD PARTICULAR', 'MODELO FREELANCE', 'MONICA B SNYDERS CONTADORA INDEPENDIENTE', 'MONOTRIBUTISTA INDEPENDIENTE EN POR CUENTA PROPIA', 'MOTO MENSAJERIA', 'MOTOMENSAJERIA', 'MOTOMENSAJERIA EV', 'MOTOMENSAJERIA INDEPENDIENTE', 'MOTOMENSAJERÃA', 'MOTOVINTRABAJO POR CUENTA PROPIA', 'MSM EMPRESA INDEPENDIENTE', 'MUEBLERIA', 'MUEBLERIA PARTICULAR', 'MUEBLERÃA', 'MÃDICA GIMÃNEZ', 'MÃDICA PARTICULAR', 'MÃDICAL HOUSE', 'MÃDICO DR CLAUDIO ALE', 'MÃDICO NUTRICIONISTA DR NORBERTO PEDEVILLA', 'MÃDICO PARTICULAR', 'NADA', 'NEGOCIO BARRIAL', 'NEGOCIO COMERCIAL FAMILIAR', 'NEGOCIO DE AGROINSUMOS', 'NEGOCIO DE ARTESANÃAS', 'NEGOCIO DE BARRIO', 'NEGOCIO DE BEBIDAS', 'NEGOCIO DE CAMISAS', 'NEGOCIO DE COLECCIONISMO', 'NEGOCIO DE COMIDAS LA FAMILIA', 'NEGOCIO DE COMPONENTES ELECTRONICOS', 'NEGOCIO DE COMPUTACION', 'NEGOCIO DE COMPUTACIÃN', 'NEGOCIO DE DECORACION', 'NEGOCIO DE DIARIOS Y REVISTAS', 'NEGOCIO DE ELECTRICIDAD', 'NEGOCIO DE EMPRENDIMIENTO FAMILIAR', 'NEGOCIO DE ESTETICA', 'NEGOCIO DE FAMILIA', 'NEGOCIO DE INDUMENTARIA FEMENINA', 'NEGOCIO DE PRODUCTOS REGIONALES Y NATURALES', 'NEGOCIO DE ROPA', 'NEGOCIO DE ROPA DE DAMA Y NIÃOS', 'NEGOCIO DE ROPA DE HOMBRE CASA BIRMIGAN', 'NEGOCIO DE ROPA DEL ORIENTE', 'NEGOCIO DE ROPA INFANTIL', 'NEGOCIO DE ROPA LUISANA', 'NEGOCIO DE VENTA DE CALZADO DE DAMA', 'NEGOCIO DE VENTA MAYORISTA', 'NEGOCIO DEL BARRIO', 'NEGOCIO DESPENSA', 'NEGOCIO EMPRENDEDOR', 'NEGOCIO EN AVELLANEDA', 'NEGOCIO EN FLORES', 'NEGOCIO EN LA AV AVELLANEDA', 'NEGOCIO ESCOLAR', 'NEGOCIO FAMILIAR', 'NEGOCIO FAMILIAR ALMACEN', 'NEGOCIO FAMILIAR ALMACÃN', 'NEGOCIO FAMILIAR CIBER LOCUTORIO', 'NEGOCIO FAMILIAR DE ENTRETENIMIENTO', 'NEGOCIO FAMILIAR DE INDUMENTARIA', 'NEGOCIO FAMILIAR DE PANIFICADOS', 'NEGOCIO FAMILIAR DE POLIRUBRO', 'NEGOCIO FAMILIAR INDEPENDIENTE', 'NEGOCIO FAMILIAR KIOSCO JUAREZ', 'NEGOCIO FAMILIAR MARÃA', 'NEGOCIO FAMILIAR MINI SUPER', 'NEGOCIO FAMILIAR MÃXICO', 'NEGOCIO FAMILIAR Y TIENDA', 'NEGOCIO FAMILIAR Y VENTA DE AUTOPARTES', 'NEGOCIO FRUTAS VERDURAS Y LIMPIEZA', 'NEGOCIO INDEPENDIENTE', 'NEGOCIO LA NELLY', 'NEGOCIO LOCAL', 'NEGOCIO MACKA', 'NEGOCIO PARTICULAR', 'NEGOCIO PARTICULAR 2013 2015', 'NEGOCIO PEQUEÃO', 'NEGOCIO PERSONAL', 'NEGOCIO POLI RUBRO', 'NEGOCIO POLIRRUBRO', 'NEGOCIO PROPIO', 'NEGOCIO PROPIO KIOSCO MONOTRIBUTO', 'NEGOCIO PROPIO PERSONAL', 'NEGOCIO VENTA DE PRODUCTOS CONGELADOS', 'NEGOCIOPYME', 'NEGOCIOS', 'NEGOCIOS DE ROPA', 'NEGOCIOS FAMILIARES', 'NEGOCIOS FAMILIARES COMERCIOS', 'NEGOCIOS FRIGORÃFICOS DEL NORTE', 'NEGOCIOS PUNTA DEL ESTE URUGUAY', 'NEGOCIOS Y CASAS DE FAMILIAS', 'NINGUNA PARTICULAR', 'NINGUNA TRABAJO INDEPENDIENTE', 'NIÃERA', 'NIÃERA A DOMICILIO', 'NIÃERA BABY SITTER', 'NIÃERA DE BAUTISTA', 'NIÃERA DE FORMA INDEPENDIENTE', 'NIÃERA DE JOAQUÃN', 'NIÃERA DE LOURDES', 'NIÃERA DE NIÃAS MENORES DE 10 AÃOS', 'NIÃERA DE NIÃOS Y AS', 'NIÃERA DOMESTICA', 'NIÃERA EN CASA DE FAMILIA', 'NIÃERA EN EL BARRIO', 'NIÃERA EN NIÃOS DE 4 Y 10 AÃOS', 'NIÃERA EN VARIAS OCASIONES', 'NIÃERA FRANQUERA', 'NIÃERA INDEPENDIENTE', 'NIÃERA MEDIO TIEMPO', 'NIÃERA PARA MÃLTIPLES CLIENTES', 'NIÃERA PARTICULAR', 'NIÃERA PARTICULAR SITLY', 'NIÃERA PEDAGÃGICA', 'NIÃERA PERSONAL', 'NIÃERA Y AMA DE CASA', 'NIÃERA Y APOYO ESCOLAR', 'NIÃERA Y CUIDADO DE PERSONAS DE LA TERCERA EDAD', 'NIÃERA Y CUIDADORA', 'NIÃERA Y EMPLEADA DOMESTICA', 'NIÃERA Y EMPLEADA DOMÃSTICA', 'NIÃERA Y LIMPIEZA', 'NIÃERA Y PLANILLERA Y CURSO DE PELUQUERÃA', 'NIÃERAS DULCE NANA', 'NO HAY 2 SIN 3', 'NO TENGO', 'NO TIENE', 'NO TUVE', 'NOSOTRAS', 'NOTARIA 37', 'NOTARIA 42', 'NOTARIA PUBLICA CUARTA DE MARACAY', 'NOTARIA PUBLICA TERCERA', 'NOTARIA PÃBLICA SAIME VENEZUELA', 'NOTARÃA PÃBLICA DE ANACO', 'NOTARÃA PÃBLICA DE SAN DIEGO DEL ESTADO CARABOBO', 'NUESTRO', 'OBRAS PARTICULARES', 'OCUPACIONES VARIAS', 'ODONTOLOGIA PRIVADA', 'OFIC DE INGENIERÃA STAMBUL', 'OFICER LIBRERIAS COMERCIAL Y ESCOLAR', 'OFICIAL HERRERIA Y CARPINTERIA', 'OFICIAL LEGAL PROPIA', 'OFICINA', 'OFICINA 1 GALERÃA DE ARTE', 'OFICINA CENTRAL INMOBILIARIA', 'OFICINA COMARCAL AGRARIA SIERRA MORENA', 'OFICINA COMERCIAL', 'OFICINA CONGRESO', 'OFICINA CONSULAR', 'OFICINA CONTABLE', 'OFICINA CONTABLE MARIA DANIELA YAYES', 'OFICINA CONTABLE PARTICULAR', 'OFICINA DE ABOGADOS', 'OFICINA DE ARQUITECTURA', 'OFICINA DE CONTADORA', 'OFICINA DE EMPLEO', 'OFICINA DE INGENIERÃA HELICOIDAL Y CA', 'OFICINA DE PROTECCIÃN', 'OFICINA DE RENAULT', 'OFICINA DE SEGUROS', 'OFICINA DE SEGUROS Y GESTORÃA', 'OFICINA DE TURISMO', 'OFICINA DE VENTA', 'OFICINA EN CASA', 'OFICINA PARTICULAR', 'OFICINA PARTICULAR DE SEGUROS', 'OFICINA PARTICULAR LIC RICARDO ESTEVES', 'OFICINA PRIVADA', 'OFICINA PROPIA', 'OFICINA TECNICA DE INGENIERÃA PEMEGAS', 'OFICINA TRIBUTARIA GOBIERNO DE ESPAÃA', 'OFICINA TÃCNICA', 'OFICINAS', 'ORGANIZADORA DE EVENTOS INDEPENDIENTE', 'OS5', 'OSDOP OBRA SOCIAL DE LOS DOCENTES PARTICULARES', 'OSDOPOSOCIAL DE DOCENTES PARTICULARES', 'OTRA', 'OTRAS', 'PAGOS Y TRANSF CASAS PARTICULARES', 'PAGOS Y TRANSFERENCIAS DE CASAS PARTICULARES', 'PANADERIA', 'PANADERIA FAMILIAR', 'PANADERIA INDEPENDIENTE', 'PANADERIA PROPIA', 'PANADERIAS', 'PANADERÃA', 'PANADERÃA DE BARRIO', 'PANADERÃA ERVIN PANIFICACION PROPIA', 'PANADERÃA PROPIA', 'PANADRIA', 'PANCHERIA', 'PANCHERÃA', 'PAPELERIA', 'PAPELERIAS', 'PARA ARQUITECTA INDEPENDIENTE', 'PARDO AUDIOVISUAL FREELANCE', 'PARTICULAR', 'PARTICULAR ARQ FLAVIA COTTITTO', 'PARTICULAR AYUDANTE PINTURA', 'PARTICULAR BELLEGGIA', 'PARTICULAR CARPINTERIA', 'PARTICULAR CASA DE FAMILIA', 'PARTICULAR COTILLON', 'PARTICULAR CUIDADORA', 'PARTICULAR DE CONTABILIDAD Y MATEMÃTICA', 'PARTICULAR DEPARTAMENTO DE 2 AMBIENTES', 'PARTICULAR E INDEPENDIENTE CABA Y CONURBANO', 'PARTICULAR EDELAP SA', 'PARTICULAR EN CASA DE FAMILIA', 'PARTICULAR EN DOMICILIO', 'PARTICULAR EN ESCUELA', 'PARTICULAR ES POR RECOMENDACION', 'PARTICULAR ESTUDIO JURIDICO', 'PARTICULAR FAMILIA SHMIDEL', 'PARTICULAR FAMILIAR', 'PARTICULAR FREELANCE', 'PARTICULAR GRACIELA ERBRSFELD', 'PARTICULAR INDEPENDIENTE MONOTRIBUTISTA', 'PARTICULAR INFORMÃTICA Y TECNOLOGÃA', 'PARTICULAR MARISOL', 'PARTICULAR MARROQUINERIA', 'PARTICULAR OFICINAS', 'PARTICULAR ONG', 'PARTICULAR POR UN AMIGO', 'PARTICULAR SRA ALEJANDRA', 'PARTICULAR TEXTIL', 'PARTICULAR VENTA DE INDUMENTARIA ONLINE', 'PARTICULAR VENTA POR INTERNET', 'PARTICULAR Y', 'PARTICULAR Y AGENCIA', 'PARTICULAR Y CONSORCIO DE EMPLEADOS', 'PARTICULAR Y CONTRATISTA', 'PARTICULAR Y EMPRESAS', 'PARTICULAR Y ESTUDIOS ZONALES', 'PARTICULAR Y FAMILIA BASSUEL DUNCAN', 'PARTICULAR Y INTERNACIÃN DOMICILIARIA', 'PARTICULAR Y POR HORA', 'PARTICULAR Y VENTA DE VIANDAS CONSTITUCIÃN', 'PARTICULAR ZULMA RAMIREZ', 'PARTICULARCONSTRUCCION EN SECO', 'PARTICULARES', 'PARTICULARES VARIOS', 'PARTICULARESCBH ASSIST Y SIRPLAST', 'PARTICULARMENTE', 'PASEO CANINO PARTICULAR', 'PASSEADOR DE PERROSPARTICULAR', 'PASTELERIA', 'PASTELERÃA FREELANCE', 'PCI PROYECTO DE CINE INDEPENDIENTE', 'PELUQERIA', 'PELUQUERIA', 'PELUQUERIA CANINA', 'PELUQUERIA PROPIA', 'PELUQUERIA UNISEX', 'PELUQUERIAS', 'PELUQUERÃA', 'PELUQUERÃA CANINA', 'PELUQUERÃA INDEPENDIENTE', 'PELUQUERÃA UNISEX', 'PELUQUERÃAS Y FREELANCE', 'PERFUMERIA', 'PERFUMERÃA', 'PERIODISTA INDEPENDIENTE', 'PERIÃDICO INDEPENDIENTE EL HERALDO', 'PERSONA INDEPENDIENTE', 'PERSONA PARTICULAR', 'PERSONAL DE CASAS PARTICULARES', 'PERSONAL TRAINER INDEPENDIENTE', 'PESCADERIA', 'PESCADERÃA', 'PHILIP MORRIS LATAM Y MASSALIN PARTICULARES', 'PHP Y VARIAS FREELANCE', 'PIEZZERIA', 'PINTOR INDEPENDIENTE', 'PINTURERIA', 'PINTURERIAS', 'PIZZERIA', 'PIZZERÃA', 'PIZZZERIA', 'PLATERIA', 'PLEGARIA', 'PLOMERIA', 'PLOMERIA EN GENERAL PARTICULAR', 'PLOMERÃA', 'PODOLOGA PARTICULAR', 'POLLERIA', 'POLLERÃA', 'POR CONTRATO Y CUENTA PROPIA', 'POR CUENTA PROPIA', 'POR CUENTA PROPIA AUTÃNOMO', 'POR CUENTA PROPIA RELACIÃN DE DEPENDENCIA', 'POR MI CUENTA', 'POR MI CUENTA GESTORIA', 'PORFESIONAL INDEPENDIENTE', 'PORTERIA', 'PORTERÃA', 'PORTERÃA DE UN EDIFICIO PARTICULAR', 'PRACTICA PROFECIONAL', 'PRACTICA PROFESIONAL', 'PRACTICA PROFESIONAL DE LA UBA', 'PRACTICA PROFESIONAL EN CET', 'PRACTICA PROFESIONAL FADU UBA', 'PRACTICA PROFESIONAL UBA', 'PRACTICANTE EN FUNDACIÃN PAREMAI FRACTAL', 'PRACTICAS DE ENSEÃANZA EN ESCUELAS DE UAI', 'PRACTICAS FORMATIVAS', 'PRACTICAS HOSPITALARIAS', 'PRACTICAS PROFESIONALISANTES', 'PRACTICAS PROFESIONALIZANTES', 'PRACTICAS PROFEZIONALIZANTES', 'PRESTACIONES DOMICILIARIA', 'PRESTACIONES DOMICILIARIAS', 'PRESTACIONES ELECTRICAS', 'PRESTACIONES ELÃCTRICAS', 'PRESTACIONES MÃDICAS', 'PRESTACIONES ODONTOLOGICAS', 'PRESTACIONES PARA BNA COMO MONOTRIBUTISTA', 'PRESTACIÃN DOMICILIARIA', 'PRINTSTORE PROPIA', 'PRIVADA', 'PRIVADA ACOMPAÃANTE TERAPÃUTICO', 'PRIVADA CASA', 'PRIVADA DE VENTAS', 'PRIVADO CASA PARTICULAR', 'PRIVILEGE BURZACO INDEPENDIENTE', 'PROCURACION INDEPENDIENTE', 'PRODUCTOR ASESOR INDEPENDIENTE', 'PRODUCTOR AUDIOVISUAL FREE LANCE', 'PRODUCTOR INDEPENDIENTE', 'PRODUCTORA INDEPENDIENTE', 'PRODUCTORA INDEPENDIENTE MORKAN', 'PRODUCTORA INTEGRAL FREELANCE', 'PROFESIONAL FREELANCER', 'PROFESIONAL INDEPENDIENTE', 'PROFESIONAL INDEPENDIENTE FREELANCE', 'PROFESIONAL INDEPENDIENTE GABSER SA', 'PROFESIONAL INDEPENDIENTE VENEZUELA', 'PROFESOR DE INGLÃS PARTICULAR', 'PROFESOR INDEPENDIENTE', 'PROFESOR PARTICULAR', 'PROFESOR PARTICULAR DE APOYO', 'PROFESOR PARTICULAR DE COMPUTACIÃN', 'PROFESOR PARTICULAR DE INGLES', 'PROFESOR PARTICULAR DE INGLÃS', 'PROFESOR PARTICULAR DE MATEMATICAS', 'PROFESOR Y DOCENTE PARTICULAR', 'PROFESORA DE INGLES PARTICULAR', 'PROFESORA DE INGLÃS PARTICULAR', 'PROFESORA PARTICULAR', 'PROFESORA PARTICULAR A DOMICILIO', 'PROFESORA PARTICULAR DE IDIOMA INGLÃS', 'PROFESORA PARTICULAR DE INGLES', 'PROFESORA PARTICULAR DE INGLÃS', 'PROFESORA PARTICULAR INDEPENDIENTE', 'PROFRESORA PARTICULAR DE MATEMÃTICA', 'PROGRAMADOR POR CUENTA PROPIA', 'PROMOTORA FREE LANCER', 'PROPIA', 'PROPIA AMBMASAJES TERAPIA1', 'PROPIA COMERCIO', 'PROPIA FREELANCE', 'PROPIA Y NEGOCIO PERSONAL', 'PROPIO Y FREELANCE', 'PROYECTO FREE LANCE', 'PROYECTO INDEPENDIENTE', 'PROYECTO PERSONAL INDEPENDIENTE', 'PROYECTO RADIAL INDEPENDIENTE', 'PROYECTOS CON INGENIERIA', 'PROYECTOS CON INGENIERÃA', 'PROYECTOS DE INGENIERÃA MULTIDISCIPLINARIOS', 'PROYECTOS E INGENIERÃA FERSA', 'PROYECTOS EMPRESARIALES DE SALUD ADMINISTRADOS', 'PROYECTOS FREE LANCE', 'PROYECTOS INDEPENDIENTES', 'PROYECTOS INDEPENDIENTES DE INGENIERA', 'PROYECTOS INDUSTRIALES SA', 'PRÃCTICA PRE PROFESIONAL FAUBA', 'PRÃCTICA PROFESIONAL EXTERNA EN ÃPTICA', 'PRÃCTICA UNIVERSITARIA EN PJN', 'PRÃCTICAS DE ENFERMERÃA', 'PRÃCTICAS DOCENTES', 'PRÃCTICAS HOSPITALARIAS', 'PRÃCTICAS LABORALES EMPRESA SOYUZ DE AVELLANEDA', 'PRÃCTICAS PROFESIONALES UNIVERSIDAD DE PALERMO', 'PRÃCTICAS PROFESIONALIZANTES', 'PRÃCTICAS PROFESIONALIZANTES DE IMPO Y EXPO', 'PRÃCTICAS PROFESIONAS I Y III', 'PSICOLOGA PARTICULAR', 'PSICÃLOGA PARTICULAR', 'PSICÃLOGO CLÃNICO PARTICULAR', 'PSICÃLOGO PARTICULAR', 'PUBLICIDAD FREELANCE', 'PUBLICITARIA FREELANCE', 'QUESERÃA', 'RADIOS INDEPENDIENTES', 'REALICE PRÃCTICAS EN EL FIORITO Y WILDE Y EVITA', 'REALIZADOR AUDIOVISUAL INDEPENDIENTE', 'RECEPCION FREELANCE SALON', 'RECLUTADORA FREELANCE', 'RECRUITER FREELANCE', 'REDACCIÃN FREELANCE', 'REDACTOR FREELANCE', 'REDACTORA INDEPENDIENTE', 'REENDERISTA FREELANCE', 'REFORMAS PARTICULARES', 'REFRIGERACION DIMA FREELANCE', 'REGALARÃA Y BAZAR', 'REGALERIA', 'REGALERÃA', 'REGALOS EMPRESARIALES', 'REGISTRO NOTARIAL', 'RELACIÃN DE AIRE ACONDICIONADO INDEPENDIENTE', 'RELOJERÃA', 'REMIERIA', 'REMIS PARTICULAR', 'REMISERIA', 'REMISERIA PARTICULAR', 'REMISERÃA', 'REMODELACIÃN Y AMPLIACIÃN PARTICULAR', 'RENDERISTA FREELANCE', 'RENDERISTA Y CADISTA FREELANCE', 'REPARACIÃN DE PC FREELANCE', 'REPARADOR DE PCS EN FORMA INDEPENDIENTE', 'REPRESENTANTE DE VENTAS INDEPENDIENTE', 'RESIDENCIA', 'RESIDENCIA UNIVERSITARIA', 'RESIDENCIA UNIVERSITARIA ALMA', 'RESIDENCIA UNIVERSITARIA DE EXTRANJEROS CRISTAL', 'RESIDENCIA UNIVERSITARIA ENTIS', 'RESIDENCIA UNIVERSITARIA LA CASA DEL GIRASOL', 'RESIDENCIA UNIVERSITARIA LARBEL', 'RESIDENCIA UNIVERSITARIA MY HOUSE', 'RESIDENCIA UNIVERSITARIA UADE', 'RESIDENCIA UNIVERSITARIA UNIVERSIS', 'RESIDENCIA UNIVERSITARIA VEDRUNA', 'RESPONSABLE EN VENTAS FREELANCE', 'RESTAURADORA EDILICIA INDEPENDIENTE', 'RESTAURANTE Y PANADERÃA PARTICULAR', 'REVENTA INDEPENDIENTE', 'ROTICERIA', 'ROTICERÃA', 'ROTISERIA', 'ROTISERIA PROPIA Y DESDE 2007 A 2009', 'ROTISERÃA', 'RUBEN APARICIOABOGADO PARTICULAR', 'SALON DE FIESTA LOS LEALES Y HOGAR PARTICULAR', 'SALON UNISEX', 'SANDWICHERIA', 'SANTA MARIA', 'SANTA MARÃA', 'SANTA VICTORIA', 'SANTORIA', 'SASTRERIA', 'SCS Y TECNICO PARTICULAR', 'SEC EXTENSIÃN UNIVERSITARIA FADU Y UBA', 'SECR OBRAS PARTICULARES MUN DE RIVADAVIA', 'SECRETARIA PERSONAL', 'SECRETARIA PRIVADA', 'SECRETARÃA GENERAL', 'SECUNDARIA PUBLICA', 'SECUNDARIA Y UTU', 'SEGURIDAD PRIVADA', 'SEGURO Y GESTORÃA', 'SELECTORA FREE LANCE', 'SELECTORA FREELANCE', 'SEORIGINAL INDEPENDIENTE', 'SERVICE SERGIO GOLER TRABAJO INDEPENDIENTE', 'SERVICIO DE CASAS PARTICULARES', 'SERVICIO DE CATERING INDEPENDIENTE', 'SERVICIO DE NIÃERA', 'SERVICIO JURÃDICO INDEPENDIENTE', 'SERVICIO JURÃDICO PROFESIONAL INDEPENDIENTE', 'SERVICIO PARTICULAR', 'SERVICIO TÃCNICO FREELANCE', 'SERVICIOS INDEPENDIENTE', 'SERVICIOS INFORMATICOS PARTICULARES', 'SERVICIOS PARTICULAR', 'SERVICIOS PARTICULARES', 'SERVICIOS PROFESIONALES INDEPENDIENTE', 'SEÃORIAL', 'SIEMPRE EN PARTICULARES', 'SIN EMPRESA Y Y TRABAJOS PARTICULARES', 'SINDICATO DEL HIELO Y MERCADOS PARTICULARES', 'SOMMELIER FREELANCE', 'SOPORTE IT FREELANCE', 'SOPORTE TÃCNICO FREELANCE', 'SOPORTE TÃCNICO IT INDEPENDIENTE', 'SPORTCASES CHARLAS MOTIVACIONALES FREELANCER', 'SUDIDEAS PRODUCTORA DE CINE INDEPENDIENTE', 'SUSANA COSIMI VIVIENDA PARTICULAR', 'SWISSTECH Y FREELANCE', 'TALLER DE CARTERAS PARTICULAR', 'TALLER DE ZAPATOS PARTICULAR', 'TALLER INDEPENDIENTE DE TAPICERIA', 'TALLER PARTICULAR', 'TALLER PARTICULAR DE CARPINTERIA', 'TALLERES DE COSTURA INDEPENDIENTE', 'TAREAS FREELANCE DE DISEÃO GRÃFICO', 'TAREAS PROFESIONALES EN FORMA INDEPENDIENTE', 'TARJETA PLATA PRÃCTICAS LABORALES NO RENTADAS', 'TAXISTA INDEPENDIENTE', 'TEATRO INDEPENDIENTE', 'TECNICO EN INFORMATICA INDEPENDIENTE', 'TECNICO FREE LANCE', 'TECNICO INDEPENDIENTE', 'TECNICO PARTICULAR', 'TECNICO REPARADOR DE PC INDEPENDIENTE', 'TERAPIA INDIVIDUAL EN CONSULTORIO PARTICULAR', 'TINTORERIA', 'TRABAJA POR CUENTA PROPIA', 'TRABAJADOR INDEPENDIENTE', 'TRABAJADOR INDEPENDIENTE Y FAMILIAR', 'TRABAJADORA INDEPENDIENTE', 'TRABAJE EN MODO FREELANCE CON VARIAS EMPRESAS', 'TRABAJO AUTÃNOMO INDEPENDIENTE', 'TRABAJO DE FOMRA INDEPENDIENTE', 'TRABAJO DE FORMA INDEPENDIENTE', 'TRABAJO DE FORMA INDEPENDIENTE COMO ESTILISTA', 'TRABAJO DE MANERA INDEPENDIENTE', 'TRABAJO DE URBANISMO INDEPENDIENTE', 'TRABAJO ELÃCTRICOS DOMICILIARIOS PARTICULAR', 'TRABAJO EN CASA PARTICULAR', 'TRABAJO EN FORMA INDEPENDIENTE', 'TRABAJO EVENTUAL POR CUENTA PROPIA', 'TRABAJO FREE LANCE', 'TRABAJO FREELANCE', 'TRABAJO INDEPENDIENTE', 'TRABAJO INDEPENDIENTE DE ARREGLO DE PCS', 'TRABAJO INDEPENDIENTE DE MECANICA EN GENERAL', 'TRABAJO INDEPENDIENTE EN MAXIQUIOSCO', 'TRABAJO INDEPENDIENTE EN PARAGUAY', 'TRABAJO INDEPENDIENTE EN SALON DE BELLEZA', 'TRABAJO INDEPENDIENTE GESTION', 'TRABAJO INDEPENDIENTE Y INTERNACIONAL', 'TRABAJO INDEPENDIENTE Y PROYECTISTA', 'TRABAJO INFORMAL DE MANERA PARTICULAR', 'TRABAJO PARTICULAR', 'TRABAJO PARTICULAR DISCONTINUO', 'TRABAJO PARTICULAR EN EVENTOS', 'TRABAJO PARTICULAR FREELANCE', 'TRABAJO POR CUENTA PROPIA', 'TRABAJO POR CUENTA PROPIA EN VENTAS', 'TRABAJO POR CUENTA PROPIA Y SELF EMPLOYED', 'TRABAJO POR CUYENTRA PROPIA', 'TRABAJO POR MI CUENTA', 'TRABAJO POR MI CUENTA MONOTRIBUTISTA', 'TRABAJO PROFESIONAL INDEPENDIENTE', 'TRABAJOS EN CASAS PARTICULARES', 'TRABAJOS EN GENERAL INDEPENDIENTE', 'TRABAJOS EVENTUALES INDEPENDIENTES', 'TRABAJOS FREE LANCE', 'TRABAJOS FREELANCE', 'TRABAJOS INDEPENDIENTE ELETRICIDAD DOMICILIARIA', 'TRABAJOS INDEPENDIENTES', 'TRABAJOS INDEPENDIENTES DE ELEC', 'TRABAJOS INDEPENDIENTES OBRA NUEVA Y REFACCION', 'TRABAJOS PARTICULARES', 'TRABAJOS PARTICULARES DE PLOMERIA', 'TRABAJOS POR MI CUENTA', 'TRADER FREELANCE', 'TRADUCCION FREELANCE', 'TRADUCCIONES EN FORMA FREELANCE', 'TRADUCCIÃNES PARTICULARES DE INGLÃS', 'TRADUCTOR INDEPENDIENTE', 'TRADUCTOR INDEPENDIENTE FREELANCE', 'TRANSPORTE ESCOLAR INDEPENDIENTE', 'TRANSPORTE INDEPENDIENTE', 'TRANSPORTISTA PARTICULAR PARA FROSINONE FRIO SA', 'TRÃMITES DEL AUTOMOTOR SC NEGOCIO FAMILIAR', 'TUS CLASES PARTICULARES', 'TUTOR PARTICULAR DE INGLÃS', 'TUTORIAS PARTICULARES', 'TÃCNICO FREELANCE', 'TÃCNICO INDEPENDIENTE', 'TÃCNICO INFORMÃTICO FREE LANCER', 'TÃCNICO INFORMÃTICO POR CUENTA PROPIA', 'TÃCNICO POR CUENTA PROPIA', 'UNIFINANZAS EMPRESA DE NEGOCIOS INMOBILIARIOS', 'UPWORK FREELANCE COMPANY', 'VENDEDOR INDEPENDIENTE INDUMENTARIA TEXTIL', 'VENDEDORA INDEPENDIENTE', 'VENTA AMBULANTE INDEPENDIENTE', 'VENTA DE ARTICULOS DE PERFUMERIA', 'VENTA DE CALZADO GALERIA ONCE', 'VENTA DE COMIDA INDEPENDIENTE', 'VENTA DE EQUIPOS ELECTRÃNICOS FREELANCE', 'VENTA DE FORMA INDEPENDIENTE', 'VENTA DE INDUMENTARIA', 'VENTA DE INDUMENTARIA EN TRAPITO ABSORBENTE', 'VENTA DE INDUMENTARIA FEMENINA', 'VENTA DE INDUMENTARIA FEMENINA Y ACCESORIOS', 'VENTA DE INDUMENTARIA FEMENINA Y MASCULINA', 'VENTA DE INDUMENTARIA MILITAR', 'VENTA DE INDUMENTARIA ONA GO', 'VENTA DE INDUMENTARIA Y NEGOCIO FAMILIAR', 'VENTA DE LENCERÃA POR INTERNET 2019 PRESENTE', 'VENTA DE MARROQUINERIA', 'VENTA DE NDUMENTARIA FEMENINA', 'VENTA DE PASAJES INDEPENDIENTES', 'VENTA DE PRENDAS INDEPENDIENTE', 'VENTA DE ROPA VARRIAL', 'VENTA EN FERIAS', 'VENTA FREELANCE', 'VENTA INDEPENDIENTE', 'VENTA INDEPENDIENTE DE COSMETICOS POR CATALOGO', 'VENTAS ACEITE COMESTIBLE INDUMENTARIA INFORMAL', 'VENTAS DE INDUMENTARIAS', 'VENTAS INDEPENDIENTE', 'VENTAS INDEPENDIENTES', 'VENTAS PARTICULAR', 'VERDULERIA', 'VERDULERIA NEGOCIO PROPIO', 'VERDULERÃA', 'VERDULERÃA NEGOCIO FAMILIAR', 'VERDULERÃA PROPIA', 'VERLUDERIA', 'VESTUARISTA FREELANCE', 'VETERINARIA', 'VEYERINARIA', 'VIDRIERIA', 'VISUALIZACIÃN ARQUITECTÃNICA FREE LANCE', 'YOLANDA TERESA PERONACE', 'ZAPATARIA', 'ZAPATERIA', 'ZONA DOMICILIARIA', 'ZURKOWSKA MARCA PROPIA')
	THEN 0
	ELSE 1 END AS empresa_valida
FROM aux
-- se numera el group by en lugar de nombrarlo para reducir el tamaÃ±o del script
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13
--</sql>--

-- Se crea tabla de similitud entre empresas de experiencia laboral y empresas de organizaciones
-- --<sql>--DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_tmp_cv_experiencia_laboral_organizacion`;--</sql>--
--<sql>--
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_cv_experiencia_laboral_organizacion" AS
WITH empresas_validas AS (
	SELECT UPPER(empresa_limpia) AS empresa,
		ARRAY_JOIN(ARRAY_SORT(SPLIT(UPPER(empresa_limpia), ' ')), ' ') AS empresa_ordenada,
		length(empresa_limpia) longitud
	FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_cv_experiencia_laboral"
	WHERE empresa_valida = 1
	GROUP BY empresa_limpia
),
organizaciones AS (
	SELECT cuit,
		UPPER(COALESCE(razon_social_new, razon_social_old)) AS razon_social,
		ARRAY_JOIN(ARRAY_SORT(SPLIT(UPPER(COALESCE(razon_social_new, razon_social_old)), ' ')), ' ') AS razon_social_ordenada,
		length(COALESCE(razon_social_new, razon_social_old)) longitud
	FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_organizaciones"
),
uf AS (
SELECT el.empresa,
    --el.empresa_ordenada,
	org.razon_social,
	--org.razon_social_ordenada,
	org.cuit,
	ROW_NUMBER() OVER(
		PARTITION BY el.empresa_ordenada,
		org.razon_social_ordenada
		ORDER BY (
				(
					CAST(greatest(el.longitud, org.longitud) AS DOUBLE) - CAST(
						levenshtein_distance(el.empresa_ordenada, org.razon_social_ordenada) AS DOUBLE
					)
				) / CAST(greatest(el.longitud, org.longitud) AS DOUBLE)
			) DESC
	) AS "orden_duplicado"
FROM empresas_validas el
	JOIN organizaciones org ON (
		(
			(
				CAST(greatest(el.longitud, org.longitud) AS DOUBLE) - CAST(
					levenshtein_distance(el.empresa, org.razon_social) AS DOUBLE
				)
			) / CAST(greatest(el.longitud, org.longitud) AS DOUBLE)
		) >= 0.99
	)
UNION DISTINCT
SELECT el.empresa,
    --el.empresa_ordenada,
	org.razon_social,
	--org.razon_social_ordenada,
	org.cuit,
	ROW_NUMBER() OVER(
		PARTITION BY el.empresa_ordenada,
		org.razon_social_ordenada
		ORDER BY (
				(
					CAST(greatest(el.longitud, org.longitud) AS DOUBLE) - CAST(
						levenshtein_distance(el.empresa_ordenada, org.razon_social_ordenada) AS DOUBLE
					)
				) / CAST(greatest(el.longitud, org.longitud) AS DOUBLE)
			) DESC
	) AS "orden_duplicado"
FROM empresas_validas el
	JOIN organizaciones org ON (
		(
			(
				CAST(greatest(el.longitud, org.longitud) AS DOUBLE) - CAST(
					levenshtein_distance(el.empresa_ordenada, org.razon_social_ordenada) AS DOUBLE
				)
			) / CAST(greatest(el.longitud, org.longitud) AS DOUBLE)
		) >= 0.99
	)
)
SELECT *
FROM uf
--</sql>--



-- Copy of 2023.05.24 step 28 - consume organizaciones.sql 



-- 1.-- Crear tabla def de organizaciones
-- --<sql>--DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_def_organizaciones`;--</sql>--
--<sql>--
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_def_organizaciones" AS
WITH org AS (
SELECT
cuit,
razon_social_old,
razon_social_new,
estado,
ente_gubernamental
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_organizaciones"
UNION
SELECT
CAST(NULL AS VARCHAR) AS cuit,
empresa_limpia,
CAST(NULL AS VARCHAR) AS razon_social_new,
CAST(NULL AS VARCHAR) AS estado,
CASE
    WHEN regexp_like(empresa_limpia ,'MINISTERIO|GOBIERNO|SUBSECRETARÃA|TSJ|GOB|AFIP|RENAPER|INTA|AGIP|PODER JUDICIAL|BANCO CENTRAL|ARBA') THEN 1
    ELSE 0
END ente_gubernamental
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_cv_experiencia_laboral"
WHERE empresa_limpia NOT IN (SELECT empresa FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_cv_experiencia_laboral_organizacion")
OR  empresa_limpia NOT IN (SELECT razon_social FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_cv_experiencia_laboral_organizacion" )
GROUP BY 1,2,3,4,5
ORDER BY 1
)
SELECT
row_number() OVER () AS id_organizacion,
org.*
FROM org
--</sql>--



-- Copy of 2023.05.24 step 29 - consume sector_estrategico.sql 



-- 1.-- Crear tabla def de Sector Estrategico
-- --<sql>--DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_def_sector_estrategico`;--</sql>--
--<sql>--
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_def_sector_estrategico" AS
WITH c AS (
SELECT
id AS codigo_sector_estrategico,
UPPER(nombre) AS sector_estrategico,
tipo
FROM "caba-piba-raw-zone-db"."api_asi_categoria_back"
WHERE tipo IN ('sector_estrategico_asociado')
GROUP BY 1,2,3
)
SELECT
codigo_sector_estrategico,
REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(sector_estrategico,'"',''),'Ã','A'),'Ã','E'),'Ã','I'),'Ã','O'),'Ã','U') AS sector_estrategico
FROM c
ORDER BY 2
--</sql>--



-- Copy of 2023.05.24 step 30 - consume match_sector_estrategico_sector_productivo.sql 



-- 1.-- Crear tabla def de match entre sector estrategico y sector productivo
-- --<sql>--DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_def_match_sector_estrategico_sector_productivo`;--</sql>--
--<sql>--
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_def_match_sector_estrategico_sector_productivo" AS
--match sector estrategico - sector productivo - CLAE
WITH clae AS (
SELECT
cod_actividad_afip_o_naes AS codigo_clae,
cod_actividad_afip_o_naes_string AS codigo_clae_string,
SUBSTRING(cod_actividad_afip_o_naes_string,1,2) AS codigo_corto_clae,
--se crea el campo sector productivo
CASE
    WHEN TRY_CAST(cod_actividad_afip_o_naes AS INT) IN (530010,492230,492290,492221,492229,492250,492190,492170,492280,491200,502200,501200,492240,493200,493110,493120,524290,524190,523020,522099,524210,524110,522092,523090,521030,521020,521010,530090,523039,801010,524230,351201,492210,492160,492150,492180,492140,492110,492130,491120,491110,502101,501100,524130,492120,309100,301100,301200,522091,524120,309900,302000,524220) THEN 'ABASTECIMIENTO Y LOGISTICA'
    WHEN TRY_CAST(cod_actividad_afip_o_naes AS INT) IN (649100,661131,823000,643009,661999,661910,821100,661992,829100,649910,702099,702092,702091,949990,661920,692000,649290,649220,661991,662010,643001,649999,821900,663000,641943,641941,641942,641920,641910,641930,661121,661111,949910,941100,941200,931010,942000,661930,642000,649991,829900,649210,949930,829200) THEN 'ADMINISTRACION, CONTABILIDAD Y FINANZAS'
    WHEN TRY_CAST(cod_actividad_afip_o_naes AS INT) IN (523019,523031,523032,523011) THEN 'ADUANA Y COMERCIO EXTERIOR'
    WHEN TRY_CAST(cod_actividad_afip_o_naes AS INT) IN (524390,524310,303000,511000,524320,524330,512000) THEN 'AERONAUTICA'
    WHEN TRY_CAST(cod_actividad_afip_o_naes AS INT) IN (462110,462132,461014,13,16292,14910,17010,14930,14990,14810,14300,14121,14113,14430,14440,14221,14211,14410,14420,14520,14510,11501,11111,11329,12510,11129,11119,12800,11911,12200,12320,12319,12490,12420,12410,12600,11291,11331,11341,11342,12311,11121,11299,11310,11130,11912,11509,12590,11211,11400,11321,11112,12709,12121,12110,12701,12900,11990,14920,14115,32000,21030,22010,22020,81100,89300,51000,52000,89200,16210,14114,461031,461032,31200,31110,31120,21010,14820,14710,14610,14620,13020,14720,13019,13013,13011,13012,31130,21020,522020,522010,16190,17020,31300,16299,16130,16220,16120,16230,16119,939010,16140,16150,24020,24010) THEN 'AGRICULTURA, GANADERIA, CAZA Y SERVICIOS CONEXOS'
    WHEN TRY_CAST(cod_actividad_afip_o_naes AS INT) IN (822000) THEN 'ATENCION AL CLIENTE, CALL CENTER Y TELEMARKETING'
    WHEN TRY_CAST(cod_actividad_afip_o_naes AS INT) IN (281100,221901,221110,452700,452101,452800,454020,452990,221120,293011,452401,452220,452210,452600,452500,452300,452910,292000,293090,291000) THEN 'AUTOMOTRIZ'
    WHEN TRY_CAST(cod_actividad_afip_o_naes AS INT) IN (465500,464910,466310,466932,463153,463170,464632,464950,466330,464620,464223,466391,464149,464112,466931,464420,464999,464631,464410,466399,466360,463129,463152,463219,463212,463220,464940,463154,464130,463121,462131,463160,463300,466110,466129,465220,466350,464212,464501,464222,465930,464502,465210,465100,463112,464991,463191,464330,469010,464930,462201,464211,464113,465920,464920,462190,462209,464122,469090,466200,465690,465610,464610,465400,465910,465990,465340,465320,465360,465310,465390,465350,463151,464221,466370,453100,463130,464141,466340,464121,464129,463199,464320,466320,464310,466920,466940,466910,466939,466990,463111,464119,464340,463159,462120,464142,464114,464111,464150,463211,461092,461094,461099,461039,461011,461040,461013,461021,461022,461093,461095,461091,461019,461029,461012,463180,463140,475300,476200,475210,478010,477830,474020,476320,475440,475230,475430,477290,477420,477210,477410,477490,475490,475250,475190,477890,453220,472200,477430,472172,477230,477220,472130,475420,473000,475120,475260,453210,476120,476310,474010,472112,477440,472160,477460,475110,472140,477140,477130,477330,476400,476110,477820,475220,475290,477450,475410,477810,477480,477840,472171,476130,475270,453291,453292,472150,475240,477150,477190,472190,477320,472120,477310,472111,478090,477470,477110,472300,477120,471900,471110,471190,471130,471120,479900,479109,479101,451110,451210,454010,451190,451290,465330,771210,771290,774000,771220,773010,771110,771190,773030,773020,772091,773040,772099,773090,772010,771110) THEN 'COMERCIAL, VENTAS Y NEGOCIOS'
    WHEN TRY_CAST(cod_actividad_afip_o_naes AS INT) IN (741000) THEN 'DISEÃO'
    WHEN TRY_CAST(cod_actividad_afip_o_naes AS INT) IN (749009,853100,854960,854920,854950,854910,854940,851020,854930,852100,852200,853201,8,853300,721030,722020,722010,721090,855000,854990,851010) THEN 'EDUCACION, DOCENCIA E INVESTIGACION'
    WHEN TRY_CAST(cod_actividad_afip_o_naes AS INT) IN (920009,900021,591200,591300,931020,591120,900011,591110,931030,182000,900030,910900,910100,939090,900091,592000,910200,920001,939030,939020,931090,931041,931042,900040) THEN 'ENTRETENIMIENTO, ESPECTACULOS Y ACTIVIDADES CULTURALES'
    WHEN TRY_CAST(cod_actividad_afip_o_naes AS INT) IN (791909,561013,552000,551022,551023,551021,551010,562099,561014,561019,551090,791901,791200,791100,561012,561011,561030,562091,562010,561020,561040) THEN 'GASTRONOMIA, HOTELERIA Y TURISMO'
    WHEN TRY_CAST(cod_actividad_afip_o_naes AS INT) IN (432110,431210,439990,432920,439100,433030,429090,410021,410011,421000,429010,422200,431100,432910,432990,332000,711009,711001,433090,433010,422100,431220,432200,433020,439910) THEN 'INGENIERIA CIVIL, ARQUITECTURA Y CONSTRUCCION'
    WHEN TRY_CAST(cod_actividad_afip_o_naes AS INT) IN (432190) THEN 'INGENIERIAS'
    WHEN TRY_CAST(cod_actividad_afip_o_naes AS INT) IN (691002,691001) THEN 'LEGALES/ABOGACIA'
    WHEN TRY_CAST(cod_actividad_afip_o_naes AS INT) IN (331290,331210,331900,16113,16112,813000,812020,331101,331400,331301,331220,949920,812010,811000,812090) THEN 'LIMPIEZA Y MANTENIMIENTO'
    WHEN TRY_CAST(cod_actividad_afip_o_naes AS INT) IN (732000,731001,731009) THEN 'MARKETING Y PUBLICIDAD'
    WHEN TRY_CAST(cod_actividad_afip_o_naes AS INT) IN (351310,351320,89900,81300,62000,72910,71000,72990,89110,89120,72100,61000,192000,466121,351130,351190,351110,351120,91000,99000,711002,353001,81400,360010,360020,352020,352010) THEN 'MINERIA, ENERGIA, PETROLEO, AGUA Y GAS'
    WHEN TRY_CAST(cod_actividad_afip_o_naes AS INT) IN (801090,9,181109,7,433040,952910,952990,952920,952300,742000,749002,749003,749001,16291,12,11,181200) THEN 'OFICIOS Y OTROS'
    WHEN TRY_CAST(cod_actividad_afip_o_naes AS INT) IN (639100,581300,581900,602100,601000,181101,639900,602320,581100,602310,602200,602900,581200) THEN 'PRENSA Y MEDIOS DE COMUNICACIÃN'
    WHEN TRY_CAST(cod_actividad_afip_o_naes AS INT) IN (110100,239421,108000,242010,110420,110492,107301,239422,239410,110300,120091,107991,101030,107110,110491,239591,104020,107410,107420,102002,107999,107309,107129,120099,105090,105020,110290,107930,105030,107121,103012,107912,110411,81200,221909,281900,201300,201210,252000,239593,239399,259302,323001,162902,170910,170990,239310,162901,309200,321020,201220,281201,259992,251101,329030,170202,259309,281400,202907,201140,281301,202320,201120,231010,259910,222010,329040,329020,202906,203000,201110,162100,281500,322001,202312,321011,324000,239201,329010,282300,282500,282901,281600,281700,282909,201130,201409,201180,201190,210010,210020,239510,310020,310010,282200,239391,321012,170201,170102,170101,202200,202311,239209,239100,162903,210090,162909,231090,259999,259993,251102,239900,222090,242090,202908,162300,201401,239202,110412,310030,251200,259991,282110,162202,241009,231020,259100,243100,243200,329090,241001,109000,204000,952200,104013,104011,101040,104012,102001,103091,103099,103030,103011,105010,101020,101099,101012,101011,101040,101091,106131,106120,106110,106139,106200,107200,107930,107911,107920,107992,110211,110212,202101,282120,282130,161001,161002,103020,162201,282400,239592,259200,251300,239600,210030,107500,191000,259301,282600,120010,102003,101013) THEN 'PRODUCCION Y MANUFACTURA (SIN TEXTIL, ELECTRONICA Y AUTOMOTRIZ)'
    WHEN TRY_CAST(cod_actividad_afip_o_naes AS INT) IN (780000) THEN 'RECURSOS HUMANOS Y CAPACITACION'
    WHEN TRY_CAST(cod_actividad_afip_o_naes AS INT) IN (721020,712000,863300,862110,931050,870100,862130,864000,702010,861020,861010,862120,863110,863190,863120,869010,862200,869090,870990,750000,870210,870910,870220,863200,870920,880000,970000) THEN 'SALUD, MEDICINA, FARMACIA Y ASISTENCIA SOCIAL'
    WHEN TRY_CAST(cod_actividad_afip_o_naes AS INT) IN (641100,842400,10,841900,842100,842200,910300,843000,16111,842500,841100,842300,841300,841200) THEN 'SECTOR PUBLICO'
    WHEN TRY_CAST(cod_actividad_afip_o_naes AS INT) IN (653000,652000,662090,651210,662020,651120,651220,651130,651320,651110,651310) THEN 'SEGUROS'
    WHEN TRY_CAST(cod_actividad_afip_o_naes AS INT) IN (390000,381100,381200,382010,382020,370000) THEN 'SERVICIOS DE MANEJO DE RESIDUOS Y DE REMEDIACION'
    WHEN TRY_CAST(cod_actividad_afip_o_naes AS INT) IN (949200,949100,990000) THEN 'SERVICIOS DE ORGANIZACIONES POLITICAS Y RELIGIOSAS'
    WHEN TRY_CAST(cod_actividad_afip_o_naes AS INT) IN (681020,681010,682099,681099,681098,682091,682010) THEN 'SERVICIOS INMOBILIARIOS Y ALQUILER DE BIENES MUEBLES E INTANGIBLES'
    WHEN TRY_CAST(cod_actividad_afip_o_naes AS INT) IN (960910,960201,960202,960990,960101,960300,960102) THEN 'SERVICIOS PERSONALES (SECTOR A INCORPORAR EN TAXONOMIA DE TYP) POSICION 96 DE LA LETRA S (FUENTE: CLAE AFIP)'
    WHEN TRY_CAST(cod_actividad_afip_o_naes AS INT) IN (279000,262000,264000,263000,721010,631200,952100,951200,951100,620200,620100,620300,620900,611010,614010,801020,619000,613000,614090,611090,612000,711003,631190,631120,631110,271020,271010,266010,266090,265101,265200,267002,265102,268000,261000,267001,266010,266090,272000,274000,275091,275020,273110,275099,275092,273190,275010 ) THEN 'TECNOLOGIA, SISTEMAS Y TELECOMUNICACIONES'
    WHEN TRY_CAST(cod_actividad_afip_o_naes AS INT) IN (131300,141130,141140,141120,141110,141191,139209,139204,131132,131131,131139,143010,143020,139900,131202,131209,131201,149000,142000,131120,131110,139400,139202,139203,139201,139100,139300,151100,141191,141199,141201,141202,152040,152011,152031,152021,151200) THEN 'TEXTIL'
END sector_productivo,
--se crea el campo sector estrategico
CASE
    WHEN SUBSTRING(cod_actividad_afip_o_naes_string,1,2)  IN ('29') THEN 'AUTOMOTRIZ'
    WHEN SUBSTRING(cod_actividad_afip_o_naes_string,1,2)  IN ('45','46') THEN 'COMERCIO'
    WHEN SUBSTRING(cod_actividad_afip_o_naes_string,1,2)  IN ('41','42','43') THEN 'CONSTRUCCION'
    WHEN SUBSTRING(cod_actividad_afip_o_naes_string,1,2)  IN ('87','88') THEN 'CUIDADOS DE PERSONA'
    WHEN SUBSTRING(cod_actividad_afip_o_naes_string,1,2)  IN ('26') THEN 'ELECTRONICA'
    WHEN SUBSTRING(cod_actividad_afip_o_naes_string,1,2)  IN ('35') THEN 'ENERGIA'
    WHEN SUBSTRING(cod_actividad_afip_o_naes_string,1,2)  IN ('72','85') THEN 'ENSEÃANZA'
    WHEN SUBSTRING(cod_actividad_afip_o_naes_string,1,2) IN ('56') THEN 'GASTRONOMIA'
    WHEN SUBSTRING(cod_actividad_afip_o_naes_string,1,2)  IN ('55') THEN 'HOTELERIA'
    WHEN SUBSTRING(cod_actividad_afip_o_naes_string,1,2)  IN ('10','11','12','17','18','19','20','21','22','23','24','25','27','28','30','31','32','33') THEN 'INDUSTRIA MANUFACTURERA (SIN TEXTIL, ELECTRONICA Y AUTOMOTRIZ)'
    WHEN SUBSTRING(cod_actividad_afip_o_naes_string,1,2)  IN ('58','59','73','90') THEN 'INDUSTRIAS CREATIVAS'
    WHEN SUBSTRING(cod_actividad_afip_o_naes_string,1,2)  IN ('62','63') THEN 'INFORMATICA'
    WHEN SUBSTRING(cod_actividad_afip_o_naes_string,1,2)  IN ('52','53') THEN 'LOGISTICA'
    WHEN SUBSTRING(cod_actividad_afip_o_naes_string,1,2)  IN ('86') THEN 'SALUD (SIN CUIDADOS DE PERSONAS)'
    WHEN SUBSTRING(cod_actividad_afip_o_naes_string,1,2)  IN ('69','70','71','74','78','82') THEN 'SERVICIOS EMPRESARIALES'
    WHEN SUBSTRING(cod_actividad_afip_o_naes_string,1,2)  IN ('64','65','66') THEN 'SERVICIOS FINANCIEROS'
    WHEN SUBSTRING(cod_actividad_afip_o_naes_string,1,2) IN ('96') THEN 'SERVICIOS PERSONALES (SECTOR A INCORPORAR EN TAXONOMIA DE TYP) POSICION 96 DE LA LETRA S (FUENTE: CLAE AFIP)'
    WHEN SUBSTRING(cod_actividad_afip_o_naes_string,1,2)  IN ('13','14','15') THEN 'TEXTIL'
    WHEN SUBSTRING(cod_actividad_afip_o_naes_string,1,2)  IN ('79','91') THEN 'TURISMO (SIN HOTELERIA Y GASTRONOMIA)'
END sector_estrategico
FROM
(SELECT
TRY_CAST(cod_actividad_afip_o_naes AS INT) AS cod_actividad_afip_o_naes,
cod_actividad_afip_o_naes AS cod_actividad_afip_o_naes_string,
UPPER(desc_actividad_afip_o_naes) AS actividad_clae
FROM "caba-piba-raw-zone-db"."tbp_typ_tmp_nomenclador_actividades_economicas"
) cl
WHERE (cod_actividad_afip_o_naes IS NOT NULL OR LENGTH(TRIM(TRY_CAST(cod_actividad_afip_o_naes AS VARCHAR))) <> 0)
GROUP BY 1,2,3,4,5
),
--Match sector estrategico - sector productivo
asi AS (
SELECT
asi.codigo_sector_estrategico,
--se crea el campo sector productivo
CASE
    WHEN asi.codigo_sector_estrategico IN (15,13) THEN 'ADMINISTRACION, CONTABILIDAD Y FINANZAS'
    WHEN asi.codigo_sector_estrategico IN (4)  THEN 'AUTOMOTRIZ'
    WHEN asi.codigo_sector_estrategico IN (7)  THEN 'COMERCIAL, VENTAS Y NEGOCIOS'
    WHEN asi.codigo_sector_estrategico IN (16) THEN 'EDUCACION, DOCENCIA E INVESTIGACION'
    WHEN asi.codigo_sector_estrategico IN (12) THEN 'ENTRETENIMIENTO, ESPECTACULOS Y ACTIVIDADES CULTURALES'
    WHEN asi.codigo_sector_estrategico IN (9,10,11) THEN 'GASTRONOMIA, HOTELERIA Y TURISMO'
    WHEN asi.codigo_sector_estrategico IN (6) THEN 'INGENIERIA CIVIL, ARQUITECTURA Y CONSTRUCCION'
    WHEN asi.codigo_sector_estrategico IN (8) THEN 'ABASTECIMIENTO Y LOGISTICA'
    WHEN asi.codigo_sector_estrategico IN (5) THEN 'MINERIA, ENERGIA, PETROLEO, AGUA Y GAS'
    WHEN asi.codigo_sector_estrategico IN (1) THEN 'PRODUCCION Y MANUFACTURA (SIN TEXTIL, ELECTRONICA Y AUTOMOTRIZ)'
    WHEN asi.codigo_sector_estrategico IN (17,18) THEN 'SALUD, MEDICINA, FARMACIA Y ASISTENCIA SOCIAL'
    WHEN asi.codigo_sector_estrategico IN (19) THEN 'SERVICIOS PERSONALES (SECTOR A INCORPORAR EN TAXONOMIA DE TYP) POSICION 96 DE LA LETRA S (FUENTE: CLAE AFIP)'
    WHEN asi.codigo_sector_estrategico IN (3,14)  THEN 'TECNOLOGIA, SISTEMAS Y TELECOMUNICACIONES'
    WHEN asi.codigo_sector_estrategico IN (2) THEN 'TEXTIL'
END sector_productivo,
--se crea el campo sector estrategico
asi.sector_estrategico
FROM (
SELECT * FROM "caba-piba-staging-zone-db"."tbp_typ_def_sector_estrategico"
) asi
),
--Match sector estrategico - sector productivo - CLANAE
clanae AS (
SELECT
cln.codigo_4_digitos_clanae,
cln.sector_productivo AS sp_clanae,
--se crea el campo sector productivo
CASE
    WHEN cln.codigo_4_digitos_clanae IN ('52.42','52.41','52.30','52.10','52.20','52.43','50.22','50.12','50.11','50.21','49.32','49.21','49.31','49.12','49.22','49.11','30.92','30.99','30.91','30.30','30.11','30.12','30.20','53.00') THEN 'ABASTECIMIENTO Y LOGISTICA'
    WHEN cln.codigo_4_digitos_clanae IN ('64.11','64.92','64.20','64.99','64.30','64.19','64.91','66.19','66.30','66.20','66.11','70.10','70.20','69.10','69.20','82.91','82.92','82.20','82.99','82.11','82.19','82.30','94.92','94.20','94.99','94.11','94.91','94.12') THEN 'ADMINISTRACION, CONTABILIDAD Y FINANZAS'
    WHEN cln.codigo_4_digitos_clanae IN ('51.10','51.20') THEN 'AERONAUTICA'
    WHEN cln.codigo_4_digitos_clanae IN ('01.27','01.48','01.47','01.14','01.26','01.13','01.29','01.12','01.23','01.62','01.45','01.21','01.61','01.42','01.28','01.41','01.11','01.22','01.70','01.30','01.15','01.44','01.43','01.46','01.24','01.49','01.19','01.25','03.20','03.12','03.11','03.13','02.10','02.20','02.40') THEN 'AGRICULTURA, GANADERIA, CAZA Y SERVICIOS CONEXOS'
    WHEN cln.codigo_4_digitos_clanae IN ('29.10','29.20','29.30') THEN 'AUTOMOTRIZ'
    WHEN cln.codigo_4_digitos_clanae IN ('45.26','45.29','45.25','45.32','45.27','45.21','45.24','45.12','45.28','45.31','45.22','45.23','45.11','45.40','46.53','46.31','46.54','46.42','46.49','46.55','46.59','46.90','46.22','46.52','46.69','46.33','46.56','46.44','46.45','46.32','46.43','46.21','46.63','46.46','46.51','46.61','46.62','46.41','47.21','47.51','47.62','47.78','47.52','47.72','47.22','47.23','47.73','47.74','47.91','47.63','47.19','47.11','47.54','47.64','47.30','47.40','47.61','47.71','47.53','47.80','47.99','77.11','77.20','77.30','77.12','77.40') THEN 'COMERCIAL, VENTAS Y NEGOCIOS'
    WHEN cln.codigo_4_digitos_clanae IN ('85.21','85.33','85.50','85.32','85.49','85.22','85.31','85.10','74.20','74.10','74.90') THEN 'EDUCACION, DOCENCIA E INVESTIGACION'
    WHEN cln.codigo_4_digitos_clanae IN ('59.11','59.13','59.12','59.20','91.03','91.02','91.01','91.09','92.00','90.00','93.10','93.90','18.20') THEN 'ENTRETENIMIENTO, ESPECTACULOS Y ACTIVIDADES CULTURALES'
    WHEN cln.codigo_4_digitos_clanae IN ('79.11','79.12','79.19','55.20','55.10','56.10','56.20') THEN 'GASTRONOMIA, HOTELERIA Y TURISMO'
    WHEN cln.codigo_4_digitos_clanae IN ('42.22','42.21','42.10','42.90','43.30','43.91','43.29','43.11','43.99','43.12','43.22','43.21','71.20','71.10','41.00') THEN 'INGENIERIA CIVIL, ARQUITECTURA Y CONSTRUCCION'
    WHEN cln.codigo_4_digitos_clanae IN ('33.12','33.20','33.14','33.19','33.13','33.11','81.30','81.10','81.20') THEN 'LIMPIEZA Y MANTENIMIENTO'
    WHEN cln.codigo_4_digitos_clanae IN ('73.10','73.20') THEN 'MARKETING Y PUBLICIDAD'
    WHEN cln.codigo_4_digitos_clanae IN ('08.13','08.92','08.14','08.11','08.93','08.99','08.91','08.12','06.20','06.10','07.21','07.29','07.10','35.13','35.12','35.30','35.20','35.11','05.20','05.10','09.10','09.90','36.00') THEN 'MINERIA, ENERGIA, PETROLEO, AGUA Y GAS'
    WHEN cln.codigo_4_digitos_clanae IN ('80.10','18.11','18.12') THEN 'OFICIOS Y OTROS'
    WHEN cln.codigo_4_digitos_clanae IN ('58.13','58.12','58.11','58.19','63.12','63.91','63.11','63.99','60.29','60.10','60.22','60.23','60.21') THEN 'PRENSA Y MEDIOS DE COMUNICACIÃN'
    WHEN cln.codigo_4_digitos_clanae IN ('10.71','10.50','10.90','10.75','10.10','10.30','10.74','10.72','10.62','10.40','10.80','10.79','10.73','10.61','10.20','28.15','28.29','28.17','28.19','28.14','28.26','28.11','28.21','28.22','28.16','28.13','28.12','28.23','28.24','28.25','20.29','20.21','20.11','20.23','20.12','20.22','20.40','20.30','20.14','20.13','32.20','32.90','32.10','32.30','32.40','22.20','22.11','22.19','16.23','16.21','16.10','16.22','16.29','11.04','11.01','11.02','11.03','25.11','25.99','25.92','25.20','25.93','25.91','25.13','25.12','17.01','17.09','17.02','24.10','24.31','24.32','24.20','31.00','23.94','23.99','23.91','23.92','23.95','23.10','23.93','23.96','21.00','19.20','19.10','12.00') THEN 'PRODUCCION Y MANUFACTURA (SIN TEXTIL, ELECTRONICA Y AUTOMOTRIZ)'
    WHEN cln.codigo_4_digitos_clanae IN ('78.00') THEN 'RECURSOS HUMANOS Y CAPACITACION'
    WHEN cln.codigo_4_digitos_clanae IN ('75.00','86.22','86.40','86.21','86.32','86.31','86.33','86.90','86.10','88.01','88.09','97.00','87.02','87.09','87.01') THEN 'SALUD, MEDICINA, FARMACIA Y ASISTENCIA SOCIAL'
    WHEN cln.codigo_4_digitos_clanae IN ('84.22','84.19','84.25','84.12','84.21','84.11','84.23','84.30','84.13','84.24') THEN 'SECTOR PUBLICO'
    WHEN cln.codigo_4_digitos_clanae IN ('65.11','65.13','65.30','65.12','65.20') THEN 'SEGUROS'
    WHEN cln.codigo_4_digitos_clanae IN ('38.11','38.12','38.20','37.00','39.00') THEN 'SERVICIOS DE MANEJO DE RESIDUOS Y DE REMEDIACION'
    WHEN cln.codigo_4_digitos_clanae IN ('99.00') THEN 'SERVICIOS DE ORGANIZACIONES POLITICAS Y RELIGIOSAS'
    WHEN cln.codigo_4_digitos_clanae IN ('68.10','68.20') THEN 'SERVICIOS INMOBILIARIOS Y ALQUILER DE BIENES MUEBLES E INTANGIBLES'
    WHEN cln.codigo_4_digitos_clanae IN ('96.01','96.02','96.03','96.09') THEN 'SERVICIOS PERSONALES (SECTOR A INCORPORAR EN TAXONOMIA DE TYP) POSICION 96 DE LA LETRA S (FUENTE: CLAE AFIP)'
    WHEN cln.codigo_4_digitos_clanae IN ('61.10','61.90','61.20','61.40','61.30','72.20','72.10','95.11','95.29','95.12','95.23','95.22','95.21','27.90','27.50','27.40','27.31','27.10','27.20','62.03','62.02','62.09','62.01','26.80','26.51','26.30','26.70','26.60','26.20','26.40','26.10','26.52') THEN 'TECNOLOGIA, SISTEMAS Y TELECOMUNICACIONES'
    WHEN cln.codigo_4_digitos_clanae IN ('13.11','13.94','13.91','13.92','13.13','13.99','13.12','13.93','14.90','14.11','14.30','14.12','14.20','15.11','15.20','15.12') THEN 'TEXTIL'
END sector_productivo,
--se crea el campo sector estrategico
CASE
    WHEN cln.codigo_4_digitos_clanae IN ('29.10','29.20','29.30') THEN 'AUTOMOTRIZ'
    WHEN cln.codigo_4_digitos_clanae IN ('45.11','45.12','45.21','45.22','45.23','45.24','45.25','45.26','45.27','45.28','45.29','45.31','45.32','45.40','46.21','46.22','46.31','46.32','46.33','46.41','46.42','46.43','46.44','46.45','46.46','46.49','46.51','46.52','46.53','46.54','46.55','46.56','46.59','46.61','46.62','46.63','46.69','46.90') THEN 'COMERCIO'
    WHEN cln.codigo_4_digitos_clanae IN ('41.00','42.10','42.21','42.22','42.90','43.11','43.12','43.21','43.22','43.29','43.30','43.91','43.99') THEN 'CONSTRUCCION'
    WHEN cln.codigo_4_digitos_clanae IN ('87.01','87.02','87.09','88.01','88.09') THEN 'CUIDADOS DE PERSONA'
    WHEN cln.codigo_4_digitos_clanae IN ('72.10','72.20','85.31','85.32','85.33','85.49','85.50') THEN 'ENSEÃANZA'
    WHEN cln.codigo_4_digitos_clanae IN ('26.10','26.20','26.30','26.40','26.51','26.52','26.60','26.70','26.80') THEN 'ELECTRONICA'
    WHEN cln.codigo_4_digitos_clanae IN ('35.13','35.12','35.30','35.20','35.11') THEN 'ENERGIA'
    WHEN cln.codigo_4_digitos_clanae IN ('56.10','56.20')  THEN 'GASTRONOMIA'
    WHEN cln.codigo_4_digitos_clanae IN ('55.10','55.20') THEN 'HOTELERIA'
    WHEN cln.codigo_4_digitos_clanae IN ('62.01','62.02','62.03','62.09','63.11','63.12','63.91','63.99') THEN 'INFORMATICA'
    WHEN cln.codigo_4_digitos_clanae IN ('58.11','58.12','58.13','58.19','59.11','59.12','59.13','59.20','73.10','73.20','90.00') THEN 'INDUSTRIAS CREATIVAS'
    WHEN cln.codigo_4_digitos_clanae IN ('10.10','10.20','10.30','10.40','10.50','10.61','10.62','10.71','10.72','10.73','10.74','10.75','10.79','10.80','10.90','11.01','11.02','11.03','11.04','17.01','17.02','17.09','18.11','18.12','18.20','19.10','19.20','20.11','20.12','20.13','20.14','20.21','20.22','20.23','20.29','20.30','20.40','21.00','22.11','22.19','22.20','23.10','23.91','23.92','23.93','23.94','23.95','23.96','23.99','24.10','24.20','24.31','24.32','25.11','25.12','25.13','25.20','25.91','25.92','25.93','25.99','27.10','27.20','27.31','27.40','27.50','27.90','28.11','28.12','28.13','28.14','28.15','28.16','28.17','28.19','28.21','28.22','28.23','28.24','28.25','28.26','28.29','30.11','30.12','30.20','30.30','30.91','30.92','30.99','31.00','32.10','32.20','32.30','32.40','32.90','33.11','33.12','33.13','33.14','33.19','33.20','12.00') THEN 'INDUSTRIA MANUFACTURERA (SIN TEXTIL, ELECTRONICA Y AUTOMOTRIZ)'
    WHEN cln.codigo_4_digitos_clanae IN ('52.10','52.20','52.30','52.41','52.42','52.43','53.00') THEN 'LOGISTICA'
    WHEN cln.codigo_4_digitos_clanae IN ('69.10','69.20','70.10','70.20','71.10','71.20','74.10','74.20','74.90','78.00','82.11','82.19','82.20','82.30','82.91','82.92','82.99') THEN 'SERVICIOS EMPRESARIALES'
    WHEN cln.codigo_4_digitos_clanae IN ('64.11','64.19','64.20','64.30','64.91','64.92','64.99','65.11','65.12','65.13','65.20','65.30','66.11','66.19','66.20','66.30') THEN 'SERVICIOS FINANCIEROS'
    WHEN cln.codigo_4_digitos_clanae IN ('86.10','86.21','86.22','86.31','86.32','86.33','86.40','86.90') THEN 'SALUD (SIN CUIDADOS DE PERSONAS)'
	WHEN cln.codigo_4_digitos_clanae IN ('96.01','96.02','96.03','96.09') THEN 'SERVICIOS PERSONALES (SECTOR A INCORPORAR EN TAXONOMIA DE TYP) POSICION 96 DE LA LETRA S (FUENTE: CLAE AFIP)'
    WHEN cln.codigo_4_digitos_clanae IN ('13.11','13.12','13.13','13.91','13.92','13.93','13.94','13.99','14.11','14.12','14.20','14.30','14.90','15.11','15.12','15.20') THEN 'TEXTIL'
    WHEN cln.codigo_4_digitos_clanae IN ('79.11','79.12','79.19','91.01','91.02','91.03','91.09') THEN 'TURISMO (SIN HOTELERIA Y GASTRONOMIA)'
END sector_estrategico
FROM (SELECT
	codigo_4_digitos AS codigo_4_digitos_clanae,
	sp.sector_productivo
FROM "caba-piba-raw-zone-db"."mec_codigos_clanae_ctividades_economicas" clae
	JOIN (
		SELECT "letra",
			"codigo_2_digitos",
			"descripcion" AS sector_productivo
		FROM "caba-piba-raw-zone-db"."mec_codigos_clanae_ctividades_economicas"
		WHERE "codigo_3_digitos" IS NULL
			AND "codigo_4_digitos" IS NULL
			AND "codigo_5_digitos" IS NULL
		GROUP BY "letra",
			"codigo_2_digitos",
			"descripcion"
	) sp ON (
		clae.letra = sp.letra
		AND clae.codigo_2_digitos = sp.codigo_2_digitos
	)
WHERE codigo_4_digitos IS NOT NULL
GROUP BY 1,2) cln
),
--Se unifican los sectores productivos y estrategicos (en caso de corresponder) provenientes de CLAE, CLANAE y la tabla de sectores estrategicos definidos por GCBA
--acÃ¡ se rompe!
uf AS (
SELECT
clae.codigo_clae_string AS codigo_clae,
asi.codigo_sector_estrategico,
'' AS codigo_4_digitos_clanae,
CASE
    WHEN clae.sector_productivo IS NULL AND asi.sector_productivo IS NOT NULL THEN asi.sector_productivo
    ELSE clae.sector_productivo
END sector_productivo,
CASE
    WHEN clae.sector_estrategico IS NULL AND asi.sector_estrategico IS NOT NULL THEN asi.sector_estrategico
    ELSE clae.sector_estrategico
END sector_estrategico
FROM clae
FULL OUTER JOIN asi ON (clae.sector_productivo = asi.sector_productivo AND clae.sector_estrategico = asi.sector_estrategico)
UNION
SELECT
NULL AS codigo_clae,
asi.codigo_sector_estrategico,
clanae.codigo_4_digitos_clanae,
CASE
    WHEN clanae.sector_productivo IS NULL THEN asi.sector_productivo
    ELSE clanae.sector_productivo
END sector_productivo,
CASE
    --WHEN asi.sector_estrategico IS NULL THEN clanae.sector_estrategico
    WHEN clanae.sector_estrategico IS NULL THEN asi.sector_estrategico
    ELSE clanae.sector_estrategico
END sector_estrategico
FROM clanae
FULL OUTER JOIN asi ON (clanae.sector_productivo = asi.sector_productivo AND clanae.sector_estrategico = asi.sector_estrategico)
),
--Se agrega el campo "codigo_sector_productivo"
sp AS (
SELECT
uf.codigo_clae,
uf.codigo_sector_estrategico,
CASE WHEN LENGTH(TRIM(uf.codigo_4_digitos_clanae))=0 THEN NULL ELSE uf.codigo_4_digitos_clanae END codigo_4_digitos_clanae,
sp.id_sector_productivo AS codigo_sector_productivo,
uf.sector_productivo,
uf.sector_estrategico
FROM uf
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_sector_productivo" sp ON (uf.sector_productivo = sp.sector_productivo)
WHERE uf.codigo_clae IS NOT NULL
OR (uf.codigo_4_digitos_clanae IS NOT NULL AND LENGTH(TRIM(uf.codigo_4_digitos_clanae))>0)
GROUP BY
uf.codigo_clae,
uf.codigo_sector_estrategico,
CASE WHEN LENGTH(TRIM(uf.codigo_4_digitos_clanae))=0 THEN NULL ELSE uf.codigo_4_digitos_clanae END,
sp.id_sector_productivo,
uf.sector_productivo,
uf.sector_estrategico
),
spf AS (
SELECT
sp.codigo_clae,
codigo_4_digitos_clanae,
CASE
    WHEN sp.codigo_sector_estrategico IS NULL THEN se.codigo_sector_estrategico
    ELSE sp.codigo_sector_estrategico
END codigo_sector_estrategico,
sp.codigo_sector_productivo,
sp.sector_productivo,
sp.sector_estrategico
FROM sp
LEFT JOIN "caba-piba-staging-zone-db".tbp_typ_def_sector_estrategico se ON (sp.sector_estrategico = se.sector_estrategico)
)
SELECT *
FROM spf
--</sql>--



-- Copy of 2023.05.24 step 31 - staging organizacion_actividad.sql 



-- 1.-- Crear tabla tmp de organizacion_actividades
-- --<sql>--DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_tmp_organizacion_actividad`;--</sql>--
--<sql>--
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_organizacion_actividad" AS
--Se agrega la data de codigo y descripcion de actividades al universo de organizaciones
WITH c AS (
SELECT
org.cuit,
ab.codigo_de_actividad,
ab.descripcion_actividad
FROM "caba-piba-staging-zone-db"."tbp_typ_def_organizaciones" org
JOIN "caba-piba-staging-zone-db"."tbp_typ_tmp_registro_laboral_formal_afip_agip_ab" ab
ON (org.cuit = ab.cuit_del_empleador)
WHERE ab.descripcion_actividad IS NOT NULL
GROUP BY 1,2,3
)
SELECT *
FROM c
--</sql>--



-- Copy of 2023.05.24 step 32 - consume actividad_area_de_interes.sql 



-- 1.-- Crear tabla def de actividad/area de interes
-- --<sql>--DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_def_actividad_area_de_interes`;--</sql>--
--<sql>--
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_def_actividad_area_de_interes" AS
--Se realiza un cruce entre la tabla de organizacion_actividades y la tabla match sp y se para obtener el cÃ³digo de sector productivo correspondiente a la actividad clae
WITH c1 AS (
SELECT
org.codigo_de_actividad,
org.descripcion_actividad,
m.codigo_clae,
m.codigo_sector_productivo
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_organizacion_actividad" org
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_match_sector_estrategico_sector_productivo" m ON (org.codigo_de_actividad = TRY_CAST(m.codigo_clae AS INT))
),
--Como se detectan casos de codigo de actividad correspondientes a AGIP (que utiliza subcategorias del CLAE) que no matchean con la tabla match sp y se, entonces se realiza un cruce con la tabla del nomeclador para obtener los codigos CLAE AFIP correspondientes a los codigos SUB CLAE de AGIP y asÃ­ poder matchear mas casos
c3 AS (
SELECT
c1.codigo_de_actividad,
ae.cod_actividad_agip_o_naecba AS codigo_agip,
c1.descripcion_actividad,
CASE
    WHEN TRY_CAST(c1.codigo_clae AS INT) IS NULL THEN TRY_CAST(ae.cod_actividad_afip_o_naes AS INT)
    ELSE  TRY_CAST(c1.codigo_clae AS INT)
END codigo_clae_clean,
c1.codigo_clae,
c1.codigo_sector_productivo
FROM c1
LEFT JOIN "caba-piba-raw-zone-db"."tbp_typ_tmp_nomenclador_actividades_economicas" ae ON (c1.codigo_de_actividad = TRY_CAST(ae.cod_actividad_afip_o_naes AS INT) OR c1.codigo_de_actividad = TRY_CAST(ae.cod_actividad_agip_o_naecba AS INT))
),
c4 AS (
SELECT
CASE
    WHEN c3.codigo_clae_clean IS NULL THEN c3.codigo_de_actividad
    ELSE c3.codigo_clae_clean
END codigo_de_actividad,
c3.descripcion_actividad,
m1.codigo_sector_productivo
FROM c3
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_match_sector_estrategico_sector_productivo" m1 ON (c3.codigo_clae_clean = TRY_CAST(m1.codigo_clae AS INT))
GROUP BY 1,2,3
),
c5 AS (
SELECT
c4.codigo_de_actividad,
c4.descripcion_actividad,
CASE
    WHEN c4.codigo_sector_productivo IS NULL THEN m1.codigo_sector_productivo
    ELSE c4.codigo_sector_productivo
END codigo_sector_productivo,
m1.sector_productivo
FROM c4
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_match_sector_estrategico_sector_productivo" m1 ON (SUBSTRING(CAST(c4.codigo_de_actividad AS VARCHAR),1,4) = REPLACE(m1.codigo_4_digitos_clanae,'.',''))
)
SELECT
c5.codigo_de_actividad,
c5.descripcion_actividad,
c5.codigo_sector_productivo
FROM c5
WHERE c5.codigo_sector_productivo IS NOT NULL
GROUP BY 1,2,3
--</sql>--



-- Copy of 2023.05.24 step 33 - consume puestos.sql 



-- Crear tabla def de puestos
-- --<sql>--DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_def_puestos`;--</sql>--
--<sql>--
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_def_puestos" AS
SELECT
	codigo_de_puesto_desempeniado AS codigo,
	UPPER(descripcion_de_puesto_desempeniado) AS puesto
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_registro_laboral_formal"
WHERE
	descripcion_de_puesto_desempeniado IS NOT NULL
	AND LENGTH(TRIM(descripcion_de_puesto_desempeniado))>0
GROUP BY
	descripcion_de_puesto_desempeniado,
	codigo_de_puesto_desempeniado
ORDER BY
	descripcion_de_puesto_desempeniado
--</sql>--



-- Copy of 2023.05.24 step 34 - staging curriculum.sql 



-- Crear tabla tmp de curriculum
-- --<sql>--DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_tmp_curriculum`;--</sql>--
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
				UPPER('Secundario TÃ©cnico')
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
		UPPER(ls.value) AS estado,
		UPPER(el.value) AS nivel_capacitacion,
		CASE
			UPPER(el.value)
			WHEN 'MAESTRÃA' THEN CASE
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
		JOIN "caba-piba-raw-zone-db"."portal_empleo_education_level_status" ls ON (af.education_level_id = ls.id)
		JOIN "caba-piba-raw-zone-db"."portal_empleo_mtr_education_level" el ON (af.education_average_id = el.id)
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
	COALESCE(MAX(application_date), CURRENT_DATE) fecha_publicacion,
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
	) broker_id
FROM "caba-piba-raw-zone-db"."portal_empleo_curriculum_vitaes" cv
	INNER JOIN "caba-piba-raw-zone-db"."portal_empleo_academic_formations" af ON (af.curriculum_id = cv.id)
	INNER JOIN "caba-piba-raw-zone-db"."portal_empleo_education_level_status" eds ON (eds.id = af.education_level_id)
	INNER JOIN nivel_educactivo_pe ne ON (ne.cv_id = CAST(cv.id AS VARCHAR))
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
	)
UNION
SELECT e.id,
	'CRMEMPLEO',
	COALESCE(MAX(e.fecha_de_entrevista__c), CURRENT_DATE) fecha_publicacion,
	-- assumption ultima fecha de entrevista es la ultima fecha de publicacion del cv
	NULL capacidades_diferentes,
	NULL presentacion,
	e.resumen_de_la_entrevista__c metas,
	CASE
		WHEN el.isdeleted = 'true' THEN 'VIGENTE' ELSE 'NO VIGENTE'
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
	CASE WHEN e.nivel_de_instruccion__c IS NOT NULL AND LENGTH(TRIM(TRY_CAST(e.nivel_de_instruccion__c AS VARCHAR)))>0 THEN
	'GRADUADO' ELSE CAST(NULL AS VARCHAR) END,
	b.id,
	b.genero,
	CAST(b.login2_id AS VARCHAR),
	el.isdeleted
UNION
SELECT COALESCE(ecc.id, NULL),
	'CRMSL',
	COALESCE(MAX(ecc.active_date), MAX(NULL), CURRENT_DATE) fecha_publicacion,
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
	CASE WHEN ne.nivel_educativo IS NOT NULL AND LENGTH(TRIM(TRY_CAST(ne.nivel_educativo AS VARCHAR)))>0 THEN
	'GRADUADO' ELSE CAST(NULL AS VARCHAR) END,
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
	CASE WHEN ne.nivel_educativo IS NOT NULL AND LENGTH(TRIM(TRY_CAST(ne.nivel_educativo AS VARCHAR)))>0 THEN
	'GRADUADO' ELSE CAST(NULL AS VARCHAR) END,
	cc.tipo_discapacidad_c,
	cc.cuil2_c
--</sql>--



-- Copy of 2023.05.24 step 35 - consume curriculum.sql 



-- Crear tabla def de curriculum
-- --<sql>--DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_def_curriculum`;--</sql>--
--<sql>--
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_def_curriculum" AS
SELECT cv.base_origen || cv.cod_origen AS id,
	cv.cod_origen id_old,
	cv.base_origen,
	vec.vecino_id,
	cv.modalidad,
	TRY_CAST(cv.fecha_publicacion AS DATE) fecha_publicacion,
	cv.disponibilidad,
	cv.presentacion,
	cv.estado,
	cv.metas,
	CASE WHEN UPPER(cv.nivel_educativo) LIKE 'OTRO%' THEN 'OTRO' ELSE UPPER(cv.nivel_educativo) END nivel_educativo,
	cv.nivel_educativo_estado,
	cv.capacidades_diferentes tipo_discapacidad
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_curriculum" cv
	JOIN "caba-piba-staging-zone-db"."tbp_typ_def_vecino" vec ON (
		cv.base_origen = vec.base_origen
		AND cv.tipo_doc_broker = vec.tipo_doc_broker
		AND CAST(cv.documento_broker AS VARCHAR) = vec.documento_broker
	)
GROUP BY
	cv.base_origen || cv.cod_origen,
	cv.cod_origen,
	cv.base_origen,
	vec.vecino_id,
	cv.modalidad,
	TRY_CAST(cv.fecha_publicacion AS DATE),
	cv.disponibilidad,
	cv.presentacion,
	cv.estado,
	cv.metas,
	CASE WHEN UPPER(cv.nivel_educativo) LIKE 'OTRO%' THEN 'OTRO' ELSE UPPER(cv.nivel_educativo) END,
	cv.nivel_educativo_estado,
	cv.capacidades_diferentes
--</sql>--



-- Copy of 2023.05.24 step 36 - consume experiencia_laboral.sql 



-- Crear tabla def de experiencia laboral dentro del cv
-- --<sql>--DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_def_cv_experiencia_laboral`;--</sql>--
--<sql>--
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_def_cv_experiencia_laboral" AS
WITH el AS (
SELECT
	cv.id AS curriculum_id,
	e.id_old,
	e.base_origen,
	e.fecha_desde AS fecha_inicio,
	e.fecha_hasta AS fecha_fin,
	e.empresa_limpia AS empresa,
	e.posicion_limpia AS puesto
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



-- Copy of 2023.05.24 step 37 - consume tipo_formacion.sql 



-- Crear tabla def de tipo de formacion
-- --<sql>--DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_def_tipo_formacion`;--</sql>--
--<sql>--
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_def_tipo_formacion" AS
SELECT id AS codigo,
	UPPER(value) AS descripcion
FROM "caba-piba-raw-zone-db"."portal_empleo_mtr_education_level"
ORDER BY value
--</sql>--



-- Copy of 2023.05.24 step 38 - staging formacion_academica.sql 



-- Crear tabla tmp de formaciÃ³n academica
-- --<sql>--DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_tmp_formacion_academica`;--</sql>--
--<sql>--
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_formacion_academica" AS
WITH
-- PORTALEMPLEO es la unica base origen que tiene informacion de carreras o formacion academica
-- (no consideramos cursos como formacion academica)
datos AS (
	SELECT
		CAST(i.id AS VARCHAR) AS codigo,
		'PORTALEMPLEO' base_origen,
		i.start_date fecha_inicio,
		i.end_date fecha_fin,
		UPPER(ls.value) AS estado,
		sa.name AS descripcion,
		inst.name AS institucion_educativa,
		CAST(af.curriculum_id AS VARCHAR) AS cv_id,
		UPPER(el.value) AS tipo_formacion
	FROM "caba-piba-raw-zone-db"."portal_empleo_instructions" i
	JOIN  "caba-piba-raw-zone-db"."portal_empleo_institutions" inst ON (i.institution_id=inst.id)
	JOIN  "caba-piba-raw-zone-db"."portal_empleo_academic_formations" af ON (i.id=af.id AND instruction_type LIKE 'ACADEMIC_FORMATION')
	JOIN  "caba-piba-raw-zone-db"."portal_empleo_study_area" sa ON (i.study_area_id=sa.id)
	JOIN "caba-piba-raw-zone-db"."portal_empleo_education_level_status" ls ON (af.education_level_id=ls.id)
	JOIN "caba-piba-raw-zone-db"."portal_empleo_mtr_education_level" el ON (af.education_average_id=el.id)
	),
cl1 AS (
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
	regexp_replace(regexp_replace(regexp_replace(descripcion,'[^a-zA-Z0-9ÃÃ±ÃÃ¡ÃÃ©ÃÃ­ÃÃ³ÃÃºÃ¼ÃÃ¤ÃÃ²Ã¹ÃÃÃÅ¡\s#\/+\:\|\-\,\(\)\Â°\&\â\Âº\Âª\_\]\[\@;.]+',''),'Ã²','Ã³'),'Ã¹','Ãº') AS descripcion_clean
FROM datos
),

cl2 AS (
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
	CASE
		WHEN cl1.descripcion LIKE '%.%' THEN regexp_replace(cl1.descripcion_clean,'[^a-zA-Z0-9ÃÃ±ÃÃ¡ÃÃ©ÃÃ­ÃÃ³ÃÃºÃ¼ÃÃ¤ÃÃ²Ã¹ÃÃÃÅ¡\s#\/+\:\|\-\,\(\)\Â°\&\â\Âº\Âª\_\]\[\@;]+',' ')
		WHEN regexp_like(cl1.descripcion ,'[a-zA-Z0-9ÃÃ±ÃÃ¡ÃÃ©ÃÃ­ÃÃ³ÃÃºÃ¼ÃÃ¤ÃÃÃÃÅ¡\s?]+\s?\-\-\s?[a-zA-Z0-9ÃÃ±ÃÃ¡ÃÃ©ÃÃ­ÃÃ³ÃÃºÃ¼ÃÃ¤ÃÃÃÃÅ¡\s?]+') THEN regexp_replace(cl1.descripcion_clean,'--','-')
		WHEN cl1.descripcion LIKE '%--%' AND NOT regexp_like(cl1.descripcion ,'[a-zA-Z0-9ÃÃ±ÃÃ¡ÃÃ©ÃÃ­ÃÃ³ÃÃºÃ¼ÃÃ¤ÃÃÃÃÅ¡\s?]+\s?\-\-\s?[a-zA-Z0-9ÃÃ±ÃÃ¡ÃÃ©ÃÃ­ÃÃ³ÃÃºÃ¼ÃÃ¤ÃÃÃÃÅ¡\s?]+') THEN regexp_replace(cl1.descripcion_clean,'-',' ')
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
	CASE
		WHEN cl2.descripcion_clean = '-' THEN regexp_replace(cl2.descripcion_clean,'\-','')
		WHEN cl2.descripcion LIKE '%(' THEN regexp_replace(cl2.descripcion_clean,'\(','')
		WHEN cl2.descripcion LIKE ':%' THEN regexp_replace(cl2.descripcion_clean,'\:','')
		WHEN cl2.descripcion LIKE '%:' THEN regexp_replace(cl2.descripcion_clean,'\:','')
		WHEN cl2.descripcion LIKE '. %' THEN replace(cl2.descripcion_clean,'.','')
		WHEN cl2.descripcion LIKE '- %' THEN regexp_replace(cl2.descripcion_clean,'\-','')
		WHEN cl2.descripcion LIKE '-%' THEN regexp_replace(cl2.descripcion_clean,'\-','')
		WHEN cl2.descripcion LIKE '%-' THEN regexp_replace(cl2.descripcion_clean,'\-','')
		WHEN cl2.descripcion LIKE '%â' THEN regexp_replace(cl2.descripcion_clean,'\â','')
		WHEN cl2.descripcion LIKE 'â%' THEN regexp_replace(cl2.descripcion_clean,'\â','')
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
	descripcion,
	institucion_educativa,
	cv_id,
	tipo_formacion,
	CASE
		WHEN LTRIM(cl3.descripcion_clean) LIKE '%â%' THEN replace(LTRIM(cl3.descripcion_clean),'â','-')
		WHEN UPPER(cl3.descripcion) LIKE '%.NET%' THEN replace(UPPER(LTRIM(cl3.descripcion_clean)),'NET','.NET')
		WHEN UPPER(cl3.descripcion) LIKE '%Âª CATEGORÃA%' THEN regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(UPPER(cl3.descripcion),'1Âª|1.Âª|1. Âª','PRIMERA'),'2Âª|2.Âª|2. Âª','SEGUNDA'),'3Âª|3.Âª|3. Âª','TERCERA'),'4Âª|4.Âª|4. Âª','CUARTA'),'5Âª|5.Âª|5. Âª','QUINTA'),'6Âª|6.Âª|6. Âª','SEXTA'),'7Âª|7.Âª|7. Âª','SEPTIMA'),'8Âª|8.Âª|8. Âª','OCTAVA'),'9Âª|9.Âª|9. Âª','NOVENA'),'10Âª|10.Âª|10. Âª','DECIMA')
		WHEN UPPER(cl3.descripcion) LIKE '%Âª' THEN regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(UPPER(cl3.descripcion),'1Âª|1.Âª|1. Âª','PRIMERA'),'2Âª|2.Âª|2. Âª','SEGUNDA'),'3Âª|3.Âª|3. Âª','TERCERA'),'4Âª|4.Âª|4. Âª','CUARTA'),'5Âª|5.Âª|5. Âª','QUINTA'),'6Âª|6.Âª|6. Âª','SEXTA'),'7Âª|7.Âª|7. Âª','SEPTIMA'),'8Âª|8.Âª|8. Âª','OCTAVA'),'9Âª|9.Âª|9. Âª','NOVENA'),'10Âª|10.Âª|10. Âª','DECIMA')
		WHEN UPPER(cl3.descripcion) LIKE '%Âª GENERACIÃN%' THEN regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(UPPER(cl3.descripcion),'1Âª|1.Âª|1. Âª','PRIMERA'),'2Âª|2.Âª|2. Âª','SEGUNDA'),'3Âª|3.Âª|3. Âª','TERCERA'),'4Âª|4.Âª|4. Âª','CUARTA'),'5Âª|5.Âª|5. Âª','QUINTA'),'6Âª|6.Âª|6. Âª','SEXTA'),'7Âª|7.Âª|7. Âª','SEPTIMA'),'8Âª|8.Âª|8. Âª','OCTAVA'),'9Âª|9.Âª|9. Âª','NOVENA'),'10Âª|10.Âª|10. Âª','DECIMA')
		WHEN UPPER(cl3.descripcion) LIKE '%Âª EDICIÃN%' THEN regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(UPPER(cl3.descripcion),'1Âª|1.Âª|1. Âª','PRIMERA'),'2Âª|2.Âª|2. Âª','SEGUNDA'),'3Âª|3.Âª|3. Âª','TERCERA'),'4Âª|4.Âª|4. Âª','CUARTA'),'5Âª|5.Âª|5. Âª','QUINTA'),'6Âª|6.Âª|6. Âª','SEXTA'),'7Âª|7.Âª|7. Âª','SEPTIMA'),'8Âª|8.Âª|8. Âª','OCTAVA'),'9Âª|9.Âª|9. Âª','NOVENA'),'10Âª|10.Âª|10. Âª','DECIMA')
		ELSE UPPER(LTRIM(cl3.descripcion_clean))
	END descripcion_clean
FROM cl3
)

SELECT *
FROM cl4
ORDER BY cl4.descripcion
--</sql>--



-- Copy of 2023.05.24 step 39 - consume formacion_academica.sql 



-- Crear tabla def de formaciÃ³n academica
--<sql>--
--DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_def_formacion_academica`;
--</sql>--

--<sql>--
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_def_formacion_academica" AS
WITH organizaciones_formadoras AS
	(SELECT
		fa.codigo,
		fa.base_origen,
		COALESCE(org1.id_organizacion, org2.id_organizacion) id_organizacion,
		ROW_NUMBER() OVER(
		PARTITION BY fa.codigo,	fa.base_origen
		ORDER BY org1.razon_social_new, org2.razon_social_old
			) AS "orden_duplicado"
	FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_formacion_academica" fa
	LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_organizaciones" org1 ON (UPPER(fa.institucion_educativa) = org1.razon_social_new)
	LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_organizaciones" org2 ON (UPPER(fa.institucion_educativa) = org2.razon_social_old )
	)
SELECT
	fa.base_origen || fa.codigo AS id,
	fa.codigo AS id_old,
	fa.base_origen,

	CASE
	-- si la fecha de inicio o de fin son null o inconsistentes, se deja ambas fechas en null
		WHEN
			try_cast(fa.fecha_inicio AS date) IS NULL
			OR YEAR(try_cast(fa.fecha_inicio AS date)) < (YEAR(CURRENT_DATE)-100)
			OR YEAR(try_cast(fa.fecha_inicio AS date)) > (YEAR(CURRENT_DATE)+1)
			OR try_cast(fa.fecha_fin AS date) IS NULL
			OR YEAR(try_cast(fa.fecha_fin AS date)) < (YEAR(CURRENT_DATE)-100)
			OR YEAR(try_cast(fa.fecha_fin AS date)) > (YEAR(CURRENT_DATE)+1)
		THEN CAST(NULL AS DATE)

		-- si la fecha de fin es menor a la de inicio se invierten
		WHEN try_cast(fa.fecha_fin AS date)< try_cast(fa.fecha_inicio AS date)
		THEN try_cast(fa.fecha_fin AS date)

		ELSE try_cast(fa.fecha_inicio AS date)
	END fecha_inicio,

	CASE
		-- si la fecha de inicio o de fin son null o inconsistentes, se deja ambas fechas en null
		WHEN
			try_cast(fa.fecha_inicio AS date) IS NULL
			OR YEAR(try_cast(fa.fecha_inicio AS date)) < (YEAR(CURRENT_DATE)-100)
			OR YEAR(try_cast(fa.fecha_inicio AS date)) > (YEAR(CURRENT_DATE)+1)
			OR try_cast(fa.fecha_fin AS date) IS NULL
			OR YEAR(try_cast(fa.fecha_fin AS date)) < (YEAR(CURRENT_DATE)-100)
			OR YEAR(try_cast(fa.fecha_fin AS date)) > (YEAR(CURRENT_DATE)+1)
		THEN CAST(NULL AS DATE)

		-- si la fecha de fin es menor a la de inicio se invierten
		WHEN try_cast(fa.fecha_fin AS date)< try_cast(fa.fecha_inicio AS date)
		THEN try_cast(fa.fecha_inicio AS date)

		ELSE try_cast(fa.fecha_fin AS date)
	END fecha_fin,

	fa.estado,
	fa.descripcion_clean AS descripcion,
	UPPER(fa.institucion_educativa) AS institucion_educativa,
	org.id_organizacion AS id_organizacion,
	cv.id AS codigo_cv,
	tf.codigo AS codigo_tipo_formacion
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_formacion_academica" fa
JOIN "caba-piba-staging-zone-db"."tbp_typ_def_tipo_formacion" tf ON (fa.tipo_formacion=tf.descripcion)
JOIN "caba-piba-staging-zone-db"."tbp_typ_def_curriculum" cv ON (cv.base_origen=fa.base_origen AND cv.id_old = fa.cv_id)
LEFT JOIN organizaciones_formadoras org ON (org.codigo=fa.codigo AND org.base_origen=fa.base_origen AND org.orden_duplicado=1)
GROUP BY
	fa.base_origen || fa.codigo,
	fa.codigo,
	fa.base_origen,
	CASE
	-- si la fecha de inicio o de fin son null o inconsistentes, se deja ambas fechas en null
		WHEN
			try_cast(fa.fecha_inicio AS date) IS NULL
			OR YEAR(try_cast(fa.fecha_inicio AS date)) < (YEAR(CURRENT_DATE)-100)
			OR YEAR(try_cast(fa.fecha_inicio AS date)) > (YEAR(CURRENT_DATE)+1)
			OR try_cast(fa.fecha_fin AS date) IS NULL
			OR YEAR(try_cast(fa.fecha_fin AS date)) < (YEAR(CURRENT_DATE)-100)
			OR YEAR(try_cast(fa.fecha_fin AS date)) > (YEAR(CURRENT_DATE)+1)
		THEN CAST(NULL AS DATE)

		-- si la fecha de fin es menor a la de inicio se invierten
		WHEN try_cast(fa.fecha_fin AS date)< try_cast(fa.fecha_inicio AS date)
		THEN try_cast(fa.fecha_fin AS date)

		ELSE try_cast(fa.fecha_inicio AS date)
	END,

	CASE
		-- si la fecha de inicio o de fin son null o inconsistentes, se deja ambas fechas en null
		WHEN
			try_cast(fa.fecha_inicio AS date) IS NULL
			OR YEAR(try_cast(fa.fecha_inicio AS date)) < (YEAR(CURRENT_DATE)-100)
			OR YEAR(try_cast(fa.fecha_inicio AS date)) > (YEAR(CURRENT_DATE)+1)
			OR try_cast(fa.fecha_fin AS date) IS NULL
			OR YEAR(try_cast(fa.fecha_fin AS date)) < (YEAR(CURRENT_DATE)-100)
			OR YEAR(try_cast(fa.fecha_fin AS date)) > (YEAR(CURRENT_DATE)+1)
		THEN CAST(NULL AS DATE)

		-- si la fecha de fin es menor a la de inicio se invierten
		WHEN try_cast(fa.fecha_fin AS date)< try_cast(fa.fecha_inicio AS date)
		THEN try_cast(fa.fecha_inicio AS date)

		ELSE try_cast(fa.fecha_fin AS date)
	END,
	fa.estado,
	fa.descripcion_clean,
	UPPER(fa.institucion_educativa),
	org.id_organizacion,
	cv.id,
	tf.codigo
--</sql>--



