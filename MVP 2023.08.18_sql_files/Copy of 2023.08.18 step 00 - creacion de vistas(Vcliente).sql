--1. RENAPER sin duplicados
--<sql>-- DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_tmp_view_ciudadanos_renaper_no_duplicates`; --</sql>--
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
--<sql>-- DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_tmp_view_crm_sociolaboral_contacts_cstm_no_duplicates`; --</sql>--
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
--<sql>-- DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_tmp_view_goet_usuarios_no_duplicates`; --</sql>--
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
--<sql>-- DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_tmp_view_siu_toba_3_3_negocio_mdp_personas_no_duplicates`; --</sql>--
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
--<sql>-- DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`goayvd_typ_view_sienfo_fichas`; --</sql>--
--<sql>--
 CREATE OR REPLACE VIEW "caba-piba-staging-zone-db"."goayvd_typ_view_sienfo_fichas" AS
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
			and nrodoc != ''
	) f1
WHERE DUP = 1
--</sql>--

--6. SIENFO FICHAS PREINSCRIPCION sin duplicados
--<sql>-- DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`goayvd_typ_view_sienfo_fichas_preinscripcion`; --</sql>--
--<sql>--
CREATE OR REPLACE VIEW "caba-piba-staging-zone-db"."goayvd_typ_view_sienfo_fichas_preinscripcion" AS
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
where dup = 1
--</sql>--

--7. VISTA DE SIU QUE SE UTILIZA PARA CALCULO DE ESTADO DE BENEFICIARIO
--<sql>-- DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`goayvd_typ_tmp_siu_cantidad_materias_plan`; --</sql>--
--<sql>--
CREATE OR REPLACE VIEW "caba-piba-staging-zone-db"."goayvd_typ_tmp_siu_cantidad_materias_plan" AS 
SELECT
  plan
, propuesta
, nombre
, (CASE WHEN ((plan = 79) AND (propuesta = 77)) THEN 15 WHEN ((plan = 101) AND (propuesta = 97)) THEN 15 WHEN ((plan = 102) AND (propuesta = 98)) THEN 15 WHEN ((plan = 103) AND (propuesta = 99)) THEN 15 WHEN ((plan = 85) AND (propuesta = 82)) THEN 24 WHEN ((plan = 92) AND (propuesta = 88)) THEN 22 WHEN ((plan = 93) AND (propuesta = 89)) THEN 22 WHEN ((plan = 94) AND (propuesta = 90)) THEN 22 WHEN ((plan = 95) AND (propuesta = 91)) THEN 22 WHEN ((plan = 96) AND (propuesta = 92)) THEN 22 WHEN ((plan = 97) AND (propuesta = 93)) THEN 22 WHEN ((plan = 98) AND (propuesta = 94)) THEN 22 WHEN ((plan = 100) AND (propuesta = 96)) THEN 22 WHEN ((plan = 99) AND (propuesta = 95)) THEN 22 WHEN ((plan = 6) AND (propuesta = 6)) THEN 15 WHEN ((plan = 84) AND (propuesta = 81)) THEN 33 WHEN ((plan = 50) AND (propuesta = 48)) THEN 31 WHEN ((plan = 121) AND (propuesta = 115)) THEN 20 WHEN ((plan = 122) AND (propuesta = 116)) THEN 20 WHEN ((plan = 120) AND (propuesta = 114)) THEN 20 WHEN ((plan = 119) AND (propuesta = 113)) THEN 20 WHEN ((plan = 54) AND (propuesta = 52)) THEN 26 WHEN ((plan = 53) AND (propuesta = 51)) THEN 27 WHEN ((plan = 56) AND (propuesta = 54)) THEN 27 WHEN ((plan = 72) AND (propuesta = 69)) THEN 27 WHEN ((plan = 86) AND (propuesta = 68)) THEN 27 WHEN ((plan = 22) AND (propuesta = 20)) THEN 21 WHEN ((plan = 35) AND (propuesta = 33)) THEN 31 WHEN ((plan = 78) AND (propuesta = 76)) THEN 22 WHEN ((plan = 124) AND (propuesta = 118)) THEN 22 WHEN ((plan = 68) AND (propuesta = 66)) THEN 22 WHEN ((plan = 123) AND (propuesta = 117)) THEN 22 WHEN ((plan = 27) AND (propuesta = 25)) THEN 29 WHEN ((plan = 40) AND (propuesta = 38)) THEN 26 WHEN ((plan = 36) AND (propuesta = 34)) THEN 35 WHEN ((plan = 110) AND (propuesta = 58)) THEN 32 WHEN ((plan = 111) AND (propuesta = 70)) THEN 32 WHEN ((plan = 71) AND (propuesta = 58)) THEN 32 WHEN ((plan = 136) AND (propuesta = 130)) THEN 23 WHEN ((plan = 55) AND (propuesta = 53)) THEN 23 WHEN ((plan = 57) AND (propuesta = 55)) THEN 23 WHEN ((plan = 106) AND (propuesta = 102)) THEN 23 WHEN ((plan = 118) AND (propuesta = 112)) THEN 33 WHEN ((plan = 58) AND (propuesta = 56)) THEN 33 WHEN ((plan = 66) AND (propuesta = 63)) THEN 30 WHEN ((plan = 51) AND (propuesta = 49)) THEN 30 WHEN ((plan = 62) AND (propuesta = 57)) THEN 30 WHEN ((plan = 67) AND (propuesta = 64)) THEN 30 WHEN ((plan = 70) AND (propuesta = 65)) THEN 30 WHEN ((plan = 52) AND (propuesta = 50)) THEN 30 WHEN ((plan = 80) AND (propuesta = 59)) THEN 30 WHEN ((plan = 65) AND (propuesta = 62)) THEN 30 WHEN ((plan = 75) AND (propuesta = 72)) THEN 22 WHEN ((plan = 88) AND (propuesta = 84)) THEN 22 WHEN ((plan = 74) AND (propuesta = 71)) THEN 22 WHEN ((plan = 107) AND (propuesta = 103)) THEN 23 WHEN ((plan = 2) AND (propuesta = 2)) THEN 14 WHEN ((plan = 31) AND (propuesta = 29)) THEN 28 WHEN ((plan = 135) AND (propuesta = 129)) THEN 19 WHEN ((plan = 115) AND (propuesta = 109)) THEN 19 WHEN ((plan = 133) AND (propuesta = 127)) THEN 19 WHEN ((plan = 134) AND (propuesta = 128)) THEN 19 WHEN ((plan = 125) AND (propuesta = 119)) THEN 27 WHEN ((plan = 18) AND (propuesta = 17)) THEN 27 WHEN ((plan = 19) AND (propuesta = 18)) THEN 27 WHEN ((plan = 126) AND (propuesta = 120)) THEN 27 WHEN ((plan = 20) AND (propuesta = 19)) THEN 30 WHEN ((plan = 47) AND (propuesta = 45)) THEN 27 WHEN ((plan = 8) AND (propuesta = 9)) THEN 12 WHEN ((plan = 9) AND (propuesta = 8)) THEN 13 WHEN ((plan = 82) AND (propuesta = 79)) THEN 23 WHEN ((plan = 109) AND (propuesta = 105)) THEN 23 WHEN ((plan = 81) AND (propuesta = 78)) THEN 23 WHEN ((plan = 105) AND (propuesta = 101)) THEN 23 WHEN ((plan = 137) AND (propuesta = 132)) THEN 23 WHEN ((plan = 83) AND (propuesta = 80)) THEN 23 WHEN ((plan = 130) AND (propuesta = 124)) THEN 23 WHEN ((plan = 34) AND (propuesta = 32)) THEN 29 WHEN ((plan = 45) AND (propuesta = 43)) THEN 28 WHEN ((plan = 7) AND (propuesta = 7)) THEN 15 WHEN ((plan = 33) AND (propuesta = 31)) THEN 24 WHEN ((plan = 15) AND (propuesta = 14)) THEN 37 WHEN ((plan = 11) AND (propuesta = 4)) THEN 28 WHEN ((plan = 5) AND (propuesta = 5)) THEN 18 WHEN ((plan = 13) AND (propuesta = 12)) THEN 28 WHEN ((plan = 12) AND (propuesta = 10)) THEN 28 WHEN ((plan = 41) AND (propuesta = 39)) THEN 41 WHEN ((plan = 42) AND (propuesta = 40)) THEN 34 WHEN ((plan = 44) AND (propuesta = 42)) THEN 29 WHEN ((plan = 29) AND (propuesta = 27)) THEN 25 WHEN ((plan = 23) AND (propuesta = 21)) THEN 34 WHEN ((plan = 76) AND (propuesta = 73)) THEN 31 WHEN ((plan = 64) AND (propuesta = 61)) THEN 31 WHEN ((plan = 63) AND (propuesta = 60)) THEN 31 WHEN ((plan = 46) AND (propuesta = 44)) THEN 32 WHEN ((plan = 10) AND (propuesta = 3)) THEN 15 WHEN ((plan = 139) AND (propuesta = 133)) THEN 18 WHEN ((plan = 108) AND (propuesta = 104)) THEN 18 WHEN ((plan = 48) AND (propuesta = 46)) THEN 30 WHEN ((plan = 89) AND (propuesta = 85)) THEN 35 WHEN ((plan = 30) AND (propuesta = 28)) THEN 41 WHEN ((plan = 132) AND (propuesta = 126)) THEN null WHEN ((plan = 43) AND (propuesta = 41)) THEN 34 WHEN ((plan = 14) AND (propuesta = 13)) THEN 34 WHEN ((plan = 90) AND (propuesta = 86)) THEN 24 WHEN ((plan = 131) AND (propuesta = 125)) THEN null WHEN ((plan = 91) AND (propuesta = 87)) THEN 24 WHEN ((plan = 49) AND (propuesta = 47)) THEN 31 WHEN ((plan = 77) AND (propuesta = 74)) THEN 33 WHEN ((plan = 104) AND (propuesta = 100)) THEN 33 WHEN ((plan = 128) AND (propuesta = 122)) THEN 33 WHEN ((plan = 129) AND (propuesta = 123)) THEN 33 WHEN ((plan = 69) AND (propuesta = 67)) THEN 33 WHEN ((plan = 127) AND (propuesta = 121)) THEN 34 WHEN ((plan = 1) AND (propuesta = 1)) THEN 3 END) cant_materias
, codigo
, version_actual
FROM
  "caba-piba-raw-zone-db"."siu_toba_3_3_negocio_sga_planes"
--</sql>--

--8. VISTA NECESARIA PARA CALCULO DE ESTADO DE BENEFICIARIO DE SIENFO
--<sql>-- DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`goayvd_typ_sienfo_view_cantidad_materias_por_trayecto`; --</sql>--
--<sql>--
CREATE OR REPLACE VIEW "caba-piba-staging-zone-db"."goayvd_typ_sienfo_view_cantidad_materias_por_trayecto" AS
with carrera_29 as ( 
SELECT
    29 as carrera,
	COUNT(*) AS cant_materias_plan_estudio,
	cast(null as int) as anio_inicio,
	cast(null as int) as anio_fin
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
), carrera_30 as ( 
SELECT
    30 as carrera,
	COUNT(*) AS cant_materias_plan_estudio,
	cast(null as int) as anio_inicio,
	cast(null as int) as anio_fin
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
), carrera_31 as ( 
SELECT
    31 as carrera,
	COUNT(*) AS cant_materias_plan_estudio,
	cast(null as int) as anio_inicio,
	cast(null as int) as anio_fin
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
), carrera_32 as ( 
SELECT
    32 as carrera,
	COUNT(*) AS cant_materias_plan_estudio,
	cast(null as int) as anio_inicio,
	cast(null as int) as anio_fin
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
), carrera_1 as (
SELECT
    1 as carrera,
	COUNT(*) AS cant_materias_plan_estudio,
	2019 as ano_inicio,
	cast(null as int) as anio_fin
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
), carrera_1_old as (
SELECT
    1 as carrera,
	COUNT(*) -1 AS cant_materias_plan_estudio_old,
	2016 as ano_inicio_old,
	2018 as anio_fin_old
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
), todas as (
Select
    *
from
    carrera_1
union
Select
    *
from
    carrera_29
union
Select
    *
from
    carrera_30
union
Select
    *
from
    carrera_31
union
Select
    *
from
    carrera_32
)
select
    todas.*,
    carrera_1_old.cant_materias_plan_estudio_old,
    carrera_1_old.ano_inicio_old,
    carrera_1_old.anio_fin_old
from
    todas
left join carrera_1_old on carrera_1_old.carrera = todas.carrera
--</sql>--