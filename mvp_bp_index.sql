-- 2023.03.31 step 00 - creacion de vistas.sql 



--1. RENAPER sin duplicados
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

--2. usuarios CRSML sin duplicados
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
  ROW_NUMBER() OVER(PARTITION BY CONCAT(CAST(c.numero_documento_c AS VARCHAR), c.genero_c) ORDER BY tipo_documento_c ASC)
    AS orden_duplicado
 FROM "caba-piba-raw-zone-db"."crm_sociolaboral_contacts_cstm" c

 ) a
 WHERE a.orden_duplicado=1

--3. usuarios GOET sin duplicados
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

--4. usuarios SIU sin duplicados
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

--5. SIENFO FICHAS sin duplicados
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

--6. SIENFO FICHAS PREINSCRIPCION sin duplicados
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

--7. VISTA DE SIU QUE SE UTILIZA PARA CALCULO DE ESTADO DE BENEFICIARIO
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



-- 2023.03.31 step 01 - consume programa.sql 



-- DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_def_programa`;

-- 19 programas
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



-- 2023.03.31 step 02 - staging establecimiento.sql 



-- 1.-- Se crea tabla tbp_typ_tmp_establecimientos_1 desde los origenes GOET, MOODLE, SIENFO, CRMSL Y SIU
-- DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_tmp_establecimientos_1`;
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

-- 2.-- Se crea tabla con domicilios estandarizados
-- DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.tbp_typ_tmp_establecimientos_domicilios_estandarizados;
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

-- 3.-- Se crea tabla tbp_typ_tmp_establecimientos cruzando tabla de domicilios estandarizados con
-- informacion proveniente de:
-- # https://www.argentina.gob.ar/educacion/evaluacion-e-informacion-educativa/padron-oficial-de-establecimientos-educativos
-- # https://data.educacion.gob.ar/
-- # https://data.buenosaires.gob.ar/dataset/establecimientos-educativos
-- esta tabla serÃ¡ la ultima tabla tmp antes de crear la tabla consume
-- DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_tmp_establecimientos`;
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
	LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_tmp_data_efectores" te ON (
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



-- 2023.03.31 step 03 - consume establecimiento.sql 



-- ESTABLECIMIENTO GOET, MOODLE, SIENFO, CRMSL Y SIU
-- 1.- Crear tabla establecimientos definitiva
-- DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_def_establecimientos`;
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



-- 2023.03.31 step 04 - staging capacitacion asi.sql 



-- 1.- Crear tabla capacitacion
-- DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_tmp_capacitacion_asi`;
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

-- 2.-  Crear tabla aptitudes para cada capacitacion
-- DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_tmp_aptitudes_asi`;
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



-- 2023.03.31 step 05 - staging capacitacion.sql 



-- CRM SOCIOLABORAL - CRMSL
--1.-- Crear tabla capacitaciones socio laboral
-- DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_tmp_crmsl_1`;
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

--2.-- Crear tabla maestro capacitaciones socio laboral
-- A) Si hay alguna capacitacion activa:
-- fecha_inicio => min(fecha de inicio de capacitacion activa)
-- fecha fin => null
-- B) Si NO hay alguna capacitacion activa:
-- fecha_inicio => min(fecha de inicio de capacitaciones)
-- fecha fin => min(fecha de fin de capacitaciones)
-- Si no hay valor para fecha_inicio o fecha_fin quedan en NULL

-- DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_tmp_crmsl_capacitaciones`;
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

--3.-- Crear tabla match capacitaciones socio laboral
-- DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_tmp_crmsl_match`;
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

-- SIENFO
--4.-- Crear tabla capacitaciones sienfo
-- DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_tmp_sienfo_1`;
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

--5.-- Crear tabla maestro capacitaciones sienfo
-- A) Si hay alguna capacitacion activa:
-- fecha_inicio => min(fecha de inicio de capacitacion activa)
-- fecha fin => null
-- B) Si NO hay alguna capacitacion activa:
-- fecha_inicio => min(fecha de inicio de capacitaciones)
-- fecha fin => max(fecha de fin de capacitaciones)
-- Si no hay valor para fecha_inicio o fecha_fin quedan en NULL

-- DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_tmp_sienfo_capacitaciones`;
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

--6.-- Crear tabla match capacitaciones sienfo
-- DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_tmp_sienfo_match`;
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

-- GOET
--7.-- Crear tabla capacitaciones goet
-- DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_tmp_goet_1`;
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

--8.-- Crear tabla maestro capacitaciones goet
-- A) Si hay alguna capacitacion activa:
-- fecha_inicio => min(fecha de inicio de capacitacion activa)
-- fecha fin => null
-- B) Si NO hay alguna capacitacion activa:
-- fecha_inicio => min(fecha de inicio de capacitaciones)
-- fecha fin => max(fecha de fin de capacitaciones)
-- Si no hay valor para fecha_inicio o fecha_fin quedan en NULL

-- DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_tmp_goet_capacitaciones`;
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

--9.-- Crear tabla match capacitaciones goet
-- DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_tmp_goet_match`;
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

-- MOODLE
--10.-- Crear tabla capacitaciones moodle
-- DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_tmp_moodle_1`;
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

--11.-- Crear tabla maestro capacitaciones moodle
-- A) Si hay alguna capacitacion activa:
-- fecha_inicio => min(fecha de inicio de capacitacion activa)
-- fecha fin => null
-- B) Si NO hay alguna capacitacion activa:
-- fecha_inicio => min(fecha de inicio de capacitaciones)
-- fecha fin => max(fecha de fin de capacitaciones)
-- Si no hay valor para fecha_inicio o fecha_fin quedan en NULL

-- DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_tmp_moodle_capacitaciones`;
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

--12.-- Crear tabla match capacitaciones moodle
-- DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_tmp_moodle_match`;
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_moodle_match" AS
SELECT sc.base_origen,
	sc.tipo_capacitacion,
	sc.id AS id_new,
	s1.capacitacion_id AS id_old
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_moodle_1" s1
INNER JOIN "caba-piba-staging-zone-db"."tbp_typ_tmp_moodle_capacitaciones" sc ON (
		sc.tipo_capacitacion = s1.tipo_capacitacion
		AND sc.descrip_normalizada = s1.descrip_normalizada
		);

-- SIU
--13.-- Crear tabla capacitaciones siu
-- DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_tmp_siu_1`;
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

--14.-- Crear tabla maestro capacitaciones siu
-- A) Si hay alguna capacitacion activa:
-- fecha_inicio => min(fecha de inicio de capacitacion activa)
-- fecha fin => null
-- B) Si NO hay alguna capacitacion activa:
-- fecha_inicio => min(fecha de inicio de capacitaciones)
-- fecha fin => max(fecha de fin de capacitaciones)
-- Si no hay valor para fecha_inicio o fecha_fin quedan en NULL

-- DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_tmp_siu_capacitaciones`;
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

--15.-- Crear tabla match capacitaciones siu
-- DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_tmp_siu_match`;
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

-- UNIFICADAS
--16.-- Crear tabla de capacitaciones de origen unificada
-- DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_tmp_capacitacion`;
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



-- 2023.03.31 step 06 - consume capacitacion.sql 



--1.-- Crear tabla de match de capacitaciones unificada
-- DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_def_capacitacion_match`;
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_def_capacitacion_match" AS
SELECT base_origen || '-' || tipo_capacitacion || '-' || CAST(id_new AS VARCHAR) AS id,
	base_origen,
	tipo_capacitacion,
	 id_new,
	id_old
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_crmsl_match"
UNION ALL
SELECT base_origen || '-' || tipo_capacitacion || '-' || CAST(id_new AS VARCHAR) AS id,
	base_origen,
	tipo_capacitacion,
	 id_new,
	id_old
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_sienfo_match"
UNION ALL
SELECT base_origen || '-' || tipo_capacitacion || '-' || CAST(id_new AS VARCHAR) AS id,
	base_origen,
	tipo_capacitacion,
	 id_new,
	id_old
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_goet_match"
UNION ALL
SELECT base_origen || '-' || tipo_capacitacion || '-' || CAST(id_new AS VARCHAR) AS id,
	base_origen,
	tipo_capacitacion,
	 id_new,
	id_old
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_moodle_match"
UNION ALL
SELECT base_origen || '-' || tipo_capacitacion || '-' || CAST(id_new AS VARCHAR) AS id,
	base_origen,
	tipo_capacitacion,
	 id_new,
	id_old
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_siu_match";

--2.--Crear tabla de maestro de capacitaciones unificada
-- DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_def_capacitacion`;
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

-- 3.-  Crear tabla aptitudes para cada capacitacion
-- DROP TABLE `caba-piba-staging-zone-db`.`tbp_typ_def_aptitudes`;
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



-- 2023.03.31 step 07 - staging vecinos.sql 



-- DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_tmp_vecino_1`;
-- 1- Se crea tabla tbp_typ_tmp_vecino_2 con todos los vecinos excepto los provenientes de la base origen IEL
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
GROUP BY nmp.persona,
		 nmtd.desc_abreviada,
		 nmpd.nro_documento,
		 nmp.sexo,
		 nmn.descripcion,
		 nmpd.nro_documento,
		 UPPER(nmp.nombres),
		 UPPER(nmp.apellido),
		 nmp.fecha_nacimiento,
		 nmp.nacionalidad,
		 CAST(nmp.persona AS VARCHAR),
		 CAST(nmp.nacionalidad AS VARCHAR),
		 CAST(nmpd.tipo_documento AS VARCHAR)
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
				CAST(cs.numero_documento_c AS varchar),
				(CASE WHEN (cs.genero_c = 'masculino') THEN 'M' WHEN (cs.genero_c = 'femenino') THEN 'F' ELSE 'X' END),
				(CASE WHEN (cs.tipo_documento_c = 'dni') THEN 'ARG' ELSE 'NN' END)) broker_id_din,
		CONCAT(RPAD(CASE WHEN (cs.tipo_documento_c = 'dni') THEN 'DNI' ELSE 'NN' END,4,' '),
				LPAD(CAST(cs.numero_documento_c AS VARCHAR),11,'0'),
				(CASE WHEN (cs.genero_c = 'masculino') THEN 'M' WHEN (cs.genero_c = 'femenino') THEN 'F' ELSE 'X' END),
				(CASE WHEN (cs.tipo_documento_c = 'dni') THEN 'ARG' ELSE 'NNN' END)) broker_id_est,
		cs.tipo_documento_c tipo_documento,
		(CASE WHEN (cs.tipo_documento_c = 'dni') THEN 'DNI' ELSE 'NN' END) tipo_doc_broker,
		CAST(cs.numero_documento_c AS VARCHAR) documento_broker,
		UPPER(co.first_name) nombre,
		UPPER(co.last_name) apellido,
		co.birthdate fecha_nacimiento,

		CASE
			WHEN cs.genero_c LIKE 'masculino' OR SUBSTRING(CAST(cs.cuil2_c AS VARCHAR),1,2) = '20' THEN 'M'
			WHEN cs.genero_c LIKE 'femenino' OR SUBSTRING(CAST(cs.cuil2_c AS VARCHAR),1,2) = '27' THEN 'F'
			ELSE 'X'
		END genero_broker,

		NULL nacionalidad,
		NULL descrip_nacionalidad,
		(CASE WHEN (cs.tipo_documento_c = 'dni') THEN 'ARG' ELSE 'NNN' END) nacionalidad_broker,
		(CASE WHEN ((UPPER(co.first_name) IS NULL) OR (("length"(UPPER(co.first_name)) < 3) AND (NOT ("upper"(UPPER(co.first_name)) IN ('BO', 'GE', 'HE', 'LI', 'LU', 'QI', 'WU', 'XI', 'XU', 'YE', 'YI', 'YU')))) OR (UPPER(co.first_name) LIKE '%PRUEBA%')) THEN 0 ELSE 1 END) nombre_valido,
	    (CASE WHEN ((UPPER(co.last_name) IS NULL) OR (("length"(UPPER(co.last_name)) < 3) AND (NOT ("upper"(UPPER(co.last_name)) IN ('AL', 'AM', 'AN', 'BO', U&'B\00D3', 'CO', 'DE', 'DO', 'DU', 'FU', 'GE', 'GO', 'GU', 'HA', 'HE', 'HO', 'HU', 'IM', 'IN', 'IS', 'JI', 'JO', 'JU', 'KE', 'KI', 'KO', 'KU', 'LI', 'LO', 'LU', 'MA', 'MO', 'MU', 'NA', 'NG', 'NI', 'NO', 'OH', 'OU', 'PI', 'PO', 'PY', 'QI', 'QU', 'RA', 'RE', U&'R\00C9', 'RO', 'RU', 'SA', U&'S\00C1', 'SO', 'SU', 'TU', 'UM', 'UZ', 'WU', 'XU', 'YA', 'YE', 'YI', 'YO', 'YU')))) OR (UPPER(co.last_name) LIKE '%PRUEBA%')) THEN 0 ELSE 1 END) apellido_valido,
		CAST(cs.numero_documento_c AS VARCHAR) documento_original
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
)
AND cs.numero_documento_c IS NOT NULL
GROUP BY cs.tipo_documento_c,
         cs.numero_documento_c,
         cs.genero_c,
         UPPER(co.last_name),
         UPPER(co.first_name),
         co.birthdate,
		 cuil2_c


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
		NULL fecha_nacimiento,
		COALESCE(do.genero,'X') genero_broker,
		NULL nacionalidad,
		NULL descrip_nacionalidad,
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
				(CASE WHEN pec.doc_type IN ('DNI', 'LC',  'CI', 'LE', 'CUIL') THEN 'ARG' ELSE 'NN' END)) broker_id_din,


		CONCAT(RPAD(CASE
				WHEN (pec.doc_type  IN ('DNI', 'LC',  'CI', 'LE', 'CUIL')) THEN pec.doc_type
				WHEN (pec.doc_type  = 'PAS') THEN 'PE'
				WHEN (pec.doc_type = 'DE') THEN 'CE'
				WHEN (pec.doc_type= 'CRP') THEN 'OTRO' ELSE 'NN' END,4,' '),
				LPAD(CAST(pec.doc_number AS VARCHAR),11,'0'),
				(CASE WHEN (pec.gender = 'M') THEN 'M' WHEN (pec.gender = 'F') THEN 'F' ELSE 'X' END),
				(CASE WHEN (pec.doc_type = 'DNI') THEN 'ARG' ELSE 'NNN' END)) broker_id_est,

		pec.doc_type tipo_documento,

		CASE
			WHEN (pec.doc_type  IN ('DNI', 'LC',  'CI', 'LE', 'CUIL')) THEN pec.doc_type
			WHEN (pec.doc_type  = 'PAS') THEN 'PE'
			WHEN (pec.doc_type = 'DE') THEN 'CE'
			WHEN (pec.doc_type= 'CRP') THEN 'OTRO' ELSE 'NN' END tipo_doc_broker,

		CAST(pec.doc_number AS VARCHAR) documento_broker,
		UPPER(u.name) nombre,
		UPPER(u.lastname) apellido,
		pec.birth_date fecha_nacimiento,
		(CASE WHEN (pec.gender = 'M') THEN 'M' WHEN (pec.gender = 'F') THEN 'F' ELSE 'X' END) genero_broker,
		pec.nationality nacionalidad,
		pec.nationality descrip_nacionalidad,

		(CASE WHEN pec.doc_type IN ('DNI', 'LC',  'CI', 'LE', 'CUIL') THEN 'ARG' ELSE 'NNN' END) nacionalidad_broker,

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
				FROM "caba-piba-raw-zone-db"."portal_empleo_job_hirings")
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
	cea.personbirthdate fecha_nacimiento,
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

-- 2- Se crea la tabla tbp_typ_tmp_vecino_2 con los vecinos de la tabla tbp_typ_tmp_vecino_1 cruzados con broker
-- DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_tmp_vecino_2`;
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

-- 3- Se crea tabla de analisis iel cruzando con tabla tbp_typ_tmp_vecino_2
-- DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_tmp_analisis_iel`;
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


-- 4- Se utilizan las tablas de los pasos 1 y 3 para crear la tabla temporal definitiva: tbp_typ_tmp_vecino
-- DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_tmp_vecino`;
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



-- 2023.03.31 step 08 - consume vecinos.sql 



-- 1.- Crear la tabla definitiva de vecinos
-- DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_def_vecino`;

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



-- 2023.03.31 step 09 - staging estado_beneficiario_crmsl.sql 



-- Query Estado de Beneficiario para athena
-- DROP TABLE `caba-piba-staging-zone-db`.`tbp_typ_tmp_estado_beneficiario_crmsl`;
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



-- 2023.03.31 step 10 - staging estado_beneficiario_sienfo.sql 



-- DROP TABLE `caba-piba-staging-zone-db`.`tbp_typ_tmp_estado_beneficiario_sienfo`;
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
), car_cur AS (
/*
 * SELECT final, estado_beneficiario2 es el calculado con el algoritmo de fechas,
 *  estado de beneficiario se basa en las columnas de la tabla
 */
SELECT
    tf2.codigo_ct,
	tf2.nrodoc,
	tf2.fecha_inc, -- fecha inicio de la persona
	tf2.fecha, -- fecha inicio del curso
	tf2.fecha_fin, -- fecha fin del curso
	'CURSO' tipo_formacion,
	UPPER(tf2.nom_curso) nombre_cur_car, --nombre del curso o materia
	tf2.id_duracion,
	tf2.baja,
	tf2.fechabaja,
	tf2.aprobado,
	UPPER(tf2.estado_beneficiario2) estado_beneficiario,
	-- tf2.estado_beneficiario estado_beneficiario_orig,
	tf2.id_curso,
	tf2.id_carrera,
    tf2.nrodoc ||'-'||tf2.codigo_ct AS llave_doc_idcap
FROM tf2
WHERE tf2.id_carrera = 0 --TOMO SOLO LOS QUE SON CURSOS
UNION (
SELECT
    cal.codigo_ct,
    cal.nrodoc,
    cal.fecha_inc,
    cal.fecha,
	cal.fecha_fin,
	'CARRERA' tipo_formacion,
	UPPER(cal.nom_carrera) nombre_cur_car,
	cal.id_duracion,
	cal.baja,
	cal.fechabaja,
	cal.aprobado,
	cal.estado_beneficiario,
	cal.id_curso,
	cal.id_carrera,
	cal.nrodoc ||'-'||CAST(cal.id_carrera AS VARCHAR) llave_doc_idcap --Esta llave es para agrupar las carreras por ediciÃ³n capacitacion anual
FROM
    carreras_al cal)
--ExISten fechas de inicio del alumno anteriores a fecha de inicio del curso
--nrodoc esta muy sucio
--Los estados de beneficiario2 nulos son cuando aprobado = 9 (nuevo id, no esta en en dump) corresponde a nombre = Actualiza, observa = CETBA [NO SE QUE SIGNIFICA]
)
SELECT
    cuca.codigo_ct,
    cuca.nrodoc,
    cuca.id_curso,
    cuca.id_carrera,
    cuca.estado_beneficiario,
    cuca.llave_doc_idcap, --CUANDO SE HAGA EL JOIN DIFERENCIAR POR CURSO O CARRERA
    cuca.tipo_formacion,
    cuca.nombre_cur_car,
	cuca.fecha fecha_inicio_edicion_capacitacion,
	cuca.fecha_fin fecha_fin_edicion_capacitacion,
	cuca.fecha_inc fecha_inscipcion
FROM
    car_cur cuca



-- 2023.03.31 step 11 - staging estado_beneficiario_goet.sql 



-- DROP TABLE `caba-piba-staging-zone-db`.`tbp_typ_tmp_estado_beneficiario_goet`;
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



-- 2023.03.31 step 12 - staging estado_beneficiario_moodle.sql 



-- DROP TABLE `caba-piba-staging-zone-db`.`tbp_typ_tmp_estado_beneficiario_moodle`;
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



-- 2023.03.31 step 13 - staging estado_beneficiario_siu.sql 



-- DROP TABLE `caba-piba-staging-zone-db`.`tbp_typ_tmp_estado_beneficiario_siu`;
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
					a.comision
					ORDER BY a.prioridad_orden ASC
				) AS "orden_duplicado"
	FROM
	(SELECT
		pii2.persona,
		pii2.alumno,
		pii2.propuesta,
		pii2.plan_version,
		pii2.comision,
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
	LEFT JOIN
		"caba-piba-staging-zone-db"."goayvd_typ_tmp_siu_cantidad_materias_plan" CMP ON
		cmp.propuesta = pii2.propuesta) a ) resultado
WHERE resultado.orden_duplicado=1
--Ver casos fecha inicio (min(fecha insc cursada)) > fecha_acta (max(fecha de acta)) 1165 casos de 50816



-- 2023.03.31 step 14 - staging edicion capacitacion.sql 



-- 1.-- Crear EDICION CAPACITACION GOET, MOODLE, SIENFO Y CRMSL paso 1
-- DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_tmp_edicion_capacitacion_1`;
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

	CAST(comi.comision AS VARCHAR) edicion_capacitacion_id,

	CAST(SPLIT_PART(date_format(pl.fecha_inicio_dictado, '%Y-%m-%d %h:%i%p'), '-', 1)  AS INTEGER) anio_inicio,

	-- El proximo campo es el semestre de inicio de la edicion capacitacion, NO indica que la misma es semestral o anual
	CASE
		WHEN CAST(SPLIT_PART(date_format(pl.fecha_inicio_dictado, '%Y-%m-%d %h:%i%p'), '-', 2)  AS INTEGER)  <= 6 THEN 1
		WHEN CAST(SPLIT_PART(date_format(pl.fecha_inicio_dictado, '%Y-%m-%d %h:%i%p'), '-', 2)  AS INTEGER)  > 6 THEN 2
		ELSE NULL
	END semestre_inicio,

	CAST(DATE_PARSE(date_format(pl.fecha_inicio_dictado, '%Y-%m-%d %h:%i%p'), '%Y-%m-%d %h:%i%p') AS DATE) fecha_inicio_dictado,

	CAST(DATE_PARSE(date_format(pl.fecha_fin_dictado, '%Y-%m-%d %h:%i%p'), '%Y-%m-%d %h:%i%p') AS DATE) fecha_fin_dictado,

	-- se obtiene calculando la primer inscripcion en la edicion
	CAST(DATE_PARSE(date_format(inscriptos.fecha_inicio_inscripcion, '%Y-%m-%d %h:%i%p'), '%Y-%m-%d %h:%i%p') AS DATE) fecha_inicio_inscripcion,

	-- se obtiene calculando la ultima inscripcion en la edicion
	CAST(DATE_PARSE(date_format(inscriptos.fecha_limite_inscripcion, '%Y-%m-%d %h:%i%p'), '%Y-%m-%d %h:%i%p') AS DATE) fecha_limite_inscripcion,

	-- se considera turno mañana si la hora de inicio es entre las 7 y 12, tarde entre 12 y 20, noche entre 20 y 24
	turno_c.nombre turno,

	-- si bien la columna dias.d trae la informacion requerida, es necesario realizar lo siguiente para que los dias queden ordenados
	CASE WHEN position('LUNES' IN UPPER(dias.d))!=0 THEN 'Lunes ' ELSE '' END ||
	CASE WHEN position('MARTES' IN UPPER(dias.d))!=0 THEN 'Martes ' ELSE '' END ||
	CASE WHEN position('MIERCOLES' IN UPPER(dias.d))!=0 THEN 'Miercoles ' ELSE '' END ||
	CASE WHEN position('JUEVES' IN UPPER(dias.d))!=0 THEN 'Jueves ' ELSE '' END ||
	CASE WHEN position('VIERNES' IN UPPER(dias.d))!=0 THEN 'Viernes ' ELSE '' END ||
	CASE WHEN position('SABADO' IN UPPER(dias.d))!=0 THEN 'Sabado ' ELSE '' END ||
	CASE WHEN position('DOMINGO' IN UPPER(dias.d))!=0 THEN 'Domingo ' ELSE '' END dias_cursada,

	CASE WHEN comi.inscripcion_cerrada LIKE 'N'	THEN 'S'
		ELSE 'N'
	END inscripcion_abierta,

	-- Valores posibles de la columna comi.estado: (A)ctivo, (P)endiente, (B)aja
	CASE WHEN comi.estado = 'A'	THEN 'S'
		ELSE 'N'
	END activo,

	-- solo se consideran los alumnos inscriptos y con el estado = A (el estado (P)endiente no se tiene en cuenta)
	CAST(inscriptos.cant_inscriptos AS VARCHAR) cant_inscriptos,

	CAST(comi.cupo AS VARCHAR) vacantes,

	-- La modalidad se toma desde la entidad capacitacion
	dc.modalidad_id,
	dc.descrip_modalidad,
	est.id cod_origen_establecimiento

FROM "caba-piba-raw-zone-db"."siu_toba_3_3_negocio_sga_planes" spl
LEFT JOIN "caba-piba-raw-zone-db"."siu_toba_3_3_negocio_sga_propuestas" spr ON (spl.propuesta = spr.propuesta)
LEFT JOIN "caba-piba-raw-zone-db"."siu_toba_3_3_negocio_sga_comisiones_propuestas" cp ON (cp.propuesta = spr.propuesta)
LEFT JOIN "caba-piba-raw-zone-db"."siu_toba_3_3_negocio_sga_comisiones" comi ON (comi.comision=cp.comision)
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_establecimientos" est ON (est.id_old=CAST(comi.ubicacion AS VARCHAR))
LEFT JOIN "caba-piba-raw-zone-db"."siu_toba_3_3_negocio_sga_periodos_lectivos" pl ON (comi.periodo_lectivo=pl.periodo_lectivo)
LEFT JOIN "caba-piba-raw-zone-db"."siu_toba_3_3_negocio_sga_turnos_cursadas" turno_c ON (turno_c.turno=comi.turno)
LEFT JOIN
		(SELECT c.comision,  array_join(array_agg(DISTINCT(asig.dia_semana)), ',') d
		FROM "caba-piba-raw-zone-db"."siu_toba_3_3_negocio_sga_comisiones" c
		LEFT JOIN "caba-piba-raw-zone-db"."siu_toba_3_3_negocio_sga_comisiones_bh" banda_horaria ON (banda_horaria.comision=c.comision)
		LEFT JOIN "caba-piba-raw-zone-db"."siu_toba_3_3_negocio_sga_asignaciones" asig ON (asig.asignacion=banda_horaria.asignacion)
		GROUP BY c.comision) dias ON (dias.comision=comi.comision)

LEFT JOIN
		(SELECT comision, count(*) AS cant_inscriptos,
		MIN(fecha_inscripcion) fecha_inicio_inscripcion,
		MAX(fecha_inscripcion) fecha_limite_inscripcion
		FROM "caba-piba-raw-zone-db"."siu_toba_3_3_negocio_sga_insc_cursada"
		WHERE estado LIKE 'A'
		GROUP BY comision) inscriptos ON (inscriptos.comision=comi.comision)
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_capacitacion_match" dcmatch ON (dcmatch.id_old = CAST(spl.plan AS VARCHAR) AND dcmatch.base_origen = 'SIU')
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_capacitacion" dc ON (dcmatch.id_new = dc.id_new AND dcmatch.base_origen = dc.base_origen)
GROUP BY
	dcmatch.tipo_capacitacion,
	dcmatch.id_new,
	dcmatch.id_old,
	comi.comision,
	pl.fecha_inicio_dictado,
	pl.fecha_fin_dictado,
	inscriptos.fecha_inicio_inscripcion,
	inscriptos.fecha_limite_inscripcion,
	turno_c.nombre,
	dias.d,
	comi.inscripcion_cerrada,
	comi.estado,
	inscriptos.cant_inscriptos,
	comi.cupo,
	dc.modalidad_id,
	dc.descrip_modalidad,
	est.id

-- 2.-- Crear EDICION CAPACITACION GOET, MOODLE, SIENFO Y CRMSL paso 2
-- se agrega un indice incremental
-- DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_tmp_edicion_capacitacion_2`;
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


-- 3.-- Crear EDICION CAPACITACION GOET, MOODLE, SIENFO Y CRMSL paso 3
-- se corrigen posibles desviasiones en fechas
-- DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_tmp_edicion_capacitacion`;
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



-- 2023.03.31 step 15 - consume edicion capacitacion.sql 



-- EDICION CAPACITACION GOET, MOODLE, SIENFO Y CRMSL
-- 1.- Crear tabla edicion capacitacion definitiva
-- DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_def_edicion_capacitacion`;
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



-- 2023.03.31 step 16 - staging cursada.sql 



-- GOET, MOODLE, SIENFO, CRMSL, SIU
-- ENTIDAD:CURSADA
-- 1.-  Crear tabla temporal de cursadas sin IEL
-- DROP TABLE `caba-piba-staging-zone-db`.`tbp_typ_tmp_cursada_1`;
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
LEFT JOIN "caba-piba-raw-zone-db"."siu_toba_3_3_negocio_mdp_personas_documentos" nmpd ON (nmpd.documento = nmp.documento_principal)
LEFT JOIN  "caba-piba-raw-zone-db"."siu_toba_3_3_negocio_sga_alumnos" alumnos ON (alumnos.persona=nmp.persona)
LEFT JOIN "caba-piba-raw-zone-db"."siu_toba_3_3_negocio_sga_insc_cursada" insc_cur ON (insc_cur.alumno=alumnos.alumno)
LEFT JOIN "caba-piba-raw-zone-db"."siu_toba_3_3_negocio_sga_comisiones" comi ON (comi.comision=insc_cur.comision)
LEFT JOIN "caba-piba-raw-zone-db"."siu_toba_3_3_negocio_sga_comisiones_propuestas" cp ON (comi.comision = cp.comision)
LEFT JOIN "caba-piba-raw-zone-db"."siu_toba_3_3_negocio_sga_planes" spl ON (spl.propuesta = cp.propuesta)
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_capacitacion_match" dcmatch ON (dcmatch.id_old = CAST(spl.plan AS VARCHAR) AND dcmatch.base_origen = 'SIU')

LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_edicion_capacitacion" ed ON
		(ed.edicion_capacitacion_id_old=CAST(comi.comision AS VARCHAR)
		AND ed.base_origen = 'SIU')
LEFT JOIN "caba-piba-raw-zone-db"."siu_toba_3_3_negocio_sga_periodos_lectivos" pl ON (comi.periodo_lectivo=pl.periodo_lectivo)
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
	CASE WHEN eb.comision  IS NOT NULL

	THEN insc_cur.comision=eb.comision AND eb.alumno=alumnos.alumno AND eb.plan_version=alumnos.plan_version

	-- cuando el estado es preinscripto no se puede obtener la comision, porque nunca se inscribio, por lo tanto
	-- solo se tendrÃ¡n las capacitaciones a las que se preinscribio y no habrÃ¡ ningun dato asociado a la edicion capacitacion
	ELSE eb.persona=alumnos.persona AND eb.propuesta=cp.propuesta AND eb.estado_beneficiario LIKE 'PREINSCRIPTO'
	END
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


-- 2.-  Crear tabla temporal incluyendo IEL
-- DROP TABLE `caba-piba-staging-zone-db`.`tbp_typ_tmp_cursada`;
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



-- 2023.03.31 step 17 - consume cursada.sql 



-- EDICION CURSADA GOET, MOODLE, SIENFO Y CRMSL
-- 1.- Crear tabla edicion cursada definitiva
-- DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_def_cursada`;
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



-- 2023.03.31 step 18 - consume trayectoria_educativa.sql 



-- TRAYECTORIA EDUCATIVA GOET, MOODLE, SIENFO Y CRMSL
-- 1.- Crear tabla trayectoria educativa definitiva
-- DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_def_trayectoria_educativa`;
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_def_trayectoria_educativa" AS
SELECT CAST(row_number() OVER () AS VARCHAR) AS cursado_id,
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
       dcu.estado_beneficiario estado
       FROM "caba-piba-staging-zone-db"."tbp_broker_def_broker_general" bg
       INNER JOIN "caba-piba-staging-zone-db"."tbp_typ_def_vecino" vec ON (bg.id = vec.broker_id)
       LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_cursada" dcu ON (dcu.broker_id = vec.broker_id)
	   LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_edicion_capacitacion" ed ON (ed.id = dcu.edicion_capacitacion_id_new AND ed.base_origen = dcu.base_origen)
       LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_capacitacion" dc ON (dc.id_new = ed.capacitacion_id_new AND dc.base_origen = ed.base_origen)
	   LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_programa" p ON (p.programa_id = dc.programa_id)
WHERE dc.base_origen != 'MOODLE' AND dcu.estado_beneficiario IS NOT NULL
GROUP BY bg.id,
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



-- 2023.03.31 step 19 - staging oportunidad_laboral.sql 



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
-- Sector Productivo => ABASTECIMIENTO Y LOGISTICA, ADMINISTRACION, CONTABILIDAD Y FINANZAS, COMERCIAL, VENTAS Y NEGOCIOS, GASTRONOMIA, HOTELERIA Y TURISMO, HIPODROMO, OFICIOS Y OTROS, PRODUCCION Y MANUFACTURA, SALUD, MEDICINA Y FARMACIA, SECTOR PUBLICO
-- Nota: la tabla deberÃ¡ estar relacionada con la entidad "Registro laboral formal" si tomo el empleo
-- y con la entidad "Programa"

-- DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_tmp_oportunidad_laboral`;
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
    WHEN regexp_like(etf.sector_productivo,'Contabilidad|Recursos Humanos y CapacitaciÃ³n|Secretarias y RecepciÃ³n|AdministraciÃ³n, Contabilidad y Finanzas|Gerencia y DirecciÃ³n General') THEN 'ADMINISTRACION, CONTABILIDAD Y FINANZAS'
    WHEN regexp_like(etf.sector_productivo,'Aduana y Comercio Exterior|Marketing y Publicidad|AtenciÃ³n al Cliente, Call Center y Telemarketing|Ventas|Comercial, Ventas y Negocios|Comercial') THEN 'COMERCIAL, VENTAS Y NEGOCIOS'
    WHEN regexp_like(etf.sector_productivo,'ComunicaciÃ³n, Relaciones Institucionales y PÃºblicas')  THEN 'COMUNICACION, RELACIONES INSTITUCIONALES Y PUBLICAS'
    WHEN regexp_like(etf.sector_productivo,'DiseÃ±o') THEN 'DISEÃO'
    WHEN regexp_like(etf.sector_productivo,'EducaciÃ³n, Docencia e InvestigaciÃ³n') THEN 'EDUCACION, DOCENCIA E INVESTIGACION'
    WHEN regexp_like(etf.sector_productivo,'HipÃ³dromo de Palermo') THEN 'HIPODROMO'
    WHEN regexp_like(etf.sector_productivo,'IngenierÃ­a Civil, Arquitectura y ConstrucciÃ³n') THEN 'INGENIERIA CIVIL, ARQUITECTURA Y CONSTRUCCION'
    WHEN regexp_like(etf.sector_productivo,'Legales/AbogacÃ­a') THEN 'LEGALES/ABOGACIA'
    WHEN regexp_like(etf.sector_productivo,'MinerÃ­a, EnergÃ­a, PetrÃ³leo y Gas') THEN 'MINERIA, ENERGIA, PETROLEO Y GAS'
    WHEN regexp_like(etf.sector_productivo,'Oficios y Otros') THEN 'OFICIOS Y OTROS'
    WHEN regexp_like(etf.sector_productivo,'IngenierÃ­as|ProducciÃ³n y Manufactura') THEN 'PRODUCCION Y MANUFACTURA'
    WHEN regexp_like(etf.sector_productivo,'Salud, Medicina y Farmacia') THEN 'SALUD, MEDICINA Y FARMACIA'
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



-- 2023.03.31 step 20 - consume oportunidad_laboral.sql 



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
-- Sector Productivo => ABASTECIMIENTO Y LOGISTICA, ADMINISTRACION, CONTABILIDAD Y FINANZAS, COMERCIAL, VENTAS Y NEGOCIOS, GASTRONOMIA, HOTELERIA Y TURISMO, HIPODROMO, OFICIOS Y OTROS, PRODUCCION Y MANUFACTURA, SALUD, MEDICINA Y FARMACIA, SECTOR PUBLICO
-- Nota: la tabla deberÃ¡ estar relacionada con la entidad "Registro laboral formal" si tomo el empleo
-- y con la entidad "Programa"

-- DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_def_oportunidad_laboral`;
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



-- 2023.03.31 step 21 - staging sector_productivo.sql 



-- 1.-- Crear tabla tmp de Sector_Productivo
-- DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_tmp_sector_productivo`;
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_sector_productivo" AS
WITH sp AS (
SELECT sector_productivo
FROM "caba-piba-staging-zone-db".tbp_typ_tmp_oportunidad_laboral
GROUP BY sector_productivo
)
SELECT
REPLACE(REPLACE(sp.sector_productivo,'Ã','I'),'Ã','O') AS sector_productivo
FROM sp
WHERE sp.sector_productivo NOT LIKE ''
GROUP BY REPLACE(REPLACE(sp.sector_productivo,'Ã','I'),'Ã','O')
ORDER BY REPLACE(REPLACE(sp.sector_productivo,'Ã','I'),'Ã','O')



-- 2023.03.31 step 22 - consume sector_productivo.sql 



-- 1.-- Crear la tabla def de Sector_Productivo
-- DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_def_sector_productivo`;
-- CAMPOS REQUERIDOS EN TABLA DEF SEGUN MODELO
-- CÃ³digo (1+)
-- Sector Productivo => ABASTECIMIENTO Y LOGISTICA, ADMINISTRACION, CONTABILIDAD Y FINANZAS, COMERCIAL, VENTAS Y NEGOCIOS, GASTRONOMIA, HOTELERIA Y TURISMO, HIPODROMO, OFICIOS Y OTROS, PRODUCCION Y MANUFACTURA, SALUD, MEDICINA Y FARMACIA, SECTOR PUBLICO
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_def_sector_productivo" AS
WITH sp AS (
SELECT sector_productivo
FROM "caba-piba-staging-zone-db".tbp_typ_tmp_oportunidad_laboral
GROUP BY sector_productivo
ORDER BY sector_productivo
)
SELECT
row_number() OVER () AS id_sector_productivo,
REPLACE(REPLACE(sp.sector_productivo,'Ã','I'),'Ã','O') AS sector_productivo
FROM sp
WHERE sp.sector_productivo NOT LIKE ''
GROUP BY REPLACE(REPLACE(sp.sector_productivo,'Ã','I'),'Ã','O')
ORDER BY id_sector_productivo



-- 2023.03.31 step 23 - staging registro_laboral_formal.sql 



-- 1.-- Crear REGISTRO LABORAL SIN CRUCE AGIP/AFIP
-- DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_tmp_registro_laboral_formal_1`;
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

-- 2.-- Crear REGISTRO LABORAL AFIP AGIP ALTAS BAJAS
--Campo descripcion_modalidad_contratacion. Existen casos con mas de una descripciÃ³n para un mismo codigo de modalidad de contrataciÃ³n. Los mismos provienen de la tabla "afip_agip_tipo_contratacion". Se optÃ³ por elegÃ­r una Ãºnica descripciÃ³n basada estrictamente en la descripciÃ³n que figura en la tabla de modalidades de contrataciÃ³n proveniente de la web de AFIP
-- DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_tmp_registro_laboral_formal_afip_agip_ab`;
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
		ab.fecha_inicio_de_relacion_laboral,
		ab.fecha_fin_de_relacion_laboral,
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
		    WHEN aa.desc_actividad_afip_o_naes IS NULL OR LENGTH(TRIM(aa.desc_actividad_afip_o_naes)) = 0 THEN ba.desc_actividad_naecba
		    ELSE aa.desc_actividad_afip_o_naes
		END descripcion_actividad,

		CASE
		    WHEN (aa.desc_actividad_afip_o_naes IS NULL OR LENGTH(TRIM(aa.desc_actividad_afip_o_naes)) = 0)
			AND LENGTH(TRIM(ba.desc_actividad_naecba)) > 0
				THEN 'AGIP-CLAE'

			WHEN (ba.desc_actividad_naecba IS NULL OR LENGTH(TRIM(ba.desc_actividad_naecba)) = 0)
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
			FROM "caba-piba-staging-zone-db".tbp_typ_tmp_nomenclador_actividades_economicas
			ORDER BY cod_actividad_afip_o_naes
		) aa ON (
			TRY_CAST(ab.codigo_de_actividad AS INT) = TRY_CAST(aa.cod_actividad_afip_o_naes AS INT)
		)
		LEFT JOIN (
			SELECT cod_actividad_naecba,
                desc_actividad_naecba
			FROM "caba-piba-staging-zone-db".tbp_typ_tmp_nomenclador_actividades_economicas
			ORDER BY cod_actividad_naecba
		) ba ON (
			TRY_CAST(ab.codigo_de_actividad AS INT) = TRY_CAST(ba.cod_actividad_naecba AS INT)
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

-- 3.-- Crear REGISTRO LABORAL FORMAL CON CRUCE AGIP/AFIP
-- DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_tmp_registro_laboral_formal`;
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

-- 4.-- Crear REGISTRO LABORAL FORMAL CON CRUCE AGIP/AFIP Y VECINOS
-- DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_tmp_registro_laboral_formal_sin_ol`;
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

-- 5.-- Crear REGISTRO LABORAL FORMAL COMPLETO CON ATRIBUTO BASE_ORIGEN
-- DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_tmp_registro_laboral_formal_completa`;
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



-- 2023.03.31 step 24 - consume_registro_laboral_formal.sql 



-- 1.-- Crear la tabla definitiva de REGISTRO LABORAL FORMAL
-- DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_def_registro_laboral_formal`;
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

-- 2.-- Crear la tabla definitiva N-N de OPORTUNIDAD LABORAL - REGISTRO LABORAL FORMAL
-- DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_def_oportunidad_laboral_registro_laboral_formal`;
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



