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
			and nrodoc != ''
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
where dup = 1