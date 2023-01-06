-- Copy of 2023.01.11 step 00 - creacion de vistas (Vcliente).sql 



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



-- Copy of 2023.01.11 step 01 - staging vecinos (Vcliente).sql 



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
		(CASE WHEN (cs.genero_c = 'masculino') THEN 'M' WHEN (cs.genero_c = 'femenino') THEN 'F' ELSE 'X' END) genero_broker,
		NULL nacionalidad,
		NULL descrip_nacionalidad,
		(CASE WHEN (cs.tipo_documento_c = 'dni') THEN 'ARG' ELSE 'NNN' END) nacionalidad_broker,
		(CASE WHEN ((UPPER(co.first_name) IS NULL) OR (("length"(UPPER(co.first_name)) < 3) AND (NOT ("upper"(UPPER(co.first_name)) IN ('BO', 'GE', 'HE', 'LI', 'LU', 'QI', 'WU', 'XI', 'XU', 'YE', 'YI', 'YU')))) OR (UPPER(co.first_name) LIKE '%PRUEBA%')) THEN 0 ELSE 1 END) nombre_valido,
	    (CASE WHEN ((UPPER(co.last_name) IS NULL) OR (("length"(UPPER(co.last_name)) < 3) AND (NOT ("upper"(UPPER(co.last_name)) IN ('AL', 'AM', 'AN', 'BO', U&'B\00D3', 'CO', 'DE', 'DO', 'DU', 'FU', 'GE', 'GO', 'GU', 'HA', 'HE', 'HO', 'HU', 'IM', 'IN', 'IS', 'JI', 'JO', 'JU', 'KE', 'KI', 'KO', 'KU', 'LI', 'LO', 'LU', 'MA', 'MO', 'MU', 'NA', 'NG', 'NI', 'NO', 'OH', 'OU', 'PI', 'PO', 'PY', 'QI', 'QU', 'RA', 'RE', U&'R\00C9', 'RO', 'RU', 'SA', U&'S\00C1', 'SO', 'SU', 'TU', 'UM', 'UZ', 'WU', 'XU', 'YA', 'YE', 'YI', 'YO', 'YU')))) OR (UPPER(co.last_name) LIKE '%PRUEBA%')) THEN 0 ELSE 1 END) apellido_valido,
		CAST(cs.numero_documento_c AS VARCHAR) documento_original
FROM "caba-piba-raw-zone-db"."crm_sociolaboral_contacts" co
INNER JOIN "caba-piba-staging-zone-db"."tbp_typ_tmp_view_crm_sociolaboral_contacts_cstm_no_duplicates" cs ON (co.id = cs.id_c)
WHERE (co.lead_source = 'sociolaboral'
OR ((co.lead_source = 'rib') AND cs.forma_parte_interm_lab_c = 'si'))
AND cs.numero_documento_c IS NOT NULL
GROUP BY cs.tipo_documento_c,
         cs.numero_documento_c,
         cs.genero_c,
         UPPER(co.last_name),
         UPPER(co.first_name),
         co.birthdate

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



-- Copy of 2023.01.11 step 02 - consume vecinos (Vcliente).sql 



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



-- Copy of 2023.01.11 step 03 - consume programa (Vcliente).sql 



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



-- Copy of 2023.01.11 step 04 - staging capacitacion asi (Vcliente).sql 



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



-- Copy of 2023.01.11 step 05 - staging capacitacion (Vcliente).sql 



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



-- Copy of 2023.01.11 step 06 - consume capacitacion (Vcliente).sql 



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



-- Copy of 2023.01.11 step 07 - staging estado_beneficiario_crmsl (Vcliente).sql 



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



-- Copy of 2023.01.11 step 08 - staging estado_beneficiario_sienfo (Vcliente) - cambios 17-1-23.sql 



-- CAMBIOS REALIZADOS: FILA 77 EGRESADOS tambien considera el f0.aprobado 5 |  FILA 227 pasa a estado reprobado despues de 1 mes de finalizado el curso (antes era 2M)
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
		WHEN f0.aprobado IN (1, 3,5) THEN 'EGRESADO' -- Cambio 17/1/23
		WHEN f0.fechabaja IS NOT NULL THEN 'BAJA'
		WHEN f0.baja NOT IN (14, 22, 24, 0)
		AND f0.baja IS NOT NULL THEN 'BAJA'
		WHEN f0.aprobado IN (2, 4, 6, 8) THEN 'REPROBADO' -- Cambio 17/1/23
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
    					WHEN DATE_ADD('month', 1, tf1.fecha_fin) <= CURRENT_DATE THEN 'REPROBADO' --Cambio 17/1/23
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



-- Copy of 2023.01.11 step 08 - staging estado_beneficiario_sienfo (Vcliente).sql 



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
		WHEN f0.aprobado IN (1, 3) THEN 'EGRESADO'
		WHEN f0.fechabaja IS NOT NULL THEN 'BAJA'
		WHEN f0.baja NOT IN (14, 22, 24, 0)
		AND f0.baja IS NOT NULL THEN 'BAJA'
		WHEN f0.aprobado IN (2, 4, 5, 6, 8) THEN 'REPROBADO'
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
    					WHEN DATE_ADD('month', 2, tf1.fecha_fin) <= CURRENT_DATE THEN 'REPROBADO'
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



-- Copy of 2023.01.11 step 09 - staging estado_beneficiario_goet (Vcliente).sql 



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



-- Copy of 2023.01.11 step 10 - staging estado_beneficiario_moodle (Vcliente).sql 



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

SELECT sc.tipo_capacitacion,
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
	sc.id_curso
FROM solo_cursos sc

UNION

SELECT ca.tipo_capacitacion,
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
	ca.id_curso
FROM carreras ca



-- Copy of 2023.01.11 step 11 - staging estado_beneficiario_siu (Vcliente).sql 



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
			WHEN date_add('year', 2, pii2.ultimo_mov) >= CURRENT_DATE THEN 2 -- 'REGULAR'
			WHEN  pii2.ultimo_mov IS NULL AND pii2.fecha_preinscripcion IS NULL THEN 3 -- 'INSCRIPTO'
			ELSE 5
		END AS prioridad_orden
	FROM
		preins_ins2 pii2
	LEFT JOIN
		"caba-piba-staging-zone-db"."goayvd_typ_tmp_siu_cantidad_materias_plan" CMP ON
		cmp.propuesta = pii2.propuesta) a ) resultado
WHERE resultado.orden_duplicado=1
--Ver casos fecha inicio (min(fecha insc cursada)) > fecha_acta (max(fecha de acta)) 1165 casos de 50816



-- Copy of 2023.01.11 step 12 - staging edicion capacitacion (Vcliente).sql 



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
		CAST(chm.IdCentro AS VARCHAR)  cod_origen_establecimiento

	FROM
	"caba-piba-raw-zone-db"."goet_nomenclador" n
	LEFT JOIN "caba-piba-raw-zone-db"."goet_centro_habilitacion_modulos" chm ON n.IdNomenclador = chm.IdNomenclador
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
		chm.IdCentro
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
	' ' cod_origen_establecimiento
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
	dc.descrip_modalidad

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
       CAST(t.id_centro AS VARCHAR) cod_origen_establecimiento
FROM "caba-piba-raw-zone-db"."sienfo_carreras" ca
LEFT JOIN "caba-piba-raw-zone-db"."sienfo_talleres" t ON (t.id_carrera = ca.id_carrera)
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
       CAST(t.id_centro AS VARCHAR) cod_origen_establecimiento
FROM "caba-piba-raw-zone-db"."sienfo_cursos" cu
LEFT JOIN "caba-piba-raw-zone-db"."sienfo_talleres" t ON (t.id_curso = cu.id_curso)
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

       CAST(of.sede AS VARCHAR) cod_origen_establecimiento
FROM "caba-piba-raw-zone-db"."crm_sociolaboral_op_oportunidades_formacion" OF
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
	of.sede

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

	CAST(comi.ubicacion AS VARCHAR)  cod_origen_establecimiento

FROM "caba-piba-raw-zone-db"."siu_toba_3_3_negocio_sga_planes" spl
LEFT JOIN "caba-piba-raw-zone-db"."siu_toba_3_3_negocio_sga_propuestas" spr ON (spl.propuesta = spr.propuesta)
LEFT JOIN "caba-piba-raw-zone-db"."siu_toba_3_3_negocio_sga_comisiones_propuestas" cp ON (cp.propuesta = spr.propuesta)
LEFT JOIN "caba-piba-raw-zone-db"."siu_toba_3_3_negocio_sga_comisiones" comi ON (comi.comision=cp.comision)
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
	comi.ubicacion

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



-- Copy of 2023.01.11 step 13 - consume edicion capacitacion (Vcliente).sql 



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



-- Copy of 2023.01.11 step 14 - staging cursada (Vcliente).sql 



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
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_tmp_estado_beneficiario_moodle" eb ON (eb.id_curso=co.id AND eb.alumno_id=usuario.id)
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
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_vecino" vec ON (vec.base_origen = 'CRMSL' AND vec.documento_broker = CAST(cs.numero_documento_c AS VARCHAR) AND (CASE WHEN (cs.genero_c = 'masculino') THEN 'M' WHEN (cs.genero_c = 'femenino') THEN 'F' ELSE 'X' END = vec.genero_broker))
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



-- Copy of 2023.01.11 step 15 - consume cursada (Vcliente).sql 



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



-- Copy of 2023.01.11 step 16 - consume trayectoria_educativa (Vcliente).sql 



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



