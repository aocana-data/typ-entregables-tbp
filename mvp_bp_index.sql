-- 2022.10.25_TyP_consume (Vcliente).sql 



-- 1.- Creación de la vista programa en base al catálogo de ASI
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


-- 2.- Creación de la vista capacitación en base al catálogo de ASI
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_def_capacitacion" AS
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

-- 3.- Creación de la tabla de dominio de aptitudes
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_def_aptitudes" AS
SELECT c.id capacitacion_id,
       c.codigo codigo_capacitacion,
       ac.aptitud_id,
       a.nombre descrip_aptitud
FROM "caba-piba-raw-zone-db"."api_asi_capacitacio	n" c
INNER JOIN "caba-piba-raw-zone-db"."api_asi_aptitud_capacitacion" ac
ON (ac.capacitacion_id = c.id)
INNER JOIN "caba-piba-raw-zone-db"."api_asi_aptitud" a
ON (ac.aptitud_id = a.id)

-- 4.- Creación de la tabla de Aptitudes necesarias para una capacitación determinada
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_def_edicion_capacitacion" AS
SELECT ec.id edicion_capcitacion_id,
       ec.capacitacion_id,
       ec.codigo codigo_edicion_capacitacion,
       ec.institucion_id,
       ec.modalidad,
       ec.fecha_inicio_inscripcion,
       ec.fecha_limite_inscripcion,
       ec.fecha_inicio_cursada,
       ec.dias_cursada,
       ec.turno,
       ec.inscripcion_abierta,
       ec.activo,
       p.base_origen
FROM "caba-piba-raw-zone-db"."api_asi_edicion" ec
INNER JOIN "caba-piba-staging-zone-db"."tbp_typ_def_capacitacion" c ON (c.capacitacion_id = ec.capacitacion_id)
INNER JOIN "caba-piba-staging-zone-db"."tbp_typ_def_programa" p ON (p.programa_id = c.programa_id)

-- 5.- Creacion de tabla Vecinos con duplicados por base REVISAR
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_def_vecino" AS
SELECT vec.broker_id_din||'-'||vec.base_origen vecino_id,
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
LEFT JOIN "caba-piba-staging-zone-db".tbp_broker_def_broker_general bo ON (bo.id = vec.broker_id_din)
LEFT JOIN "caba-piba-staging-zone-db".tbp_broker_def_broker_general DO ON (CAST(do.documento_broker AS VARCHAR) = vec.documento_broker AND do.id != vec.broker_id_din)
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
		 CAST(do.documento_broker AS VARCHAR)



-- 2022.10.25_TyP_staging (Vvliente).sql 



--1.- Creación de la tabla VECINOS en forma temporal previa a la validación BROKER/SINTYS
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_vecino" AS
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
            WHEN nmtd.desc_abreviada IN ('CI', 'CDI', 'CC') THEN 'NN'
            ELSE 'NN'
            END
            ),
           CAST(nmpd.nro_documento AS VARCHAR),
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
           LPAD(CAST(nmpd.nro_documento AS VARCHAR),11,'0'),
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
       nmpd.nro_documento documento_broker,
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
	   (CASE WHEN ((UPPER(nmp.apellido) IS NULL) OR (("length"(UPPER(nmp.apellido)) < 3) AND (NOT ("upper"(UPPER(nmp.apellido)) IN ('AL', 'AM', 'AN', 'BO', U&'B\00D3', 'CO', 'DE', 'DO', 'DU', 'FU', 'GE', 'GO', 'GU', 'HA', 'HE', 'HO', 'HU', 'IM', 'IN', 'IS', 'JI', 'JO', 'JU', 'KE', 'KI', 'KO', 'KU', 'LI', 'LO', 'LU', 'MA', 'MO', 'MU', 'NA', 'NG', 'NI', 'NO', 'OH', 'OU', 'PI', 'PO', 'PY', 'QI', 'QU', 'RA', 'RE', U&'R\00C9', 'RO', 'RU', 'SA', U&'S\00C1', 'SO', 'SU', 'TU', 'UM', 'UZ', 'WU', 'XU', 'YA', 'YE', 'YI', 'YO', 'YU')))) OR (UPPER(nmp.apellido) LIKE '%PRUEBA%')) THEN 0 ELSE 1 END) apellido_valido
FROM "caba-piba-raw-zone-db"."siu_toba_3_3_negocio_mdp_personas" nmp
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
				goetu.ndocumento,
				goetu.sexo,
				(CASE WHEN (goettd.detalle IN ('DNI', 'CUIT', 'CUIL', 'LC', 'LE', 'CI')) THEN 'ARG' ELSE 'NN' END)) broker_id_din,
	    CONCAT(RPAD(CASE WHEN (goettd.detalle IN ('DNI', 'CUIT', 'CUIL', 'LC', 'LE', 'CI')) THEN goettd.detalle
		        ELSE 'NN' END,4,' '),
				LPAD(CAST(goetu.ndocumento AS VARCHAR),11,'0'),
				goetu.sexo,
				(CASE WHEN (goettd.detalle IN ('DNI', 'CUIT', 'CUIL', 'LC', 'LE', 'CI')) THEN 'ARG' ELSE 'NNN' END)) broker_id_est,
		goettd.detalle tipo_documento,
		(CASE WHEN (goettd.detalle IN ('DNI', 'CUIT', 'CUIL', 'LC', 'LE', 'CI')) THEN goettd.detalle
		        ELSE 'NN' END) tipo_doc_broker,
		goetu.ndocumento documento_broker,
		UPPER(goetu.nombres) nombre,
        UPPER(goetu.apellidos) apellido,
		goetu.fechanacimiento fecha_nacimiento,
		goetu.sexo genero_broker,
		CASE WHEN (goettd.detalle IN ('DNI', 'CUIT', 'CUIL', 'LC', 'LE', 'CI')) THEN 'ARG' ELSE 'NNN' END nacionalidad,
		NULL descrip_nacionalidad,
		CASE WHEN (goettd.detalle IN ('DNI', 'CUIT', 'CUIL', 'LC', 'LE', 'CI')) THEN 'ARG' ELSE 'NNN' END nacionalidad_broker,
		(CASE WHEN ((UPPER(goetu.nombres) IS NULL) OR (("length"(UPPER(goetu.nombres)) < 3) AND (NOT ("upper"(UPPER(goetu.nombres)) IN ('BO', 'GE', 'HE', 'LI', 'LU', 'QI', 'WU', 'XI', 'XU', 'YE', 'YI', 'YU')))) OR (UPPER(goetu.nombres) LIKE '%PRUEBA%')) THEN 0 ELSE 1 END) nombre_valido,
	    (CASE WHEN ((UPPER(goetu.apellidos) IS NULL) OR (("length"(UPPER(goetu.apellidos)) < 3) AND (NOT ("upper"(UPPER(goetu.apellidos)) IN ('AL', 'AM', 'AN', 'BO', U&'B\00D3', 'CO', 'DE', 'DO', 'DU', 'FU', 'GE', 'GO', 'GU', 'HA', 'HE', 'HO', 'HU', 'IM', 'IN', 'IS', 'JI', 'JO', 'JU', 'KE', 'KI', 'KO', 'KU', 'LI', 'LO', 'LU', 'MA', 'MO', 'MU', 'NA', 'NG', 'NI', 'NO', 'OH', 'OU', 'PI', 'PO', 'PY', 'QI', 'QU', 'RA', 'RE', U&'R\00C9', 'RO', 'RU', 'SA', U&'S\00C1', 'SO', 'SU', 'TU', 'UM', 'UZ', 'WU', 'XU', 'YA', 'YE', 'YI', 'YO', 'YU')))) OR (UPPER(goetu.apellidos) LIKE '%PRUEBA%')) THEN 0 ELSE 1 END) apellido_valido
FROM  "caba-piba-raw-zone-db"."goet_usuarios" goetu
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
				a.nrodoc,
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
				LPAD(CAST(a.nrodoc AS VARCHAR),11,'0'),
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
		a.nrodoc documento_broker,
		UPPER(CASE WHEN (a.apenom LIKE '%,%') THEN "trim"("split_part"(apenom, ',', 2)) ELSE a.apenom END) nombre,
        UPPER((CASE WHEN (a.apenom LIKE '%,%') THEN "trim"("split_part"(apenom, ',', 1)) ELSE a.apenom END)) apellido,
		a.fechanac fecha_nacimiento,
		(CASE WHEN (g.sexo = 'Masculino') THEN 'M' WHEN (g.sexo = 'Femenino') THEN 'F' ELSE 'X' END) genero_broker,
		CAST(a.nacionalidad AS VARCHAR) nacionalidad,
		n.nombre descrip_nacionalidad,
		(CASE WHEN (d.nombre IN ('D.N.I.', 'C.I.', 'L.E.', 'L.C.', 'CUIL', 'CUIT')) THEN 'ARG'
				 ELSE 'NN' END) nacionalidad_broker,
		(CASE WHEN ((UPPER(a.apenom) IS NULL) OR (("length"(UPPER(a.apenom)) < 3) AND (NOT ("upper"(UPPER(a.apenom)) IN ('BO', 'GE', 'HE', 'LI', 'LU', 'QI', 'WU', 'XI', 'XU', 'YE', 'YI', 'YU')))) OR (UPPER(a.apenom) LIKE '%PRUEBA%')) THEN 0 ELSE 1 END) nombre_valido,
	    (CASE WHEN ((UPPER(a.apenom) IS NULL) OR (("length"(UPPER(a.apenom)) < 3) AND (NOT ("upper"(UPPER(a.apenom)) IN ('AL', 'AM', 'AN', 'BO', U&'B\00D3', 'CO', 'DE', 'DO', 'DU', 'FU', 'GE', 'GO', 'GU', 'HA', 'HE', 'HO', 'HU', 'IM', 'IN', 'IS', 'JI', 'JO', 'JU', 'KE', 'KI', 'KO', 'KU', 'LI', 'LO', 'LU', 'MA', 'MO', 'MU', 'NA', 'NG', 'NI', 'NO', 'OH', 'OU', 'PI', 'PO', 'PY', 'QI', 'QU', 'RA', 'RE', U&'R\00C9', 'RO', 'RU', 'SA', U&'S\00C1', 'SO', 'SU', 'TU', 'UM', 'UZ', 'WU', 'XU', 'YA', 'YE', 'YI', 'YO', 'YU')))) OR (UPPER(a.apenom) LIKE '%PRUEBA%')) THEN 0 ELSE 1 END) apellido_valido
FROM "caba-piba-raw-zone-db"."sienfo_tdoc" d
INNER JOIN "caba-piba-raw-zone-db"."sienfo_alumnos" a ON (a.tipodoc = d.tipodoc)
INNER JOIN "caba-piba-raw-zone-db"."sienfo_tgenero" g ON (CAST(g.id AS INT) = CAST(a.sexo AS INT))
INNER JOIN "caba-piba-raw-zone-db"."sienfo_tnacionalidad"  n ON (a.nacionalidad = n.nacionalidad)
UNION ALL
SELECT 'CRMSL' base_origen,
	    CAST(co.id AS VARCHAR) codigo_origen,
		CONCAT((CASE WHEN (cs.tipo_documento_c = 'dni') THEN 'DNI' ELSE 'NN' END),
				COALESCE(CAST(cs.numero_documento_c AS varchar),CASE WHEN LENGTH(CAST(cs.cuil2_c AS VARCHAR)) != 11 THEN CAST(cs.cuil2_c AS VARCHAR) ELSE CAST(CAST(SUBSTR(CAST(cs.cuil2_c AS VARCHAR),3,8) AS INTEGER) AS VARCHAR) END),
				(CASE WHEN (cs.genero_c = 'masculino') THEN 'M' WHEN (cs.genero_c = 'femenino') THEN 'F' ELSE 'X' END),
				(CASE WHEN (cs.tipo_documento_c = 'dni') THEN 'ARG' ELSE 'NN' END)) broker_id_din,
		CONCAT(RPAD(CASE WHEN (cs.tipo_documento_c = 'dni') THEN 'DNI' ELSE 'NN' END,4,' '),
				LPAD(COALESCE(CAST(cs.numero_documento_c AS varchar),CASE WHEN LENGTH(CAST(cs.cuil2_c AS VARCHAR)) != 11 THEN CAST(cs.cuil2_c AS VARCHAR) ELSE CAST(CAST(SUBSTR(CAST(cs.cuil2_c AS VARCHAR),3,8) AS INTEGER) AS VARCHAR) END)  ,11,'0'),
				(CASE WHEN (cs.genero_c = 'masculino') THEN 'M' WHEN (cs.genero_c = 'femenino') THEN 'F' ELSE 'X' END),
				(CASE WHEN (cs.tipo_documento_c = 'dni') THEN 'ARG' ELSE 'NNN' END)) broker_id_est,
		cs.tipo_documento_c tipo_documento,
		(CASE WHEN (cs.tipo_documento_c = 'dni') THEN 'DNI' ELSE 'NN' END) tipo_doc_broker,
		COALESCE(CAST(cs.numero_documento_c AS VARCHAR),CASE WHEN LENGTH(CAST(cs.cuil2_c AS VARCHAR)) != 11 THEN CAST(cs.cuil2_c AS VARCHAR) ELSE CAST(CAST(SUBSTR(CAST(cs.cuil2_c AS VARCHAR),3,8) AS INTEGER) AS VARCHAR) END) documento_broker,
		UPPER(co.first_name) nombre,
		UPPER(co.last_name) apellido,
		co.birthdate fecha_nacimiento,
		(CASE WHEN (cs.genero_c = 'masculino') THEN 'M' WHEN (cs.genero_c = 'femenino') THEN 'F' ELSE 'X' END) genero_broker,
		NULL nacionalidad,
		NULL descrip_nacionalidad,
		(CASE WHEN (cs.tipo_documento_c = 'dni') THEN 'ARG' ELSE 'NNN' END) nacionalidad_broker,
		(CASE WHEN ((UPPER(co.first_name) IS NULL) OR (("length"(UPPER(co.first_name)) < 3) AND (NOT ("upper"(UPPER(co.first_name)) IN ('BO', 'GE', 'HE', 'LI', 'LU', 'QI', 'WU', 'XI', 'XU', 'YE', 'YI', 'YU')))) OR (UPPER(co.first_name) LIKE '%PRUEBA%')) THEN 0 ELSE 1 END) nombre_valido,
	    (CASE WHEN ((UPPER(co.last_name) IS NULL) OR (("length"(UPPER(co.last_name)) < 3) AND (NOT ("upper"(UPPER(co.last_name)) IN ('AL', 'AM', 'AN', 'BO', U&'B\00D3', 'CO', 'DE', 'DO', 'DU', 'FU', 'GE', 'GO', 'GU', 'HA', 'HE', 'HO', 'HU', 'IM', 'IN', 'IS', 'JI', 'JO', 'JU', 'KE', 'KI', 'KO', 'KU', 'LI', 'LO', 'LU', 'MA', 'MO', 'MU', 'NA', 'NG', 'NI', 'NO', 'OH', 'OU', 'PI', 'PO', 'PY', 'QI', 'QU', 'RA', 'RE', U&'R\00C9', 'RO', 'RU', 'SA', U&'S\00C1', 'SO', 'SU', 'TU', 'UM', 'UZ', 'WU', 'XU', 'YA', 'YE', 'YI', 'YO', 'YU')))) OR (UPPER(co.last_name) LIKE '%PRUEBA%')) THEN 0 ELSE 1 END) apellido_valido
FROM "caba-piba-raw-zone-db"."crm_sociolaboral_contacts" co
INNER JOIN "caba-piba-raw-zone-db"."crm_sociolaboral_contacts_cstm" cs ON (co.id = cs.id_c)
WHERE (co.lead_source = 'sociolaboral'
OR ((co.lead_source = 'rib') AND cs.forma_parte_interm_lab_c = 'si'))
UNION ALL
SELECT 'MOODLE' base_origen,
	    CAST(mu.id AS VARCHAR) codigo_origen,
		CONCAT('DNI',
				CASE WHEN REGEXP_LIKE(mu.username, '@') THEN
					CASE WHEN REGEXP_LIKE(LOWER(mu.username), '\.ar') THEN
						split_part(split_part(mu.username, '@', 2),'.',4)
					ELSE split_part(split_part(mu.username, '@', 2),'.',3) END
				ELSE split_part(mu.username, '.', 1) END,
                CASE WHEN ui.data = 'Femenino' THEN 'F'
				     WHEN ui.data = 'Masculino' THEN 'M'
				ELSE 'X' END,
				'ARG') broker_id_din,
		CONCAT('DNI ',
				LPAD(CASE WHEN REGEXP_LIKE(mu.username, '@') THEN
					CASE WHEN REGEXP_LIKE(LOWER(mu.username), '\.ar') THEN
						split_part(split_part(mu.username, '@', 2),'.',4)
					ELSE split_part(split_part(mu.username, '@', 2),'.',3) END
				ELSE split_part(mu.username, '.', 1) END,11,'0'),
                CASE WHEN ui.data = 'Femenino' THEN 'F'
				     WHEN ui.data = 'Masculino' THEN 'M'
				ELSE 'X' END,
				'ARG') broker_id_est,
		'DNI' tipo_documento,
		'DNI' tipo_doc_broker,
		CAST(CASE WHEN REGEXP_LIKE(mu.username, '@') THEN
					CASE WHEN REGEXP_LIKE(LOWER(mu.username), '\.ar') THEN
						split_part(split_part(mu.username, '@', 2),'.',4)
					ELSE split_part(split_part(mu.username, '@', 2),'.',3) END
					ELSE split_part(mu.username, '.', 1) END AS VARCHAR) documento_broker,
		UPPER(mu.firstname) nombre,
		UPPER(mu.lastname) apellido,
		NULL fecha_nacimiento,
		(CASE WHEN ui.data = 'Femenino' THEN 'F'
			  WHEN ui.data = 'Masculino' THEN 'M'
		 ELSE 'X' END) genero_broker,
		NULL nacionalidad,
		NULL descrip_nacionalidad,
		'ARG' nacionalidad_broker,
		(CASE WHEN ((UPPER(mu.firstname) IS NULL) OR (("length"(UPPER(mu.firstname)) < 3) AND (NOT ("upper"(UPPER(mu.firstname)) IN ('BO', 'GE', 'HE', 'LI', 'LU', 'QI', 'WU', 'XI', 'XU', 'YE', 'YI', 'YU')))) OR (UPPER(mu.firstname) LIKE '%PRUEBA%')) THEN 0 ELSE 1 END) nombre_valido,
	    (CASE WHEN ((UPPER(mu.lastname) IS NULL) OR (("length"(UPPER(mu.lastname)) < 3) AND (NOT ("upper"(UPPER(mu.lastname)) IN ('AL', 'AM', 'AN', 'BO', U&'B\00D3', 'CO', 'DE', 'DO', 'DU', 'FU', 'GE', 'GO', 'GU', 'HA', 'HE', 'HO', 'HU', 'IM', 'IN', 'IS', 'JI', 'JO', 'JU', 'KE', 'KI', 'KO', 'KU', 'LI', 'LO', 'LU', 'MA', 'MO', 'MU', 'NA', 'NG', 'NI', 'NO', 'OH', 'OU', 'PI', 'PO', 'PY', 'QI', 'QU', 'RA', 'RE', U&'R\00C9', 'RO', 'RU', 'SA', U&'S\00C1', 'SO', 'SU', 'TU', 'UM', 'UZ', 'WU', 'XU', 'YA', 'YE', 'YI', 'YO', 'YU')))) OR (UPPER(mu.lastname) LIKE '%PRUEBA%')) THEN 0 ELSE 1 END) apellido_valido
FROM "caba-piba-raw-zone-db"."moodle_dgtedu01_mdl_user" mu
LEFT JOIN "caba-piba-raw-zone-db"."moodle_dgtedu01_mdl_user_info_data" ui ON (mu.id = ui.userid AND ui.fieldid = 7)
LEFT JOIN "caba-piba-raw-zone-db"."moodle_dgtedu01_mdl_user_enrolments" mue ON (mue.userid = mu.id)
LEFT JOIN "caba-piba-raw-zone-db"."moodle_dgtedu01_mdl_enrol" me ON (me.id = mue.enrolid)
LEFT JOIN "caba-piba-raw-zone-db"."moodle_dgtedu01_mdl_course" mc ON(mc.id = me.courseid)
WHERE (CASE WHEN REGEXP_LIKE(mu.username, '@') THEN
        CASE WHEN REGEXP_LIKE(LOWER(mu.username), '\.ar') THEN
          split_part(split_part(mu.username, '@', 2),'.',4)
        ELSE split_part(split_part(mu.username, '@', 2),'.',3) END
       ELSE split_part(mu.username, '.', 1)  END) IS NOT NULL
	  AND mc.category IN (966, 1375,901,902,903,904,905,906, 969,970,971,972,973,974,975,976, 987, 1343,1344,1345,1346,1347,1348,1349,1350,1351,1352, 1390,1391,1393,1394,1395,1397)
GROUP BY mu.id,
		 mu.username,
		 ui.data,
		 mu.firstname,
		 mu.lastname



-- 2.- Creacion Tabla establecimientos temporal de las dos primeras bases
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_establecimientos" AS
SELECT 'SIU' base_origen,
       su.ubicacion cod_origen,
	   su.nombre,
       su.calle,
       su.numero,
       su.localidad,
       sl.nombre nombre_localidad
FROM "caba-piba-raw-zone-db"."siu_toba_3_3_negocio_sga_ubicaciones" su
LEFT JOIN "caba-piba-raw-zone-db"."siu_toba_3_3_negocio_mug_localidades" sl ON (sl.localidad = su.localidad)
UNION
SELECT 'GOET' base_origen,
	   cf.idcentro cod_origen,
	   cf.nombre,
	   cf.address calle,
	   NULL numero,
	   cf.idlocalidad,
       l.detalle nombre_localidad
FROM "caba-piba-raw-zone-db"."goet_centro_formacion" cf
INNER JOIN "caba-piba-raw-zone-db"."goet_localidades" l ON (l.idlocalidad = cf.idlocalidad)

--- 3.- Creacion tabla temporal de edición capacitiacion
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_edicion_capacitacion" AS
-- PENDIENTE SI USAR PLAN O PROPUESTA A NIVEL DE CAPACITACION
SELECT 'SIU' base_origen,
       dc.capacitacion_id capacitacion_id_typ,
	   CAST(sp.propuesta AS VARCHAR) capacitacion_id,
       sp.nombre descrip_capacitacion,
       sp.nombre_abreviado descrip_capacitacion_abrev,
       CAST(scp.comision AS VARCHAR) edicion_capacitacion_id,
       sc.nombre descrip_ed_capacitacion,
       sple.fecha_inicio_dictado,
       sple.fecha_fin_dictado,
       sple.fecha_tope_movimientos,
       stc.nombre nombre_turno,
       stc.descripcion descrip_turno,
	   CASE WHEN sple.fecha_fin_dictado IS NULL THEN (CASE WHEN sc.inscripcion_habilitada = 'N' THEN 'N' ELSE 'S' END) ELSE (CASE WHEN sple.fecha_fin_dictado > CURRENT_DATE THEN 'S' ELSE 'N' END) END inscripcion_habilitada, -- S, N y NULL
       CASE WHEN sple.fecha_fin_dictado IS NULL THEN (CASE WHEN sc.estado != 'A' THEN 'N' ELSE 'S' END) ELSE (CASE WHEN sple.fecha_fin_dictado > CURRENT_DATE THEN 'S' ELSE 'N' END) END activo, --  A, B y P cual es la tabla de dominio?
       scc.cant_inscriptos,
       CAST(scc.cupo AS VARCHAR) cupo,
       splm.modalidad, -- P
       smc.nombre nombre_modalidad, -- Presencial
       smc.descripcion descrip_modalidad,
       CAST(sc.ubicacion AS VARCHAR) cod_origen_establecimiento
FROM "caba-piba-raw-zone-db"."siu_toba_3_3_negocio_sga_propuestas" sp
LEFT JOIN "caba-piba-raw-zone-db"."siu_toba_3_3_negocio_sga_comisiones_propuestas" scp ON (scp.propuesta = sp.propuesta)
LEFT JOIN "caba-piba-raw-zone-db"."siu_toba_3_3_negocio_sga_comisiones" sc ON (sc.comision = scp.comision)
LEFT JOIN "caba-piba-raw-zone-db"."siu_toba_3_3_negocio_sga_comisiones_cupo" scc ON (scc.comision = scp.comision)
LEFT JOIN "caba-piba-raw-zone-db"."siu_toba_3_3_negocio_sga_planes" spl ON (spl.propuesta = sp.propuesta)
LEFT JOIN "caba-piba-raw-zone-db"."siu_toba_3_3_negocio_sga_planes_modalidad" splm ON (splm.plan = scp.plan)
LEFT JOIN "caba-piba-raw-zone-db"."siu_toba_3_3_negocio_sga_periodos_lectivos" sple ON (sple.periodo_lectivo = sc.periodo_lectivo)
LEFT JOIN "caba-piba-raw-zone-db"."siu_toba_3_3_negocio_sga_turnos_cursadas" stc ON (stc.turno = sc.turno)
LEFT JOIN "caba-piba-raw-zone-db"."siu_toba_3_3_negocio_sga_modalidad_cursada" smc ON (smc.modalidad = splm.modalidad)
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_capacitacion" dc ON (dc.codigo_capacitacion = CAST(scp.propuesta AS VARCHAR) AND dc.base_origen = 'SIU')
UNION
SELECT 'GOET' base_origen,
       dc.capacitacion_id capacitacion_id_typ,
	   CAST(n.idnomenclador AS VARCHAR) capacitacion_id,
	   n.detalle descrip_capacitacion,
	   NULL descrip_capacitacion_abrev,
	   CAST(ccc.idctrcdcurso AS VARCHAR) edicion_capacitacion_id,
	   NULL descrip_ed_capacitacion,
	   ccc.iniciocurso fec_inicio_curso,
	   ccc.fincurso fec_fin_curso,
	   ccc.cierreinscripcion fec_cierre_inscripcion,
	   ccc.diayhorario nombre_turno,
	   NULL descrip_turno,
	   CASE WHEN ccc.fincurso IS NULL THEN (CASE WHEN cce.detalle = 'Abierta la Inscripcion' THEN 'S' ELSE 'N' END) ELSE (CASE WHEN ccc.fincurso > CURRENT_DATE THEN 'S' ELSE 'N' END) END inscripcion_habilitada, -- Cursada - Inscripcion Finalizada ,Inscripcion Cerrada, Abierta la Inscripcion, Curso lleno, Curso Finalizado
       CASE WHEN ccc.fincurso IS NULL THEN (CASE WHEN ccce.detalle = 'Habilitado' THEN 'S' ELSE 'N' END) ELSE (CASE WHEN ccc.fincurso > CURRENT_DATE THEN 'S' ELSE 'N' END) END activo, -- Inabilitado, Habilitado,  Esperando Habilitación,  Dado de Baja
	   (100 - ccc.porcentajevacantes)/100 * ccc.topematricula cant_inscriptos,  --VER
       CAST(ccc.topematricula AS VARCHAR) cupo,
	   NULL modalidad,
	   NULL nombre_modalidad,
       NULL descrip_modalidad,
	   CAST(chm.idcentro AS VARCHAR) cod_origen_establecimiento
FROM "caba-piba-raw-zone-db"."goet_nomenclador" n
INNER JOIN "caba-piba-raw-zone-db"."goet_centro_habilitacion_modulos" chm ON (chm.idnomenclador = n.idnomenclador)
INNER JOIN "caba-piba-raw-zone-db"."goet_centro_codigo_curso" ccc ON (ccc.idctrhbmodulo = chm.idctrhbmodulo)
LEFT JOIN "caba-piba-raw-zone-db"."goet_centro_codigo_curso_estado" ccce ON (ccce.idctrcdcursoestado = ccc.idctrcdcursoestado)
LEFT JOIN "caba-piba-raw-zone-db"."goet_centro_curso_estado" cce ON (cce.idcursoestado = ccc.idcursoestado)
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_capacitacion" dc ON (dc.codigo_capacitacion = CAST(n.idnomenclador AS VARCHAR) AND dc.base_origen = 'GOET')
UNION
SELECT 'SIENFO' base_origen,
       dc.capacitacion_id capacitacion_id_typ,
       CAST(ca.id_carrera AS VARCHAR) capacitacion_id,
       ca.nom_carrera descrip_capacitacion,
       ca.nombre_corto descrip_capacitacion_abrev,
       t.codigo_ct edicion_capacitacion_id,
       'Carrera '||COALESCE(t.obs,' ') descrip_ed_capacitacion,
       DATE_PARSE(CASE WHEN t.fecha = '0000-00-00' THEN NULL ELSE t.fecha END, '%Y-%m-%d') fecha_inicio_dictado,
       DATE_PARSE(CASE WHEN t.fecha_fin = '0000-00-00' THEN NULL ELSE t.fecha_fin END, '%Y-%m-%d') fecha_fin_dictado,
       NULL fecha_tope_movimientos, ---t.inscripcionh se puede usar esta columna sumada a la fecha de inicio
       CASE WHEN CAST(SUBSTR(COALESCE(t.horaini_lu, t.horaini_ma, t.horaini_mi, t.horaini_ju, t.horaini_vi, t.horaini_sa),1,2) AS INTEGER) BETWEEN 7 AND 12 THEN 'Mañana'
            WHEN CAST(SUBSTR(COALESCE(t.horaini_lu, t.horaini_ma, t.horaini_mi, t.horaini_ju, t.horaini_vi, t.horaini_sa),1,2) AS INTEGER) BETWEEN 13 AND 17 THEN 'Tarde'
			WHEN CAST(SUBSTR(COALESCE(t.horaini_lu, t.horaini_ma, t.horaini_mi, t.horaini_ju, t.horaini_vi, t.horaini_sa),1,2) AS INTEGER) BETWEEN 18 AND 24 THEN 'Noche'
			ELSE NULL END nombre_turno,
	   CASE WHEN t.lu = 1 THEN 'Lunes ' ELSE '' END ||
	   CASE WHEN t.ma = 1 THEN 'Martes ' ELSE '' END ||
	   CASE WHEN t.mi = 1 THEN 'Miercoles ' ELSE '' END ||
	   CASE WHEN t.ju = 1 THEN 'Jueves ' ELSE '' END ||
	   CASE WHEN t.vi = 1 THEN 'Viernes ' ELSE '' END ||
	   CASE WHEN t.sa = 1 THEN 'Sabado ' ELSE '' END descrip_turno,
	   CASE WHEN COALESCE(t.fecha_fin,'0000-00-00') = '0000-00-00' THEN (CASE WHEN te.nombre = 'Activo' THEN 'S' ELSE 'N' END) ELSE (CASE WHEN DATE_PARSE(CASE WHEN t.fecha_fin = '0000-00-00' THEN NULL ELSE t.fecha_fin END, '%Y-%m-%d') > CURRENT_DATE THEN 'S' ELSE 'N' END) END inscripcion_habilitada,
       CASE WHEN COALESCE(t.fecha_fin,'0000-00-00') = '0000-00-00' THEN (CASE WHEN te.nombre = 'Activo' THEN 'S' ELSE 'N' END) ELSE (CASE WHEN DATE_PARSE(CASE WHEN t.fecha_fin = '0000-00-00' THEN NULL ELSE t.fecha_fin END, '%Y-%m-%d') > CURRENT_DATE THEN 'S' ELSE 'N' END) END activo,
	   t.altas_total cant_inscriptos,
	   CAST(t.vacantes AS VARCHAR) cupo,
	   NULL modalidad,
       NULL nombre_modalidad,
       NULL descrip_modalidad,
       CAST(t.id_centro AS VARCHAR) cod_origen_establecimiento
FROM "caba-piba-raw-zone-db"."sienfo_carreras" ca
INNER JOIN "caba-piba-raw-zone-db"."sienfo_talleres" t ON (t.id_carrera = ca.id_carrera)
LEFT JOIN "caba-piba-raw-zone-db"."sienfo_testado" te ON (t.estado = te.valor)
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_capacitacion" dc ON (dc.codigo_capacitacion = CAST(ca.id_carrera AS VARCHAR) AND dc.base_origen = 'SIENFO')
WHERE t.id_carrera != 0
UNION
SELECT 'SIENFO' base_origen,
       dc.capacitacion_id capacitacion_id_typ,
       CAST(cu.id_curso AS VARCHAR) capacitacion_id,
       cu.nom_curso descrip_capacitacion,
       cu.descrip descrip_capacitacion_abrev,
       t.codigo_ct edicion_capacitacion_id,
       'Curso '||COALESCE(t.obs,' ') descrip_ed_capacitacion,
       DATE_PARSE(CASE WHEN t.fecha = '0000-00-00' THEN NULL ELSE t.fecha END, '%Y-%m-%d') fecha_inicio_dictado,
       DATE_PARSE(CASE WHEN t.fecha_fin = '0000-00-00' THEN NULL ELSE t.fecha_fin END, '%Y-%m-%d') fecha_fin_dictado,
       NULL fecha_tope_movimientos, ---t.inscripcionh se puede usar esta columna sumada a la fecha de inicio
       CASE WHEN CAST(SUBSTR(COALESCE(t.horaini_lu, t.horaini_ma, t.horaini_mi, t.horaini_ju, t.horaini_vi, t.horaini_sa),1,2) AS INTEGER) BETWEEN 7 AND 12 THEN 'Mañana'
            WHEN CAST(SUBSTR(COALESCE(t.horaini_lu, t.horaini_ma, t.horaini_mi, t.horaini_ju, t.horaini_vi, t.horaini_sa),1,2) AS INTEGER) BETWEEN 13 AND 17 THEN 'Tarde'
			WHEN CAST(SUBSTR(COALESCE(t.horaini_lu, t.horaini_ma, t.horaini_mi, t.horaini_ju, t.horaini_vi, t.horaini_sa),1,2) AS INTEGER) BETWEEN 18 AND 24 THEN 'Noche'
			ELSE NULL END nombre_turno,
	   CASE WHEN t.lu = 1 THEN 'Lunes ' ELSE '' END ||
	   CASE WHEN t.ma = 1 THEN 'Martes ' ELSE '' END ||
	   CASE WHEN t.mi = 1 THEN 'Miercoles ' ELSE '' END ||
	   CASE WHEN t.ju = 1 THEN 'Jueves ' ELSE '' END ||
	   CASE WHEN t.vi = 1 THEN 'Viernes ' ELSE '' END ||
	   CASE WHEN t.sa = 1 THEN 'Sabado ' ELSE '' END descrip_turno,
	   CASE WHEN COALESCE(t.fecha_fin,'0000-00-00') = '0000-00-00' THEN (CASE WHEN te.nombre = 'Activo' THEN 'S' ELSE 'N' END) ELSE (CASE WHEN DATE_PARSE(CASE WHEN t.fecha_fin = '0000-00-00' THEN NULL ELSE t.fecha_fin END, '%Y-%m-%d') > CURRENT_DATE THEN 'S' ELSE 'N' END) END inscripcion_habilitada,
       CASE WHEN COALESCE(t.fecha_fin,'0000-00-00') = '0000-00-00' THEN (CASE WHEN te.nombre = 'Activo' THEN 'S' ELSE 'N' END) ELSE (CASE WHEN DATE_PARSE(CASE WHEN t.fecha_fin = '0000-00-00' THEN NULL ELSE t.fecha_fin END, '%Y-%m-%d') > CURRENT_DATE THEN 'S' ELSE 'N' END) END activo,
	   t.altas_total cant_inscriptos,
	   CAST(t.vacantes AS VARCHAR) cupo,
	   NULL modalidad,
       NULL nombre_modalidad,
       NULL descrip_modalidad,
       CAST(t.id_centro AS VARCHAR) cod_origen_establecimiento
FROM "caba-piba-raw-zone-db"."sienfo_cursos" cu
INNER JOIN "caba-piba-raw-zone-db"."sienfo_talleres" t ON (t.id_curso = cu.id_curso)
LEFT JOIN "caba-piba-raw-zone-db"."sienfo_testado" te ON (t.estado = te.valor)
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_capacitacion" dc ON (dc.codigo_capacitacion = CAST(cu.id_curso AS VARCHAR) AND dc.base_origen = 'SIENFO')
WHERE COALESCE(t.id_carrera,0) = 0
UNION
SELECT 'CRMSL' base_origen,
       dc.capacitacion_id capacitacion_id_typ,
       CAST(of.id AS VARCHAR) capacitacion_id,
       of.name descrip_capacitacion,
       NULL descrip_capacitacion_abrev,
       CAST(of.id AS VARCHAR) edicion_capacitacion_id,
       of.name descrip_ed_capacitacion,
       of.inicio fecha_inicio_dictado,
       of.fin fecha_fin_dictado,
       NULL fecha_tope_movimientos,
       NULL nombre_turno,
       NULL descrip_turno,
       CASE WHEN of.fin IS NULL THEN (CASE WHEN estado_inscripcion = 'finalizada' THEN 'N' ELSE 'S' END) ELSE (CASE WHEN of.fin > CURRENT_DATE THEN 'S' ELSE 'N' END) END inscripcion_habilitada,
       CASE WHEN of.fin IS NULL THEN (CASE WHEN estado_curso = 'finalizada' THEN 'N' ELSE 'S' END) ELSE (CASE WHEN of.fin > CURRENT_DATE THEN 'S' ELSE 'N' END) END activo,
       of.inscriptos cant_inscriptos,
       CAST(of.cupos AS VARCHAR) cupo,
       CASE WHEN of.modalidad = 'virtual' THEN 'V'
			WHEN of.modalidad = 'semi' THEN 'S'
			WHEN of.modalidad = 'presencial' THEN 'P' END modalidad, -- P, S, V
       CASE WHEN of.modalidad = 'virtual' THEN 'Virtual'
			WHEN of.modalidad = 'semi' THEN 'Presencial y Virtual'
			WHEN of.modalidad = 'presencial' THEN 'Presencial' END nombre_modalidad, -- presencial, semipresncial, virtual
       NULL descrip_modalidad,
       CAST(of.sede AS VARCHAR) cod_origen_establecimiento
FROM "caba-piba-raw-zone-db"."crm_sociolaboral_op_oportunidades_formacion" OF
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_capacitacion" dc ON (dc.codigo_capacitacion = CAST(of.id AS VARCHAR) AND dc.base_origen = 'CRMSL')
UNION
-- NO HAY COINCIDENCIAS
SELECT 'MOODLE' base_origen,
       dc.capacitacion_id capacitacion_id_typ,
       CAST(co.id AS VARCHAR) capacitacion_id,
       co.fullname descrip_capacitacion,
       co.shortname descrip_capacitacion_abrev,
       CAST(co.id AS VARCHAR) edicion_capacitacion_id,
       co.fullname descrip_ed_capacitacion,
       date_parse(date_format(from_unixtime(co.startdate),'%Y-%m-%d %h:%i%p'),'%Y-%m-%d %h:%i%p') fecha_inicio_dictado,
       CASE WHEN co.enddate != 0 THEN
       date_parse(date_format(from_unixtime(co.enddate),'%Y-%m-%d %h:%i%p'),'%Y-%m-%d %h:%i%p')
       ELSE NULL END fecha_fin_dictado,
       NULL fecha_tope_movimientos,
       NULL nombre_turno,
       NULL descrip_turno,
       CASE WHEN co.visible THEN 'S' ELSE 'N' END inscripcion_habilitada,
       CASE WHEN co.enddate = 0 THEN 'S' ELSE 'N' END activo,
       cc.coursecount cant_inscriptos,
       NULL cupo,
       NULL modalidad,
       NULL nombre_modalidad,
       NULL descrip_modalidad,
       NULL cod_origen_establecimiento
FROM "caba-piba-raw-zone-db"."moodle_dgtedu01_mdl_course" co
LEFT JOIN "caba-piba-raw-zone-db"."moodle_dgtedu01_mdl_course_categories" cc ON (co.category = cc.id)
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_capacitacion" dc ON (dc.codigo_capacitacion = CAST(co.id AS VARCHAR) AND dc.base_origen = 'MOODLE')



