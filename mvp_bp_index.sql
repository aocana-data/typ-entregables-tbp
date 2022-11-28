-- 2022.11.28_step_01_Copy of 2022.11.28 staging vecinos step 1(Vcliente).sql 



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
	    CAST(co.id AS VARCHAR) codigo_origen,
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
INNER JOIN "caba-piba-raw-zone-db"."crm_sociolaboral_contacts_cstm" cs ON (co.id = cs.id_c)
WHERE (co.lead_source = 'sociolaboral'
OR ((co.lead_source = 'rib') AND cs.forma_parte_interm_lab_c = 'si'))
AND cs.numero_documento_c IS NOT NULL

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
    -- Habilidades/Formación para la empleabilidad
GROUP BY mu.id,
		 mu.username,
		 ui.data,
		 mu.firstname,
		 mu.lastname,
		 do.genero



-- 2022.11.28_step_02_Copy of 2022.11.28 consume vecinos step 2 (Vcliente).sql 



-- 1.- Crear la tabla definitiva de vecinos
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_def_vecino" AS
WITH tmp_vec AS
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
		 CAST(do.documento_broker AS VARCHAR))
SELECT tmp_vec.vecino_id,
	   tmp_vec.base_origen,
	   tmp_vec.cod_origen,
	   tmp_vec.broker_id,
	   tmp_vec.broker_id_est,
	   tmp_vec.tipo_documento,
	   tmp_vec.tipo_doc_broker,
	   tmp_vec.documento_broker,
	   tmp_vec.nombre,
	   tmp_vec.apellido,
	   tmp_vec.fecha_nacimiento,
	   tmp_vec.genero_broker,
	   tmp_vec.nacionalidad,
	   tmp_vec.descrip_nacionalidad,
	   tmp_vec.nacionalidad_broker,
	   tmp_vec.nombre_valido,
	   tmp_vec.apellido_valido,
	   tmp_vec.broker_id_valido,
	   CASE WHEN tmp_vec.broker_id_valido = 1 AND tmp_vec.dni_valido = 1 THEN 0
	        WHEN tmp_vec.broker_id_valido = 0 AND tmp_vec.dni_valido = 1 THEN 1
			ELSE 0 END dni_valido
FROM tmp_vec



-- 2022.11.28_step_03_Copy of 2022.11.28 consume programa step 3 (Vcliente).sql 



DROP TABLE IF EXISTS "caba-piba-staging-zone-db"."tbp_typ_def_programa";
DROP TABLE `tbp_typ_def_programa`;
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



-- 2022.11.28_step_04_Copy of 2022.11.28 staging capacitacion asi step 4 (Vcliente).sql 



-- 1.- Crear tabla capacitacion
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
DROP TABLE `tbp_typ_def_aptitudes_asi`;
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



-- 2022.11.28_step_05_Copy of 2022.11.28 staging capacitacion step 5 (Vcliente).sql 



-- CRM SOCIO LABORAL
--1.-- Crear tabla capacitaciones socio laboral
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_crmsl_1" AS
SELECT 'CRMSL' base_origen,
       'CURSO' tipo_capacitacion,
        CAST(of.id AS VARCHAR) capacitacion_id,
		CASE WHEN TRIM(UPPER(of.area)) IN ('GESTIÓN COMERCIAL','LIMPIEZA Y MANTENIMIENTO', 'MOZO Y CAMARERA', 'OPERARIO CALIFICADO') THEN
			TRIM(UPPER(of.area))
		 ELSE TRIM(REGEXP_REPLACE(TRIM(REGEXP_REPLACE(TRIM(REGEXP_REPLACE(REGEXP_REPLACE(UPPER(CASE
			WHEN of.name LIKE '%|%' THEN split_part(of.name,'|',2)
			WHEN of.name LIKE '%/%' THEN split_part(of.name,'/',2)
		ELSE of.name
		END),'[0-9]+|\ II$|\ I$|TURNO|MAÑANA|NOCHE|TARDE|TURNOS|LUNES|MARTES|MIERCOLES|JUEVES|VIERNES|BARRIO|MIÉRCOLES|MARZO|SEPTIEMBRE',''),'\.|\-', ' ')),'\ Y$|','')),'\(\ \)|\º|\  S$|PLAYON DE CHACARITA| RODRIGO BUENO Y  MUGICA|\ S$|RICCIARDELLI|FRAGA','')) END descrip_normalizada,
		of.name descrip_capacitacion,
        of.inicio fecha_inicio_dictado,
        of.fin fecha_fin_dictado,
		CASE WHEN UPPER(of.estado_curso) = 'FINALIZADA' THEN 0 ELSE 1 END estado,
		TRIM(UPPER(of.area)) categoria
FROM "caba-piba-raw-zone-db"."crm_sociolaboral_op_oportunidades_formacion" OF
INNER JOIN "caba-piba-raw-zone-db"."crm_sociolaboral_op_oportunidades_formacion_contacts_c" ofc  ON (of.id = ofc.op_oportun1d35rmacion_ida)
INNER JOIN "caba-piba-raw-zone-db"."crm_sociolaboral_contacts" co ON (co.id = ofc.op_oportunidades_formacion_contactscontacts_idb)
INNER JOIN "caba-piba-raw-zone-db"."crm_sociolaboral_contacts_cstm" cs ON (co.id = cs.id_c)
WHERE (co.lead_source = 'sociolaboral'
OR ((co.lead_source = 'rib') AND cs.forma_parte_interm_lab_c = 'si'))
GROUP BY CAST(of.id AS VARCHAR),
       of.name,
       of.inicio,
       of.fin,
       UPPER(of.estado_curso),
	   UPPER(of.area)

--2.-- Crear tabla maestro capacitaciones socio laboral
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_crmsl_capacitaciones" AS
SELECT row_number() OVER () AS id,
       base_origen,
       tipo_capacitacion,
       descrip_normalizada,
       MIN(fecha_inicio_dictado) fecha_inicio,
       MAX(fecha_fin_dictado) fecha_fin,
       categoria,
       CASE WHEN SUM(estado) > 0 THEN 'ACTIVO' ELSE 'BAJA' END estado
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_crmsl_1" s1
GROUP BY base_origen,
       tipo_capacitacion,
       descrip_normalizada,
       categoria

--3.-- Crear tabla match capacitaciones socio laboral
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_crmsl_match" AS
SELECT sc.base_origen,
       sc.tipo_capacitacion,
	   sc.id id_new,
       s1.capacitacion_id id_old
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_crmsl_1" s1
INNER JOIN "caba-piba-staging-zone-db"."tbp_typ_tmp_crmsl_capacitaciones" sc ON (sc.descrip_normalizada = s1.descrip_normalizada) AND (COALESCE(CAST(sc.categoria AS VARCHAR),'-') = COALESCE(CAST(s1.categoria AS VARCHAR),'-'))

-- SIENFO
--4.-- Crear tabla capacitaciones sienfo
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_sienfo_1" AS
WITH sienfo_cap AS
(SELECT 'SIENFO' base_origen,
      'CARRERA' tipo_capacitacion,
       CAST(ca.id_carrera AS VARCHAR) capacitacion_id,
	   TRIM(REGEXP_REPLACE(TRIM(REGEXP_REPLACE(REGEXP_REPLACE(UPPER(ca.nom_carrera),'Í','I'),'[0-9]+|\ II$|\ I$|\(MÓDULO?\)', '')),'\(?MODULO\s?[A-Za-z0-9\s]+\)?|\(?MÓDULO\s?[A-Za-z0-9\s]+\)?','')) descrip_normalizada,
       UPPER(ca.nom_carrera) descrip_capacitacion,
       MIN(DATE_PARSE(CASE WHEN t.fecha = '0000-00-00' THEN NULL ELSE t.fecha END, '%Y-%m-%d')) fecha_inicio_dictado,
       MAX(DATE_PARSE(CASE WHEN t.fecha_fin = '0000-00-00' THEN NULL ELSE t.fecha_fin END, '%Y-%m-%d')) fecha_fin_dictado,
	   CASE WHEN SUM(CASE WHEN UPPER(te.nombre) = 'ACTIVO' THEN 1 ELSE 0 END) > 0 THEN 1 ELSE 0 END estado,
       UPPER(tc.nom_categoria) categoria
FROM "caba-piba-raw-zone-db"."sienfo_carreras" ca
INNER JOIN "caba-piba-raw-zone-db"."sienfo_talleres" t ON (t.id_carrera = ca.id_carrera)
LEFT JOIN "caba-piba-raw-zone-db"."sienfo_testado" te ON (ca.estado = te.valor)
-- REVISAR
LEFT JOIN "caba-piba-raw-zone-db"."sienfo_tcategorias" tc ON (ca.id_tcarrera = tc.id_tcategoria)
WHERE COALESCE(t.id_carrera,0) != 0
GROUP BY  CAST(ca.id_carrera AS VARCHAR),
       ca.nom_carrera,
       UPPER(tc.nom_categoria)
UNION
SELECT 'SIENFO' base_origen,
       'CURSO',
       CAST(cu.id_curso AS VARCHAR) capacitacion_id,
	   TRIM(REGEXP_REPLACE(TRIM(UPPER(cu.nom_curso)),'\(?MODULO\ CONTABLE\)?|\(?MÓDULO\ CONTABLE\)?|\(?MODULO\ JURIDICO\)?|\(?MÓDULO\ JURÍDICO\)?| \(NIDO SOLDATI\-CARRILLO\)?|\(RETIRO NORTE\)?|\(PRINGLES\)?|\(ARANGUREN\)?|\(ARANGUREN\-?JUNCAL\)?|\(ARANGUREN\-\MANANTIALES\)?|\(LAMBARÉ\)?|\(SAN NICOLÁS\)?|\(CORRALES\)?|\(JUNCAL\)?|\(JUAN A\.\ \GARCIA\)?|\(SARAZA\)?|\(RETIRO\)?|\(DON BOSCO\)?|\(ANCHORENA\)?','')) descrip_normalizada,
       UPPER(cu.nom_curso) descrip_capacitacion,
       MIN(DATE_PARSE(CASE WHEN t.fecha = '0000-00-00' THEN NULL ELSE t.fecha END, '%Y-%m-%d')) fecha_inicio_dictado,
       MAX(DATE_PARSE(CASE WHEN t.fecha_fin = '0000-00-00' THEN NULL ELSE t.fecha_fin END, '%Y-%m-%d')) fecha_fin_dictado,
       CASE WHEN SUM(CASE WHEN UPPER(te.nombre) = 'ACTIVO' THEN 1 ELSE 0 END) > 0 THEN 1 ELSE 0 END estado,
       UPPER(tc.nom_categoria)
FROM "caba-piba-raw-zone-db"."sienfo_cursos" cu
INNER JOIN "caba-piba-raw-zone-db"."sienfo_talleres" t ON (t.id_curso = cu.id_curso)
LEFT JOIN "caba-piba-raw-zone-db"."sienfo_testado" te ON (t.estado = te.valor)
LEFT JOIN "caba-piba-raw-zone-db"."sienfo_tcategorias" tc ON (tc.id_tcategoria = cu.id_tcategoria)
WHERE COALESCE(t.id_carrera,0) = 0
GROUP BY  CAST(cu.id_curso AS VARCHAR),
       cu.nom_curso,
       UPPER(tc.nom_categoria))
SELECT sienfo_cap.*
FROM sienfo_cap

--5.-- Crear tabla maestro capacitaciones sienfo
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_sienfo_capacitaciones" AS
SELECT row_number() OVER () AS id,
       base_origen,
       tipo_capacitacion,
       descrip_normalizada,
       MIN(fecha_inicio_dictado) fecha_inicio,
       MAX(fecha_fin_dictado) fecha_fin,
       categoria,
       CASE WHEN SUM(estado) > 0 THEN 'ACTIVO' ELSE 'BAJA' END estado
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_sienfo_1" s1
GROUP BY base_origen,
       tipo_capacitacion,
       descrip_normalizada,
       categoria

--6.-- Crear tabla match capacitaciones sienfo
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_sienfo_match" AS
SELECT sc.base_origen,
       sc.tipo_capacitacion,
	   sc.id id_new,
       s1.capacitacion_id id_old
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_sienfo_1" s1
INNER JOIN "caba-piba-staging-zone-db"."tbp_typ_tmp_sienfo_capacitaciones" sc ON (sc.tipo_capacitacion = s1.tipo_capacitacion AND sc.descrip_normalizada = s1.descrip_normalizada) AND (COALESCE(CAST(sc.categoria AS VARCHAR),'-') = COALESCE(CAST(s1.categoria AS VARCHAR),'-'))


-- GOET


--7.-- Crear tabla capacitaciones goet
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_goet_1" AS
SELECT  'GOET' base_origen,
        CASE WHEN UPPER(t.detalle) IS NOT NULL THEN 'CARRERA' ELSE 'CURSO' END tipo_capacitacion,
        CASE WHEN UPPER(t.detalle) IS NOT NULL THEN CAST(n.IdNomenclador AS VARCHAR)||'-'||CAST(t.IdKeyTrayecto AS VARCHAR) ELSE CAST(n.IdNomenclador AS VARCHAR) END capacitacion_id,
        TRIM(REGEXP_REPLACE(REGEXP_REPLACE(TRIM(REGEXP_REPLACE(CASE WHEN UPPER(t.detalle) IS NOT NULL THEN UPPER(t.Detalle) ELSE UPPER(n.Detalle) END,'[0-9]+|\ II$|\ I$|\ III$|\ IV$|\ V$|\ VI$|\(MÓDULO?\)', '')),'\(?MODULO\s?[A-Za-z0-9\s]+\)?|\(?MÓDULO\s?[A-Za-z0-9\s]+\)?',''),'\(?NIVEL\s?[A-Za-z0-9\s]+\)?','')) descrip_normalizada,
		CASE WHEN UPPER(t.detalle) IS NOT NULL THEN UPPER(t.Detalle) ELSE UPPER(n.Detalle) END descrip_capacitacion,
        MIN(cc.iniciocurso) fecha_inicio_dictado,
	    MAX(cc.fincurso) fecha_fin_dictado,
	    CASE WHEN UPPER(en.Detalle) = 'ACTIVO' THEN 1 ELSE 0 END estado,
        COALESCE(UPPER(a.Detalle),  UPPER(f.Detalle)) AS categoria
FROM
    "caba-piba-raw-zone-db"."goet_centro_habilitacion_modulos" chm
INNER JOIN "caba-piba-raw-zone-db"."goet_centro_formacion" cf ON cf.IdCentro  = chm.IdCentro
INNER JOIN "caba-piba-raw-zone-db"."goet_centro_codigo_curso" cc ON cc.IdCtrHbModulo = chm.IdCtrHbModulo
INNER JOIN "caba-piba-raw-zone-db"."goet_nomenclador"  n ON n.IdNomenclador = chm.IdNomenclador
LEFT JOIN "caba-piba-raw-zone-db"."goet_area" a ON a.IdArea = n.IdArea
LEFT JOIN "caba-piba-raw-zone-db"."goet_familia" f ON f.IdFamilia = n.IdFamilia
LEFT JOIN "caba-piba-raw-zone-db"."goet_mudulosxtrayecto" m ON m.IdNomenclador = n.IdNomenclador
LEFT JOIN "caba-piba-raw-zone-db"."goet_trayecto" t ON t.IdKeyTrayecto = m.IdKeyTrayecto
LEFT JOIN "caba-piba-raw-zone-db"."goet_trayecto_nivel" tn ON tn.IdNivelTrayecto = t.IdNivelTrayecto
LEFT JOIN "caba-piba-raw-zone-db"."goet_estado_nomenclador" en ON en.IdEstadoNomenclador = n.IdEstadoNomenclador
GROUP BY UPPER(t.detalle),
         CAST(n.IdNomenclador AS VARCHAR)||'-'||CAST(t.IdKeyTrayecto AS VARCHAR),
		 CAST(n.IdNomenclador AS VARCHAR),
         UPPER(n.Detalle),
         UPPER(en.Detalle),
         UPPER(a.Detalle),
         UPPER(f.Detalle)

--8.-- Crear tabla maestro capacitaciones goet
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_goet_capacitaciones" AS
SELECT row_number() OVER () AS id,
       base_origen,
       tipo_capacitacion,
       CASE WHEN length(descrip_normalizada) = 0 THEN descrip_capacitacion ELSE descrip_normalizada END descrip_normalizada,
       MIN(fecha_inicio_dictado) fecha_inicio,
       MAX(fecha_fin_dictado) fecha_fin,
       categoria,
       CASE WHEN SUM(estado) > 0 THEN 'ACTIVO' ELSE 'BAJA' END estado
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_goet_1" s1
GROUP BY base_origen,
       tipo_capacitacion,
       CASE WHEN length(descrip_normalizada) = 0 THEN descrip_capacitacion ELSE descrip_normalizada END,
       categoria

--9.-- Crear tabla match capacitaciones goet
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_goet_match" AS
SELECT sc.base_origen,
       sc.tipo_capacitacion,
	   sc.id id_new,
       s1.capacitacion_id id_old
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_goet_1" s1
INNER JOIN "caba-piba-staging-zone-db"."tbp_typ_tmp_goet_capacitaciones" sc ON (sc.tipo_capacitacion = s1.tipo_capacitacion AND sc.descrip_normalizada = (CASE WHEN length(s1.descrip_normalizada) = 0 THEN s1.descrip_capacitacion ELSE s1.descrip_normalizada END)) AND (COALESCE(CAST(sc.categoria AS VARCHAR),'-') = COALESCE(CAST(s1.categoria AS VARCHAR),'-'))


-- MOODLE
--10.-- Crear tabla capacitaciones moodle
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_moodle_1" AS
SELECT 'MOODLE' base_origen,
       'CURSO' tipo_capacitacion,
       CAST(co.id AS VARCHAR) capacitacion_id,
       TRIM(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(UPPER(co.fullname),'[0-9]+|\ II$|\ I$|\(MÓDULO?\)', ''),'TURNO|MAÑANA|NOCHE|TARDE|\_ENE|\_FEB|\_MAR|\_ABR|\_MAY|\_JUN|\_JUL|\_AGO|\_SEP|\_OCT|\_NOV|\_DIC|\(\COMISIÓN \)|\_AULAA|CURSO\:|\_AULAB|\_AULAC|\_AULAD|\_AULA|\_F|\_E|_A|_B|_C|_D|\(\COMISIÓN\)|\ C$',''),'\.|\/|\__',' '),'\(?MODULO\s?[A-Za-z0-9\s]+\)?|\(?MÓDULO\s?[A-Za-z0-9\s]+\)?|MODULO','')) descrip_normalizada,
       UPPER(co.fullname) descrip_capacitacion,
       MIN(date_parse(date_format(from_unixtime(co.startdate),'%Y-%m-%d %h:%i%p'),'%Y-%m-%d %h:%i%p')) fecha_inicio_dictado,
       MAX(CASE WHEN co.enddate != 0 THEN
       date_parse(date_format(from_unixtime(co.enddate),'%Y-%m-%d %h:%i%p'),'%Y-%m-%d %h:%i%p')
       ELSE NULL END) fecha_fin_dictado,
       CASE WHEN co.enddate = 0 THEN 1 ELSE 0 END estado,
       TRIM(REGEXP_REPLACE(REGEXP_REPLACE(UPPER(cc.name),'[0-9]+|\ II$|\ I$|\(MÓDULO?\)', ''),'_','')) categoria
FROM "caba-piba-raw-zone-db"."moodle_dgtedu01_mdl_course" co
LEFT JOIN "caba-piba-raw-zone-db"."moodle_dgtedu01_mdl_course_categories" cc ON (co.category = cc.id)
WHERE cc.idnumber LIKE 'CAC%'
    -- CAC
    OR cc.idnumber LIKE 'FPE%'
    -- Habilidades/Formación para la empleabilidad
GROUP BY CAST(co.id AS VARCHAR),
       UPPER(co.fullname),
       co.enddate,
       UPPER(cc.name)

--11.-- Crear tabla maestro capacitaciones moodle
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_moodle_capacitaciones" AS
SELECT row_number() OVER () AS id,
       base_origen,
       tipo_capacitacion,
       descrip_normalizada,
       MIN(fecha_inicio_dictado) fecha_inicio,
       MAX(fecha_fin_dictado) fecha_fin,
       categoria,
       CASE WHEN SUM(estado) > 0 THEN 'ACTIVO' ELSE 'BAJA' END estado
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_moodle_1" s1
GROUP BY base_origen,
       tipo_capacitacion,
       descrip_normalizada,
       categoria

--12.-- Crear tabla match capacitaciones moodle
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_moodle_match" AS
SELECT sc.base_origen,
       sc.tipo_capacitacion,
	   sc.id id_new,
       s1.capacitacion_id id_old
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_moodle_1" s1
INNER JOIN "caba-piba-staging-zone-db"."tbp_typ_tmp_moodle_capacitaciones" sc ON (sc.tipo_capacitacion = s1.tipo_capacitacion AND sc.descrip_normalizada = (CASE WHEN length(s1.descrip_normalizada) = 0 THEN s1.descrip_capacitacion ELSE s1.descrip_normalizada END)) AND (COALESCE(CAST(sc.categoria AS VARCHAR),'-') = COALESCE(CAST(s1.categoria AS VARCHAR),'-'))

-- SIU
--13.-- Crear tabla capacitaciones siu
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_siu_1" AS
WITH siu AS
(SELECT 'SIU' base_origen,
       'CARRERA' tipo_capacitacion,
       CAST(spl.plan AS VARCHAR) capacitacion_id,
       TRIM(REGEXP_REPLACE(UPPER(spl.nombre),'[0-9]+|\ II$|\ I$|\(MÓDULO?\)|IFTS|MODALIDAD|MODALIDAD I|\-', '')) descrip_normalizada,
       spl.nombre descrip_capacitacion,
       spl.fecha_entrada_vigencia fecha_inicio_dictado,
       spl.fecha_baja fecha_fin_dictado,
       CASE WHEN spl.estado = 'V' THEN 1 ELSE 0 END estado,
       'SIN CATEGORIA' categoria
FROM "caba-piba-raw-zone-db"."siu_toba_3_3_negocio_sga_planes" spl
GROUP BY CAST(spl.plan AS VARCHAR),
       spl.nombre,
       spl.fecha_entrada_vigencia,
       spl.fecha_baja,
       spl.estado)
SELECT *
FROM siu
WHERE siu.descrip_normalizada != ''

--14.-- Crear tabla maestro capacitaciones siu
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_siu_capacitaciones" AS
SELECT row_number() OVER () AS id,
       base_origen,
       tipo_capacitacion,
       descrip_normalizada,
       MIN(fecha_inicio_dictado) fecha_inicio,
       MAX(fecha_fin_dictado) fecha_fin,
       categoria,
       CASE WHEN SUM(estado) > 0 THEN 'ACTIVO' ELSE 'BAJA' END estado
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_siu_1" s1
GROUP BY base_origen,
       tipo_capacitacion,
       descrip_normalizada,
       categoria

--15.-- Crear tabla match capacitaciones siu
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_siu_match" AS
SELECT sc.base_origen,
       sc.tipo_capacitacion,
	   sc.id id_new,
       s1.capacitacion_id id_old
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_siu_1" s1
INNER JOIN "caba-piba-staging-zone-db"."tbp_typ_tmp_siu_capacitaciones" sc ON (sc.tipo_capacitacion = s1.tipo_capacitacion AND sc.descrip_normalizada = (CASE WHEN length(s1.descrip_normalizada) = 0 THEN s1.descrip_capacitacion ELSE s1.descrip_normalizada END)) AND (COALESCE(CAST(sc.categoria AS VARCHAR),'-') = COALESCE(CAST(s1.categoria AS VARCHAR),'-'))

--16.- Armar tabla de  maestro de capacitaciones unificada
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_capacitacion" AS
SELECT base_origen||'-'||tipo_capacitacion||'-'||CAST(id AS VARCHAR) id,
	   id id_new,
       base_origen,
       tipo_capacitacion,
       descrip_normalizada,
       fecha_inicio,
       fecha_fin,
       categoria,
       estado
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_crmsl_capacitaciones"
UNION ALL
SELECT base_origen||'-'||tipo_capacitacion||'-'||CAST(id AS VARCHAR),
	   id,
       base_origen,
       tipo_capacitacion,
       descrip_normalizada,
       fecha_inicio,
       fecha_fin,
       categoria,
       estado
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_sienfo_capacitaciones"
UNION ALL
SELECT base_origen||'-'||tipo_capacitacion||'-'||CAST(id AS VARCHAR),
	   id,
       base_origen,
       tipo_capacitacion,
       descrip_normalizada,
       fecha_inicio,
       fecha_fin,
       categoria,
       estado
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_goet_capacitaciones"
UNION ALL
SELECT base_origen||'-'||tipo_capacitacion||'-'||CAST(id AS VARCHAR),
	   id,
       base_origen,
       tipo_capacitacion,
       descrip_normalizada,
       fecha_inicio,
       fecha_fin,
       categoria,
       estado
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_moodle_capacitaciones"
UNION ALL
SELECT base_origen||'-'||tipo_capacitacion||'-'||CAST(id AS VARCHAR),
	   id,
       base_origen,
       tipo_capacitacion,
       descrip_normalizada,
       fecha_inicio,
       fecha_fin,
       categoria,
       estado
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_siu_capacitaciones"



-- 2022.11.28_step_06_Copy of 2022.11.28 consume capacitacion step 6 (Vcliente).sql 



--- CRM SOCIO LABORAL

CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_def_capacitacion_match" AS
SELECT base_origen,
       tipo_capacitacion,
	   id_new,
       id_old
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_crmsl_match"
UNION ALL
SELECT base_origen,
       tipo_capacitacion,
	   id_new,
       id_old
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_sienfo_match"
UNION ALL
SELECT base_origen,
       tipo_capacitacion,
	   id_new,
       id_old
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_goet_match"
UNION ALL
SELECT base_origen,
       tipo_capacitacion,
	   id_new,
       id_old
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_moodle_match"
UNION ALL
SELECT base_origen,
       tipo_capacitacion,
	   id_new,
       id_old
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_siu_match"

CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_def_capacitacion" AS
SELECT tc.id,
	   tc.id_new,
       tc.base_origen,
       tc.tipo_capacitacion,
       tc.descrip_normalizada,
       tc.fecha_inicio,
       tc.fecha_fin,
       tc.categoria,
       tc.estado,
	   ca.capacitacion_id capacitacion_id_asi,
	   ca.programa_id,
	   ca.descrip_capacitacion,
	   ca.tipo_formacion,
	   ca.descrip_tipo_formacion,
	   ca.modalidad_id,
	   ca.descrip_modalidad,
	   ca.tipo_capacitacion tipo_capacitacion_asi,
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
INNER JOIN "caba-piba-staging-zone-db"."tbp_typ_def_capacitacion_match" cm ON (cm.base_origen = tc.base_origen AND cm.id_new = tc.id_new)
INNER JOIN "caba-piba-staging-zone-db"."tbp_typ_tmp_capacitacion_asi" ca ON (ca.base_origen = tc.base_origen AND ca.codigo_capacitacion = cm.id_old)
GROUP BY tc.id,
	   tc.id_new,
       tc.base_origen,
       tc.tipo_capacitacion,
       tc.descrip_normalizada,
       tc.fecha_inicio,
       tc.fecha_fin,
       tc.categoria,
       tc.estado,
	   ca.capacitacion_id,
	   ca.programa_id,
	   ca.descrip_capacitacion,
	   ca.tipo_formacion,
	   ca.descrip_tipo_formacion,
	   ca.modalidad_id,
	   ca.descrip_modalidad,
	   ca.tipo_capacitacion,
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
UNION
SELECT tc.id,
	   tc.id_new,
       tc.base_origen,
       tc.tipo_capacitacion,
       tc.descrip_normalizada,
       tc.fecha_inicio,
       tc.fecha_fin,
       tc.categoria,
       tc.estado,
	   NULL capacitacion_id_asi,
	   NULL programa_id,
	   NULL descrip_capacitacion,
	   NULL tipo_formacion,
	   NULL descrip_tipo_formacion,
	   NULL modalidad_id,
	   NULL descrip_modalidad,
	   NULL tipo_capacitacion_asi,
	   NULL estado_capacitacion,
	   NULL seguimiento_personalizado,
	   NULL soporte_online,
	   NULL incentivos_terminalidad,
	   NULL exclusividad_participantes,
	   NULL categoria_back_id,
	   NULL descrip_back,
	   NULL categoria_front_id,
	   NULL descrip_front,
	   NULL detalle_capacitacion,
	   NULL otorga_certificado,
	   NULL filtro_ingreso,
	   NULL frecuencia_oferta_anual,
	   NULL duracion_cantidad,
	   NULL duracion_medida,
	   NULL duracion_dias,
	   NULL duracion_hs_reloj,
	   NULL vacantes
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_capacitacion" tc
WHERE NOT EXISTS (SELECT 1 FROM "caba-piba-staging-zone-db"."tbp_typ_def_capacitacion_match" cm,
	                          "caba-piba-staging-zone-db"."tbp_typ_tmp_capacitacion_asi" ca
			    WHERE cm.base_origen = tc.base_origen AND
					  cm.id_new = tc.id_new AND
					  ca.base_origen = tc.base_origen AND
					  ca.codigo_capacitacion = cm.id_old)
GROUP BY tc.id,
	   tc.id_new,
       tc.base_origen,
       tc.tipo_capacitacion,
       tc.descrip_normalizada,
       tc.fecha_inicio,
       tc.fecha_fin,
       tc.categoria,
       tc.estado



-- 2022.11.28_step_07_Copy of 2022.11.28 staging edicion capacitacion step 7 (Vcliente).sql 



-- 1.-- Crear EDICION CAPACITACION SIENFO Y CRMSL
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_edicion_capacitacion" AS
SELECT cm.base_origen,
	   cm.tipo_capacitacion,
       cm.id_new capacitacion_id_new,
       CAST(ca.id_carrera AS VARCHAR) capacitacion_id_old,
       ca.nom_carrera descrip_capacitacion_old,
       t.codigo_ct edicion_capacitacion_id,
       SPLIT_PART(CASE WHEN t.fecha = '0000-00-00' THEN NULL ELSE t.fecha END,'-',1) anio_dictado,
       CASE WHEN CAST(SPLIT_PART(CASE WHEN t.fecha = '0000-00-00' THEN NULL ELSE t.fecha END,'-',2) AS INTEGER) <= 6 THEN 1 ELSE 2 END semestre_dictado,
       DATE_PARSE(CASE WHEN t.fecha = '0000-00-00' THEN NULL ELSE t.fecha END, '%Y-%m-%d') fecha_inicio_dictado,
       DATE_PARSE(CASE WHEN t.fecha_fin = '0000-00-00' THEN NULL ELSE t.fecha_fin END, '%Y-%m-%d') fecha_fin_dictado,
       DATE_PARSE('1900-01-01', '%Y-%m-%d') fecha_tope_movimientos, ---t.inscripcionh se puede usar esta columna sumada a la fecha de inicio
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
	   ' ' modalidad,
       ' ' nombre_modalidad,
       ' ' descrip_modalidad,
       CAST(t.id_centro AS VARCHAR) cod_origen_establecimiento
FROM "caba-piba-raw-zone-db"."sienfo_carreras" ca
INNER JOIN "caba-piba-raw-zone-db"."sienfo_talleres" t ON (t.id_carrera = ca.id_carrera)
LEFT JOIN "caba-piba-raw-zone-db"."sienfo_testado" te ON (t.estado = te.valor)
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_capacitacion_match" cm ON (cm.base_origen = 'SIENFO' AND cm.tipo_capacitacion = 'CARRERA' AND cm.id_old = CAST(ca.id_carrera AS VARCHAR))
WHERE t.id_carrera != 0
UNION
SELECT cm.base_origen,
	   cm.tipo_capacitacion,
       cm.id_new capacitacion_id_new,
       CAST(cu.id_curso AS VARCHAR) capacitacion_id_old,
       cu.nom_curso descrip_capacitacion_old,
       t.codigo_ct edicion_capacitacion_id,
       --'Curso '||COALESCE(t.obs,' ') descrip_ed_capacitacion,
	   SPLIT_PART(CASE WHEN t.fecha = '0000-00-00' THEN NULL ELSE t.fecha END,'-',1) anio_dictado,
       CASE WHEN CAST(SPLIT_PART(CASE WHEN t.fecha = '0000-00-00' THEN NULL ELSE t.fecha END,'-',2) AS INTEGER) <= 6 THEN 1 ELSE 2 END semestre_dictado,
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
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_capacitacion_match" cm ON (cm.base_origen = 'SIENFO' AND cm.tipo_capacitacion = 'CURSO' AND cm.id_old = CAST(cu.id_curso AS VARCHAR))
WHERE COALESCE(t.id_carrera,0) = 0
UNION
-- EDICIONES CAPACITACIONES CRMSL

SELECT cm.base_origen,
	   cm.tipo_capacitacion,
       cm.id_new capacitacion_id_new,
       CAST(of.id AS VARCHAR) capacitacion_id_old,
       of.name descrip_capacitacion_old,
       CAST(of.id AS VARCHAR) edicion_capacitacion_id,
       --of.name descrip_ed_capacitacion,
	   CASE WHEN of.inicio IS NOT NULL THEN SPLIT_PART(CAST(of.inicio AS VARCHAR),'-',1) ELSE NULL END anio_dictado,
       CASE WHEN of.inicio IS NOT NULL THEN (CASE WHEN CAST(SPLIT_PART(CAST(of.inicio AS VARCHAR),'-',2) AS INTEGER) <= 6 THEN 1 ELSE 2 END) ELSE NULL END semestre_dictado,
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
INNER JOIN "caba-piba-raw-zone-db"."crm_sociolaboral_op_oportunidades_formacion_contacts_c" ofc  ON (of.id = ofc.op_oportun1d35rmacion_ida)
INNER JOIN "caba-piba-raw-zone-db"."crm_sociolaboral_contacts" co ON (co.id = ofc.op_oportunidades_formacion_contactscontacts_idb)
INNER JOIN "caba-piba-raw-zone-db"."crm_sociolaboral_contacts_cstm" cs ON (co.id = cs.id_c)
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_capacitacion_match" cm ON (cm.base_origen = 'CRMSL' AND cm.tipo_capacitacion = 'CURSO' AND cm.id_old = CAST(of.id AS VARCHAR))
WHERE (co.lead_source = 'sociolaboral'
OR ((co.lead_source = 'rib') AND cs.forma_parte_interm_lab_c = 'si'))
GROUP BY cm.base_origen,
	   cm.tipo_capacitacion,
       cm.id_new,
       CAST(of.id AS VARCHAR),
	   of.name,
       CAST(of.id AS VARCHAR),
	   of.inicio,
       of.fin,
	   of.estado_inscripcion,
	   of.estado_curso,
	   of.inscriptos,
       CAST(of.cupos AS VARCHAR),
       of.modalidad,
	   CAST(of.sede AS VARCHAR)



-- 2022.11.28_step_08_Copy of 2022.11.28 consume edicion capacitacion step 8 (Vcliente).sql 



-- EDICION CAPACITACION SIENFO
-- 1.- Crear tabla edicion capacitacion definitiva
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_def_edicion_capacitacion" AS
SELECT row_number() OVER () AS id,
       ed.base_origen,
	   ed.tipo_capacitacion,
       ed.capacitacion_id_new,
       ed.capacitacion_id_old,
       ed.edicion_capacitacion_id edicion_capacitacion_id_old,
       ed.anio_dictado,
       ed.semestre_dictado,
       ed.fecha_inicio_dictado,
       ed.fecha_fin_dictado,
       ed.fecha_tope_movimientos,
       ed.nombre_turno,
	   ed.descrip_turno,
	   ed.inscripcion_habilitada,
       ed.activo,
	   ed.cant_inscriptos,
	   ed.cupo,
	   ed.modalidad,
       ed.nombre_modalidad,
       ed.descrip_modalidad,
       ed.cod_origen_establecimiento
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_edicion_capacitacion" ed



-- 2022.11.28_step_09_Copy of 2022.11.28 staging estado_beneficiario_crmsl step 9 (Vcliente).sql 



-- Query Estado de Beneficiario para athena
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
INNER JOIN "caba-piba-raw-zone-db".crm_sociolaboral_contacts_cstm cc ON
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



-- 2022.11.28_step_10_Copy of 2022.11.28 staging estado_beneficiario_sienfo step 10 (Vcliente).sql 



CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_estado_beneficiario_sienfo" AS
WITH t AS (
	/*Limpieza de talleres, me quedo con los cursos validos,
	 * transformo a fechas las columnas, filtro fechas menores a 2003 o fechas de fin mayores a curdate + 1 aÃ±o
	 * merge id_duracion entre duracion de taller y la generica de cursos porque esta incompleta
	 */
	SELECT codigo_ct,
		CASE
			WHEN fecha = '0000-00-00' THEN NULL
			WHEN cast(fecha AS date) < cast('2003-01-01' AS date) THEN NULL ELSE cast(fecha AS date)
		END AS "fecha",
		CASE
			WHEN fecha_fin = '0000-00-00' THEN NULL
			WHEN cast(fecha_fin AS date) < cast('2003-01-01' AS date) THEN NULL
			WHEN cast(fecha_fin AS date) > date_add('year', 1, CURRENT_DATE) THEN NULL ELSE cast(fecha_fin AS date)
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
		AND t.id_carrera = 0
),
/*
 * baja como id normalizado
 * fecha de inicio de curso menor a 2003 como nulo y transformo a fecha
 * normalizo aprobado
 * primer estado de beneficiario basado en los datos de la tabla original
 */
f1 AS (
	SELECT f.nrodoc,
		f.codigo_ct,
		CASE
			WHEN baja IS NULL THEN 0 ELSE baja
		END AS "baja",
		f.fechabaja,
		CASE
			WHEN fecha_inc < cast('2003-01-01' AS date) THEN NULL ELSE cast(fecha_inc AS date)
		END AS "fecha_inc",
		CASE
			WHEN f.aprobado IS NULL THEN 0
			WHEN f.aprobado = '' THEN 0 ELSE cast(f.aprobado AS int)
		END AS "aprobado",
		CASE
			WHEN f.aprobado IN ('1', '3') THEN 'aprobado'
			WHEN f.fechabaja IS NOT NULL THEN 'baja'
			WHEN f.baja NOT IN (14, 22, 24, 0)
			AND baja IS NOT NULL THEN 'baja'
			WHEN f.aprobado IN ('2', '4', '5', '6', '8') THEN 'reprobado'
		END AS "estado_beneficiario"
	FROM "caba-piba-raw-zone-db"."sienfo_fichas" f
	WHERE nrodoc IS NOT NULL
),
/*
 *para completar fechas de inicio que no existen agrupo por fichas, la minima fecha de inicio de un alumno
 */
fechas_ct AS (
	SELECT codigo_ct,
		min(fecha_inc) AS fecha_inc_min
	FROM f1
	GROUP BY codigo_ct
),
/*
 * join de fichas y talleres
 * si la fecha de inicio del taller es nula uso la fecha de inicio del alumno
 * se invalidan fechas de fin inconsistentes para luego ser recalculadas por duracion
 */
tf AS (
	SELECT t.codigo_ct,
		CASE
			WHEN t.fecha IS NOT NULL THEN fecha ELSE fct.fecha_inc_min
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
		 * y las previamente invalidadas por inconsistencia, al ser fechas de fin menores que fecha de inicio
		 */
),
tf1 AS (
	SELECT tf.codigo_ct,
		tf.fecha,
		CASE
			WHEN fecha_fin IS NOT NULL THEN fecha_fin
			WHEN fecha_fin IS NULL THEN (
				CASE
					WHEN id_duracion = 1 THEN date_add('month', 9, fecha)
					WHEN id_duracion IN (2, 4) THEN date_add('month', 4, fecha)
					WHEN id_duracion = 3 THEN date_add('month', 2, fecha)
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
tf3 AS (
	SELECT tf1.codigo_ct,
		tf1.nrodoc,
		CASE
			WHEN tf1.fecha_inc IS NULL
			AND tf1.id_duracion IS NOT NULL
			AND tf1.fecha IS NOT NULL THEN (
				CASE
					WHEN id_duracion = 1 THEN date_add('month', -9, fecha)
					WHEN id_duracion IN (2, 4) THEN date_add('month', -4, fecha)
					WHEN id_duracion = 3 THEN date_add('month', -2, fecha)
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
			WHEN estado_beneficiario IS NOT NULL THEN estado_beneficiario
			WHEN baja = 0
			AND fechabaja IS NULL
			AND aprobado = 0 THEN (
				CASE
					WHEN date_add('month', 2, fecha_fin) <= CURRENT_DATE THEN 'reprobado'
					WHEN fecha_fin <= CURRENT_DATE THEN 'finalizo_cursada'
					WHEN fecha_inc < CURRENT_DATE
					AND fecha_fin > CURRENT_DATE THEN 'regular'
				END
			)
		END AS "estado_beneficiario2"
	FROM tf1
	WHERE nrodoc != ''
)
/*
 * Select final, estado_beneficiario2 es el calculado con el algoritmo de fechas,
 *  estado de beneficiario se basa en las columnas de la tabla
 */
SELECT tf3.codigo_ct,
	tf3.nrodoc,
	tf3.fecha_inc,
	tf3.fecha,
	tf3.fecha_fin,
	tf3.id_duracion,
	tf3.baja,
	tf3.fechabaja,
	tf3.aprobado,
	UPPER(tf3.estado_beneficiario2) estado_beneficiario,
	--tf3.estado_beneficiario,
	UPPER(tf3.nom_curso) nom_curso,
	tf3.id_curso
FROM tf3
--Existen fechas de inicio del alumno anteriores a fecha de inicio del curso
--nrodoc esta muy sucio
--Los estados de beneficiario2 nulos son cuando aprobado = 9 (nuevo id, no esta en en dump) corresponde a nombre = Actualiza, observa = CETBA [NO SE QUE SIGNIFICA]



-- 2022.11.28_step_11_Copy of 2022.11.28 staging cursada step 11 (Vcliente).sql 



-- EDICION CAPACITACION SIENFO
-- 1.-  Crear tabla temporal de cursadas
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_cursada" AS
SELECT 'SIENFO' base_origen,
       --sf.codigo_ct edicion_capacitacion_id_old,
	   CAST(sf.nrodoc AS VARCHAR) identificacion_alumno,
       CAST(sf.nrodoc AS VARCHAR) documento_broker,
       sfp.fecha_inc fecha_preinscripcion,
       sf.fecha_inc fecha_inicio,
       sf.fechabaja fecha_abandono,
       ed.fecha_fin_dictado fecha_egreso,
	   0 porcentaje_asistencia,
       '' estado,
       sf.aprobado cant_aprobadas,
       ed.id edicion_capacitacion_id_new,
	   ed.edicion_capacitacion_id_old,
       ed.capacitacion_id_new,
	   ed.capacitacion_id_old,
       ed.tipo_capacitacion,
       vec.vecino_id,
       vec.broker_id,
	   ebs.estado_beneficiario
FROM "caba-piba-raw-zone-db"."sienfo_fichas" sf
LEFT JOIN "caba-piba-raw-zone-db"."sienfo_fichas_preinscripcion" sfp ON (sfp.codigo_ct = sf.codigo_ct AND sf.nrodoc = sfp.nrodoc)
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_edicion_capacitacion" ed ON (ed.edicion_capacitacion_id_old = sf.codigo_ct AND ed.base_origen = 'SIENFO')
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_vecino" vec ON (vec.base_origen = 'SIENFO' AND vec.documento_broker = TRIM(sf.nrodoc))
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_tmp_estado_beneficiario_sienfo" ebs ON (ebs.nrodoc = sf.nrodoc AND ebs.codigo_ct = ed.edicion_capacitacion_id_old)
UNION
SELECT 'CRMSL' base_origen,
       --ofc.op_oportun1d35rmacion_ida edicion_capacitacion_id_old,
       vec.cod_origen indentificacion_alumno,
       CAST(cs.numero_documento_c AS VARCHAR) documento_broker,
       ofc.date_modified fecha_preinscripcion,
       NULL fecha_inicio,
       NULL fecha_abandono,
       NULL fecha_egreso,
       sc.porcentaje_asistencia_c porcentaje_asistencia,
       sc.estado_c estado,
       NULL cant_aprobadas,
       ed.id edicion_capacitacion_id_new,
	   ed.edicion_capacitacion_id_old,
       ed.capacitacion_id_new,
	   ed.capacitacion_id_old,
       ed.tipo_capacitacion,
       vec.vecino_id,
       vec.broker_id,
	   ebc.estado_beneficiario
FROM "caba-piba-raw-zone-db"."crm_sociolaboral_op_oportunidades_formacion_contacts_c" ofc
INNER JOIN "caba-piba-raw-zone-db"."crm_sociolaboral_contacts" co ON (co.id = ofc.op_oportunidades_formacion_contactscontacts_idb)
INNER JOIN "caba-piba-raw-zone-db"."crm_sociolaboral_contacts_cstm" cs ON (ofc.op_oportunidades_formacion_contactscontacts_idb = cs.id_c)
LEFT JOIN "caba-piba-raw-zone-db"."crm_sociolaboral_se_seguimiento_cstm" sc ON (sc.id_c = ofc.op_oportunidades_formacion_contactscontacts_idb)
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_edicion_capacitacion" ed ON (ed.edicion_capacitacion_id_old = ofc.op_oportun1d35rmacion_ida AND ed.base_origen = 'CRMSL')
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_vecino" vec ON (vec.base_origen = 'CRMSL' AND vec.cod_origen = CAST(co.id AS VARCHAR))
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_tmp_estado_beneficiario_crmsl" ebc ON (ebc.edicion_capacitacion_id_old = ed.edicion_capacitacion_id_old AND ebc.alumno_id_old = vec.cod_origen )
WHERE (co.lead_source = 'sociolaboral'
OR ((co.lead_source = 'rib') AND cs.forma_parte_interm_lab_c = 'si'))
AND cs.numero_documento_c IS NOT NULL



-- 2022.11.28_step_12_Copy of 2022.11.28 consume cursada step 12 (Vcliente)_.sql 



CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_def_cursada" AS
SELECT broker_id||'-'||CAST(edicion_capacitacion_id_new AS VARCHAR) id,
       base_origen,
	   edicion_capacitacion_id_old,
	   edicion_capacitacion_id_new,
	   identificacion_alumno identificacion_alumno_old,
       documento_broker,
       fecha_preinscripcion,
       fecha_inicio,
       fecha_abandono,
       fecha_egreso,
	   porcentaje_asistencia,
       estado,
       cant_aprobadas,
       vecino_id,
       broker_id,
	   estado_beneficiario
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_cursada"



-- 2022.11.28_step_13_Copy of 2022.11.28 consume trayectoria_educativa step 13 (Vcliente).sql 



CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_def_trayectoria_educativa" AS
SELECT row_number() OVER () AS tray_edu_id,
	   bg.id,
       bg.apellidos_nombres,
       bg.nombre,
       bg.apellido,
       bg.tipo_doc_broker,
       bg.documento_broker,
       vec.nacionalidad_broker,
       bg.genero,
       dc.id capacitacion_id,
       dc.descrip_normalizada descrip_capacitacion,
       dc.programa_id,
	   p.codigo_programa,
	   p.nombre_programa descrip_programa,
       dcu.fecha_inicio fecha_inicio_cursada,
       dcu.estado_beneficiario
       FROM "caba-piba-staging-zone-db"."tbp_broker_def_broker_general" bg
       INNER JOIN "caba-piba-staging-zone-db"."tbp_typ_def_vecino" vec ON (bg.id = vec.broker_id)
       LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_cursada" dcu ON (dcu.broker_id = vec.broker_id)
	   LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_edicion_capacitacion" ed ON (ed.id = dcu.edicion_capacitacion_id_new AND ed.base_origen = dcu.base_origen)
       LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_capacitacion" dc ON (dc.id_new = ed.capacitacion_id_new AND dc.base_origen = ed.base_origen)
	   LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_programa" p ON (p.programa_id = dc.programa_id)
GROUP BY bg.id,
       bg.apellidos_nombres,
       bg.nombre,
       bg.apellido,
       bg.tipo_doc_broker,
       bg.documento_broker,
       vec.nacionalidad_broker,
       bg.genero,
       dc.id,
       dc.descrip_normalizada,
       dc.programa_id,
	   p.codigo_programa,
	   p.nombre_programa,
       dcu.fecha_inicio,
       dcu.estado_beneficiario



-- 2022.12.12_step_05_2022.12.12 step 5 staging capacitacion (Vcliente).sql 



-- CRM SOCIOLABORAL - CRMSL
--1.-- Crear tabla capacitaciones socio laboral
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_crmsl_1" AS
SELECT
  'CRMSL'                AS base_origen,
  'CURSO'                AS tipo_capacitacion,
  CAST(of.id AS VARCHAR) AS capacitacion_id,
  TRIM(
      REGEXP_REPLACE(
          REGEXP_REPLACE(
              REGEXP_REPLACE(
                  REGEXP_REPLACE(
                      REGEXP_REPLACE(
                          REGEXP_REPLACE(
                              REGEXP_REPLACE(
                                  REGEXP_REPLACE(
                                      REGEXP_REPLACE(
                                          REGEXP_REPLACE(
                                              REGEXP_REPLACE(
                                                  REGEXP_REPLACE(
                                                      REGEXP_REPLACE(
                                                          REGEXP_REPLACE(
                                                              REGEXP_REPLACE(
                                                                  REGEXP_REPLACE(
                                                                      REGEXP_REPLACE(
                                                                          UPPER(
                                                                              CASE
                                                                                WHEN TRIM(UPPER(of.area)) IN
                                                                                     (
                                                                                      'GESTIÓN COMERCIAL',
                                                                                      'LIMPIEZA Y MANTENIMIENTO',
                                                                                      'MOZO Y CAMARERA',
                                                                                      'OPERARIO CALIFICADO'
                                                                                       )
                                                                                  THEN of.area
                                                                                WHEN of.name LIKE '%|%'
                                                                                  THEN split_part(of.name, '|', 2)
                                                                                WHEN of.name LIKE '%/%'
                                                                                  THEN split_part(of.name, '/', 2)
                                                                                  ELSE of.name
                                                                              END
                                                                            ),
                                                                          CHR(160),
                                                                          ' '
                                                                        ),
                                                                      'Á',
                                                                      'A'
                                                                    ),
                                                                  'É',
                                                                  'E'
                                                                ),
                                                              'Í',
                                                              'I'
                                                            ),
                                                          'Ó',
                                                          'O'
                                                        ),
                                                      '[ÚÜ]',
                                                      'U'
                                                    ),
                                                  '[0-9]+',
                                                  ''
                                                ),
                                              'TURNOS?|MAÑANA|NOCHE|TARDE',
                                              ''
                                            ),
                                          'LUNES|MARTES|MIERCOLES|JUEVES|VIERNES',
                                          ''
                                        ),
                                      'ENERO|FEBRERO|MARZO|ABRIL|MAYO|JUNIO|JUILO|AGOSTO|SEPTIEMBRE|OCTUBRE|NOVIEMBRE|DICIEMBRE',
                                      ''
                                    ),
                                  'BARRIO|PLAYON *DE *CHACARITA|RODRIGO *BUENO|MUGICA|RICCIARDELLI|FRAGA|MATADEROS|LACARRA|ZAVALETA|PUERTA|CHARRUA|SOLDATI',
                                  ''
                                ),
                              'EDICION|º|IRAM|GCBA|AKOMPANI|ARLOG|COOKMASTER',
                              ''
                            ),
                          '(CURSO|CAPACITACION) (EN|DE)?',
                          ''
                        ),
                      '[.\-]',
                      ' '
                    ),
                  'AIRES ACONDICIONADOS',
                  'AIRE ACONDICIONADO'
                ),
              '\( \)| Y *$',
              ''
            ),
          ' +',
          ' '
        )
    )                    AS descrip_normalizada,
  of.name                AS descrip_capacitacion,
  of.inicio              AS fecha_inicio_dictado,
  of.fin                 AS fecha_fin_dictado,
  CASE
    WHEN UPPER(of.estado_curso) = 'FINALIZADA'
      THEN 0
      ELSE 1
  END                    AS estado,
  TRIM(UPPER(of.area))   AS categoria
FROM
  "caba-piba-raw-zone-db"."crm_sociolaboral_op_oportunidades_formacion" OF
;
--2.-- Crear tabla maestro capacitaciones socio laboral
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_crmsl_capacitaciones" AS
SELECT
  ROW_NUMBER() OVER ()         AS id,
  s1.base_origen,
  s1.tipo_capacitacion,
  s1.descrip_normalizada,
  MIN(s1.fecha_inicio_dictado) AS fecha_inicio,
  MAX(s1.fecha_fin_dictado)    AS fecha_fin,
  CAST(NULL AS VARCHAR)        AS "categoria",
  CASE
    WHEN SUM(s1.estado) > 0
      THEN 'ACTIVO'
      ELSE 'BAJA'
  END                          AS estado
FROM
  "caba-piba-staging-zone-db"."tbp_typ_tmp_crmsl_1" s1
GROUP BY
  s1.base_origen,
  s1.tipo_capacitacion,
  s1.descrip_normalizada
;
--3.-- Crear tabla match capacitaciones socio laboral
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_crmsl_match" AS
SELECT
  sc.base_origen,
  sc.tipo_capacitacion,
  sc.id              AS id_new,
  s1.capacitacion_id AS id_old
FROM
  "caba-piba-staging-zone-db"."tbp_typ_tmp_crmsl_1"                         s1
  INNER JOIN "caba-piba-staging-zone-db"."tbp_typ_tmp_crmsl_capacitaciones" sc
             ON (
                   sc.tipo_capacitacion = s1.tipo_capacitacion
                 AND sc.descrip_normalizada = s1.descrip_normalizada
               )
;

-- SIENFO
--4.-- Crear tabla capacitaciones sienfo
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_sienfo_1" AS
SELECT
  'SIENFO'                                                                AS base_origen,
  'CARRERA'                                                               AS tipo_capacitacion,
  -- CAST(ca.id_carrera AS VARCHAR) AS capacitacion_id,
  CAST(ca.id_carrera AS VARCHAR) || '-' || CAST(ca.id_carrera AS VARCHAR) AS capacitacion_id,
  TRIM(
      REGEXP_REPLACE(
          REGEXP_REPLACE(
              REGEXP_REPLACE(
                  REGEXP_REPLACE(
                      REGEXP_REPLACE(
                          REGEXP_REPLACE(
                              REGEXP_REPLACE(UPPER(ca.nom_carrera), CHR(160), ' '),
                              'Á',
                              'A'
                            ),
                          'É',
                          'E'
                        ),
                      'Í',
                      'I'
                    ),
                  'Ó',
                  'O'
                ),
              '[ÚÜ]',
              'U'
            ),
          ' +',
          ' '
        )
    )                                                                     AS descrip_normalizada,
  UPPER(ca.nom_carrera)                                                   AS descrip_capacitacion,
  MIN(
      date_parse(
          CASE
            WHEN t.fecha = '0000-00-00'
              THEN NULL
              ELSE t.fecha
          END,
          '%Y-%m-%d'
        )
    )                                                                     AS fecha_inicio_dictado,
  MAX(
      date_parse(
          CASE
            WHEN t.fecha_fin = '0000-00-00'
              THEN NULL
              ELSE t.fecha_fin
          END,
          '%Y-%m-%d'
        )
    )                                                                     AS fecha_fin_dictado,
  CASE
    WHEN SUM(
             CASE
               WHEN UPPER(te.nombre) = 'ACTIVO'
                 THEN 1
                 ELSE 0
             END
           ) > 0
      THEN 1
      ELSE 0
  END                                                                     AS estado,
  UPPER(tc.nom_categoria)                                                 AS categoria
FROM
  "caba-piba-raw-zone-db"."sienfo_carreras"               ca
  INNER JOIN "caba-piba-raw-zone-db"."sienfo_talleres"    t
             ON ( t.id_carrera = ca.id_carrera )
  LEFT JOIN  "caba-piba-raw-zone-db"."sienfo_testado"     te
             ON ( ca.estado = te.valor ) -- REVISAR
  LEFT JOIN  "caba-piba-raw-zone-db"."sienfo_tcategorias" tc
             ON ( ca.id_tcarrera = tc.id_tcategoria )
WHERE
  COALESCE(t.id_carrera, 0) != 0
GROUP BY
  CAST(ca.id_carrera AS VARCHAR),
  ca.nom_carrera,
  UPPER(tc.nom_categoria)
UNION
SELECT
  'SIENFO'                                                                          AS base_origen,
  'CURSO'                                                                           AS tipo_capacitacion,
  CAST(COALESCE(t.id_carrera, 0) AS VARCHAR) || '-' || CAST(cu.id_curso AS VARCHAR) AS capacitacion_id,
  TRIM(
      REGEXP_REPLACE(
          REGEXP_REPLACE(
              REGEXP_REPLACE(
                  REGEXP_REPLACE(
                      REGEXP_REPLACE(
                          REGEXP_REPLACE(
                              REGEXP_REPLACE(
                                  REGEXP_REPLACE(UPPER(cu.nom_curso), CHR(160), ' '),
                                  'Á',
                                  'A'
                                ),
                              'É',
                              'E'
                            ),
                          'Í',
                          'I'
                        ),
                      'Ó',
                      'O'
                    ),
                  '[ÚÜ]',
                  'U'
                ),
              '\(ANCHORENA\)|\(ARANGUREN\)|\(ARANGUREN-JUNCAL\)|\(ARANGUREN-MANANTIALES\)|\(CORRALES\)|\(DON BOSCO\)|\(INTEGRADOR ALMAFUERTE\)|\(JUAN A. GARCIA\)|\(JUNCAL\)|\(LAMBARE\)|\(NIDO SOLDATI-CARRILLO\)|\(PRINGLES\)|\(RETIRO NORTE\)|\(RETIRO\)|\(SAN NICOLAS\)|\(SARAZA\)|\(SEIS ESQUINAS\)|HT18',
              ''
            ),
          ' +',
          ' '
        )
    )                                                                               AS descrip_normalizada,
  UPPER(cu.nom_curso)                                                               AS descrip_capacitacion,
  MIN(
      date_parse(
          CASE
            WHEN t.fecha = '0000-00-00'
              THEN NULL
              ELSE t.fecha
          END,
          '%Y-%m-%d'
        )
    )                                                                               AS fecha_inicio_dictado,
  MAX(
      date_parse(
          CASE
            WHEN t.fecha_fin = '0000-00-00'
              THEN NULL
              ELSE t.fecha_fin
          END,
          '%Y-%m-%d'
        )
    )                                                                               AS fecha_fin_dictado,
  CASE
    WHEN SUM(
             CASE
               WHEN UPPER(te.nombre) = 'ACTIVO'
                 THEN 1
                 ELSE 0
             END
           ) > 0
      THEN 1
      ELSE 0
  END                                                                               AS estado,
  UPPER(tc.nom_categoria)
FROM
  "caba-piba-raw-zone-db"."sienfo_cursos"                 cu
  INNER JOIN "caba-piba-raw-zone-db"."sienfo_talleres"    t
             ON ( t.id_curso = cu.id_curso )
  LEFT JOIN  "caba-piba-raw-zone-db"."sienfo_testado"     te
             ON ( t.estado = te.valor )
  LEFT JOIN  "caba-piba-raw-zone-db"."sienfo_tcategorias" tc
             ON ( tc.id_tcategoria = cu.id_tcategoria )
WHERE
  COALESCE(t.id_carrera, 0) = 0
GROUP BY
  CAST(cu.id_curso AS VARCHAR),
  cu.nom_curso,
  UPPER(tc.nom_categoria)
;
--5.-- Crear tabla maestro capacitaciones sienfo
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_sienfo_capacitaciones" AS
SELECT
  ROW_NUMBER() OVER ()         AS id,
  s1.base_origen,
  s1.tipo_capacitacion,
  s1.descrip_normalizada,
  MIN(s1.fecha_inicio_dictado) AS fecha_inicio,
  MAX(s1.fecha_fin_dictado)    AS fecha_fin,
  CAST(NULL AS VARCHAR)        AS "categoria",
  CASE
    WHEN SUM(s1.estado) > 0
      THEN 'ACTIVO'
      ELSE 'BAJA'
  END                          AS estado
FROM
  "caba-piba-staging-zone-db"."tbp_typ_tmp_sienfo_1" s1
GROUP BY
  s1.base_origen,
  s1.tipo_capacitacion,
  s1.descrip_normalizada
;
--6.-- Crear tabla match capacitaciones sienfo
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_sienfo_match" AS
SELECT
  sc.base_origen,
  sc.tipo_capacitacion,
  sc.id              AS id_new,
  s1.capacitacion_id AS id_old
FROM
  "caba-piba-staging-zone-db"."tbp_typ_tmp_sienfo_1"                         s1
  INNER JOIN "caba-piba-staging-zone-db"."tbp_typ_tmp_sienfo_capacitaciones" sc
             ON (
                   sc.tipo_capacitacion = s1.tipo_capacitacion
                 AND sc.descrip_normalizada = s1.descrip_normalizada
               )
;

-- GOET
--7.-- Crear tabla capacitaciones goet
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_goet_1" AS
SELECT
  'GOET'                                AS base_origen,
  CASE
    WHEN t.detalle IS NOT NULL
      THEN 'CARRERA'
      ELSE 'CURSO'
  END                                   AS tipo_capacitacion,
  CASE
    WHEN t.detalle IS NOT NULL
      THEN CAST(n.idnomenclador AS VARCHAR) || '-' || CAST(t.idkeytrayecto AS VARCHAR)
      ELSE CAST(n.idnomenclador AS VARCHAR)
  END                                   AS capacitacion_id,
  TRIM(
      REGEXP_REPLACE(
          REGEXP_REPLACE(
              REGEXP_REPLACE(
                  REGEXP_REPLACE(
                      REGEXP_REPLACE(
                          REGEXP_REPLACE(
                              REGEXP_REPLACE(
                                  REGEXP_REPLACE(
                                      REGEXP_REPLACE(
                                          UPPER(COALESCE(t.detalle, n.detalle)),
                                          CHR(160),
                                          ' '
                                        ),
                                      'Á',
                                      'A'
                                    ),
                                  'É',
                                  'E'
                                ),
                              'Í',
                              'I'
                            ),
                          'Ó',
                          'O'
                        ),
                      '[ÚÜ]',
                      'U'
                    ),
                  '\([0-9 ]*\)',
                  ''
                ),
              '["?¿]',
              ''
            ),
          ' +',
          ' '
        )
    )                                   AS descrip_normalizada,
  TRIM(COALESCE(t.detalle, n.detalle))  AS descrip_capacitacion,
  MIN(cc.iniciocurso)                   AS fecha_inicio_dictado,
  MAX(cc.fincurso)                      AS fecha_fin_dictado,
  CASE
    WHEN UPPER(en.detalle) = 'ACTIVO'
      THEN 1
      ELSE 0
  END                                   AS estado,
  UPPER(COALESCE(a.detalle, f.detalle)) AS categoria
FROM
  "caba-piba-raw-zone-db"."goet_centro_habilitacion_modulos"    chm
  INNER JOIN "caba-piba-raw-zone-db"."goet_centro_formacion"    cf
             ON ( cf.idcentro = chm.idcentro )
  INNER JOIN "caba-piba-raw-zone-db"."goet_centro_codigo_curso" cc
             ON ( cc.idctrhbmodulo = chm.idctrhbmodulo )
  INNER JOIN "caba-piba-raw-zone-db"."goet_nomenclador"         n
             ON ( n.idnomenclador = chm.idnomenclador )
  LEFT JOIN  "caba-piba-raw-zone-db"."goet_area"                a
             ON ( a.idarea = n.idarea )
  LEFT JOIN  "caba-piba-raw-zone-db"."goet_familia"             f
             ON ( f.idfamilia = n.idfamilia )
  LEFT JOIN  "caba-piba-raw-zone-db"."goet_mudulosxtrayecto"    m
             ON ( m.idnomenclador = n.idnomenclador )
  LEFT JOIN  "caba-piba-raw-zone-db"."goet_trayecto"            t
             ON ( t.idkeytrayecto = m.idkeytrayecto )
  LEFT JOIN  "caba-piba-raw-zone-db"."goet_trayecto_nivel"      tn
             ON ( tn.idniveltrayecto = t.idniveltrayecto )
  LEFT JOIN  "caba-piba-raw-zone-db"."goet_estado_nomenclador"  en
             ON ( en.idestadonomenclador = n.idestadonomenclador )
GROUP BY
  t.detalle,
  CAST(n.idnomenclador AS VARCHAR) || '-' || CAST(t.idkeytrayecto AS VARCHAR),
  CAST(n.idnomenclador AS VARCHAR),
  n.detalle,
  UPPER(en.detalle),
  a.detalle,
  f.detalle
;
--8.-- Crear tabla maestro capacitaciones goet
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_goet_capacitaciones" AS
SELECT
  ROW_NUMBER() OVER ()         AS id,
  s1.base_origen,
  s1.tipo_capacitacion,
  s1.descrip_normalizada,
  MIN(s1.fecha_inicio_dictado) AS fecha_inicio,
  MAX(s1.fecha_fin_dictado)    AS fecha_fin,
  CAST(NULL AS VARCHAR)        AS "categoria",
  CASE
    WHEN SUM(s1.estado) > 0
      THEN 'ACTIVO'
      ELSE 'BAJA'
  END                          AS estado
FROM
  "caba-piba-staging-zone-db"."tbp_typ_tmp_goet_1" s1
GROUP BY
  s1.base_origen,
  s1.tipo_capacitacion,
  s1.descrip_normalizada
;
--9.-- Crear tabla match capacitaciones sienfo
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_goet_match" AS
SELECT
  sc.base_origen,
  sc.tipo_capacitacion,
  sc.id              AS id_new,
  s1.capacitacion_id AS id_old
FROM
  "caba-piba-staging-zone-db"."tbp_typ_tmp_goet_1"                         s1
  INNER JOIN "caba-piba-staging-zone-db"."tbp_typ_tmp_goet_capacitaciones" sc
             ON (
                   sc.tipo_capacitacion = s1.tipo_capacitacion
                 AND sc.descrip_normalizada = s1.descrip_normalizada
               )
;

-- MOODLE
--10.-- Crear tabla capacitaciones moodle
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_moodle_1" AS
SELECT
  'MOODLE'               AS base_origen,
  -- En CAC son cursos de dos módulos
  'CURSO'                AS tipo_capacitacion,
  CAST(co.id AS VARCHAR) AS capacitacion_id,
  TRIM(
      REGEXP_REPLACE(
          REGEXP_REPLACE(
              REGEXP_REPLACE(
                  REGEXP_REPLACE(
                      REGEXP_REPLACE(
                          REGEXP_REPLACE(
                              REGEXP_REPLACE(
                                  REGEXP_REPLACE(
                                      REGEXP_REPLACE(
                                          REGEXP_REPLACE(UPPER(cc.name), CHR(160), ' '),
                                          'Á',
                                          'A'
                                        ),
                                      'É',
                                      'E'
                                    ),
                                  'Í',
                                  'I'
                                ),
                              'Ó',
                              'O'
                            ),
                          '[ÚÜ]',
                          'U'
                        ),
                      '[0-9_\-.]+',
                      ' '
                    ),
                  'FORMACION PARA EMPLEABILIDAD',
                  'FORMACION PARA LA EMPLEABILIDAD'
                ),
              ' FS ',
              ' FULLSTACK '
            ),
          ' +',
          ' '
        )
    )                    AS descrip_normalizada,
  TRIM(cc.name)          AS descrip_capacitacion,
  TRIM(
      REGEXP_REPLACE(
          REGEXP_REPLACE(
              REGEXP_REPLACE(
                  REGEXP_REPLACE(
                      REGEXP_REPLACE(
                          REGEXP_REPLACE(
                              REGEXP_REPLACE(
                                  REGEXP_REPLACE(
                                      REGEXP_REPLACE(
                                          REGEXP_REPLACE(
                                              REGEXP_REPLACE(
                                                  REGEXP_REPLACE(UPPER(co.fullname), CHR(160), ' '),
                                                  'Á',
                                                  'A'
                                                ),
                                              'É',
                                              'E'
                                            ),
                                          'Í',
                                          'I'
                                        ),
                                      'Ó',
                                      'O'
                                    ),
                                  '[ÚÜ]',
                                  'U'
                                ),
                              '[0-9./_]',
                              ' '
                            ),
                          'CATAMARCA|CHACO|MAR DEL PLATA|NEUQUEN|TUCUMAN',
                          ' '
                        ),
                      ' ENE| FEB| MAR| ABR| MAY| JUN| JUL| AGO| SEP| OCT| NOV| DIC',
                      ' '
                    ),
                  '\(COMISION *\)|AULA[A-Z]? *$|^CURSO:* | [A-HJ-Z]$|^CAC',
                  ' '
                ),
              '-',
              ' - '
            ),
          ' +',
          ' '
        )
    )                    AS descrip_normalizada_modulo,
  TRIM(co.fullname)      AS descrip_modulo,
  MIN(
      CASE
        WHEN co.startdate != 0
          THEN date_parse(
            date_format(from_unixtime(co.startdate), '%Y-%m-%d %h:%i%p'),
            '%Y-%m-%d %h:%i%p'
          )
      END
    )                    AS fecha_inicio_dictado,
  MAX(
      CASE
        WHEN co.enddate != 0
          THEN date_parse(
            date_format(from_unixtime(co.enddate), '%Y-%m-%d %h:%i%p'),
            '%Y-%m-%d %h:%i%p'
          )
      END
    )                    AS fecha_fin_dictado,
  CASE
    WHEN co.enddate = 0
      THEN 1
      ELSE 0
  END                    AS estado,
  CASE
    WHEN cc.idnumber LIKE 'CAC%'
      THEN 'CAC'
    WHEN cc.idnumber LIKE 'FPE%'
      THEN 'FPE'
  END                    AS categoria
FROM
  "caba-piba-raw-zone-db"."moodle_dgtedu01_mdl_course"                       co
  INNER JOIN "caba-piba-raw-zone-db"."moodle_dgtedu01_mdl_course_categories" cc
             ON ( co.category = cc.id )
WHERE
  (
        cc.idnumber LIKE 'CAC%' -- CAC
      OR cc.idnumber LIKE 'FPE%' -- Habilidades/Formación para la empleabilidad
    )
GROUP BY
  CAST(co.id AS VARCHAR),
  co.fullname,
  co.enddate,
  cc.name,
  cc.idnumber
;
--11.-- Crear tabla maestro capacitaciones moodle
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_moodle_capacitaciones" AS
SELECT
  ROW_NUMBER() OVER ()         AS id,
  s1.base_origen,
  s1.tipo_capacitacion,
  s1.descrip_normalizada,
  MIN(s1.fecha_inicio_dictado) AS fecha_inicio,
  MAX(s1.fecha_fin_dictado)    AS fecha_fin,
  CAST(NULL AS VARCHAR)        AS "categoria",
  CASE
    WHEN SUM(s1.estado) > 0
      THEN 'ACTIVO'
      ELSE 'BAJA'
  END                          AS estado
FROM
  "caba-piba-staging-zone-db"."tbp_typ_tmp_moodle_1" s1
GROUP BY
  s1.base_origen,
  s1.tipo_capacitacion,
  s1.descrip_normalizada
;
--12.-- Crear tabla match capacitaciones moodle
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_moodle_match" AS
SELECT
  sc.base_origen,
  sc.tipo_capacitacion,
  sc.id              AS id_new,
  s1.capacitacion_id AS id_old
FROM
  "caba-piba-staging-zone-db"."tbp_typ_tmp_moodle_1"                         s1
  INNER JOIN "caba-piba-staging-zone-db"."tbp_typ_tmp_moodle_capacitaciones" sc
             ON (
                   sc.tipo_capacitacion = s1.tipo_capacitacion
                 AND sc.descrip_normalizada = s1.descrip_normalizada
               )
;

-- SIU
--13.-- Crear tabla capacitaciones siu
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_siu_1" AS
WITH
  siu AS (
  SELECT
    'SIU'                      AS base_origen,
    'CARRERA'                  AS tipo_capacitacion,
    CAST(spl.plan AS VARCHAR)  AS capacitacion_id,
    TRIM(
        REGEXP_REPLACE(
            REGEXP_REPLACE(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(
                            REGEXP_REPLACE(
                                REGEXP_REPLACE(
                                    REGEXP_REPLACE(
                                        REGEXP_REPLACE(UPPER(spl.nombre), CHR(160), ' '),
                                        'Á',
                                        'A'
                                      ),
                                    'É',
                                    'E'
                                  ),
                                'Í',
                                'I'
                              ),
                            'Ó',
                            'O'
                          ),
                        '[ÚÜ]',
                        'U'
                      ),
                    '[0-9\-]+|IFTS| RM |NUEVO|OK!',
                    ''
                  ),
                '\( *\)',
                ''
              ),
            ' +',
            ' '
          )
      )                        AS descrip_normalizada_plan,
    spl.nombre                 AS descrip_capacitacion_plan,
    TRIM(
        REGEXP_REPLACE(
            REGEXP_REPLACE(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(
                            REGEXP_REPLACE(
                                REGEXP_REPLACE(
                                    REGEXP_REPLACE(
                                        REGEXP_REPLACE(UPPER(spr.nombre), CHR(160), ' '),
                                        'Á',
                                        'A'
                                      ),
                                    'É',
                                    'E'
                                  ),
                                'Í',
                                'I'
                              ),
                            'Ó',
                            'O'
                          ),
                        '[ÚÜ]',
                        'U'
                      ),
                    '[0-9\-]+|IFTS| RM |NUEVO|OK!',
                    ''
                  ),
                '\( *\)',
                ''
              ),
            ' +',
            ' '
          )
      )                        AS descrip_normalizada_propuesta,
    spr.nombre                 AS descrip_capacitacion_propuesta,
    spl.fecha_entrada_vigencia AS fecha_inicio_dictado,
    spl.fecha_baja             AS fecha_fin_dictado,
    CASE
      WHEN spl.estado = 'V'
        THEN 1
        ELSE 0
    END                        AS estado,
    CAST(NULL AS VARCHAR)      AS categoria
  FROM
    "caba-piba-raw-zone-db"."siu_toba_3_3_negocio_sga_planes"                spl
    INNER JOIN "caba-piba-raw-zone-db"."siu_toba_3_3_negocio_sga_propuestas" spr
               ON ( spl.propuesta = spr.propuesta )
  GROUP BY
    CAST(spl.plan AS VARCHAR),
    spl.nombre,
    spr.nombre,
    spl.fecha_entrada_vigencia,
    spl.fecha_baja,
    spl.estado
  )
SELECT
  base_origen,
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
FROM
  siu
;
--14.-- Crear tabla maestro capacitaciones siu
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_siu_capacitaciones" AS
SELECT
  ROW_NUMBER() OVER ()         AS id,
  s1.base_origen,
  s1.tipo_capacitacion,
  s1.descrip_normalizada,
  MIN(s1.fecha_inicio_dictado) AS fecha_inicio,
  MAX(s1.fecha_fin_dictado)    AS fecha_fin,
  CAST(NULL AS VARCHAR)        AS "categoria",
  CASE
    WHEN SUM(s1.estado) > 0
      THEN 'ACTIVO'
      ELSE 'BAJA'
  END                          AS estado
FROM
  "caba-piba-staging-zone-db"."tbp_typ_tmp_siu_1" s1
GROUP BY
  s1.base_origen,
  s1.tipo_capacitacion,
  s1.descrip_normalizada
;
--15.-- Crear tabla match capacitaciones siu
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_siu_match" AS
SELECT
  sc.base_origen,
  sc.tipo_capacitacion,
  sc.id              AS id_new,
  s1.capacitacion_id AS id_old
FROM
  "caba-piba-staging-zone-db"."tbp_typ_tmp_siu_1"                         s1
  INNER JOIN "caba-piba-staging-zone-db"."tbp_typ_tmp_siu_capacitaciones" sc
             ON (
                   sc.tipo_capacitacion = s1.tipo_capacitacion
                 AND sc.descrip_normalizada = s1.descrip_normalizada
               )
;

-- UNIFICADAS
--16.-- Crear tabla de maestro de capacitaciones unificada
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_capacitacion" AS
SELECT
  base_origen || '-' || tipo_capacitacion || '-' || CAST(id AS VARCHAR) AS id,
  id                                                                    AS id_new,
  base_origen,
  tipo_capacitacion,
  descrip_normalizada,
  fecha_inicio,
  fecha_fin,
  categoria,
  estado
FROM
  "caba-piba-staging-zone-db"."tbp_typ_tmp_crmsl_capacitaciones"
UNION ALL
SELECT
  base_origen || '-' || tipo_capacitacion || '-' || CAST(id AS VARCHAR) AS id,
  id                                                                    AS id_new,
  base_origen,
  tipo_capacitacion,
  descrip_normalizada,
  fecha_inicio,
  fecha_fin,
  categoria,
  estado
FROM
  "caba-piba-staging-zone-db"."tbp_typ_tmp_sienfo_capacitaciones"
UNION ALL
SELECT
  base_origen || '-' || tipo_capacitacion || '-' || CAST(id AS VARCHAR) AS id,
  id                                                                    AS id_new,
  base_origen,
  tipo_capacitacion,
  descrip_normalizada,
  fecha_inicio,
  fecha_fin,
  categoria,
  estado
FROM
  "caba-piba-staging-zone-db"."tbp_typ_tmp_goet_capacitaciones"
UNION ALL
SELECT
  base_origen || '-' || tipo_capacitacion || '-' || CAST(id AS VARCHAR) AS id,
  id                                                                    AS id_new,
  base_origen,
  tipo_capacitacion,
  descrip_normalizada,
  fecha_inicio,
  fecha_fin,
  categoria,
  estado
FROM
  "caba-piba-staging-zone-db"."tbp_typ_tmp_moodle_capacitaciones"
UNION ALL
SELECT
  base_origen || '-' || tipo_capacitacion || '-' || CAST(id AS VARCHAR) AS id,
  id                                                                    AS id_new,
  base_origen,
  tipo_capacitacion,
  descrip_normalizada,
  fecha_inicio,
  fecha_fin,
  categoria,
  estado
FROM
  "caba-piba-staging-zone-db"."tbp_typ_tmp_siu_capacitaciones"
;
--17.-- Crear tabla de capacitaciones de origen unificada
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_def_capacitacion_origen" AS
SELECT
  base_origen || '-' || tipo_capacitacion || '-' || CAST(capacitacion_id AS VARCHAR) AS id,
  base_origen,
  tipo_capacitacion,
  capacitacion_id                                                                    AS id_old,
  descrip_normalizada,
  descrip_capacitacion,
  fecha_inicio_dictado,
  fecha_fin_dictado,
  estado,
  categoria
FROM
  "caba-piba-staging-zone-db"."tbp_typ_tmp_crmsl_1"
UNION ALL
SELECT
  base_origen || '-' || tipo_capacitacion || '-' || CAST(capacitacion_id AS VARCHAR) AS id,
  base_origen,
  tipo_capacitacion,
  capacitacion_id                                                                    AS id_old,
  descrip_normalizada,
  descrip_capacitacion,
  fecha_inicio_dictado,
  fecha_fin_dictado,
  estado,
  categoria
FROM
  "caba-piba-staging-zone-db"."tbp_typ_tmp_sienfo_1"
UNION ALL
SELECT
  base_origen || '-' || tipo_capacitacion || '-' || CAST(capacitacion_id AS VARCHAR) AS id,
  base_origen,
  tipo_capacitacion,
  capacitacion_id                                                                    AS id_old,
  descrip_normalizada,
  descrip_capacitacion,
  fecha_inicio_dictado,
  fecha_fin_dictado,
  estado,
  categoria
FROM
  "caba-piba-staging-zone-db"."tbp_typ_tmp_goet_1"
UNION ALL
SELECT
  base_origen || '-' || tipo_capacitacion || '-' || CAST(capacitacion_id AS VARCHAR) AS id,
  base_origen,
  tipo_capacitacion,
  capacitacion_id                                                                    AS id_old,
  descrip_normalizada,
  descrip_capacitacion,
  fecha_inicio_dictado,
  fecha_fin_dictado,
  estado,
  categoria
FROM
  "caba-piba-staging-zone-db"."tbp_typ_tmp_moodle_1"
UNION ALL
SELECT
  base_origen || '-' || tipo_capacitacion || '-' || CAST(capacitacion_id AS VARCHAR) AS id,
  base_origen,
  tipo_capacitacion,
  capacitacion_id                                                                    AS id_old,
  descrip_normalizada,
  descrip_capacitacion,
  fecha_inicio_dictado,
  fecha_fin_dictado,
  estado,
  categoria
FROM
  "caba-piba-staging-zone-db"."tbp_typ_tmp_siu_1"
;



-- 2022.12.12_step_06_2022.12.12 step 6 consume capacitacion (Vcliente).sql 



--1.-- Crear tabla de match de capacitaciones unificada
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_def_capacitacion_match" AS
SELECT
  base_origen || '-' || tipo_capacitacion || '-' || CAST(id_new AS VARCHAR) AS id,
  base_origen,
  tipo_capacitacion,
  id_new,
  id_old
FROM
  "caba-piba-staging-zone-db"."tbp_typ_tmp_crmsl_match"
UNION ALL
SELECT
  base_origen || '-' || tipo_capacitacion || '-' || CAST(id_new AS VARCHAR) AS id,
  base_origen,
  tipo_capacitacion,
  id_new,
  id_old
FROM
  "caba-piba-staging-zone-db"."tbp_typ_tmp_sienfo_match"
UNION ALL
SELECT
  base_origen || '-' || tipo_capacitacion || '-' || CAST(id_new AS VARCHAR) AS id,
  base_origen,
  tipo_capacitacion,
  id_new,
  id_old
FROM
  "caba-piba-staging-zone-db"."tbp_typ_tmp_goet_match"
UNION ALL
SELECT
  base_origen || '-' || tipo_capacitacion || '-' || CAST(id_new AS VARCHAR) AS id,
  base_origen,
  tipo_capacitacion,
  id_new,
  id_old
FROM
  "caba-piba-staging-zone-db"."tbp_typ_tmp_moodle_match"
UNION ALL
SELECT
  base_origen || '-' || tipo_capacitacion || '-' || CAST(id_new AS VARCHAR) AS id,
  base_origen,
  tipo_capacitacion,
  id_new,
  id_old
FROM
  "caba-piba-staging-zone-db"."tbp_typ_tmp_siu_match"
;
--2.--Crear tabla de maestro de capacitaciones unificada
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_def_capacitacion" AS
SELECT
  tc.id,
  tc.id_new,
  tc.base_origen,
  tc.tipo_capacitacion,
  tc.descrip_normalizada,
  tc.fecha_inicio,
  tc.fecha_fin,
  tc.categoria,
  tc.estado,
  ca.capacitacion_id   AS capacitacion_id_asi,
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
FROM
  "caba-piba-staging-zone-db"."tbp_typ_tmp_capacitacion" tc
  LEFT JOIN (
            SELECT
              ca1.*,
              cm.id
            FROM
              "caba-piba-staging-zone-db"."tbp_typ_tmp_capacitacion_asi"              ca1
              INNER JOIN "caba-piba-staging-zone-db"."tbp_typ_def_capacitacion_match" cm
                         ON ( ca1.base_origen = cm.base_origen AND ca1.codigo_capacitacion2 = cm.id_old )
            ) AS                                         ca
            ON ( tc.id = ca.id )



-- Copy of 2022.11.25 create_tbp_typ_tmp_edicion_capacitacion (Vcliente).sql 



-- 1.-- Crear EDICION CAPACITACION SIENFO Y CRMSL
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_edicion_capacitacion" AS
SELECT cm.base_origen,
	   cm.tipo_capacitacion,
       cm.id_new capacitacion_id_new,
	   dc.id capacitacion_id,
	   dc.descrip_normalizada,
	   dc.categoria,
       CAST(ca.id_carrera AS VARCHAR) capacitacion_id_old,
       ca.nom_carrera descrip_capacitacion_old,
       t.codigo_ct edicion_capacitacion_id,
       --'Carrera '||COALESCE(t.obs,' ') descrip_ed_capacitacion,
       SPLIT_PART(CASE WHEN t.fecha = '0000-00-00' THEN NULL ELSE t.fecha END,'-',1) anio_dictado,
       CASE WHEN CAST(SPLIT_PART(CASE WHEN t.fecha = '0000-00-00' THEN NULL ELSE t.fecha END,'-',2) AS INTEGER) <= 6 THEN 1 ELSE 2 END semestre_dictado,
       DATE_PARSE(CASE WHEN t.fecha = '0000-00-00' THEN NULL ELSE t.fecha END, '%Y-%m-%d') fecha_inicio_dictado,
       DATE_PARSE(CASE WHEN t.fecha_fin = '0000-00-00' THEN NULL ELSE t.fecha_fin END, '%Y-%m-%d') fecha_fin_dictado,
       DATE_PARSE('1900-01-01', '%Y-%m-%d') fecha_tope_movimientos, ---t.inscripcionh se puede usar esta columna sumada a la fecha de inicio
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
	   ' ' modalidad,
       ' ' nombre_modalidad,
       ' ' descrip_modalidad,
       CAST(t.id_centro AS VARCHAR) cod_origen_establecimiento
FROM "caba-piba-raw-zone-db"."sienfo_carreras" ca
INNER JOIN "caba-piba-raw-zone-db"."sienfo_talleres" t ON (t.id_carrera = ca.id_carrera)
LEFT JOIN "caba-piba-raw-zone-db"."sienfo_testado" te ON (t.estado = te.valor)
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_capacitacion_match" cm ON (cm.base_origen = 'SIENFO' AND cm.tipo_capacitacion = 'CARRERA' AND cm.id_old = CAST(ca.id_carrera AS VARCHAR))
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_capacitacion" dc ON (dc.id_new = cm.id_new AND dc.base_origen = cm.base_origen AND dc.tipo_capacitacion = cm.tipo_capacitacion)
WHERE t.id_carrera != 0
UNION
SELECT cm.base_origen,
	   cm.tipo_capacitacion,
       cm.id_new capacitacion_id_new,
	   dc.id,
	   dc.descrip_normalizada,
	   dc.categoria,
       CAST(cu.id_curso AS VARCHAR) capacitacion_id_old,
       cu.nom_curso descrip_capacitacion_old,
       t.codigo_ct edicion_capacitacion_id,
       --'Curso '||COALESCE(t.obs,' ') descrip_ed_capacitacion,
	   SPLIT_PART(CASE WHEN t.fecha = '0000-00-00' THEN NULL ELSE t.fecha END,'-',1) anio_dictado,
       CASE WHEN CAST(SPLIT_PART(CASE WHEN t.fecha = '0000-00-00' THEN NULL ELSE t.fecha END,'-',2) AS INTEGER) <= 6 THEN 1 ELSE 2 END semestre_dictado,
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
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_capacitacion_match" cm ON (cm.base_origen = 'SIENFO' AND cm.tipo_capacitacion = 'CURSO' AND cm.id_old = CAST(cu.id_curso AS VARCHAR))
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_capacitacion" dc ON (dc.id_new = cm.id_new AND dc.base_origen = cm.base_origen AND dc.tipo_capacitacion = cm.tipo_capacitacion)
WHERE COALESCE(t.id_carrera,0) = 0
UNION
-- EDICIONES CAPACITACIONES CRMSL

SELECT cm.base_origen,
	   cm.tipo_capacitacion,
       cm.id_new capacitacion_id_new,
	   dc.id,
	   dc.descrip_normalizada,
	   dc.categoria,
       CAST(of.id AS VARCHAR) capacitacion_id_old,
       of.name descrip_capacitacion_old,
       CAST(of.id AS VARCHAR) edicion_capacitacion_id,
       --of.name descrip_ed_capacitacion,
	   CASE WHEN of.inicio IS NOT NULL THEN SPLIT_PART(CAST(of.inicio AS VARCHAR),'-',1) ELSE NULL END anio_dictado,
       CASE WHEN of.inicio IS NOT NULL THEN (CASE WHEN CAST(SPLIT_PART(CAST(of.inicio AS VARCHAR),'-',2) AS INTEGER) <= 6 THEN 1 ELSE 2 END) ELSE NULL END semestre_dictado,
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
INNER JOIN "caba-piba-raw-zone-db"."crm_sociolaboral_op_oportunidades_formacion_contacts_c" ofc  ON (of.id = ofc.op_oportun1d35rmacion_ida)
INNER JOIN "caba-piba-raw-zone-db"."crm_sociolaboral_contacts" co ON (co.id = ofc.op_oportunidades_formacion_contactscontacts_idb)
INNER JOIN "caba-piba-raw-zone-db"."crm_sociolaboral_contacts_cstm" cs ON (co.id = cs.id_c)
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_capacitacion_match" cm ON (cm.base_origen = 'CRMSL' AND cm.tipo_capacitacion = 'CURSO' AND cm.id_old = CAST(of.id AS VARCHAR))
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_capacitacion" dc ON (dc.id_new = cm.id_new AND dc.base_origen = cm.base_origen AND dc.tipo_capacitacion = cm.tipo_capacitacion)
WHERE (co.lead_source = 'sociolaboral'
OR ((co.lead_source = 'rib') AND cs.forma_parte_interm_lab_c = 'si'))
GROUP BY cm.base_origen,
	   cm.tipo_capacitacion,
       cm.id_new,
	   dc.id,
	   dc.descrip_normalizada,
	   dc.categoria,
       CAST(of.id AS VARCHAR),
	   of.name,
       CAST(of.id AS VARCHAR),
	   of.inicio,
       of.fin,
	   of.estado_inscripcion,
	   of.estado_curso,
	   of.inscriptos,
       CAST(of.cupos AS VARCHAR),
       of.modalidad,
	   CAST(of.sede AS VARCHAR)



