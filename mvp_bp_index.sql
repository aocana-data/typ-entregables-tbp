-- 2022.12.16 step 01 - staging vecinos (Vcliente).sql 



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



-- 2022.12.16 step 02 - consume vecinos (Vcliente).sql 



-- 1.- Crear la tabla definitiva de vecinos
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
LEFT JOIN "caba-piba-raw-zone-db"."ciudadanos_renaper" cr ON (cr.dni = tvb.documento_broker AND cr.sexo = tvb.genero_broker))
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



-- 2022.12.16 step 03 - consume programa (Vcliente).sql 



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



-- 2022.12.16 step 04 - staging capacitacion asi (Vcliente).sql 



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



-- 2022.12.16 step 05 - staging capacitacion (Vcliente).sql 



-- CRM SOCIOLABORAL - CRMSL
--1.-- Crear tabla capacitaciones socio laboral
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
-- fecha fin => min(fecha de fin de capacitaciones)
-- Si no hay valor para fecha_inicio o fecha_fin quedan en NULL
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
-- fecha fin => min(fecha de fin de capacitaciones)
-- Si no hay valor para fecha_inicio o fecha_fin quedan en NULL
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
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_moodle_1" AS

SELECT 'MOODLE' AS base_origen,
	-- En CAC son cursos de dos módulos
	'CURSO' AS tipo_capacitacion,
	CAST(co.id AS VARCHAR) AS capacitacion_id,
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
GROUP BY CAST(co.id AS VARCHAR),
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
-- fecha fin => min(fecha de fin de capacitaciones)
-- Si no hay valor para fecha_inicio o fecha_fin quedan en NULL
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
-- fecha fin => min(fecha de fin de capacitaciones)
-- Si no hay valor para fecha_inicio o fecha_fin quedan en NULL
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



-- 2022.12.16 step 06 - consume capacitacion (Vcliente).sql 



--1.-- Crear tabla de match de capacitaciones unificada
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



-- 2022.12.16 step 07 - staging estado_beneficiario_crmsl (Vcliente).sql 



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



-- 2022.12.16 step 08 - staging estado_beneficiario_sienfo(Vcliente).sql 



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
		AND t.id_carrera IN (0,1,2,18)
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
			WHEN f.aprobado IN ('1', '3') THEN 'EGRESADO'
			WHEN f.fechabaja IS NOT NULL THEN 'BAJA'
			WHEN f.baja NOT IN (14, 22, 24, 0)
			AND baja IS NOT NULL THEN 'BAJA'
			WHEN f.aprobado IN ('2', '4', '5', '6', '8') THEN 'REPROBADO'
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
					WHEN id_duracion = 1 THEN date_add('month', 9, fecha) -- anual
					WHEN id_duracion IN (2, 4) THEN date_add('month', 4, fecha) -- cuatrimestral
					WHEN id_duracion = 3 THEN date_add('month', 2, fecha) -- bimestral
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
			WHEN tf1.estado_beneficiario IS NOT NULL THEN tf1.estado_beneficiario
			WHEN tf1.baja = 0
    			AND tf1.fechabaja IS NULL
    			AND tf1.aprobado NOT IN (1,2,3,4,5,6,8) THEN (
    				CASE
    					WHEN date_add('month', 2, tf1.fecha_fin) <= CURRENT_DATE THEN 'REPROBADO'
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
	t.codigo_ct,
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
	cast(NULL AS date) fechabaja,
	0 aprobado,
	'INSCRIPTO' estado_beneficiario
FROM
    tf2
LEFT JOIN "caba-piba-raw-zone-db"."sienfo_carrera" sc ON sc.id_carrera = tf2.id_carrera
LEFT JOIN "caba-piba-raw-zone-db"."sienfo_talleres" t ON (t.id_carrera = sc.id_carrera)
WHERE tf2.id_carrera != 0
GROUP BY (tf2.id_carrera, tf2.nrodoc,sc.nom_carrera,t.codigo_ct)
HAVING min(tf2.fecha) IS NOT NULL AND max(tf2.fecha_fin) IS NOT NULL
), car_cur AS (
/*
 * Select final, estado_beneficiario2 es el calculado con el algoritmo de fechas,
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
	cal.nrodoc ||'-'||cast(cal.id_carrera AS varchar) llave_doc_idcap --Esta llave es para agrupar las carreras por ediciÃ³n capacitacion anual
FROM
    carreras_al cal)
--Existen fechas de inicio del alumno anteriores a fecha de inicio del curso
--nrodoc esta muy sucio
--Los estados de beneficiario2 nulos son cuando aprobado = 9 (nuevo id, no esta en en dump) corresponde a nombre = Actualiza, observa = CETBA [NO SE QUE SIGNIFICA]
)
-- dejo solo los campos que interesan por el momento para joinear con el modelo
SELECT
    cuca.codigo_ct,
    cuca.nrodoc,
    cuca.id_curso,
    cuca.id_carrera,
    cuca.estado_beneficiario,
    cuca.llave_doc_idcap, --CUANDO SE HAGA EL JOIN DIFERENCIAR POR CURSO O CARRERA
    tipo_formacion,
    nombre_cur_car,
	fecha fecha_inicio_edicion_capacitacion,
	fecha_fin fecha_fin_edicion_capacitacion,
	fecha_inc fecha_inscipcion
FROM
    car_cur cuca



-- 2022.12.16 step 09 - staging estado_beneficiario_goet(Vcliente).sql 



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
		"caba-piba-raw-zone-db"."goet_usuarios" u
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



-- 2022.12.16 step 10 - staging estado_beneficiario_moodle (Vcliente).sql 



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



-- 2022.12.16 step 11 - staging edicion capacitacion (Vcliente).sql 



-- 1.-- Crear EDICION CAPACITACION GOET, MOODLE, SIENFO Y CRMSL
-- DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_tmp_edicion_capacitacion`;
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_edicion_capacitacion" AS
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

		-- cc.iniciocurso tiene 100% de completitud, VER posibles casos fuera de rango logico
		CAST(SPLIT_PART(date_format(cc.iniciocurso, '%Y-%m-%d %h:%i%p'), '-', 1)  AS INTEGER) anio_inicio,

		-- El proximo campo es el semestre de inicio de la edicion capacitacion, NO indica que la misma es semestral o anual
		-- cc.iniciocurso tiene 100% de completitud, VER posibles casos fuera de rango logico
		CASE WHEN CAST(SPLIT_PART(date_format(cc.iniciocurso, '%Y-%m-%d %h:%i%p'), '-', 2)  AS INTEGER)  <= 6
			THEN 1
				ELSE 2
			END semestre_inicio,

		-- cc.iniciocurso tiene 100% de completitud, VER posibles casos fuera de rango logico
		CAST(DATE_PARSE(date_format(cc.iniciocurso, '%Y-%m-%d %h:%i%p'), '%Y-%m-%d %h:%i%p') AS DATE) fecha_inicio_dictado,

		-- cc.fincurso tiene 100% de completitud, VER posibles casos fuera de rango logico
		CAST(DATE_PARSE(date_format(cc.fincurso, '%Y-%m-%d %h:%i%p'), '%Y-%m-%d %h:%i%p') AS DATE) fecha_fin_dictado,

		-- el valor tiene una completitud del 100%
		CAST(DATE_PARSE(date_format(cc.inicioinscripcion, '%Y-%m-%d %h:%i%p'), '%Y-%m-%d %h:%i%p') AS DATE) fecha_inicio_inscripcion,
		-- el valor tiene una completitud del 100%
		CAST(DATE_PARSE(date_format(cc.cierreinscripcion, '%Y-%m-%d %h:%i%p'), '%Y-%m-%d %h:%i%p') AS DATE) fecha_limite_inscripcion,

		-- se considera turno mañana si la hora de inicio es entre las 7 y 12, tarde entre 13 y 17, noche entre 18 y 24
		-- SI MENOR A 7 en GOET son datos inconsistens, segun lo controlado en el dataleak
		CASE WHEN CAST(regexp_extract(cc.diayhorario, '(([0-9|:])\w+)') AS INTEGER) BETWEEN 7 AND 12 THEN 'Mañana'
		WHEN CAST(regexp_extract(cc.diayhorario, '(([0-9|:])\w+)') AS INTEGER) BETWEEN 13 AND 17 THEN 'Tarde'
		WHEN CAST(regexp_extract(cc.diayhorario, '(([0-9|:])\w+)') AS INTEGER) BETWEEN 18 AND 24 THEN 'Noche' ELSE NULL END turno,

		-- se convierte los dias a las siguientes posibilidades => Lunes,Martes,Miercoles,Jueves,Sabado,Domingo
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
		CAST(cc.matricula AS VARCHAR) cant_inscriptos,
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
	LEFT JOIN "caba-piba-raw-zone-db"."goet_usuarios" u ON u."IdUsuario" = ia."IdUsuario"
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
	GROUP BY
		dcmatch.tipo_capacitacion,
		dcmatch.id_new,
		dcmatch.id_old,
		UPPER(t.detalle),
		UPPER(n.Detalle),
		UPPER(en.Detalle),
		cc.idctrcdcurso,
		cc.iniciocurso,
		dc.fecha_inicio,
		cc.fincurso,
		dc.fecha_fin,
		cc.inicioinscripcion,
		cc.cierreinscripcion,
		cc.diayhorario,
		cc.idcursoestado,
		cc.matricula,
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
	CAST(co.id AS VARCHAR) capacitacion_id_old,
	co.fullname descrip_capacitacion_old,

	-- el id de capacitacion y de edicion capacitacion es el mismo,
	-- dado que para moodle cada edicion capacitacion es un curso/capacitacion en si mismo
	CAST(co.id AS VARCHAR) edicion_capacitacion_id,

	-- se utiliza la fecha calculada en el script de estado de beneficiario
	-- En caso que este en null, se utiliza co.startdate (tiene 89.87% de completitud), en otro caso queda en null
	CAST(SPLIT_PART(CASE
	WHEN fechas.fecha_inicio = '0000-00-00'
		THEN date_format(from_unixtime(co.startdate), '%Y-%m-%d %h:%i%p')
	ELSE date_format(cast(fechas.fecha_inicio AS date), '%Y-%m-%d %h:%i%p')
	END, '-', 1)  AS INTEGER) anio_inicio,

	-- El proximo campo es el semestre de inicio de la edicion capacitacion, NO indica que la misma es semestral o anual
	-- se utiliza la fecha calculada en el script de estado de beneficiario
	-- En caso que este en null, se utiliza co.startdate (tiene 89.87% de completitud), en otro caso queda en null
	CASE
	WHEN CAST(SPLIT_PART(CASE
		WHEN fechas.fecha_inicio = '0000-00-00'
			THEN date_format(from_unixtime(co.startdate), '%Y-%m-%d %h:%i%p')
		ELSE date_format(cast(fechas.fecha_inicio AS date), '%Y-%m-%d %h:%i%p')
		END, '-', 2) AS INTEGER) <= 6
		THEN 1
	ELSE 2
	END semestre_inicio,

	-- se utiliza la fecha calculada en el script de estado de beneficiario
	-- En caso que este en null, se utiliza co.startdate (tiene 89.87% de completitud), en otro caso queda en null
	CAST(DATE_PARSE(CASE
	WHEN  fechas.fecha_inicio = '0000-00-00'
		THEN date_format(from_unixtime(co.startdate), '%Y-%m-%d %h:%i%p')
	ELSE date_format(cast(fechas.fecha_inicio AS date), '%Y-%m-%d %h:%i%p')
	END , '%Y-%m-%d %h:%i%p') AS DATE) fecha_inicio_dictado,

	-- se utiliza la fecha calculada en el script de estado de beneficiario
	-- En caso que este en null, se utiliza co.enddate (tiene 24.90% de completitud), en otro caso queda en null
	CAST(DATE_PARSE(CASE
	WHEN  fechas.fecha_fin = '0000-00-00'
		THEN date_format(from_unixtime(co.enddate), '%Y-%m-%d %h:%i%p')
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

	' ' turno,
	' ' dias_cursada,
	CASE
		WHEN co.visible
			THEN 'S'
		ELSE 'N'
		END inscripcion_abierta,
	CASE
		WHEN co.enddate = 0
			THEN 'S'
		ELSE 'N'
		END activo,
	CAST(cc.coursecount AS VARCHAR) cant_inscriptos,
	' ' vacantes,
	-- Si la modalidad no esta en la edicion se toma desde la entidad capacitacion, NULL en otro caso
	dc.modalidad_id,
	dc.descrip_modalidad,
	' ' cod_origen_establecimiento
FROM "caba-piba-raw-zone-db"."moodle_dgtedu01_mdl_course" co
LEFT JOIN "caba-piba-raw-zone-db"."moodle_dgtedu01_mdl_course_categories" cc ON (co.category = cc.id)
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_capacitacion_match" dcmatch ON (
		dcmatch.id_old = CAST(co.id AS VARCHAR)
		AND dcmatch.base_origen = 'MOODLE'
		)
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_capacitacion" dc ON (
		dcmatch.id_new = dc.id_new
		AND dcmatch.base_origen = dc.base_origen
		)
LEFT JOIN (SELECT
    id_curso,
	CAST(MIN(inicio_curso) AS VARCHAR) fecha_inicio,
	CAST(MAX(fin_curso) AS VARCHAR) fecha_fin,
	CAST(MIN(inscripcion_inicio_cursada) AS VARCHAR) fecha_inicio_inscripcion,
	CAST(MAX(inscripcion_final_cursada) AS VARCHAR) fecha_fin_inscripcion,
	COUNT(*) cantidad_inscriptos
FROM
"caba-piba-staging-zone-db"."tbp_typ_tmp_estado_beneficiario_moodle"
GROUP BY
id_curso
) fechas ON (co.id=fechas.id_curso)

UNION
-- SIENFO CARRERA
SELECT 'SIENFO' base_origen,
	   cm.tipo_capacitacion,
       cm.id_new capacitacion_id_new,
       CAST(ca.id_carrera AS VARCHAR) capacitacion_id_old,
       ca.nom_carrera descrip_capacitacion_old,
       t.codigo_ct edicion_capacitacion_id,

		-- se utiliza la fecha calculada en el script de estado de beneficiario
		-- En caso que este en null, se utiliza la fecha de la tabla taller, en otro caso queda en null
		CAST(SPLIT_PART(CASE
		WHEN fechas.fecha_inicio = '0000-00-00'
			THEN date_format(cast(t.fecha AS date), '%Y-%m-%d %h:%i%p')
		ELSE date_format(cast(fechas.fecha_inicio AS date), '%Y-%m-%d %h:%i%p')
		END, '-', 1)  AS INTEGER) anio_inicio,

		-- El proximo campo es el semestre de inicio de la edicion capacitacion, NO indica que la misma es semestral o anual
		-- se utiliza la fecha calculada en el script de estado de beneficiario
		-- En caso que este en null, se utiliza la fecha de la tabla taller, en otro caso queda en null
		CASE
		WHEN CAST(SPLIT_PART(CASE
						WHEN fechas.fecha_inicio = '0000-00-00'
							THEN date_format(cast(t.fecha AS date), '%Y-%m-%d %h:%i%p')
						ELSE date_format(cast(fechas.fecha_inicio AS date), '%Y-%m-%d %h:%i%p')
						END, '-', 2) AS INTEGER) <= 6
			THEN 1
		ELSE 2
		END semestre_inicio,

		-- se utiliza la fecha calculada en el script de estado de beneficiario
		-- En caso que este en null, se utiliza la fecha de la tabla taller, en otro caso queda en null
		CAST(DATE_PARSE(CASE
		WHEN  fechas.fecha_inicio = '0000-00-00'
			THEN date_format(cast(t.fecha AS date), '%Y-%m-%d %h:%i%p')
		ELSE date_format(cast(fechas.fecha_inicio AS date), '%Y-%m-%d %h:%i%p')
		END , '%Y-%m-%d %h:%i%p') AS DATE) fecha_inicio_dictado,

		-- se utiliza la fecha calculada en el script de estado de beneficiario
		-- En caso que este en null, se utiliza la fecha de la tabla taller, en otro caso queda en null
		CAST(DATE_PARSE(CASE
		WHEN  fechas.fecha_fin = '0000-00-00'
			THEN date_format(cast(t.fecha_fin AS date), '%Y-%m-%d %h:%i%p')
		ELSE date_format(cast(fechas.fecha_fin AS date), '%Y-%m-%d %h:%i%p')
		END , '%Y-%m-%d %h:%i%p') AS DATE) fecha_fin_dictado,

	   -- no se encuentra en bbdd origen un valor para el atributo
	   -- se utiliza la fecha calculada en el script de estado de beneficiario
	   -- En caso que este en null, se utiliza la fecha de la tabla taller, en otro caso queda en null
	   	CAST(DATE_PARSE(CASE
		WHEN  fechas.fecha_inicio_inscripcion = '0000-00-00'
			THEN NULL
		ELSE date_format(cast(fechas.fecha_inicio_inscripcion AS date), '%Y-%m-%d %h:%i%p')
		END , '%Y-%m-%d %h:%i%p') AS DATE) fecha_inicio_inscripcion,

	   -- no se encuentra en bbdd origen un valor para el atributo
	   -- se utiliza la fecha calculada en el script de estado de beneficiario
	   -- En caso que este en null, se utiliza la fecha de la tabla taller, en otro caso queda en null
	   CAST(DATE_PARSE(CASE
		WHEN  fechas.fecha_fin_inscripcion = '0000-00-00'
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
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_capacitacion_match" cm ON (cm.base_origen = 'SIENFO' AND cm.tipo_capacitacion = 'CARRERA' AND cm.id_old = CAST(ca.id_carrera AS VARCHAR))
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_capacitacion" dc ON (cm.id_new = dc.id_new AND cm.base_origen = dc.base_origen)
LEFT JOIN (
SELECT
    codigo_ct,
	CAST(MIN(fecha_inicio_edicion_capacitacion) AS VARCHAR) fecha_inicio,
	CAST(MAX(fecha_fin_edicion_capacitacion) AS VARCHAR) fecha_fin,
	CAST(MIN(fecha_inscipcion) AS VARCHAR) fecha_inicio_inscripcion,
	CAST(MAX(fecha_inscipcion) AS VARCHAR) fecha_fin_inscripcion,
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
	   cm.tipo_capacitacion,
       cm.id_new capacitacion_id_new,
       CAST(cu.id_curso AS VARCHAR) capacitacion_id_old,
       cu.nom_curso descrip_capacitacion_old,
       t.codigo_ct edicion_capacitacion_id,

		-- se utiliza la fecha calculada en el script de estado de beneficiario
		-- En caso que este en null, se utiliza la fecha de la tabla taller, en otro caso queda en null
		CAST(SPLIT_PART(CASE
		WHEN fechas.fecha_inicio = '0000-00-00'
			THEN date_format(cast(t.fecha AS date), '%Y-%m-%d %h:%i%p')
		ELSE date_format(cast(fechas.fecha_inicio AS date), '%Y-%m-%d %h:%i%p')
		END, '-', 1)  AS INTEGER) anio_inicio,

		-- El proximo campo es el semestre de inicio de la edicion capacitacion, NO indica que la misma es semestral o anual
		-- se utiliza la fecha calculada en el script de estado de beneficiario
		-- En caso que este en null, se utiliza la fecha de la tabla taller, en otro caso queda en null
		CASE
		WHEN CAST(SPLIT_PART(CASE
						WHEN fechas.fecha_inicio = '0000-00-00'
							THEN date_format(cast(t.fecha AS date), '%Y-%m-%d %h:%i%p')
						ELSE date_format(cast(fechas.fecha_inicio AS date), '%Y-%m-%d %h:%i%p')
						END, '-', 2) AS INTEGER) <= 6
			THEN 1
		ELSE 2
		END semestre_inicio,

		-- se utiliza la fecha calculada en el script de estado de beneficiario
		-- En caso que este en null, se utiliza la fecha de la tabla taller, en otro caso queda en null
		CAST(DATE_PARSE(CASE
		WHEN  fechas.fecha_inicio = '0000-00-00'
			THEN date_format(cast(t.fecha AS date), '%Y-%m-%d %h:%i%p')
		ELSE date_format(cast(fechas.fecha_inicio AS date), '%Y-%m-%d %h:%i%p')
		END , '%Y-%m-%d %h:%i%p') AS DATE) fecha_inicio_dictado,

		-- se utiliza la fecha calculada en el script de estado de beneficiario
		-- En caso que este en null, se utiliza la fecha de la tabla taller, en otro caso queda en null
		CAST(DATE_PARSE(CASE
		WHEN  fechas.fecha_fin = '0000-00-00'
			THEN date_format(cast(t.fecha_fin AS date), '%Y-%m-%d %h:%i%p')
		ELSE date_format(cast(fechas.fecha_fin AS date), '%Y-%m-%d %h:%i%p')
		END , '%Y-%m-%d %h:%i%p') AS DATE) fecha_fin_dictado,

		-- se utiliza la fecha calculada en el script de estado de beneficiario
		-- En caso que este en null, se utiliza la fecha de la tabla taller, en otro caso queda en null
	   	CAST(DATE_PARSE(CASE
		WHEN  fechas.fecha_inicio_inscripcion = '0000-00-00'
			THEN NULL
		ELSE date_format(cast(fechas.fecha_inicio_inscripcion AS date), '%Y-%m-%d %h:%i%p')
		END , '%Y-%m-%d %h:%i%p') AS DATE) fecha_inicio_inscripcion,

		-- se utiliza la fecha calculada en el script de estado de beneficiario
		-- En caso que este en null, se utiliza la fecha de la tabla taller, en otro caso queda en null
	   CAST(DATE_PARSE(CASE
		WHEN  fechas.fecha_fin_inscripcion = '0000-00-00'
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
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_capacitacion_match" cm ON (cm.base_origen = 'SIENFO' AND cm.tipo_capacitacion = 'CURSO' AND cm.id_old = CAST(cu.id_curso AS VARCHAR))
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_capacitacion" dc ON (cm.id_new = dc.id_new AND cm.base_origen = dc.base_origen)
LEFT JOIN (
SELECT
    codigo_ct,
	CAST(MIN(fecha_inicio_edicion_capacitacion) AS VARCHAR) fecha_inicio,
	CAST(MAX(fecha_fin_edicion_capacitacion) AS VARCHAR) fecha_fin,
	CAST(MIN(fecha_inscipcion) AS VARCHAR) fecha_inicio_inscripcion,
	CAST(MAX(fecha_inscipcion) AS VARCHAR) fecha_fin_inscripcion,
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
		ELSE 2
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
       CAST(of.inscriptos AS VARCHAR) cant_inscriptos,
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
LEFT JOIN "caba-piba-raw-zone-db"."crm_sociolaboral_contacts_cstm" cs ON (co.id = cs.id_c)
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
	CAST(of.id AS VARCHAR),
	of.name,
	CAST(of.id AS VARCHAR),
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
	CAST(of.inscriptos AS VARCHAR),
	CAST(of.cupos AS VARCHAR),
	of.modalidad,
	dc.modalidad_id,
	dc.descrip_modalidad,
	CAST(of.sede AS VARCHAR)



-- 2022.12.16 step 12 - consume edicion capacitacion (Vcliente).sql 



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



-- 2022.12.16 step 13 - staging cursada (Vcliente).sql 



-- GOET, MOODLE, SIENFO, CRMSL
-- ENTIDAD:CURSADA
-- 1.-  Crear tabla temporal de cursadas
-- DROP TABLE `caba-piba-staging-zone-db`.`tbp_typ_tmp_cursada`;
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_cursada" AS
-- GOET

SELECT
     'GOET' base_origen,
	 CAST(gu.idusuario AS VARCHAR) identificacion_alumno,
	 CAST(gu.ndocumento AS VARCHAR) documento_broker,
	-- no esta en la BBDD, SE ASUME la fecha de inicio de la capacitacion como la fecha de preinscripcion
	date_parse(date_format(cc.iniciocurso, '%Y-%m-%d %h:%i%p'), '%Y-%m-%d %h:%i%p') fecha_preinscripcion,

	-- fecha de la primer asistencia del alumno a la edicion capacitacion
	date_parse(date_format(inicio_cursada.fecha_inicio, '%Y-%m-%d %h:%i%p'), '%Y-%m-%d %h:%i%p') fecha_inicio,

	-- para obtener la fecha de abondono se busca alumnos inscriptos que no tengan certificado y se establece
	-- como fecha de abandono la fecha de ultima asistencia, siempre que la edicion capacitacion haya finalizado (fecha fin menor a hoy)
	-- IMPORTANTE! hay que evaluar si la logica es correcta, porque Â¿talvez se pueden demorar en generar el certificado respectivo?
	date_parse(date_format(datos_abandono.fecha_abandono, '%Y-%m-%d %h:%i%p'), '%Y-%m-%d %h:%i%p') fecha_abandono,

	-- en el caso que la persona haya abandonado la fecha de egreso es la fecha de abandono, en caso contrario
	-- es la fecha del certificado y de no existir el mismo, es la fecha de fin de curso
	-- VER ESTADO BENEFICIARIO!!
	CASE
		WHEN cc.fincurso IS NOT NULL AND datos_abandono.fecha_abandono IS NULL AND certificado.fecha IS NOT NULL
			THEN date_parse(date_format(certificado.fecha, '%Y-%m-%d %h:%i%p'), '%Y-%m-%d %h:%i%p')
		WHEN cc.fincurso IS NOT NULL AND datos_abandono.fecha_abandono IS NULL AND certificado.fecha IS NULL
			THEN date_parse(date_format(cc.fincurso, '%Y-%m-%d %h:%i%p'), '%Y-%m-%d %h:%i%p')
		ELSE date_parse(date_format(datos_abandono.fecha_abandono, '%Y-%m-%d %h:%i%p'), '%Y-%m-%d %h:%i%p')
	END fecha_egreso,

	0 porcentaje_asistencia,
    ' ' estado,
	-- Se podria calcular el promedio desde la tabla calificacion_alumnos, pero no la cantidad de materias aprobadas
	-- dado que no esta contemplado en el modelo de GOET
	' ' cant_aprobadas,

	ed.id edicion_capacitacion_id_new,
	ed.edicion_capacitacion_id_old,

	ed.capacitacion_id_new,
	ed.capacitacion_id_old,
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
LEFT JOIN "caba-piba-raw-zone-db"."goet_usuarios" gu ON (gu.idusuario = gia.idusuario)
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
ed.id,
ed.edicion_capacitacion_id_old,
ed.capacitacion_id_new,
ed.capacitacion_id_old,
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
	-- Analizar de donde se puede sacar el valor, cuando este el script de estado de beneficiario
	NULL fecha_preinscripcion,
	date_parse(date_format(from_unixtime(uenrolments.timestart), '%Y-%m-%d %h:%i%p'), '%Y-%m-%d %h:%i%p') fecha_inicio,
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

	0 porcentaje_asistencia,
	-- no se puede determinar el significado de los valores del uenrolments.STATUS, por lo que se pone estado como ' '
	' ' estado,
	' ' cant_aprobadas,
	ed.id edicion_capacitacion_id_new,
	ed.edicion_capacitacion_id_old,
	ed.capacitacion_id_new,
	ed.capacitacion_id_old,

	ed.tipo_capacitacion,
	vec.vecino_id,
	vec.broker_id,
	eb.estado AS "estado_beneficiario"
FROM "caba-piba-raw-zone-db"."moodle_dgtedu01_mdl_course" co
LEFT JOIN "caba-piba-raw-zone-db"."moodle_dgtedu01_mdl_enrol" enrol ON (co.id = enrol.courseid)
LEFT JOIN "caba-piba-raw-zone-db"."moodle_dgtedu01_mdl_user_enrolments" uenrolments ON (uenrolments.enrolid = enrol.id)
LEFT JOIN "caba-piba-raw-zone-db"."moodle_dgtedu01_mdl_user" usuario ON (usuario.id = uenrolments.userid)
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_edicion_capacitacion" ed ON (
		ed.edicion_capacitacion_id_old = CAST(co.id AS VARCHAR)
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
	ed.capacitacion_id_old,
	ed.tipo_capacitacion,
	vec.vecino_id,
	vec.broker_id,
	eb.estado
UNION
-- SIENFO  ver xq hay fichas cuyo nro de documento no esta en la tabla vecinos!!!
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
LEFT JOIN "caba-piba-raw-zone-db"."sienfo_alumnos" a ON (sf.nrodoc = a.nrodoc)
LEFT JOIN "caba-piba-raw-zone-db"."sienfo_tdoc" d ON (a.tipodoc = d.tipodoc)
LEFT JOIN "caba-piba-raw-zone-db"."sienfo_fichas_preinscripcion" sfp ON (sfp.codigo_ct = sf.codigo_ct AND sf.nrodoc = sfp.nrodoc)
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_edicion_capacitacion" ed ON (ed.edicion_capacitacion_id_old = sf.codigo_ct AND ed.base_origen = 'SIENFO')
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_vecino" vec ON
(vec.base_origen = 'SIENFO'
AND vec.documento_broker = CASE WHEN d.nombre IN ('D.N.I.', 'L.C.', 'L.E.', 'C.I.', 'CUIT', 'CUIL') THEN REGEXP_REPLACE(UPPER(a.nrodoc),'[A-Za-z]+|\.|\,','') ELSE
           CAST(a.nrodoc AS VARCHAR) END
)
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_tmp_estado_beneficiario_sienfo" ebs
ON (ebs.nrodoc = sf.nrodoc AND (
	(ebs.codigo_ct = ed.edicion_capacitacion_id_old AND ebs.tipo_formacion like 'CURSO' )
	OR
	(split_part(ebs.llave_doc_idcap,'-',2) = ed.capacitacion_id_old AND ebs.tipo_formacion like 'CARRERA' AND ebs.codigo_ct = ed.edicion_capacitacion_id_old))
	)
UNION
-- CRMSL
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
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_vecino" vec ON (vec.base_origen = 'CRMSL' AND vec.documento_broker = CAST(cs.numero_documento_c AS VARCHAR))
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_tmp_estado_beneficiario_crmsl" ebc ON (ebc.edicion_capacitacion_id_old = ed.edicion_capacitacion_id_old AND ebc.alumno_id_old = vec.cod_origen )
WHERE (co.lead_source = 'sociolaboral'
OR ((co.lead_source = 'rib') AND cs.forma_parte_interm_lab_c = 'si'))
AND cs.numero_documento_c IS NOT NULL



-- 2022.12.16 step 14 - consume cursada (Vcliente).sql 



-- EDICION CURSADA GOET, MOODLE, SIENFO Y CRMSL
-- 1.- Crear tabla edicion cursada definitiva
-- DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_def_cursada`;
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
WHERE
edicion_capacitacion_id_new IS NOT NULL
AND
(broker_id||'-'||CAST(edicion_capacitacion_id_new AS VARCHAR)) IS NOT NULL



-- 2022.12.16 step 15 - consume trayectoria_educativa (Vcliente).sql 



-- TRAYECTORIA EDUCATIVA GOET, MOODLE, SIENFO Y CRMSL
-- 1.- Crear tabla trayectoria educativa definitiva
-- DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_def_trayectoria_educativa`;
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
       dc.capacitacion_id_asi capacitacion_id,
       COALESCE(dc.descrip_capacitacion, dc.descrip_normalizada) descrip_capacitacion,
	   dc.detalle_capacitacion,
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
-- WHERE dc.base_origen != 'GOET'
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
       dcu.fecha_inicio,
       dcu.estado_beneficiario



