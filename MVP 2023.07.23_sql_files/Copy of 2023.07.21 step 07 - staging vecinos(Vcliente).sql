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
		   REGEXP_REPLACE(CONCAT(' ', UPPER(nmpd.nro_documento)), '[A-Za-z\.\-\,\(\)\@\_\ ]+', ''),
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
       REGEXP_REPLACE(CONCAT(' ', UPPER(nmpd.nro_documento)), '[A-Za-z\.\-\,\(\)\@\_\ ]+', '') AS documento_broker,
       UPPER(nmp.nombres) nombre,
       UPPER(nmp.apellido) apellido,
       nmp.fecha_nacimiento,
       nmp.sexo genero_broker,
       CAST(nmp.nacionalidad AS VARCHAR) nacionalidad,
       nmn.descripcion descrip_nacionalidad,
       CASE WHEN UPPER(SUBSTR(nmn.descripcion,1,3)) = 'ARG' THEN 'ARG'
       ELSE 'NNN' 
       END nacionalidad_broker,
	   (CASE WHEN ((UPPER(nmp.nombres) IS NULL) OR (("length"(UPPER(nmp.nombres)) < 3) AND (NOT ("upper"(UPPER(nmp.nombres)) IN ('BO', 'GE', 'HE', 'LI', 'LU', 'QI', 'WU', 'XI', 'XU', 'YE', 'YI', 'YU')))) OR (UPPER(nmp.nombres) LIKE '%PRUEBA%')) THEN 0 ELSE 1 END) nombre_valido, 
	   (CASE WHEN ((UPPER(nmp.apellido) IS NULL) OR (("length"(UPPER(nmp.apellido)) < 3) AND (NOT ("upper"(UPPER(nmp.apellido)) IN ('AL', 'AM', 'AN', 'BO', U&'B\00D3', 'CO', 'DE', 'DO', 'DU', 'FU', 'GE', 'GO', 'GU', 'HA', 'HE', 'HO', 'HU', 'IM', 'IN', 'IS', 'JI', 'JO', 'JU', 'KE', 'KI', 'KO', 'KU', 'LI', 'LO', 'LU', 'MA', 'MO', 'MU', 'NA', 'NG', 'NI', 'NO', 'OH', 'OU', 'PI', 'PO', 'PY', 'QI', 'QU', 'RA', 'RE', U&'R\00C9', 'RO', 'RU', 'SA', U&'S\00C1', 'SO', 'SU', 'TU', 'UM', 'UZ', 'WU', 'XU', 'YA', 'YE', 'YI', 'YO', 'YU')))) OR (UPPER(nmp.apellido) LIKE '%PRUEBA%')) THEN 0 ELSE 1 END) apellido_valido,
	   CAST(nmpd.nro_documento AS VARCHAR) documento_original
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_view_siu_toba_3_3_negocio_mdp_personas_no_duplicates" nmp
INNER JOIN "caba-piba-raw-zone-db"."siu_toba_3_3_negocio_sga_alumnos" nsa ON nsa.persona = nmp.persona
LEFT JOIN "caba-piba-raw-zone-db"."siu_toba_3_3_negocio_mdp_nacionalidades" nmn ON nmn.nacionalidad = nmp.nacionalidad
LEFT JOIN "caba-piba-raw-zone-db"."siu_toba_3_3_negocio_mdp_personas_documentos" nmpd ON nmpd.documento = nmp.documento_principal
LEFT JOIN "caba-piba-raw-zone-db"."siu_toba_3_3_negocio_mdp_tipo_documento" nmtd ON nmtd.tipo_documento = nmpd.tipo_documento 
WHERE LENGTH(TRIM(CAST(nmpd.nro_documento AS VARCHAR)))>0 AND nmpd.nro_documento IS NOT NULL
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
	   CAST(nmpd.nro_documento AS VARCHAR) END,
	   REGEXP_REPLACE(CONCAT(' ', UPPER(nmpd.nro_documento)), '[A-Za-z\.\-\,\(\)\@\_\ ]+', '')
UNION ALL

SELECT 'GOET' base_origen,
	    CAST(goetu.idusuario AS VARCHAR) codigo_origen,
		CONCAT((CASE WHEN (goettd.detalle IN ('DNI', 'CUIT', 'CUIL', 'LC', 'LE', 'CI')) THEN goettd.detalle 
		        ELSE 'NN' END), 
				REGEXP_REPLACE(CONCAT(' ', UPPER(goetu.ndocumento)), '[A-Za-z\.\-\,\(\)\@\_\ ]+', ''),
				goetu.sexo, 
				(CASE WHEN (goettd.detalle IN ('DNI', 'CUIT', 'CUIL', 'LC', 'LE', 'CI')) THEN 'ARG' ELSE 'NN' END)) broker_id_din,
	    CONCAT(RPAD(CASE WHEN (goettd.detalle IN ('DNI', 'CUIT', 'CUIL', 'LC', 'LE', 'CI')) THEN goettd.detalle 
		        ELSE 'NN' END,4,' '), 
				LPAD((CASE WHEN goettd.detalle IN ('DNI',
                                         'LC',
                                         'LE',
                                         'CI',
                                         'CUIT',
                                         'CUIL') THEN REGEXP_REPLACE(UPPER(goetu.ndocumento),'[A-Za-z\.\-\,\(\)\@\_\Ñ\ ]+','') ELSE
           CAST(goetu.ndocumento AS VARCHAR) END),11,'0'),
				goetu.sexo, 
				(CASE WHEN (goettd.detalle IN ('DNI', 'CUIT', 'CUIL', 'LC', 'LE', 'CI')) THEN 'ARG' ELSE 'NNN' END)) broker_id_est,
		goettd.detalle tipo_documento,
		(CASE WHEN (goettd.detalle IN ('DNI', 'CUIT', 'CUIL', 'LC', 'LE', 'CI')) THEN goettd.detalle 
		        ELSE 'NN' END) tipo_doc_broker,
		REGEXP_REPLACE(CONCAT(' ', UPPER(goetu.ndocumento)), '[A-Za-z\.\-\,\(\)\@\_\Ñ\ ]+', '') AS documento_broker,
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
WHERE LENGTH(TRIM(CAST(goetu.ndocumento AS VARCHAR)))>0 AND goetu.ndocumento IS NOT NULL
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
				REGEXP_REPLACE(CONCAT(' ', UPPER(a.nrodoc)), '[A-Za-z\.\-\,\(\)\@\+\/\Ñ\ ]+', ''), 
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
				LPAD((CASE WHEN d.nombre IN ('D.N.I.', 'L.C.', 'L.E.', 'C.I.', 'CUIT', 'CUIL') THEN REGEXP_REPLACE(UPPER(a.nrodoc),'[A-Za-z\.\-\,\(\)\@\+\/\Ñ\ ]+','') ELSE
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
		REGEXP_REPLACE(CONCAT(' ', UPPER(a.nrodoc)), '[A-Za-z\.\-\,\(\)\@\+\/\ ]+', '') AS documento_broker,
		UPPER(CASE WHEN (a.apenom LIKE '%,%') THEN "trim"("split_part"(apenom, ',', 2)) ELSE a.apenom END) nombre,
        UPPER((CASE WHEN (a.apenom LIKE '%,%') THEN "trim"("split_part"(apenom, ',', 1)) ELSE a.apenom END)) apellido,
		a.fechanac fecha_nacimiento,
		(CASE WHEN (g.sexo = 'Masculino') THEN 'M' WHEN (g.sexo = 'Femenino') THEN 'F' ELSE 'X' END) genero_broker,
		CAST(a.nacionalidad AS VARCHAR) nacionalidad,
		n.nombre descrip_nacionalidad,
		(CASE WHEN (d.nombre IN ('D.N.I.', 'C.I.', 'L.E.', 'L.C.', 'CUIL', 'CUIT')) THEN 'ARG' 
				 ELSE 'NNN' END) nacionalidad_broker,
		(CASE WHEN ((UPPER(a.apenom) IS NULL) OR (("length"(UPPER(a.apenom)) < 3) AND (NOT ("upper"(UPPER(a.apenom)) IN ('BO', 'GE', 'HE', 'LI', 'LU', 'QI', 'WU', 'XI', 'XU', 'YE', 'YI', 'YU')))) OR (UPPER(a.apenom) LIKE '%PRUEBA%')) THEN 0 ELSE 1 END) nombre_valido, 
	    (CASE WHEN ((UPPER(a.apenom) IS NULL) OR (("length"(UPPER(a.apenom)) < 3) AND (NOT ("upper"(UPPER(a.apenom)) IN ('AL', 'AM', 'AN', 'BO', U&'B\00D3', 'CO', 'DE', 'DO', 'DU', 'FU', 'GE', 'GO', 'GU', 'HA', 'HE', 'HO', 'HU', 'IM', 'IN', 'IS', 'JI', 'JO', 'JU', 'KE', 'KI', 'KO', 'KU', 'LI', 'LO', 'LU', 'MA', 'MO', 'MU', 'NA', 'NG', 'NI', 'NO', 'OH', 'OU', 'PI', 'PO', 'PY', 'QI', 'QU', 'RA', 'RE', U&'R\00C9', 'RO', 'RU', 'SA', U&'S\00C1', 'SO', 'SU', 'TU', 'UM', 'UZ', 'WU', 'XU', 'YA', 'YE', 'YI', 'YO', 'YU')))) OR (UPPER(a.apenom) LIKE '%PRUEBA%')) THEN 0 ELSE 1 END) apellido_valido,
		CAST(a.nrodoc AS VARCHAR) documento_original
FROM "caba-piba-raw-zone-db"."sienfo_tdoc" d
INNER JOIN "caba-piba-raw-zone-db"."sienfo_alumnos" a ON (a.tipodoc = d.tipodoc)
LEFT JOIN "caba-piba-raw-zone-db"."sienfo_tgenero" g ON (CAST(g.id AS INT) = CAST(a.sexo AS INT))	
LEFT JOIN "caba-piba-raw-zone-db"."sienfo_tnacionalidad"  n ON (a.nacionalidad = n.nacionalidad)	
WHERE LENGTH(TRIM(CAST(a.nrodoc AS VARCHAR)))>0 AND a.nrodoc  IS NOT NULL
UNION ALL

SELECT 'CRMSL' base_origen,
	    CAST(MAX(co.id) AS VARCHAR) codigo_origen,
		CONCAT((CASE WHEN (cs.tipo_documento_c = 'dni') THEN 'DNI' ELSE 'NN' END), 
			COALESCE(
					CAST(cs.numero_documento_c as VARCHAR), 
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
					CAST(cs.numero_documento_c as VARCHAR), 
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
		
		CASE WHEN UPPER(SUBSTR(cs.nacionalidad_c, 1, 3)) = 'ARG' THEN 'ARG' ELSE 'NNN' END nacionalidad_broker,
		
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
	(co.id IN (SELECT DISTINCT(op_oportunidades_laborales_contactscontacts_idb) FROM "caba-piba-raw-zone-db"."crm_sociolaboral_op_oportunidades_laborales_contacts_c" WHERE deleted = false))
	OR
	-- esta condicion es para traer adicionalmente los vecinos que han formado parte de entrevistas laborales
	(co.id IN (SELECT DISTINCT(id_c) FROM "caba-piba-raw-zone-db".crm_sociolaboral_contacts_cstm))
	OR
	-- esta condicion es para traer adicionalmente los vecinos que han cargado curriculum vitae
	(co.id IN (SELECT DISTINCT(per_entrevista_laboral_contactscontacts_ida) FROM "caba-piba-raw-zone-db".crm_sociolaboral_per_entrevista_laboral_contacts_c WHERE deleted = false))
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
					CAST(cs.numero_documento_c as VARCHAR), 
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
				CAST(cs.numero_documento_c as VARCHAR), 
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
					ELSE split_part(mu.username, '..', 1) END AS VARCHAR)),'[A-Za-z\.\-\,\(\)\@\_\ ]+', ''),
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
					ELSE split_part(mu.username, '..', 1) END AS VARCHAR)),'[A-Za-z\.\-\,\(\)\@\_\ ]+', '') AS documento_broker,
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
LEFT JOIN "caba-piba-staging-zone-db"."tbp_broker_def_broker_general" do ON (CAST(do.documento_broker AS VARCHAR) = CAST(CASE WHEN REGEXP_LIKE(mu.username, '@') THEN 
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
    -- Habilidades/Formaci�n para la empleabilidad
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
		
		REGEXP_REPLACE(CONCAT(' ', CAST(pec.doc_number AS VARCHAR)), '[A-Za-z\.\-\,\(\)\@\_\ ]+', '') AS documento_broker,
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
			length(regexp_replace(UPPER(u.name),'[^a-zA-ZáéíóúÁÉÍÓÚüÜñÑàèìòùÀÈÌÒÙâêîôûÂÊÎÔÛäëïöüÿÄËÏÖÜŸçÇ\s\`\´]+', '')) = 0 
			OR
			UPPER(u.name) IN ('GAMMA', 'RATABBOYPDA', 'PINCHARRATA')) 
		THEN 0 ELSE 1 END) nombre_valido, 
		
	    (CASE 
			WHEN ((UPPER(u.lastname) IS NULL) OR (("length"(UPPER(u.lastname)) < 3) AND (NOT ("upper"(UPPER(u.lastname)) IN ('AL', 'AM', 'AN', 'BO', U&'B\00D3', 'CO', 'DE', 'DO', 'DU', 'FU', 'GE', 'GO', 'GU', 'HA', 'HE', 'HO', 'HU', 'IM', 'IN', 'IS', 'JI', 'JO', 'JU', 'KE', 'KI', 'KO', 'KU', 'LI', 'LO', 'LU', 'MA', 'MO', 'MU', 'NA', 'NG', 'NI', 'NO', 'OH', 'OU', 'PI', 'PO', 'PY', 'QI', 'QU', 'RA', 'RE', U&'R\00C9', 'RO', 'RU', 'SA', U&'S\00C1', 'SO', 'SU', 'TU', 'UM', 'UZ', 'WU', 'XU', 'YA', 'YE', 'YI', 'YO', 'YU')))) 
			OR 
			length(regexp_replace(UPPER(u.lastname),'[^a-zA-ZáéíóúÁÉÍÓÚüÜñÑàèìòùÀÈÌÒÙâêîôûÂÊÎÔÛäëïöüÿÄËÏÖÜŸçÇ\s\`\´]+', '')) = 0 
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
		REGEXP_REPLACE(UPPER(CAST(CASE WHEN REGEXP_LIKE(cea.numero_de_documento__c, '@') THEN 
					CASE WHEN REGEXP_LIKE(LOWER(cea.numero_de_documento__c), '\.ar') THEN 
						split_part(split_part(cea.numero_de_documento__c, '@', 2),'.',4) 
					ELSE split_part(split_part(cea.numero_de_documento__c, '@', 2),'.',3) END
					ELSE split_part(cea.numero_de_documento__c, '..', 1) END AS VARCHAR)),'[A-Za-z\.\-\,\(\)\@\_\Ñ\ ]+', ''), 
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
	
	REGEXP_REPLACE(UPPER(CAST(CASE WHEN REGEXP_LIKE(cea.numero_de_documento__c, '@') THEN 
					CASE WHEN REGEXP_LIKE(LOWER(cea.numero_de_documento__c), '\.ar') THEN 
						split_part(split_part(cea.numero_de_documento__c, '@', 2),'.',4) 
					ELSE split_part(split_part(cea.numero_de_documento__c, '@', 2),'.',3) END
					ELSE split_part(cea.numero_de_documento__c, '..', 1) END AS VARCHAR)),'[A-Za-z\.\-\,\(\)\@\_\Ñ\ ]+', '') AS documento_broker,
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
		length(regexp_replace(UPPER(cea.firstname),'[^a-zA-ZáéíóúÁÉÍÓÚüÜñÑàèìòùÀÈÌÒÙâêîôûÂÊÎÔÛäëïöüÿÄËÏÖÜŸçÇ\s\`\´]+', '')) = 0 ) 
	THEN 0 ELSE 1 END) nombre_valido, 
	
	(CASE 
		WHEN ((UPPER(cea.lastname) IS NULL) OR (("length"(UPPER(cea.lastname)) < 3) AND (NOT ("upper"(UPPER(cea.lastname)) IN ('AL', 'AM', 'AN', 'BO', U&'B\00D3', 'CO', 'DE', 'DO', 'DU', 'FU', 'GE', 'GO', 'GU', 'HA', 'HE', 'HO', 'HU', 'IM', 'IN', 'IS', 'JI', 'JO', 'JU', 'KE', 'KI', 'KO', 'KU', 'LI', 'LO', 'LU', 'MA', 'MO', 'MU', 'NA', 'NG', 'NI', 'NO', 'OH', 'OU', 'PI', 'PO', 'PY', 'QI', 'QU', 'RA', 'RE', U&'R\00C9', 'RO', 'RU', 'SA', U&'S\00C1', 'SO', 'SU', 'TU', 'UM', 'UZ', 'WU', 'XU', 'YA', 'YE', 'YI', 'YO', 'YU')))) 
		OR 
		length(regexp_replace(UPPER(cea.lastname),'[^a-zA-ZáéíóúÁÉÍÓÚüÜñÑàèìòùÀÈÌÒÙâêîôûÂÊÎÔÛäëïöüÿÄËÏÖÜŸçÇ\s\`\´]+', '')) = 0
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
(SELECT replace(replace(vec.broker_id_din, ' ', '') || '-' || replace(vec.base_origen, ' ', ''), ' ', '') AS vecino_id,
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
LEFT JOIN "caba-piba-staging-zone-db"."tbp_broker_def_broker_general" do ON (CAST(do.documento_broker AS VARCHAR) = vec.documento_broker AND do.id != vec.broker_id_din)
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
   , REGEXP_REPLACE(CONCAT(' ', CAST(nrodocumento AS VARCHAR)), '[A-Za-z\.\-\,\(\)\@\_\ñ\Ñ\ ]+', '') AS nrodocumento
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
   , CASE WHEN sexo IN ('F', 'M', 'X') THEN sexo
		    WHEN sexo LIKE 'Masculino' THEN 'M'
		    WHEN sexo LIKE 'Femenino' THEN 'F'
		    ELSE 'X' END sexo
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
		CASE WHEN tmp.sexo IN ('F', 'M', 'X') THEN tmp.sexo
		    WHEN tmp.sexo LIKE 'Masculino' THEN 'M'
		    WHEN tmp.sexo LIKE 'Femenino' THEN 'F'
		    ELSE 'X' END genero_broker,
		CASE WHEN (tmp.tipo_documento IN ('DNI', 'CUIT', 'CUIL', 'LC', 'LE', 'CI')) THEN 'ARG' ELSE 'NNN' END nacionalidad,
		NULL descrip_nacionalidad,
		CASE WHEN (tmp.tipo_documento IN ('DNI', 'CUIT', 'CUIL', 'LC', 'LE', 'CI')) THEN 'ARG' ELSE 'NNN' END nacionalidad_broker,
		(CASE WHEN ((UPPER(tmp.nombre) IS NULL) OR (("length"(UPPER(tmp.nombre)) < 3) AND (NOT ("upper"(UPPER(tmp.nombre)) IN ('BO', 'GE', 'HE', 'LI', 'LU', 'QI', 'WU', 'XI', 'XU', 'YE', 'YI', 'YU')))) OR (UPPER(tmp.nombre) LIKE '%PRUEBA%')) THEN 0 ELSE 1 END) nombre_valido, 
	    (CASE WHEN ((UPPER(tmp.apellido) IS NULL) OR (("length"(UPPER(tmp.apellido)) < 3) AND (NOT ("upper"(UPPER(tmp.apellido)) IN ('AL', 'AM', 'AN', 'BO', U&'B\00D3', 'CO', 'DE', 'DO', 'DU', 'FU', 'GE', 'GO', 'GU', 'HA', 'HE', 'HO', 'HU', 'IM', 'IN', 'IS', 'JI', 'JO', 'JU', 'KE', 'KI', 'KO', 'KU', 'LI', 'LO', 'LU', 'MA', 'MO', 'MU', 'NA', 'NG', 'NI', 'NO', 'OH', 'OU', 'PI', 'PO', 'PY', 'QI', 'QU', 'RA', 'RE', U&'R\00C9', 'RO', 'RU', 'SA', U&'S\00C1', 'SO', 'SU', 'TU', 'UM', 'UZ', 'WU', 'XU', 'YA', 'YE', 'YI', 'YO', 'YU')))) OR (UPPER(tmp.apellido) LIKE '%PRUEBA%')) THEN 0 ELSE 1 END) apellido_valido,
		REGEXP_REPLACE(CONCAT(' ', CAST(tmp.nrodocumento AS VARCHAR)), '[A-Za-z\.\-\,\(\)\@\_\ñ\ ]+', '') documento_original
FROM tmp
INNER JOIN "caba-piba-staging-zone-db"."tbp_typ_tmp_view_goet_usuarios_no_duplicates" gu ON (gu.ndocumento = tmp.nrodocumento)		
WHERE tmp.broker_id IS NULL
GROUP BY 
tmp.tipo_documento, tmp.nrodocumento, tmp.nivel, tmp.sede, tmp.curso, tmp.descrip_normalizada, tmp.comision, tmp.inscripcion, tmp.estado, tmp.apellido, tmp.nombre, tmp.nacionalidad, tmp.sexo, tmp.fecha_nacimiento, tmp.tipo_doc_broker, tmp.broker_id, tmp.broker_id_valido, tmp.base_origen,
tmp.base_origen_ok, gu.idusuario
--</sql>--