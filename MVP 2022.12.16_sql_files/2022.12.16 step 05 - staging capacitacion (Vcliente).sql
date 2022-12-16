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
	TRIM(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(UPPER(COALESCE(t.detalle, n.detalle)), CHR(160), ' '), 'Á', 'A'), 'É', 'E'), 'Í', 'I'), 'Ó', 'O'), '[ÚÜ]', 'U'), '\([0-9 ]*\)', ''), '["“”?¿]', ''), ' +', ' ')) AS descrip_normalizada,
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
