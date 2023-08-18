-- --<sql>--DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_tmp_estado_beneficiario_moodle`;--</sql>--
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
						THEN cast(NULL as date)
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
					) -- Habilidades/Formación para la empleabilidad
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