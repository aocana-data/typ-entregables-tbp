-- --<sql>--DROP TABLE `caba-piba-staging-zone-db`.`tbp_typ_tmp_estado_beneficiario_sienfo`;--</sql>--
--<sql>--
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_estado_beneficiario_sienfo" AS
with t AS (
	/*Limpieza de talleres, me quedo con los cursos validos,
	 * transformo a fechas las columnas, filtro fechas menores a 2003 o fechas de fin mayores a curdate + 1 año
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
			WHEN CAST(fecha_fin AS DATE) > DATE_ADD('year', 1, current_date) THEN NULL ELSE CAST(fecha_fin AS DATE)
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
	WHERE nrodoc IS NOT NULL and nrodoc != ''
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
f02 as (
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
    f01 on a.nrodoc = f01.nrodoc
), preins_sin_ficha as ( 
select
    p.*,
    'PREINSCRIPTO' as estado_beneficiario
from
    "caba-piba-staging-zone-db"."goayvd_typ_vw_sienfo_fichas_preinscripcion" p
left join f02 on f02.nrodoc = p.nrodoc and f02.codigo_ct = p.codigo_ct
where f02.nrodoc is null
), f1 as (
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
	0 as aprobado,
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
			else fct.fecha_inc_min
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
				end
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
						WHEN DATE_ADD('month', 1, tf1.fecha_fin) <= current_date THEN 'REPROBADO' -- GCBA cambia de 2 meses a 1 el 17/1/23
    					WHEN tf1.fecha_fin <= current_date THEN 'FINALIZO_CURSADA'
    					WHEN tf1.fecha_inc <= current_date
    					    AND tf1.fecha_fin > current_date THEN 'REGULAR'
    					WHEN tf1.fecha_inc > current_date THEN 'INSCRIPTO'
    				end
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
	CAST(null AS DATE) fechabaja,
	0 aprobado,
	'INSCRIPTO' estado_beneficiario
FROM
    tf2
LEFT JOIN "caba-piba-raw-zone-db"."sienfo_carreras" sc ON sc.id_carrera = tf2.id_carrera
WHERE tf2.id_carrera != 0
GROUP BY (tf2.id_carrera, tf2.nrodoc,sc.nom_carrera)
HAVING min(tf2.fecha) IS NOT NULL AND max(tf2.fecha_fin) IS NOT NULL
), car_cur AS ( --SOLO LA CARRERA 2 que todavia no esta definido como calcularla correctamente, el resto se calcula a continuación
/*
 * SELECT final, estado_beneficiario2 es el calculado con el algoritmo de fechas,
 *  estado de beneficiario se basa en las columnas de la tabla
 */
SELECT 
    tf2.codigo_ct,
	tf2.nrodoc,
	tf2.id_curso,
	tf2.id_carrera,
	tf2.estado_beneficiario2 estado_beneficiario,
	tf2.nrodoc ||'-'||tf2.codigo_ct AS llave_doc_idcap, --si es curso es nrodoc y codigo_ct / si es carrera es nrodoc id_carrera
	'CURSO' tipo_formacion,
	UPPER(tf2.nom_curso) nombre_cur_car, --nombre del curso
	tf2.fecha fecha_inicio_edicion_capacitacion, -- fecha inicio del curso
	tf2.fecha_fin fecha_fin_edicion_capacitacion, -- fecha fin del curso
	tf2.fecha_inc fecha_inscipcion -- fecha inicio de la persona
	--tf2.id_duracion,
	--tf2.baja,
	--tf2.fechabaja,
	--tf2.aprobado,
	-- tf2.estado_beneficiario estado_beneficiario_orig,
FROM tf2
WHERE tf2.id_carrera = 0 --TOMO SOLO LOS QUE SON CURSOS
UNION ( 
SELECT
    cal.codigo_ct,
    cal.nrodoc,
    cal.id_curso,
    cal.id_carrera,
    cal.estado_beneficiario,
    cal.nrodoc ||'-'||CAST(cal.id_carrera AS VARCHAR) llave_doc_idcap, --Esta llave es para agrupar las carreras por edición capacitacion anual
    'CARRERA' tipo_formacion,
    UPPER(cal.nom_carrera) nombre_cur_car,
    cal.fecha fecha_inicio_edicion_capacitacion,
    cal.fecha_fin fecha_fin_edicion_capacitacion,
    cal.fecha_inc fecha_inscipcion
    --cal.id_duracion,
	--cal.baja,
	--cal.fechabaja,
	--cal.aprobado
FROM
    carreras_al cal
where id_carrera = 2) --Este calculo es provisorio hasta que el area nos de la información de como calcular carrera 2, el resto se calculan de otra forma
--Existen fechas de inicio del alumno anteriores a fecha de inicio del curso
--nrodoc esta muy sucio
--Los estados de beneficiario2 nulos son cuando aprobado = 9 (nuevo id, no esta en en dump) corresponde a nombre = Actualiza, observa = CETBA [NO SE QUE SIGNIFICA]
), carrera_18 as (
SELECT
    NULL as "codigo_ct",
    a.nrodoc,
    --a.id_alumno,
    0 as id_curso,
    t.id_carrera,
    case
        when count(ca.nrocertificado) > 1 and count( distinct t.id_curso) > 1 THEN 'EGRESADO'
        when (ca.baja != 0 and ca.baja is not null) or max(ca.fechabaja) is not null then 'BAJA'
        when DATE_ADD('year', 2, max(am.maxft)) < current_date and (count(ca.nrocertificado) < 2 or count( distinct t.id_curso) < 2) THEN 'BAJA' -- CRITERIO A CONSULTAR 2 años desde la finalizacion del ultimo curso de la carrera y no tiene cert
        when count(ca.nrocertificado) < 2 or count( distinct t.id_curso) < 2 THEN 'REGULAR'
    end as "estado_beneficiario",
    a.nrodoc ||'-'||CAST(t.id_carrera AS VARCHAR) llave_doc_idcap,
    'CARRERA' as tipo_formacion,
    sc.nom_carrera as nombre_cur_car,
    min(am.miff) as fecha_inscipcion,
    min(am.mift) as fecha_inicio_edicion_capacitacion,
    DATE_ADD('month', 4, max(am.maxft)) as fecha_fin_edicion_capacitacion
    --La duración del ultimo curso es la fecha de inicio del curso + 4 meses
    --ca.baja,
    --max(ca.fechabaja) as fechabaja,
    --count(ca.nrocertificado) as cant_cert,
    --count( distinct t.id_curso) as cant_cur
FROM "caba-piba-raw-zone-db".sienfo_alumnos a 
	INNER JOIN "caba-piba-raw-zone-db".sienfo_fichas_carreras ca ON a.nrodoc = ca.nrodoc
	INNER JOIN "caba-piba-raw-zone-db".sienfo_talleres t ON ca.id_carrera = t.id_carrera
	LEFT JOIN "caba-piba-raw-zone-db".sienfo_carreras sc ON sc.id_carrera = t.id_carrera
	LEFT JOIN (
	/* aca toma la maxima fecha de inscripción a una cursada de la cual no se dió de baja 
	no confundir con fechas de baja y aprobación de la carrera que provienen de sienfo_fichas_carrera*/
		SELECT 
		    ff.nrodoc AS fi_nrodoc,
			MAX((cast(tt.fecha as date))) AS maxft,
			MIN((cast(ff.fecha_inc as date))) AS miff,
			MIN((cast(tt.fecha as date))) AS mift
		FROM "caba-piba-raw-zone-db".sienfo_cursos cu
		LEFT JOIN "caba-piba-raw-zone-db".sienfo_talleres tt ON cu.id_curso = tt.id_curso
		LEFT JOIN "caba-piba-raw-zone-db".sienfo_fichas ff ON tt.codigo_ct = ff.codigo_ct
		WHERE cu.id_carrera = '18'
			--AND ff.baja = 0 
			--AND aprobado in('1', '3','5')
			--AND cu.baja IN ('0', '1')
		GROUP BY 
		    ff.nrodoc,
			cu.id_carrera
	        ) AS am ON ca.nrodoc = am.fi_nrodoc
WHERE ca.id_carrera = 18
group by
    a.nrodoc,
    a.id_alumno,
    t.id_carrera,
    ca.baja,
    sc.nom_carrera
/* existe gente que está como regular (2029) porque no tiene fecha de inicio ni de fin, posiblemente no hace match con el leftjoin de la subconsulta*/ 
), carrera_1 as (
SELECT
    NULL as "codigo_ct",
    fc.nrodoc,
    --al.id_alumno,
    0 as id_curso,
    q1.id_carrera_1 as id_carrera,
    CASE
        when fc.id_carrera = 1 AND (year(maxft) between cmt.ano_inicio_old and cmt.anio_fin_old AND cant_mat_ap >= cmt.cant_materias_plan_estudio_old) OR (year(maxft) >=cmt.ano_inicio and cant_mat_ap >= cmt.cant_materias_plan_estudio) AND fc.baja = 0 THEN 'EGRESADO'
        when fc.fechabaja IS NOT NULL THEN 'BAJA'
    	WHEN fc.baja NOT IN (14, 22, 24, 0) AND fc.baja IS NOT NULL THEN 'BAJA'
    	ELSE 'REGULAR'
        end as estado_beneficiario,
    fc.nrodoc ||'-'||CAST(q1.id_carrera_1 AS VARCHAR) llave_doc_idcap,
    'CARRERA' as tipo_formacion,
    ca.nom_carrera as nombre_cur_car,
    q1.miff as fecha_inscipcion,
    q1.mift as fecha_inicio_edicion_capacitacion,
    DATE_ADD('month', 6, q1.maxft) as fecha_fin_edicion_capacitacion
    --q1.cant_mat_ap
FROM "caba-piba-raw-zone-db".sienfo_fichas_carreras fc
	LEFT JOIN "caba-piba-raw-zone-db".sienfo_alumnos al ON fc.nrodoc = al.nrodoc
	LEFT JOIN (
		SELECT fi.nrodoc,
			cu.id_carrera_1,
			COUNT(fi.nrodoc) AS cant_mat_ap,
			MAX(cast(tt.fecha as date)) AS maxft,
			MIN((cast(fi.fecha_inc as date))) AS miff,
			MIN((cast(tt.fecha as date))) AS mift
		FROM "caba-piba-raw-zone-db".sienfo_cursos cu
			LEFT JOIN "caba-piba-raw-zone-db".sienfo_talleres tt ON cu.id_curso = tt.id_curso
			LEFT JOIN "caba-piba-raw-zone-db".sienfo_fichas fi ON tt.codigo_ct = fi.codigo_ct
		WHERE cu.id_carrera_1 = 1
			AND fi.baja = 0
			AND aprobado in('1', '3','5')
			AND cu.baja IN('0', '1')
		GROUP BY fi.nrodoc,
			cu.id_carrera_1
		ORDER BY cant_mat_ap DESC
	) q1 ON al.nrodoc = q1.nrodoc
	LEFT JOIN "caba-piba-raw-zone-db".sienfo_carreras ca ON q1.id_carrera_1 = ca.id_carrera
	LEFT JOIN "caba-piba-staging-zone-db".goayvd_typ_sienfo_vw_cantidad_materias_por_trayecto cmt ON fc.id_carrera = cast(cmt.carrera as int)
WHERE fc.id_carrera = 1
and id_carrera_1 = 1
), carrera_29 as (
SELECT
    NULL as "codigo_ct",
    fc.nrodoc,
    --al.id_alumno,
    0 as id_curso,
    q1.id_carrera_29 as id_carrera,
    CASE
        when fc.id_carrera = 29 AND (year(maxft) >=cmt.ano_inicio and cant_mat_ap >= cmt.cant_materias_plan_estudio) AND fc.baja = 0 THEN 'EGRESADO'
        when fc.fechabaja IS NOT NULL THEN 'BAJA'
    	WHEN fc.baja NOT IN (14, 22, 24, 0) AND fc.baja IS NOT NULL THEN 'BAJA'
    	ELSE 'REGULAR'
        end as estado_beneficiario,
    fc.nrodoc ||'-'||CAST(q1.id_carrera_29 AS VARCHAR) llave_doc_idcap,
    'CARRERA' as tipo_formacion,
    ca.nom_carrera as nombre_cur_car,
    q1.miff as fecha_inscipcion,
    q1.mift as fecha_inicio_edicion_capacitacion,
    DATE_ADD('month', 6, q1.maxft) as fecha_fin_edicion_capacitacion
    --q1.cant_mat_ap
FROM "caba-piba-raw-zone-db".sienfo_fichas_carreras fc
	LEFT JOIN "caba-piba-raw-zone-db".sienfo_alumnos al ON fc.nrodoc = al.nrodoc
	LEFT JOIN (
		SELECT fi.nrodoc,
			cu.id_carrera_29,
			COUNT(fi.nrodoc) AS cant_mat_ap,
			MAX(cast(tt.fecha as date)) AS maxft,
			MIN((cast(fi.fecha_inc as date))) AS miff,
			MIN((cast(tt.fecha as date))) AS mift
		FROM "caba-piba-raw-zone-db".sienfo_cursos cu
			LEFT JOIN "caba-piba-raw-zone-db".sienfo_talleres tt ON cu.id_curso = tt.id_curso
			LEFT JOIN "caba-piba-raw-zone-db".sienfo_fichas fi ON tt.codigo_ct = fi.codigo_ct
		WHERE cu.id_carrera_29 = 29
			AND fi.baja = 0
			AND aprobado in('1', '3','5')
			AND cu.baja IN('0', '1')
		GROUP BY fi.nrodoc,
			cu.id_carrera_29
		ORDER BY cant_mat_ap DESC
	) q1 ON al.nrodoc = q1.nrodoc
	LEFT JOIN "caba-piba-raw-zone-db".sienfo_carreras ca ON q1.id_carrera_29 = ca.id_carrera
	LEFT JOIN "caba-piba-staging-zone-db".goayvd_typ_sienfo_vw_cantidad_materias_por_trayecto cmt ON fc.id_carrera = cmt.carrera
WHERE fc.id_carrera = 29
and id_carrera_29 = 29
), carrera_30 as (
SELECT
    NULL as "codigo_ct",
    fc.nrodoc,
    --al.id_alumno,
    0 as id_curso,
    q1.id_carrera_30 as id_carrera,
    CASE
        when fc.id_carrera = 30 AND (year(maxft) >=cmt.ano_inicio and cant_mat_ap >= cmt.cant_materias_plan_estudio) AND fc.baja = 0 THEN 'EGRESADO'
        when fc.fechabaja IS NOT NULL THEN 'BAJA'
    	WHEN fc.baja NOT IN (14, 22, 24, 0) AND fc.baja IS NOT NULL THEN 'BAJA'
    	ELSE 'REGULAR'
        end as estado_beneficiario,
    fc.nrodoc ||'-'||CAST(q1.id_carrera_30 AS VARCHAR) llave_doc_idcap,
    'CARRERA' as tipo_formacion,
    ca.nom_carrera as nombre_cur_car,
    q1.miff as fecha_inscipcion,
    q1.mift as fecha_inicio_edicion_capacitacion,
    DATE_ADD('month', 6, q1.maxft) as fecha_fin_edicion_capacitacion
    --q1.cant_mat_ap
FROM "caba-piba-raw-zone-db".sienfo_fichas_carreras fc
	LEFT JOIN "caba-piba-raw-zone-db".sienfo_alumnos al ON fc.nrodoc = al.nrodoc
	LEFT JOIN (
		SELECT fi.nrodoc,
			cu.id_carrera_30,
			COUNT(fi.nrodoc) AS cant_mat_ap,
			MAX(cast(tt.fecha as date)) AS maxft,
			MIN((cast(fi.fecha_inc as date))) AS miff,
			MIN((cast(tt.fecha as date))) AS mift
		FROM "caba-piba-raw-zone-db".sienfo_cursos cu
			LEFT JOIN "caba-piba-raw-zone-db".sienfo_talleres tt ON cu.id_curso = tt.id_curso
			LEFT JOIN "caba-piba-raw-zone-db".sienfo_fichas fi ON tt.codigo_ct = fi.codigo_ct
		WHERE cu.id_carrera_30 = 30
			AND fi.baja = 0
			AND aprobado in('1', '3','5')
			AND cu.baja IN('0', '1')
		GROUP BY fi.nrodoc,
			cu.id_carrera_30
		ORDER BY cant_mat_ap DESC
	) q1 ON al.nrodoc = q1.nrodoc
	LEFT JOIN "caba-piba-raw-zone-db".sienfo_carreras ca ON q1.id_carrera_30 = ca.id_carrera
	LEFT JOIN "caba-piba-staging-zone-db".goayvd_typ_sienfo_vw_cantidad_materias_por_trayecto cmt ON fc.id_carrera = cmt.carrera
WHERE fc.id_carrera = 30
and id_carrera_30 = 30
), carrera_31 as (
SELECT
    NULL as "codigo_ct",
    fc.nrodoc,
    --al.id_alumno,
    0 as id_curso,
    q1.id_carrera_31 as id_carrera,
    CASE
        when fc.id_carrera = 31 AND (year(maxft) >=cmt.ano_inicio and cant_mat_ap >= cmt.cant_materias_plan_estudio) AND fc.baja = 0 THEN 'EGRESADO'
        when fc.fechabaja IS NOT NULL THEN 'BAJA'
    	WHEN fc.baja NOT IN (14, 22, 24, 0) AND fc.baja IS NOT NULL THEN 'BAJA'
    	ELSE 'REGULAR'
        end as estado_beneficiario,
    fc.nrodoc ||'-'||CAST(q1.id_carrera_31 AS VARCHAR) llave_doc_idcap,
    'CARRERA' as tipo_formacion,
    ca.nom_carrera as nombre_cur_car,
    q1.miff as fecha_inscipcion,
    q1.mift as fecha_inicio_edicion_capacitacion,
    DATE_ADD('month', 6, q1.maxft) as fecha_fin_edicion_capacitacion
    --q1.cant_mat_ap
FROM "caba-piba-raw-zone-db".sienfo_fichas_carreras fc
	LEFT JOIN "caba-piba-raw-zone-db".sienfo_alumnos al ON fc.nrodoc = al.nrodoc
	LEFT JOIN (
		SELECT fi.nrodoc,
			cu.id_carrera_31,
			COUNT(fi.nrodoc) AS cant_mat_ap,
			MAX(cast(tt.fecha as date)) AS maxft,
			MIN((cast(fi.fecha_inc as date))) AS miff,
			MIN((cast(tt.fecha as date))) AS mift
		FROM "caba-piba-raw-zone-db".sienfo_cursos cu
			LEFT JOIN "caba-piba-raw-zone-db".sienfo_talleres tt ON cu.id_curso = tt.id_curso
			LEFT JOIN "caba-piba-raw-zone-db".sienfo_fichas fi ON tt.codigo_ct = fi.codigo_ct
		WHERE cu.id_carrera_31 = 31
			AND fi.baja = 0
			AND aprobado in('1', '3','5')
			AND cu.baja IN('0', '1')
		GROUP BY fi.nrodoc,
			cu.id_carrera_31
		ORDER BY cant_mat_ap DESC
	) q1 ON al.nrodoc = q1.nrodoc
	LEFT JOIN "caba-piba-raw-zone-db".sienfo_carreras ca ON q1.id_carrera_31 = ca.id_carrera
	LEFT JOIN "caba-piba-staging-zone-db".goayvd_typ_sienfo_vw_cantidad_materias_por_trayecto cmt ON fc.id_carrera = cmt.carrera
WHERE fc.id_carrera = 31
and id_carrera_31 = 31
), carrera_32 as ( 
SELECT
    NULL as "codigo_ct",
    fc.nrodoc,
    --al.id_alumno,
    0 as id_curso,
    q1.id_carrera_32 as id_carrera,
    CASE
        when fc.id_carrera = 32 AND (year(maxft) >=cmt.ano_inicio and cant_mat_ap >= cmt.cant_materias_plan_estudio) AND fc.baja = 0 THEN 'EGRESADO'
        when fc.fechabaja IS NOT NULL THEN 'BAJA'
    	WHEN fc.baja NOT IN (14, 22, 24, 0) AND fc.baja IS NOT NULL THEN 'BAJA'
    	ELSE 'REGULAR'
        end as estado_beneficiario,
    fc.nrodoc ||'-'||CAST(q1.id_carrera_32 AS VARCHAR) llave_doc_idcap,
    'CARRERA' as tipo_formacion,
    ca.nom_carrera as nombre_cur_car,
    q1.miff as fecha_inscipcion,
    q1.mift as fecha_inicio_edicion_capacitacion,
    DATE_ADD('month', 6, q1.maxft) as fecha_fin_edicion_capacitacion
    --q1.cant_mat_ap
FROM "caba-piba-raw-zone-db".sienfo_fichas_carreras fc
	LEFT JOIN "caba-piba-raw-zone-db".sienfo_alumnos al ON fc.nrodoc = al.nrodoc
	LEFT JOIN (
		SELECT fi.nrodoc,
			cu.id_carrera_32,
			COUNT(fi.nrodoc) AS cant_mat_ap,
			MAX(cast(tt.fecha as date)) AS maxft,
			MIN((cast(fi.fecha_inc as date))) AS miff,
			MIN((cast(tt.fecha as date))) AS mift
		FROM "caba-piba-raw-zone-db".sienfo_cursos cu
			LEFT JOIN "caba-piba-raw-zone-db".sienfo_talleres tt ON cu.id_curso = tt.id_curso
			LEFT JOIN "caba-piba-raw-zone-db".sienfo_fichas fi ON tt.codigo_ct = fi.codigo_ct
		WHERE cu.id_carrera_32 = 32
			AND fi.baja = 0
			AND aprobado in('1', '3','5')
			AND cu.baja IN('0', '1')
		GROUP BY fi.nrodoc,
			cu.id_carrera_32
		ORDER BY cant_mat_ap DESC
	) q1 ON al.nrodoc = q1.nrodoc
	LEFT JOIN "caba-piba-raw-zone-db".sienfo_carreras ca ON q1.id_carrera_32 = ca.id_carrera
	LEFT JOIN "caba-piba-staging-zone-db".goayvd_typ_sienfo_vw_cantidad_materias_por_trayecto cmt ON fc.id_carrera = cmt.carrera
WHERE fc.id_carrera = 32
and id_carrera_32 = 32
)
SELECT
    *
FROM
    carrera_18
UNION
SELECT
    *
FROM
    carrera_1
UNION
SELECT
    *
FROM
    carrera_29
UNION
SELECT
    *
FROM
    carrera_30
UNION
SELECT
    *
FROM
    carrera_31
UNION
SELECT
    *
FROM
    carrera_32
UNION
SELECT
    *
FROM
    car_cur
--</sql>--