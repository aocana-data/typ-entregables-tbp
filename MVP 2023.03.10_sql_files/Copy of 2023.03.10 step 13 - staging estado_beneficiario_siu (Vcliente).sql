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
		--sa.comision, -- NO TENEMOS COMISIÓN PORQUE FUERON MIGRADOS PERO NO IMPORTA PORQUE SOLO VAMOS A HACER UN COUNT
		sad.plan_version,
		sad.fecha,
		sad.fecha_vigencia,
		--- Estos campos siguientes son los que voy a filtrar en el where de la tabla siguiente para saber cuantas aprobó el alumno
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
-- PARA ESTAR APROBADO Y CERRADO YA NO DEBE TENER CONDICIÓN DE REGULARIDAD Y 
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
		-- 'E' 'P' Corresponden a actas de aprobación de materia, no de parciales
		--'A','B' Aprobados por equivalencias en otro país u otra provincia consultar con el area si se incluyen
		AND ac.nota IS NOT NULL -- tiene una nota 
		AND ac.cond_regularidad IS NULL -- NO tiene condición de regularidad porque ya la aprobó
		AND ac.resultado = 'A' -- Está aprobado
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
), preins_ins AS (  -- UNIÓN DE LOS PREINSCRIPTOS INNER JOIN PERSONAS CON TABLA DE ALUMNOS INNER JOIN PERSONAS
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
	    pr.fecha as fecha_baja, -- CONFIRMAR SI ES PERDIDA DE REGULARIDAD DE MATERIA O DE CARRERA (ASUMMO CARRERA PORQUE NO TIENE ID DE MATERIA)
	    CASE
	        WHEN  r.ultima_acta IS NULL AND ic.ultima_insc IS NOT NULL THEN ic.ultima_insc
    	    WHEN  r.ultima_acta IS NOT NULL AND ic.ultima_insc IS NULL THEN r.ultima_acta
	        WHEN r.ultima_acta is not NULL and ic.ultima_insc is not NULL THEN ( 
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
		round((cast(pii2.cantidad_materias_aprobadas as double) / cmp.cant_materias)*100, 0) as porcentaje_aprob,
		pii2.fecha_preinscripcion,
		pii2.fecha_baja,
		pii2.ultima_acta,   --ACTA DE EXAMEN APROBADO (CONSULTAR CON EL AREA SI DEBO SUMAR ACTAS REPROBADAS)
		pii2.fecha_inicio,  --PRIMERA INSCRIPCION A MATERIA
		pii2.ultima_insc,   --ULTIMA INSCRIPCION A MATERIA
		pii2.ultimo_mov,    --EVALUA SI LA ULTIMA ACTIVIDAD FUE UNA INSCRIPCION O UN ACTA DE EXAMEN APROBADO (CONSULTAR CON EL AREA SI DEBO SUMAR ACTAS REPROBADAS)
		CASE
			WHEN cmp.cant_materias <= cantidad_materias_aprobadas then 'EGRESADO'
			WHEN pii2.fecha_baja IS NOT NULL AND pii2.ultimo_mov IS NULL THEN 'BAJA'
			WHEN pii2.fecha_baja IS NOT NULL AND pii2.ultimo_mov IS NOT NULL AND pii2.fecha_baja > pii2.ultimo_mov THEN 'BAJA' --EXISTEN FECHAS DE ACTIVIDAD POSTERIORES A LA FECHA DE BAJA CONSULTAR CON EL AREA
			WHEN date_add('year', 2, pii2.ultimo_mov) < current_date then 'BAJA'
			WHEN date_add('year', 2, pii2.ultimo_mov) >= current_date then 'REGULAR'
			WHEN  pii2.ultimo_mov IS NULL AND pii2.fecha_preinscripcion IS NULL THEN 'INSCRIPTO'
			ELSE pii2.estado_beneficiario
		END AS estado_beneficiario,
		CASE
			WHEN cmp.cant_materias <= cantidad_materias_aprobadas then 1 -- EGRESADO
			WHEN pii2.fecha_baja IS NOT NULL AND pii2.ultimo_mov IS NULL THEN 4 -- 'BAJA'
			WHEN pii2.fecha_baja IS NOT NULL AND pii2.ultimo_mov IS NOT NULL AND pii2.fecha_baja > pii2.ultimo_mov THEN 4 -- 'BAJA' --EXISTEN FECHAS DE ACTIVIDAD POSTERIORES A LA FECHA DE BAJA CONSULTAR CON EL AREA
			WHEN date_add('year', 2, pii2.ultimo_mov) < current_date then 4 -- 'BAJA'
			WHEN date_add('year', 2, pii2.ultimo_mov) >= current_date then 5 -- 'REGULAR'
			WHEN  pii2.ultimo_mov IS NULL AND pii2.fecha_preinscripcion IS NULL THEN 6 -- 'INSCRIPTO'
			ELSE 7
		END AS prioridad_orden
	FROM 
		preins_ins2 pii2
	left join 
		"caba-piba-staging-zone-db"."goayvd_typ_tmp_siu_cantidad_materias_plan" cmp ON
		cmp.propuesta = pii2.propuesta) a ) resultado
WHERE resultado.orden_duplicado=1
--Ver casos fecha inicio (min(fecha insc cursada)) > fecha_acta (max(fecha de acta)) 1165 casos de 50816