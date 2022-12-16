CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_estado_beneficiario_goet" AS 
WITH master AS (
	SELECT
	    ia."IdUsuario",
		ia."IdCtrCdCurso", 
		u."NDocumento",
		cast(ia."FechaInscripcion" as DATE) as "FechaInscripcion",
		ccc."InicioCurso",
		ccc."FinCurso",
		ccc."IdKeyTrayecto",
		cast(u."IdUsuario" as VARCHAR) || '-' || cast(ia."IdCtrCdCurso" as varchar) as "IdCertificadoCurso", --- ID PARA MATCH CON ALUMNOS Y CURSOS SI SOLO SI IdKeyTrayecto ES 0 (curso)
		cast(u."IdUsuario" as VARCHAR) || '-' || cast(ccc."IdKeyTrayecto" as varchar) as "IdCertificadoCarrera"  --- ID PARA MATCH CON ALUMNOS Y CARRERAS SI SOLO SI IdKeyTrayecto ES !=0
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
		AND ia."IdInscripcionEstado" = 2 --Inscripción aceptada	--AND ia."IdInscripcionEstadoAL" IN (2,4) --confirmar con el area --AND ia."IDInscripcionOrigen" IN (1,2) --confirmar con el area 
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
			row_number() over (partition by ca."IdUsuario", ca."IdCtrCdCurso" order by ca."Fecha" DESC, ca."IdCertificadoEstado" DESC) as ranking
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
	    'CURSO' as "tipo_capacitacion",
		m."FechaInscripcion",
		m."InicioCurso",
		m."FinCurso",
		m."IdCertificadoCurso",
		null as "IdCertificadoCarrera",
		ccu."Fehca_cert",
		CASE
		    WHEN ccu."estado_beneficiario" = 'DESAPROBADO' then 'REPROBADO'
			WHEN ccu."estado_beneficiario" IS NULL THEN 
			(
			    CASE
			        WHEN date_add('month', 24, m."FinCurso") <= current_date then 'REPROBADO' -- CONFIRMAR CON EL AREA CUANTO TIEMPO DESPUES DE FINALIZADO QUEDA LIBRE
			    	WHEN m."FinCurso" <= current_date then 'FINALIZO_CURSADA'
				    WHEN m."InicioCurso" < current_date AND m."FinCurso" > current_date then 'REGULAR'
				    WHEN m."InicioCurso" > current_date then 'INSCRIPTO'
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
				row_number() over (partition by cet."IdUsuario", cet."IdKeyTrayecto" order by cet."Fecha" DESC, cet."IdCertificadoEstado" DESC) as ranking
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
	    'CARRERA' as "tipo_capacitacion",
		cg."FechaInscripcion",
		cg."InicioCurso",
		cg."FinCurso",
		null AS "IdCertificadoCurso",
		cg."IdCertificadoCarrera",
		cca."Fehca_cert",
		CASE 
		    WHEN cca."estado_beneficiario" = 'DESAPROBADO' then 'REPROBADO'
			WHEN cca."estado_beneficiario" IS NULL THEN
			( 
		        CASE
			        WHEN date_add('month', 24, cg."FinCurso") <= current_date then 'REPROBADO' -- CONFIRMAR CON EL AREA CUANTO TIEMPO DESPUES DE FINALIZADO QUEDA LIBRE
			    	WHEN cg."FinCurso" <= current_date then 'FINALIZO_CURSADA'
				    WHEN cg."InicioCurso" < current_date AND cg."FinCurso" > current_date then 'REGULAR'
				    WHEN cg."InicioCurso" > current_date then 'INSCRIPTO'
			    END
			)
			ELSE cca."estado_beneficiario"
		END AS "estado_beneficiario"
	FROM 
		carrera_group cg
	LEFT JOIN 
		cer_car cca ON 
		cg."IdCertificadoCarrera" = cca."IdCertificadoCarrera"
), cur_car as ( 
SELECT
    *
from
    car1
UNION
SELECT
    *
from
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