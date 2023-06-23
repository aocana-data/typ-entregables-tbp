-- 1.-- Crear EDICION CAPACITACION GOET, MOODLE, SIENFO Y CRMSL paso 1
-- --<sql>--DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_tmp_edicion_capacitacion_1`;--</sql>--
--<sql>--
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
		est.id cod_origen_establecimiento
		
	FROM 
	"caba-piba-raw-zone-db"."goet_nomenclador" n 
	LEFT JOIN "caba-piba-raw-zone-db"."goet_centro_habilitacion_modulos" chm ON n.IdNomenclador = chm.IdNomenclador
	LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_establecimientos" est ON (cast(chm.IdCentro as varchar)=est.id_old)
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
		est.id
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
	ELSE date_format(cast(fechas.fecha_inicio as date), '%Y-%m-%d %h:%i%p')
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
				date_format(cast(fechas.fecha_inicio as date), '%Y-%m-%d %h:%i%p') 
			END, '-', 2) AS INTEGER) <= 6
		THEN 1
		
	WHEN CAST(SPLIT_PART(CASE 
		WHEN fechas.fecha_inicio IS NULL
			THEN 
				date_format(from_unixtime(MIN(TRY_CAST(co.startdate AS BIGINT))), '%Y-%m-%d %h:%i%p')
			ELSE 
				date_format(cast(fechas.fecha_inicio as date), '%Y-%m-%d %h:%i%p') 
			END, '-', 2) AS INTEGER) > 6
		THEN 2
	ELSE NULL 
	END semestre_inicio,			
	
	-- se utiliza la fecha calculada en el script de estado de beneficiario
	-- En caso que este en null, se utiliza MIN(co.startdate) (tiene 89.87% de completitud), en otro caso queda en null
	CAST(DATE_PARSE(CASE 
	WHEN  fechas.fecha_inicio IS NULL
		THEN date_format(from_unixtime(MIN(TRY_CAST(co.startdate AS BIGINT))), '%Y-%m-%d %h:%i%p')
	ELSE date_format(cast(fechas.fecha_inicio as date), '%Y-%m-%d %h:%i%p')
	END , '%Y-%m-%d %h:%i%p') AS DATE) fecha_inicio_dictado,

	-- se utiliza la fecha calculada en el script de estado de beneficiario
	-- En caso que este en null, se utiliza MAX(co.enddate) (tiene 24.90% de completitud), en otro caso queda en null
	CAST(DATE_PARSE(CASE 
	WHEN  fechas.fecha_fin IS NULL
		THEN date_format(from_unixtime(MAX(TRY_CAST(co.enddate AS BIGINT))), '%Y-%m-%d %h:%i%p')
	ELSE date_format(cast(fechas.fecha_fin as date), '%Y-%m-%d %h:%i%p')
	END , '%Y-%m-%d %h:%i%p') AS DATE) fecha_fin_dictado,
	
   -- no se encuentra en bbdd origen un valor para el atributo
   -- se utiliza la fecha calculada en el script de estado de beneficiario, en otro caso queda en null
	CAST(DATE_PARSE(CASE 
	WHEN  fechas.fecha_inicio_inscripcion IS NULL
		THEN date_format(TRY_CAST(NULL AS DATE), '%Y-%m-%d %h:%i%p')
	ELSE date_format(cast(fechas.fecha_inicio_inscripcion as date), '%Y-%m-%d %h:%i%p')
	END , '%Y-%m-%d %h:%i%p') AS DATE) fecha_inicio_inscripcion,
	
   -- no se encuentra en bbdd origen un valor para el atributo
   -- se utiliza la fecha calculada en el script de estado de beneficiario, en otro caso queda en null
   CAST(DATE_PARSE(CASE 
	WHEN  fechas.fecha_fin_inscripcion IS NULL
		THEN date_format(TRY_CAST(NULL AS DATE), '%Y-%m-%d %h:%i%p')
	ELSE date_format(cast(fechas.fecha_fin_inscripcion as date), '%Y-%m-%d %h:%i%p')
	END , '%Y-%m-%d %h:%i%p') AS DATE) fecha_limite_inscripcion,
	
	' ' turno,
	' ' dias_cursada,
	
	CASE 
		WHEN sum(CASE WHEN co.visible = true THEN 1 ELSE 0 END)  > 0
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
	est.id cod_origen_establecimiento
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
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_establecimientos"  est ON (dcmatch.base_origen=est.base_origen)		
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
	dc.descrip_modalidad,
	est.id	

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
			THEN date_format(try_cast(t.fecha as date), '%Y-%m-%d %h:%i%p')
		ELSE date_format(cast(fechas.fecha_inicio as date), '%Y-%m-%d %h:%i%p')
		END, '-', 1)  AS INTEGER) anio_inicio,
		
		-- El proximo campo es el semestre de inicio de la edicion capacitacion, NO indica que la misma es semestral o anual
		-- se utiliza la fecha calculada en el script de estado de beneficiario
		-- En caso que este en null, se utiliza la fecha de la tabla taller, en otro caso queda en null
		CASE 
		WHEN CAST(SPLIT_PART(CASE 
						WHEN fechas.fecha_inicio IS NULL
							THEN date_format(try_cast(t.fecha as date), '%Y-%m-%d %h:%i%p')
						ELSE date_format(cast(fechas.fecha_inicio as date), '%Y-%m-%d %h:%i%p')
						END, '-', 2) AS INTEGER) <= 6
			THEN 1
		WHEN CAST(SPLIT_PART(CASE 
						WHEN fechas.fecha_inicio IS NULL
							THEN date_format(try_cast(t.fecha as date), '%Y-%m-%d %h:%i%p')
						ELSE date_format(cast(fechas.fecha_inicio as date), '%Y-%m-%d %h:%i%p')
						END, '-', 2) AS INTEGER) > 6
			THEN 2
		ELSE NULL
		END semestre_inicio,			
		
		-- se utiliza la fecha calculada en el script de estado de beneficiario
		-- En caso que este en null, se utiliza la fecha de la tabla taller, en otro caso queda en null
		CAST(DATE_PARSE(CASE 
		WHEN fechas.fecha_inicio IS NULL
			THEN date_format(try_cast(t.fecha as date), '%Y-%m-%d %h:%i%p')
		ELSE date_format(cast(fechas.fecha_inicio as date), '%Y-%m-%d %h:%i%p')
		END , '%Y-%m-%d %h:%i%p') AS DATE) fecha_inicio_dictado,
		
		-- se utiliza la fecha calculada en el script de estado de beneficiario
		-- En caso que este en null, se utiliza la fecha de la tabla taller, en otro caso queda en null
		CAST(DATE_PARSE(CASE 
		WHEN  fechas.fecha_fin IS NULL
			THEN date_format(try_cast(t.fecha_fin as date), '%Y-%m-%d %h:%i%p')
		ELSE date_format(cast(fechas.fecha_fin as date), '%Y-%m-%d %h:%i%p')
		END , '%Y-%m-%d %h:%i%p') AS DATE) fecha_fin_dictado,
		
	   -- no se encuentra en bbdd origen un valor para el atributo
	   -- se utiliza la fecha calculada en el script de estado de beneficiario, en otro caso queda en null
	   	CAST(DATE_PARSE(CASE 
		WHEN fechas.fecha_inicio_inscripcion IS NULL
			THEN NULL
		ELSE date_format(cast(fechas.fecha_inicio_inscripcion as date), '%Y-%m-%d %h:%i%p')
		END , '%Y-%m-%d %h:%i%p') AS DATE) fecha_inicio_inscripcion,
	   
	   -- no se encuentra en bbdd origen un valor para el atributo
	   -- se utiliza la fecha calculada en el script de estado de beneficiario, en otro caso queda en null
	   CAST(DATE_PARSE(CASE 
		WHEN fechas.fecha_fin_inscripcion IS NULL
			THEN NULL
		ELSE date_format(cast(fechas.fecha_fin_inscripcion as date), '%Y-%m-%d %h:%i%p')
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
	   CASE WHEN COALESCE(t.fecha_fin,'0000-00-00') = '0000-00-00' THEN (CASE WHEN te.nombre = 'Activo' THEN 'S' ELSE 'N' END) ELSE (CASE WHEN DATE_PARSE(CASE WHEN t.fecha_fin = '0000-00-00' THEN NULL ELSE t.fecha_fin END, '%Y-%m-%d') > current_date THEN 'S' ELSE 'N' END) END inscripcion_abierta,
       CASE WHEN COALESCE(t.fecha_fin,'0000-00-00') = '0000-00-00' THEN (CASE WHEN te.nombre = 'Activo' THEN 'S' ELSE 'N' END) ELSE (CASE WHEN DATE_PARSE(CASE WHEN t.fecha_fin = '0000-00-00' THEN NULL ELSE t.fecha_fin END, '%Y-%m-%d') > current_date THEN 'S' ELSE 'N' END) END activo,
	   CAST(t.altas_total AS VARCHAR) cant_inscriptos,
	   CAST(t.vacantes AS VARCHAR) vacantes,
		
		-- Si la modalidad no esta en la edicion se toma desde la entidad capacitacion, NULL en otro caso
		dc.modalidad_id,
		dc.descrip_modalidad,
		est.id cod_origen_establecimiento
FROM "caba-piba-raw-zone-db"."sienfo_carreras" ca
LEFT JOIN "caba-piba-raw-zone-db"."sienfo_talleres" t ON (t.id_carrera = ca.id_carrera)
LEFT JOIN  "caba-piba-staging-zone-db"."tbp_typ_def_establecimientos" est ON (est.id_old=CAST(t.id_centro AS VARCHAR))
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
			THEN date_format(try_cast(t.fecha as date), '%Y-%m-%d %h:%i%p')
		ELSE date_format(cast(fechas.fecha_inicio as date), '%Y-%m-%d %h:%i%p')
		END, '-', 1)  AS INTEGER) anio_inicio,
		
		-- El proximo campo es el semestre de inicio de la edicion capacitacion, NO indica que la misma es semestral o anual
		-- se utiliza la fecha calculada en el script de estado de beneficiario
		-- En caso que este en null, se utiliza la fecha de la tabla taller, en otro caso queda en null
		CASE 
		WHEN CAST(SPLIT_PART(CASE 
						WHEN fechas.fecha_inicio IS NULL
							THEN date_format(try_cast(t.fecha as date), '%Y-%m-%d %h:%i%p')
						ELSE date_format(cast(fechas.fecha_inicio as date), '%Y-%m-%d %h:%i%p')
						END, '-', 2) AS INTEGER) <= 6
			THEN 1
		WHEN CAST(SPLIT_PART(CASE 
						WHEN fechas.fecha_inicio IS NULL
							THEN date_format(try_cast(t.fecha as date), '%Y-%m-%d %h:%i%p')
						ELSE date_format(cast(fechas.fecha_inicio as date), '%Y-%m-%d %h:%i%p')
						END, '-', 2) AS INTEGER) > 6
			THEN 2
		ELSE NULL
		END semestre_inicio,		
		
		-- se utiliza la fecha calculada en el script de estado de beneficiario
		-- En caso que este en null, se utiliza la fecha de la tabla taller, en otro caso queda en null
		CAST(DATE_PARSE(CASE 
		WHEN  fechas.fecha_inicio IS NULL
			THEN date_format(try_cast(t.fecha as date), '%Y-%m-%d %h:%i%p')
		ELSE date_format(cast(fechas.fecha_inicio as date), '%Y-%m-%d %h:%i%p')
		END , '%Y-%m-%d %h:%i%p') AS DATE) fecha_inicio_dictado,
		
		-- se utiliza la fecha calculada en el script de estado de beneficiario
		-- En caso que este en null, se utiliza la fecha de la tabla taller, en otro caso queda en null
		CAST(DATE_PARSE(CASE 
		WHEN  fechas.fecha_fin IS NULL
			THEN date_format(try_cast(t.fecha_fin as date), '%Y-%m-%d %h:%i%p')
		ELSE date_format(cast(fechas.fecha_fin as date), '%Y-%m-%d %h:%i%p')
		END , '%Y-%m-%d %h:%i%p') AS DATE) fecha_fin_dictado,
		
		-- se utiliza la fecha calculada en el script de estado de beneficiario, en otro caso queda en null
	   	CAST(DATE_PARSE(CASE 
		WHEN  fechas.fecha_inicio_inscripcion IS NULL
			THEN NULL
		ELSE date_format(cast(fechas.fecha_inicio_inscripcion as date), '%Y-%m-%d %h:%i%p')
		END , '%Y-%m-%d %h:%i%p') AS DATE) fecha_inicio_inscripcion,
	   
		-- se utiliza la fecha calculada en el script de estado de beneficiario, en otro caso queda en null
	   CAST(DATE_PARSE(CASE 
		WHEN  fechas.fecha_fin_inscripcion IS NULL
			THEN NULL
		ELSE date_format(cast(fechas.fecha_fin_inscripcion as date), '%Y-%m-%d %h:%i%p')
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
	   CASE WHEN COALESCE(t.fecha_fin,'0000-00-00') = '0000-00-00' THEN (CASE WHEN te.nombre = 'Activo' THEN 'S' ELSE 'N' END) ELSE (CASE WHEN DATE_PARSE(CASE WHEN t.fecha_fin = '0000-00-00' THEN NULL ELSE t.fecha_fin END, '%Y-%m-%d') > current_date THEN 'S' ELSE 'N' END) END inscripcion_abierta,
       CASE WHEN COALESCE(t.fecha_fin,'0000-00-00') = '0000-00-00' THEN (CASE WHEN te.nombre = 'Activo' THEN 'S' ELSE 'N' END) ELSE (CASE WHEN DATE_PARSE(CASE WHEN t.fecha_fin = '0000-00-00' THEN NULL ELSE t.fecha_fin END, '%Y-%m-%d') > current_date THEN 'S' ELSE 'N' END) END activo,
	   CAST(t.altas_total AS VARCHAR) cant_inscriptos,
	   CAST(t.vacantes AS VARCHAR) vacantes,
		-- Si la modalidad no esta en la edicion se toma desde la entidad capacitacion, NULL en otro caso
		dc.modalidad_id,
		dc.descrip_modalidad,
       est.id cod_origen_establecimiento
FROM "caba-piba-raw-zone-db"."sienfo_cursos" cu
LEFT JOIN "caba-piba-raw-zone-db"."sienfo_talleres" t ON (t.id_curso = cu.id_curso)
LEFT JOIN  "caba-piba-staging-zone-db"."tbp_typ_def_establecimientos" est ON (est.id_old=CAST(t.id_centro AS VARCHAR))
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
			THEN date_format(cast(of.inicio as date), '%Y-%m-%d %h:%i%p')
		ELSE date_format(cast(fechas.fecha_inicio as date), '%Y-%m-%d %h:%i%p')
		END, '-', 1)  AS INTEGER) anio_inicio,
						
						
		-- El proximo campo es el semestre de inicio de la edicion capacitacion, NO indica que la misma es semestral o anual
		-- se utiliza la fecha calculada en el script de estado de beneficiario. 
		-- en caso que este en null, se utiliza la fecha de la tabla oportunidades_formacion, en otro caso queda en null
		CASE 
		WHEN CAST(SPLIT_PART(CASE 
						WHEN fechas.fecha_inicio = '0000-00-00'
							THEN date_format(cast(of.inicio as date), '%Y-%m-%d %h:%i%p')
						ELSE date_format(cast(fechas.fecha_inicio as date), '%Y-%m-%d %h:%i%p')
						END, '-', 2) AS INTEGER) <= 6
			THEN 1
		WHEN CAST(SPLIT_PART(CASE 
						WHEN fechas.fecha_inicio = '0000-00-00'
							THEN date_format(cast(of.inicio as date), '%Y-%m-%d %h:%i%p')
						ELSE date_format(cast(fechas.fecha_inicio as date), '%Y-%m-%d %h:%i%p')
						END, '-', 2) AS INTEGER) > 6
			THEN 2			
		ELSE NULL
		END semestre_inicio,			
		
		-- se utiliza la fecha calculada en el script de estado de beneficiario. 
		-- en caso que este en null, se utiliza la fecha de la tabla oportunidades_formacion, en otro caso queda en null
		CAST(DATE_PARSE(CASE 
		WHEN  fechas.fecha_inicio = '0000-00-00'
			THEN date_format(cast(of.inicio as date), '%Y-%m-%d %h:%i%p')
		ELSE date_format(cast(fechas.fecha_inicio as date), '%Y-%m-%d %h:%i%p')
		END , '%Y-%m-%d %h:%i%p') AS DATE) fecha_inicio_dictado,			


		-- se utiliza la fecha calculada en el script de estado de beneficiario. 
		-- en caso que este en null, se utiliza la fecha de la tabla oportunidades_formacion, en otro caso queda en null
		CAST(DATE_PARSE(CASE 
		WHEN  fechas.fecha_fin = '0000-00-00'
			THEN date_format(of.fin, '%Y-%m-%d %h:%i%p')
		ELSE date_format(cast(fechas.fecha_fin as date), '%Y-%m-%d %h:%i%p')
		END , '%Y-%m-%d %h:%i%p') AS DATE) fecha_fin_dictado,


	   -- no se encuentra en bbdd origen un valor para el atributo
	   -- se utiliza la fecha calculada en el script de estado de beneficiario, en otro caso queda en null
	   	CAST(DATE_PARSE(CASE 
		WHEN  fechas.fecha_inicio_inscripcion = '0000-00-00'
			THEN NULL
		ELSE date_format(cast(fechas.fecha_inicio_inscripcion as date), '%Y-%m-%d %h:%i%p')
		END , '%Y-%m-%d %h:%i%p') AS DATE) fecha_inicio_inscripcion,
	   
	   -- no se encuentra en bbdd origen un valor para el atributo
	   -- se utiliza la fecha calculada en el script de estado de beneficiario, en otro caso queda en null
	   CAST(DATE_PARSE(CASE 
		WHEN  fechas.fecha_fin_inscripcion = '0000-00-00'
			THEN NULL
		ELSE date_format(cast(fechas.fecha_fin_inscripcion as date), '%Y-%m-%d %h:%i%p')
		END , '%Y-%m-%d %h:%i%p') AS DATE) fecha_limite_inscripcion,
		       
       NULL turno,
       NULL dias_cursada,
       CASE WHEN of.fin IS NULL THEN (CASE WHEN estado_inscripcion = 'finalizada' THEN 'N' ELSE 'S' END) ELSE (CASE WHEN of.fin > current_date THEN 'S' ELSE 'N' END) END inscripcion_abierta,
       CASE WHEN of.fin IS NULL THEN (CASE WHEN estado_curso = 'finalizada' THEN 'N' ELSE 'S' END) ELSE (CASE WHEN of.fin > current_date THEN 'S' ELSE 'N' END) END activo,
	   
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

       est.id cod_origen_establecimiento
	   
FROM "caba-piba-raw-zone-db"."crm_sociolaboral_op_oportunidades_formacion" of 
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_establecimientos" est ON (of.sede in (est.nombres_old))
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
	est.id
	
UNION 
-- SIU
SELECT 
	'SIU' base_origen,
	dcmatch.tipo_capacitacion,
	dcmatch.id_new capacitacion_id_new,
	CAST(dcmatch.id_old AS VARCHAR) capacitacion_id_old,
	
	-- no se completa dado que no se usa en la tabla def
	' ' descrip_capacitacion_old,
		
	-- la clave de edicion capacitacion se compone los identificadores del 1-plan de estudio, 
	-- 2-la propuesta academica, 3-el periodo lectivo, 4-el establecimiento donde se dicta la capacitacion 
	-- 5-la modalidad de la capacitacion y 6-Turno
	CAST(spl.plan AS VARCHAR) || '-' || CAST(spl.propuesta AS VARCHAR) || '-' || 
	CAST(pl.periodo_lectivo AS VARCHAR)  || '-' || CAST(est.id AS VARCHAR) || '-' ||
	CAST(dc.modalidad_id AS VARCHAR) || '-' ||
	CAST(turno_c.turno AS VARCHAR) edicion_capacitacion_id, 
	
	CAST(SPLIT_PART(MIN(date_format(pl.fecha_inicio_dictado, '%Y-%m-%d %h:%i%p')), '-', 1)  AS INTEGER) anio_inicio,
	
	-- El proximo campo es el semestre de inicio de la edicion capacitacion, NO indica que la misma es semestral o anual
	CASE 
		WHEN CAST(SPLIT_PART(MIN(date_format(pl.fecha_inicio_dictado, '%Y-%m-%d %h:%i%p')), '-', 2)  AS INTEGER)  <= 6 THEN 1
		WHEN CAST(SPLIT_PART(MIN(date_format(pl.fecha_inicio_dictado, '%Y-%m-%d %h:%i%p')), '-', 2)  AS INTEGER)  > 6 THEN 2
		ELSE NULL
	END semestre_inicio,
	
	CAST(DATE_PARSE(MIN(date_format(pl.fecha_inicio_dictado, '%Y-%m-%d %h:%i%p')), '%Y-%m-%d %h:%i%p') AS DATE) fecha_inicio_dictado,

	CAST(DATE_PARSE(MAX(date_format(pl.fecha_fin_dictado, '%Y-%m-%d %h:%i%p')), '%Y-%m-%d %h:%i%p') AS DATE) fecha_fin_dictado,
	
	-- se obtiene calculando la primer inscripcion en la edicion
	CAST(DATE_PARSE(date_format(inscriptos.min_fecha_inscripcion, '%Y-%m-%d %h:%i%p'), '%Y-%m-%d %h:%i%p') AS DATE) fecha_inicio_inscripcion,
	
	-- se obtiene calculando la ultima inscripcion en la edicion
	CAST(DATE_PARSE(date_format(inscriptos.max_fecha_inscripcion, '%Y-%m-%d %h:%i%p'), '%Y-%m-%d %h:%i%p') AS DATE) fecha_limite_inscripcion,

	-- se considera turno mañana si la hora de inicio es entre las 7 y 12, tarde entre 12 y 20, noche entre 20 y 24
	array_join(array_agg(DISTINCT(turno_c.nombre)), ',') turno,
	
	-- si bien la columna dias.d trae la informacion requerida, hay que agruparlo a trabes de array join y realizar lo siguiente para que los dias queden ordenados
	CASE WHEN position('LUNES' IN UPPER(array_join(array_agg(DISTINCT(dias.d)), ',')))!=0 THEN 'Lunes ' ELSE '' END ||
	CASE WHEN position('MARTES' IN UPPER(array_join(array_agg(DISTINCT(dias.d)), ',')))!=0 THEN 'Martes ' ELSE '' END ||
	CASE WHEN position('MIERCOLES' IN UPPER(array_join(array_agg(DISTINCT(dias.d)), ',')))!=0 THEN 'Miercoles ' ELSE '' END ||
	CASE WHEN position('JUEVES' IN UPPER(array_join(array_agg(DISTINCT(dias.d)), ',')))!=0 THEN 'Jueves ' ELSE '' END ||
	CASE WHEN position('VIERNES' IN UPPER(array_join(array_agg(DISTINCT(dias.d)), ',')))!=0 THEN 'Viernes ' ELSE '' END ||
	CASE WHEN position('SABADO' IN UPPER(array_join(array_agg(DISTINCT(dias.d)), ',')))!=0 THEN 'Sabado ' ELSE '' END ||
	CASE WHEN position('DOMINGO' IN UPPER(array_join(array_agg(DISTINCT(dias.d)), ',')))!=0 THEN 'Domingo ' ELSE '' END dias_cursada,
	
	-- Si al menos una comision esta abierta, se considera la edicion capacitacion abierta
	CASE WHEN SUM(CASE WHEN comi.inscripcion_cerrada IN ('N') THEN 1 ELSE 0 END) > 0 THEN 'S'
		ELSE 'N'
	END inscripcion_abierta,
	
	-- Valores posibles de la columna comi.estado: (A)ctivo, (P)endiente, (B)aja
	-- Si al menos una comision esta activa, se considera la edicion capacitacion activa
	CASE WHEN SUM(CASE WHEN comi.estado IN ('A') THEN 1 ELSE 0 END) > 0 THEN 'S'
		ELSE 'N'
	END activo,
		
	-- solo se consideran los alumnos inscriptos y con el estado = A (el estado (P)endiente no se tiene en cuenta)
	CAST(inscriptos.cant_alumnos AS VARCHAR) cant_inscriptos,
	
	CAST(cupo.cupo AS VARCHAR) as vacantes,

	-- La modalidad se toma desde la entidad capacitacion
	dc.modalidad_id,
	dc.descrip_modalidad,
	
	est.id cod_origen_establecimiento
			
FROM "caba-piba-raw-zone-db"."siu_toba_3_3_negocio_sga_planes" spl
LEFT JOIN "caba-piba-raw-zone-db"."siu_toba_3_3_negocio_sga_propuestas" spr ON (spl.propuesta = spr.propuesta)
LEFT JOIN "caba-piba-raw-zone-db"."siu_toba_3_3_negocio_sga_comisiones_propuestas" cp ON (cp.propuesta = spr.propuesta AND cp.plan=spl.plan)
LEFT JOIN "caba-piba-raw-zone-db"."siu_toba_3_3_negocio_sga_comisiones" comi ON (comi.comision=cp.comision)
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_establecimientos" est ON (est.id_old=CAST(comi.ubicacion AS VARCHAR) AND est.base_origen = 'SIU')
LEFT JOIN "caba-piba-raw-zone-db"."siu_toba_3_3_negocio_sga_periodos_lectivos" pl ON (comi.periodo_lectivo=pl.periodo_lectivo)
LEFT JOIN "caba-piba-raw-zone-db"."siu_toba_3_3_negocio_sga_turnos_cursadas" turno_c ON (turno_c.turno=comi.turno)
LEFT JOIN 
		(SELECT c.comision,  array_join(array_agg(DISTINCT(asig.dia_semana)), ',') d
		FROM "caba-piba-raw-zone-db"."siu_toba_3_3_negocio_sga_comisiones" c 
		LEFT JOIN "caba-piba-raw-zone-db"."siu_toba_3_3_negocio_sga_comisiones_bh" banda_horaria ON (banda_horaria.comision=c.comision)
		LEFT JOIN "caba-piba-raw-zone-db"."siu_toba_3_3_negocio_sga_asignaciones" asig ON (asig.asignacion=banda_horaria.asignacion)
		GROUP BY c.comision) dias ON (dias.comision=comi.comision)
LEFT JOIN 
		(SELECT 
			spl.plan,
			spl.propuesta,
			c.periodo_lectivo,
			est.id id_est, 
			c.turno,
			SUM(CAST(c.cupo AS INTEGER)) as cupo
		FROM "caba-piba-raw-zone-db"."siu_toba_3_3_negocio_sga_comisiones" c 
			JOIN "caba-piba-raw-zone-db"."siu_toba_3_3_negocio_sga_comisiones_propuestas" cp ON (c.comision=cp.comision)
			JOIN "caba-piba-raw-zone-db"."siu_toba_3_3_negocio_sga_propuestas" spr ON (cp.propuesta = spr.propuesta)
			JOIN "caba-piba-raw-zone-db"."siu_toba_3_3_negocio_sga_planes" spl ON (spl.propuesta = spr.propuesta AND cp.plan=spl.plan)
			JOIN "caba-piba-staging-zone-db"."tbp_typ_def_establecimientos" est ON (est.id_old=CAST(c.ubicacion AS VARCHAR) AND est.base_origen = 'SIU')
		    JOIN "caba-piba-raw-zone-db"."siu_toba_3_3_negocio_sga_turnos_cursadas" turno_c ON (turno_c.turno=c.turno)
		GROUP BY 
			spl.plan,
			spl.propuesta,
			c.periodo_lectivo,
			c.turno,
			est.id
		) cupo ON (cupo.plan=spl.plan AND cupo.propuesta=spr.propuesta AND cupo.periodo_lectivo=pl.periodo_lectivo 
		AND cupo.id_est=est.id AND cupo.turno = turno_c.turno)		

LEFT JOIN
		(SELECT 
		COUNT(DISTINCT cu.alumno) AS cant_alumnos, 
		MIN(CAST(cu.fecha_inscripcion AS DATE)) min_fecha_inscripcion,
		MAX(CAST(cu.fecha_inscripcion AS DATE)) max_fecha_inscripcion,
		spl.plan,
		spl.propuesta,
		c.periodo_lectivo,
		est.id id_est, 
		c.turno
		FROM "caba-piba-raw-zone-db"."siu_toba_3_3_negocio_sga_insc_cursada" cu  
		JOIN "caba-piba-raw-zone-db"."siu_toba_3_3_negocio_sga_comisiones" c ON (cu.comision=c.comision)
		JOIN "caba-piba-raw-zone-db"."siu_toba_3_3_negocio_sga_comisiones_propuestas" cp ON (cu.comision=cp.comision)
			JOIN "caba-piba-raw-zone-db"."siu_toba_3_3_negocio_sga_propuestas" spr ON (cp.propuesta = spr.propuesta)
			JOIN "caba-piba-raw-zone-db"."siu_toba_3_3_negocio_sga_planes" spl ON (spl.propuesta = spr.propuesta AND cp.plan=spl.plan)
			JOIN "caba-piba-staging-zone-db"."tbp_typ_def_establecimientos" est ON (est.id_old=CAST(c.ubicacion AS VARCHAR) AND est.base_origen = 'SIU')
		    JOIN "caba-piba-raw-zone-db"."siu_toba_3_3_negocio_sga_turnos_cursadas" turno_c ON (turno_c.turno=c.turno)
		WHERE cu.estado LIKE 'A'
		GROUP BY 
		spl.plan,
		spl.propuesta,
		c.periodo_lectivo,
		c.turno,
		est.id ) inscriptos
		ON (inscriptos.plan=spl.plan AND inscriptos.propuesta=spr.propuesta AND inscriptos.periodo_lectivo=pl.periodo_lectivo 
		AND inscriptos.id_est=est.id AND inscriptos.turno = turno_c.turno)	
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_capacitacion_match" dcmatch ON (dcmatch.id_old = CAST(spl.plan AS VARCHAR) AND dcmatch.base_origen = 'SIU')
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_capacitacion" dc ON (dcmatch.id_new = dc.id_new AND dcmatch.base_origen = dc.base_origen)
GROUP BY 
	dcmatch.tipo_capacitacion,
	dcmatch.id_new,
	dcmatch.id_old,
	spl.plan,
	spl.propuesta, 
	pl.periodo_lectivo,
	dc.modalidad_id,
	dc.descrip_modalidad,
	est.id,
	turno_c.turno,
	cupo.cupo,
	inscriptos.cant_alumnos,
	inscriptos.min_fecha_inscripcion,
	inscriptos.max_fecha_inscripcion
--</sql>--
	
-- 2.-- Crear EDICION CAPACITACION GOET, MOODLE, SIENFO Y CRMSL paso 2 
-- se agrega un indice incremental
-- --<sql>--DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_tmp_edicion_capacitacion_2`;--</sql>--
--<sql>--
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
--</sql>--

-- 3.-- Crear EDICION CAPACITACION GOET, MOODLE, SIENFO Y CRMSL paso 3
-- se corrigen posibles desviasiones en fechas
-- --<sql>--DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_tmp_edicion_capacitacion`;--</sql>--
--<sql>--
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
--</sql>--