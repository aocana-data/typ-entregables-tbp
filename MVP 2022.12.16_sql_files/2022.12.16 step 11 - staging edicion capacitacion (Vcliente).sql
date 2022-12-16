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
	ELSE date_format(cast(fechas.fecha_inicio as date), '%Y-%m-%d %h:%i%p')
	END, '-', 1)  AS INTEGER) anio_inicio,
	
	-- El proximo campo es el semestre de inicio de la edicion capacitacion, NO indica que la misma es semestral o anual
	-- se utiliza la fecha calculada en el script de estado de beneficiario
	-- En caso que este en null, se utiliza co.startdate (tiene 89.87% de completitud), en otro caso queda en null
	CASE 
	WHEN CAST(SPLIT_PART(CASE 
		WHEN fechas.fecha_inicio = '0000-00-00'
			THEN date_format(from_unixtime(co.startdate), '%Y-%m-%d %h:%i%p')
		ELSE date_format(cast(fechas.fecha_inicio as date), '%Y-%m-%d %h:%i%p')
		END, '-', 2) AS INTEGER) <= 6
		THEN 1
	ELSE 2
	END semestre_inicio,			
	
	-- se utiliza la fecha calculada en el script de estado de beneficiario
	-- En caso que este en null, se utiliza co.startdate (tiene 89.87% de completitud), en otro caso queda en null
	CAST(DATE_PARSE(CASE 
	WHEN  fechas.fecha_inicio = '0000-00-00'
		THEN date_format(from_unixtime(co.startdate), '%Y-%m-%d %h:%i%p')
	ELSE date_format(cast(fechas.fecha_inicio as date), '%Y-%m-%d %h:%i%p')
	END , '%Y-%m-%d %h:%i%p') AS DATE) fecha_inicio_dictado,

	-- se utiliza la fecha calculada en el script de estado de beneficiario
	-- En caso que este en null, se utiliza co.enddate (tiene 24.90% de completitud), en otro caso queda en null
	CAST(DATE_PARSE(CASE 
	WHEN  fechas.fecha_fin = '0000-00-00'
		THEN date_format(from_unixtime(co.enddate), '%Y-%m-%d %h:%i%p')
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
			THEN date_format(cast(t.fecha as date), '%Y-%m-%d %h:%i%p')
		ELSE date_format(cast(fechas.fecha_inicio as date), '%Y-%m-%d %h:%i%p')
		END, '-', 1)  AS INTEGER) anio_inicio,
		
		-- El proximo campo es el semestre de inicio de la edicion capacitacion, NO indica que la misma es semestral o anual
		-- se utiliza la fecha calculada en el script de estado de beneficiario
		-- En caso que este en null, se utiliza la fecha de la tabla taller, en otro caso queda en null
		CASE 
		WHEN CAST(SPLIT_PART(CASE 
						WHEN fechas.fecha_inicio = '0000-00-00'
							THEN date_format(cast(t.fecha as date), '%Y-%m-%d %h:%i%p')
						ELSE date_format(cast(fechas.fecha_inicio as date), '%Y-%m-%d %h:%i%p')
						END, '-', 2) AS INTEGER) <= 6
			THEN 1
		ELSE 2
		END semestre_inicio,			
		
		-- se utiliza la fecha calculada en el script de estado de beneficiario
		-- En caso que este en null, se utiliza la fecha de la tabla taller, en otro caso queda en null
		CAST(DATE_PARSE(CASE 
		WHEN  fechas.fecha_inicio = '0000-00-00'
			THEN date_format(cast(t.fecha as date), '%Y-%m-%d %h:%i%p')
		ELSE date_format(cast(fechas.fecha_inicio as date), '%Y-%m-%d %h:%i%p')
		END , '%Y-%m-%d %h:%i%p') AS DATE) fecha_inicio_dictado,
		
		-- se utiliza la fecha calculada en el script de estado de beneficiario
		-- En caso que este en null, se utiliza la fecha de la tabla taller, en otro caso queda en null
		CAST(DATE_PARSE(CASE 
		WHEN  fechas.fecha_fin = '0000-00-00'
			THEN date_format(cast(t.fecha_fin as date), '%Y-%m-%d %h:%i%p')
		ELSE date_format(cast(fechas.fecha_fin as date), '%Y-%m-%d %h:%i%p')
		END , '%Y-%m-%d %h:%i%p') AS DATE) fecha_fin_dictado,
		
	   -- no se encuentra en bbdd origen un valor para el atributo
	   -- se utiliza la fecha calculada en el script de estado de beneficiario
	   -- En caso que este en null, se utiliza la fecha de la tabla taller, en otro caso queda en null
	   	CAST(DATE_PARSE(CASE 
		WHEN  fechas.fecha_inicio_inscripcion = '0000-00-00'
			THEN NULL
		ELSE date_format(cast(fechas.fecha_inicio_inscripcion as date), '%Y-%m-%d %h:%i%p')
		END , '%Y-%m-%d %h:%i%p') AS DATE) fecha_inicio_inscripcion,
	   
	   -- no se encuentra en bbdd origen un valor para el atributo
	   -- se utiliza la fecha calculada en el script de estado de beneficiario
	   -- En caso que este en null, se utiliza la fecha de la tabla taller, en otro caso queda en null
	   CAST(DATE_PARSE(CASE 
		WHEN  fechas.fecha_fin_inscripcion = '0000-00-00'
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
			THEN date_format(cast(t.fecha as date), '%Y-%m-%d %h:%i%p')
		ELSE date_format(cast(fechas.fecha_inicio as date), '%Y-%m-%d %h:%i%p')
		END, '-', 1)  AS INTEGER) anio_inicio,
		
		-- El proximo campo es el semestre de inicio de la edicion capacitacion, NO indica que la misma es semestral o anual
		-- se utiliza la fecha calculada en el script de estado de beneficiario
		-- En caso que este en null, se utiliza la fecha de la tabla taller, en otro caso queda en null
		CASE 
		WHEN CAST(SPLIT_PART(CASE 
						WHEN fechas.fecha_inicio = '0000-00-00'
							THEN date_format(cast(t.fecha as date), '%Y-%m-%d %h:%i%p')
						ELSE date_format(cast(fechas.fecha_inicio as date), '%Y-%m-%d %h:%i%p')
						END, '-', 2) AS INTEGER) <= 6
			THEN 1
		ELSE 2
		END semestre_inicio,			
		
		-- se utiliza la fecha calculada en el script de estado de beneficiario
		-- En caso que este en null, se utiliza la fecha de la tabla taller, en otro caso queda en null
		CAST(DATE_PARSE(CASE 
		WHEN  fechas.fecha_inicio = '0000-00-00'
			THEN date_format(cast(t.fecha as date), '%Y-%m-%d %h:%i%p')
		ELSE date_format(cast(fechas.fecha_inicio as date), '%Y-%m-%d %h:%i%p')
		END , '%Y-%m-%d %h:%i%p') AS DATE) fecha_inicio_dictado,
		
		-- se utiliza la fecha calculada en el script de estado de beneficiario
		-- En caso que este en null, se utiliza la fecha de la tabla taller, en otro caso queda en null
		CAST(DATE_PARSE(CASE 
		WHEN  fechas.fecha_fin = '0000-00-00'
			THEN date_format(cast(t.fecha_fin as date), '%Y-%m-%d %h:%i%p')
		ELSE date_format(cast(fechas.fecha_fin as date), '%Y-%m-%d %h:%i%p')
		END , '%Y-%m-%d %h:%i%p') AS DATE) fecha_fin_dictado,
		
		-- se utiliza la fecha calculada en el script de estado de beneficiario
		-- En caso que este en null, se utiliza la fecha de la tabla taller, en otro caso queda en null
	   	CAST(DATE_PARSE(CASE 
		WHEN  fechas.fecha_inicio_inscripcion = '0000-00-00'
			THEN NULL
		ELSE date_format(cast(fechas.fecha_inicio_inscripcion as date), '%Y-%m-%d %h:%i%p')
		END , '%Y-%m-%d %h:%i%p') AS DATE) fecha_inicio_inscripcion,
	   
		-- se utiliza la fecha calculada en el script de estado de beneficiario
		-- En caso que este en null, se utiliza la fecha de la tabla taller, en otro caso queda en null
	   CAST(DATE_PARSE(CASE 
		WHEN  fechas.fecha_fin_inscripcion = '0000-00-00'
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
		ELSE 2
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
FROM "caba-piba-raw-zone-db"."crm_sociolaboral_op_oportunidades_formacion" of 
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