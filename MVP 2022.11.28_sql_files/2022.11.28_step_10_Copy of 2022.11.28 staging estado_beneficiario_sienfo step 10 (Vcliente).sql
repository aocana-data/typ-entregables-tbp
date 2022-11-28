CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_estado_beneficiario_sienfo" AS
with t as (
	/*Limpieza de talleres, me quedo con los cursos validos,
	 * transformo a fechas las columnas, filtro fechas menores a 2003 o fechas de fin mayores a curdate + 1 año
	 * merge id_duracion entre duracion de taller y la generica de cursos porque esta incompleta
	 */
	select codigo_ct,
		case
			when fecha = '0000-00-00' then null
			when cast(fecha as date) < cast('2003-01-01' as date) then null else cast(fecha as date)
		end as "fecha",
		case
			when fecha_fin = '0000-00-00' then null
			when cast(fecha_fin as date) < cast('2003-01-01' as date) then null
			when cast(fecha_fin as date) > date_add('year', 1, current_date) then null else cast(fecha_fin as date)
		end as "fecha_fin",
		case
			when t.id_duracion is null then c.id_duracion else t.id_duracion
		end as "id_duracion",
		t.id_curso,
		t.id_carrera,
		d.nombre as "duracion",
		c.nom_curso
	from "caba-piba-raw-zone-db"."sienfo_talleres" t
		left join "caba-piba-raw-zone-db"."sienfo_duracion" d on d.id_duracion = t.id_duracion
		left join (
			select id_curso,
				max(id_duracion) as "id_duracion",
				nom_curso
			from "caba-piba-raw-zone-db"."sienfo_cursos"
			group by id_curso,
				nom_curso
			having max(id_duracion) > 0
		) c on t.id_curso = c.id_curso
	where codigo_ct is not null
		and t.id_curso is not null
		and t.id_curso != 0
		and t.id_carrera = 0
),
/*
 * baja como id normalizado
 * fecha de inicio de curso menor a 2003 como nulo y transformo a fecha
 * normalizo aprobado
 * primer estado de beneficiario basado en los datos de la tabla original
 */
f1 as (
	select f.nrodoc,
		f.codigo_ct,
		case
			when baja is null then 0 else baja
		end as "baja",
		f.fechabaja,
		case
			when fecha_inc < cast('2003-01-01' as date) then null else cast(fecha_inc as date)
		end as "fecha_inc",
		case
			when f.aprobado is null then 0
			when f.aprobado = '' then 0 else cast(f.aprobado as int)
		end as "aprobado",
		case
			when f.aprobado in ('1', '3') then 'aprobado'
			when f.fechabaja is not null then 'baja'
			when f.baja not in (14, 22, 24, 0)
			and baja is not null then 'baja'
			when f.aprobado in ('2', '4', '5', '6', '8') then 'reprobado'
		end as "estado_beneficiario"
	from "caba-piba-raw-zone-db"."sienfo_fichas" f
	where nrodoc is not null
),
/*
 *para completar fechas de inicio que no existen agrupo por fichas, la minima fecha de inicio de un alumno 
 */
fechas_ct as (
	select codigo_ct,
		min(fecha_inc) as fecha_inc_min
	from f1
	group by codigo_ct
),
/*
 * join de fichas y talleres
 * si la fecha de inicio del taller es nula uso la fecha de inicio del alumno
 * se invalidan fechas de fin inconsistentes para luego ser recalculadas por duracion
 */
tf as (
	select t.codigo_ct,
		case
			when t.fecha is not null then fecha else fct.fecha_inc_min
		end as "fecha",
		case
			when t.fecha is not null
			and t.fecha_fin < t.fecha then null
			when t.fecha_fin < fct.fecha_inc_min then null else t.fecha_fin
		end as "fecha_fin",
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
	from t
		inner join f1 on f1.codigo_ct = t.codigo_ct
		left join fechas_ct fct on fct.codigo_ct = t.codigo_ct
		/*
		 * Calcula la fecha de fin segun en campo id_duracion para fechas nulas 
		 * y las previamente invalidadas por inconsistencia, al ser fechas de fin menores que fecha de inicio
		 */
),
tf1 as (
	select tf.codigo_ct,
		tf.fecha,
		case
			when fecha_fin is not null then fecha_fin
			when fecha_fin is null then (
				case
					when id_duracion = 1 then date_add('month', 9, fecha)
					when id_duracion in (2, 4) then date_add('month', 4, fecha)
					when id_duracion = 3 then date_add('month', 2, fecha)
				end
			)
		end as "fecha_fin",
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
	from tf
),
tf3 as (
	select tf1.codigo_ct,
		tf1.nrodoc,
		case
			when tf1.fecha_inc is null
			and tf1.id_duracion is not null
			and tf1.fecha is not null then (
				case
					when id_duracion = 1 then date_add('month', -9, fecha)
					when id_duracion in (2, 4) then date_add('month', -4, fecha)
					when id_duracion = 3 then date_add('month', -2, fecha)
				end
			) else tf1.fecha_inc
		end as "fecha_inc",
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
		case
			when estado_beneficiario is not null then estado_beneficiario
			when baja = 0
			and fechabaja is null
			and aprobado = 0 then (
				case
					when date_add('month', 2, fecha_fin) <= current_date then 'reprobado'
					when fecha_fin <= current_date then 'finalizo_cursada'
					when fecha_inc < current_date
					and fecha_fin > current_date then 'regular'
				end
			)
		end as "estado_beneficiario2"
	from tf1
	where nrodoc != ''
)
/*
 * Select final, estado_beneficiario2 es el calculado con el algoritmo de fechas,
 *  estado de beneficiario se basa en las columnas de la tabla
 */
select tf3.codigo_ct,
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
from tf3 
--Existen fechas de inicio del alumno anteriores a fecha de inicio del curso
--nrodoc esta muy sucio
--Los estados de beneficiario2 nulos son cuando aprobado = 9 (nuevo id, no esta en en dump) corresponde a nombre = Actualiza, observa = CETBA [NO SE QUE SIGNIFICA]