-- Query Estado de Beneficiario para athena
-- --<sql>--DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_tmp_estado_beneficiario_crmsl`;--</sql>--
--<sql>--
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_estado_beneficiario_crmsl" AS
-- Query Estado de Beneficiario para athena
with seguimientos_calculado0 as (
select
	off.id,
	ofs.op_oportunidades_formacion_se_seguimiento_1se_seguimiento_idb ,
	s.name
	 , s.date_modified
	 , dense_rank() OVER(PARTITION BY off.id , s.name order by s.date_modified desc) as max_date
from
	"caba-piba-raw-zone-db".crm_sociolaboral_op_oportunidades_formacion off
left join "caba-piba-raw-zone-db".crm_sociolaboral_op_oportunidades_formacion_se_seguimiento_1_c ofs on
	ofs.op_oportun868armacion_ida = off.id
left join "caba-piba-raw-zone-db".crm_sociolaboral_se_seguimiento s on
	s.id = ofs.op_oportunidades_formacion_se_seguimiento_1se_seguimiento_idb
	-- group by off.id, s.name
	),
seguimientos_calculado as (
-- max_date : es usado como col de seguimiento0
-- se usa dense_rank buscando la fecha maxima
	select
		sc0.id,
		sc0.op_oportunidades_formacion_se_seguimiento_1se_seguimiento_idb ,
		sc0.name
		, sc0.date_modified
		from seguimientos_calculado0 as sc0
		 where sc0.max_date = 1
),
resultado as (
select
	c.id,
	c.first_name,
	c.last_name,
	c.lead_source,
	cc.forma_parte_interm_lab_c,
	off.id AS id_formacion,
	off.name,
	off.inicio,
	off.fin,
	case
		when estado_c = 'incompleto' then 'baja'
		-- convertir sus opciones a nuestras opciones
		when estado_c = 'en_curso' then 'regular'
		when estado_c = 'finalizado' then 'egresado'
		when estado_c = 'nunca_inicio' then 'baja'
		-- abandono al inicio
		when estado_c is not null then estado_c
		when estado_c is null then (
	case
			-- when off.name is null then 'no aplica_c'
		when off.name is not null then (
		case
				when off.inicio < off.fin then 'egresado'
				when off.fin is null then 'regular'
				when /*off.inicio is null and*/
				off.fin < current_date then 'egresado'
				-- when off.inicio > off.fin then 'error_fecha_c'
			end
		 )
		end)
	end as estado_n,
	sc.estado_c
from
	"caba-piba-raw-zone-db".crm_sociolaboral_contacts c
inner join "caba-piba-staging-zone-db"."tbp_typ_tmp_view_crm_sociolaboral_contacts_cstm_no_duplicates" cc on
	cc.id_c = c.id
inner join "caba-piba-raw-zone-db".crm_sociolaboral_op_oportunidades_formacion_contacts_c ofc on
	ofc.op_oportunidades_formacion_contactscontacts_idb = c.id
inner join "caba-piba-raw-zone-db".crm_sociolaboral_op_oportunidades_formacion off on
	off.id = ofc.op_oportun1d35rmacion_ida
	-- left join crm_sociolaboral_op_oportunidades_formacion_se_seguimiento_1_c ofs ON ofs.op_oportun868armacion_ida = off.id -- acá limitr para que solo traiga el que coincide con c.id
left join seguimientos_calculado scn on
	off.id = scn.id
	and replace(trim(concat_ws(' ', c.first_name, c.last_name)), ' ', ' ') = replace(trim(scn.name), '  ', ' ')
left join "caba-piba-raw-zone-db".crm_sociolaboral_op_oportunidades_formacion_se_seguimiento_1_c ofs on
	ofs.op_oportunidades_formacion_se_seguimiento_1se_seguimiento_idb = scn.op_oportunidades_formacion_se_seguimiento_1se_seguimiento_idb
left join "caba-piba-raw-zone-db".crm_sociolaboral_se_seguimiento s on
	s.id = ofs.op_oportunidades_formacion_se_seguimiento_1se_seguimiento_idb
left join "caba-piba-raw-zone-db".crm_sociolaboral_se_seguimiento_cstm sc on
	sc.id_c = s.id
where
	(c.lead_source in ('sociolaboral')
		or ((c.lead_source in ('rib'))
			and forma_parte_interm_lab_c in ('si')
		))
		)
select id alumno_id_old,
	   id_formacion edicion_capacitacion_id_old,
	   UPPER(first_name) first_name,
	   UPPER(last_name) last_name,
	   name descrip_capacitacion,
	   inicio,
	   fin,
	   UPPER(estado_n) estado_beneficiario
from resultado
--</sql>--