-- 1.-- Crear la tabla def de entrevista
--<sql>--
DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_def_postulaciones`;
--</sql>--

--<sql>--
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_def_postulaciones" AS
SELECT row_number() OVER () AS id_postulacion,
	p.vecino_id,
	p.base_origen,
	ol.oportunidad_laboral_id,
	p.consiguio_trabajo,
	p.fecha_notificacion,
	p.tuvo_entrevista,
	CASE
		WHEN p.tuvo_entrevista = 0 THEN CAST(NULL AS DATE) ELSE CAST(p.fecha_entrevista AS DATE)
	END fecha_entrevista,
	CASE
		WHEN p.tuvo_entrevista = 0 THEN CAST(NULL AS VARCHAR) ELSE p.tipo_entrevista
	END tipo_entrevista,
	CASE
		WHEN p.tuvo_entrevista = 0 THEN CAST(NULL AS VARCHAR) ELSE p.estado_entrevista
	END estado_entrevista
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_postulaciones" p
	LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_oportunidad_laboral" ol ON (
		p.oportunidad_laboral_id_old = ol.oportunidad_laboral_id_old
		and ol.base_origen = p.base_origen
	)
--</sql>--