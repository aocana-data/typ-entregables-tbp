--- CRM SOCIO LABORAL

CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_def_capacitacion" AS
SELECT base_origen||'-'||tipo_capacitacion||'-'||CAST(id AS VARCHAR) id,
	   id id_new,
       base_origen,
       tipo_capacitacion,
       descrip_normalizada,
       fecha_inicio,
       fecha_fin,
       categoria,
       estado
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_crmsl_capacitaciones"
UNION ALL
SELECT base_origen||'-'||tipo_capacitacion||'-'||CAST(id AS VARCHAR),
	   id,
       base_origen,
       tipo_capacitacion,
       descrip_normalizada,
       fecha_inicio,
       fecha_fin,
       categoria,
       estado
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_sienfo_capacitaciones"
UNION ALL
SELECT base_origen||'-'||tipo_capacitacion||'-'||CAST(id AS VARCHAR),
	   id,
       base_origen,
       tipo_capacitacion,
       descrip_normalizada,
       fecha_inicio,
       fecha_fin,
       categoria,
       estado
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_goet_capacitaciones"
UNION ALL
SELECT base_origen||'-'||tipo_capacitacion||'-'||CAST(id AS VARCHAR),
	   id,
       base_origen,
       tipo_capacitacion,
       descrip_normalizada,
       fecha_inicio,
       fecha_fin,
       categoria,
       estado
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_moodle_capacitaciones"
UNION ALL
SELECT base_origen||'-'||tipo_capacitacion||'-'||CAST(id AS VARCHAR),
	   id,
       base_origen,
       tipo_capacitacion,
       descrip_normalizada,
       fecha_inicio,
       fecha_fin,
       categoria,
       estado
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_siu_capacitaciones"


CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_def_capacitacion_match" AS
SELECT base_origen,
       tipo_capacitacion,
	   id_new,
       id_old
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_crmsl_match"
UNION ALL
SELECT base_origen,
       tipo_capacitacion,
	   id_new,
       id_old
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_sienfo_match"
UNION ALL
SELECT base_origen,
       tipo_capacitacion,
	   id_new,
       id_old
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_goet_match"
UNION ALL
SELECT base_origen,
       tipo_capacitacion,
	   id_new,
       id_old
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_moodle_match"
UNION ALL
SELECT base_origen,
       tipo_capacitacion,
	   id_new,
       id_old
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_siu_match"