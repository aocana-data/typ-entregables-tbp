-- ORIGEN DE tbp_typ_tmp_actividades_afip => https: / / serviciosweb.afip.gob.ar / genericos / nomencladorActividades / index.aspx - - ORIGEN DE tbp_typ_tmp_actividades_naiib_agip_res_13_2019 => https: / / www.agip.gob.ar / filemanager / source / Normativas / 2019 / 20190201 - Resol13 - AGIP -19 - Anexo.pdf -- se realizan chequeos de que esta en ambas tablas y que esta solo de un lado o del otro
SELECT count(1)
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_actividades_afip" a
	left JOIN "caba-piba-staging-zone-db"."tbp_typ_tmp_actividades_naiib_agip_res_13_2019" r on try_cast(a.cod_actividad_f883 as integer) = try_cast(r.actividad_naes as integer)
where r.actividad_naes is null
limit 100;
-- 180 registros que estan en aFip pero no en aGip
SELECT count(1)
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_actividades_afip" a
	right JOIN "caba-piba-staging-zone-db"."tbp_typ_tmp_actividades_naiib_agip_res_13_2019" r on try_cast(a.cod_actividad_f883 as integer) = try_cast(r.actividad_naes as integer)
where a.cod_actividad_f883 is null
limit 100;
-- 64 registros que estan en aGip pero no en aFip
-- se crea una tabla con un full join entre las dos mediante el campo en comun 
create table "caba-piba-staging-zone-db"."tbp_typ_tmp_actividades_afip_2" as
SELECT r.*,
	a.*
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_actividades_afip" a
	full JOIN "caba-piba-staging-zone-db"."tbp_typ_tmp_actividades_naiib_agip_res_13_2019" r on try_cast(a.cod_actividad_f883 as integer) = try_cast(r.actividad_naes as integer) -- se crea la tabla final de nomenclador de actividades economicas de afip actualizada (NO contiene codigos clanae)
	CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_nomenclador_actividades_economicas" as
SELECT actividad_naecba AS cod_actividad_agip_o_naecba,
	descripcion_naecba AS desc_actividad_agip_o_naecba,
	cod_actividad_f883 AS cod_actividad_afip_o_naes,
	desc_actividad_f883 AS desc_actividad_afip_o_naes,
	desc_larga_actividad_f883 AS desc_larga_actividad_afip_o_naes
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_actividades_afip_2"
GROUP BY 1,	2,	3,	4,	5