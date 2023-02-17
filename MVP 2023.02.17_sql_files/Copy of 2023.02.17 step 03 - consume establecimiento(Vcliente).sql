-- ESTABLECIMIENTO GOET, MOODLE, SIENFO, CRMSL Y SIU
-- 1.- Crear tabla establecimientos definitiva
-- DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_def_establecimientos`;
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_def_establecimientos" AS
SELECT 
	row_number() OVER () AS id,
	tmp.*
FROM (
	SELECT 
		base_origen,
		codigo id_old,
		cue,
		nombre_formateado nombre,
		nombre_data nombre_publicado,
		array_join(array_agg(DISTINCT(nombre_old)), ',') nombres_old,
		tipo,
		calle,
		numero,
		'CABA' localidad,
		codigo_postal,
		-- se elimina la palabra "comuna"
		-- luego se convierte a un numero entero y si es 0 se lo cambia por null
		CASE 
		WHEN TRY_CAST(replace(comuna, 'Comuna ') AS int) = 0 THEN
			NULL
		ELSE 
			TRY_CAST(replace(comuna, 'Comuna ') AS int)
		END comuna
	FROM 
		(SELECT * FROM 
		"caba-piba-staging-zone-db"."tbp_typ_tmp_establecimientos"
		WHERE orden_duplicado = 1) sin_duplicados
	GROUP BY 1,2,3,4,5,7,8,9,10,11,12
	) tmp
ORDER BY tmp.base_origen, tmp.nombre