-- 1.-- Se crea tabla tbp_typ_tmp_establecimientos_1 desde los origenes GOET, MOODLE, SIENFO, CRMSL Y SIU
-- DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_tmp_establecimientos_1`;
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_establecimientos_1" AS
SELECT 
	'SIU' base_origen,
	CAST(est.ubicacion AS VARCHAR) codigo,
	CAST('' AS VARCHAR) cue,
	CAST(upper(est.nombre) AS VARCHAR) nombre_formateado,
	CAST(est.nombre AS VARCHAR) nombre_old,
	CAST('' AS VARCHAR) clave_tipo,
	CAST('' AS VARCHAR) tipo,
	CAST(est.calle AS VARCHAR) calle,
	CAST(est.numero AS VARCHAR) numero,
	CAST(est.localidad AS VARCHAR) clave_localidad,
	CAST(loc.nombre AS VARCHAR) localidad,
	CAST(est.codigo_postal AS VARCHAR) codigo_postal,
	CAST('' AS VARCHAR) comuna
FROM "caba-piba-raw-zone-db"."siu_toba_3_3_negocio_sga_ubicaciones" est 
LEFT JOIN "caba-piba-raw-zone-db"."siu_toba_3_3_negocio_mug_localidades" loc ON (est.localidad=loc.localidad)
UNION
-- la tabla que existe de centros y sedes NO tienen datos, solo ids
SELECT 
    'GOET' base_origen,
    CAST(c.idcentro AS VARCHAR) codigo,	
	CAST(c.cueanexo AS VARCHAR) cue,
	CAST(UPPER(c.nombre) AS VARCHAR) nombre_formateado,
	CAST(c.nombre AS VARCHAR) nombre_old,
	CAST(c.idcentrotipo AS VARCHAR) clave_tipo,
	CAST(ct.detalle AS VARCHAR) tipo,
	-- separar nombre de calle de numero, normalizar
	CAST(c.direccion AS VARCHAR) calle,
	-- separar nombre de calle de numero, normalizar
	CAST(c.direccion AS VARCHAR) numero,
	CAST(c.idlocalidad AS VARCHAR) clave_localidad,
	CAST(l.detalle AS VARCHAR) localidad,
	CAST(l.cdpostal AS VARCHAR) codigo_postal,
	CAST(c.comuna AS VARCHAR)  comuna
FROM "caba-piba-raw-zone-db"."goet_centro_formacion" c 
LEFT JOIN "caba-piba-raw-zone-db"."goet_localidades" l ON (c.idlocalidad=l.idlocalidad) 
LEFT JOIN "caba-piba-raw-zone-db"."goet_centro_tipo" ct ON (c.idcentrotipo=ct.idtipocentro)
UNION
SELECT
	'SIENFO' base_origen,
    CAST(c.id_centro AS VARCHAR) codigo,	
	CAST(c.cue_anexo AS VARCHAR) cue,
	CAST(UPPER(c.nom_centro) AS VARCHAR) nombre_formateado,
	CAST(c.nom_centro AS VARCHAR) nombre_old,
	CAST(c.tipo_centro AS VARCHAR) clave_tipo,
	-- verificar si hay tablas de tipos
	CAST(tc.denominacion AS VARCHAR) tipo,
	-- separar nombre de calle de numero, normalizar
	CAST(c.direccion AS VARCHAR) calle,
	-- separar nombre de calle de numero, normalizar
	CAST(c.direccion AS VARCHAR) numero,
	CAST('' AS VARCHAR) clave_localidad,
	CAST('' AS VARCHAR) localidad,
	CAST('' AS VARCHAR) codigo_postal,
	CAST('' AS VARCHAR) comuna
FROM "caba-piba-raw-zone-db"."sienfo_centros" c
LEFT JOIN "caba-piba-raw-zone-db"."sienfo_tcentro" tc ON (c.tipo_centro=tc.tipo_centro)
LEFT JOIN "caba-piba-raw-zone-db"."sienfo_cgps" cgp ON (c.id_cgp=cgp.id_cgp)
LEFT JOIN "caba-piba-raw-zone-db"."sienfo_distritos" d ON (c.id_distrito=d.id_distrito)
UNION	
SELECT
	'CRMSL' base_origen,
	CASE 
	WHEN sede LIKE '21-24- CEMAR' THEN cast(crc32(cast('CEMAR 21 24' as varbinary)) as varchar)
	WHEN sede LIKE 'Bariro Padre Mugica (Ex 31)' THEN cast(crc32(cast(UPPER('Barrio Padre Mugica (Ex 31)') as varbinary )) as varchar)
	WHEN sede LIKE 'Mugica' THEN cast(crc32(cast(UPPER('Barrio Padre Mugica (Ex 31)') as varbinary )) as varchar)
	WHEN sede LIKE 'Barrio 15' THEN cast(crc32(cast(UPPER('Villa Lugano - Barrio 15') as varbinary )) as varchar)
	WHEN sede LIKE 'Fraga' THEN cast(crc32(cast(UPPER('Barrio Fraga') as varbinary )) as varchar)
	WHEN sede LIKE 'Villa Lugano- Barrio 15' THEN cast(crc32(cast(UPPER('Villa Lugano - Barrio 15') as varbinary )) as varchar)
	WHEN sede LIKE 'Rodrigo Bueno' THEN cast(crc32(cast(UPPER('Barrio Rodrigo Bueno') as varbinary )) as varchar)
	WHEN sede LIKE 'Club Copelo' THEN cast(crc32(cast(UPPER('Club Copello') as varbinary )) as varchar)
	WHEN sede LIKE 'Ricchiardelli' THEN cast(crc32(cast(UPPER('Ricciardelli') as varbinary )) as varchar)
	ELSE cast(crc32(cast(UPPER(sede) as varbinary))  as varchar)
	END codigo,
	
	CAST('' AS VARCHAR) cue,
	
	CASE 
	WHEN sede LIKE '21-24- CEMAR' THEN 'CEMAR 21 24'
	WHEN sede LIKE 'Bariro Padre Mugica (Ex 31)' THEN UPPER('Barrio Padre Mugica (Ex 31)')
	WHEN sede LIKE 'Mugica' THEN UPPER('Barrio Padre Mugica (Ex 31)')
	WHEN sede LIKE 'Barrio 15' THEN UPPER('Villa Lugano - Barrio 15')
	WHEN sede LIKE 'Fraga' THEN UPPER('Barrio Fraga')
	WHEN sede LIKE 'Villa Lugano- Barrio 15' THEN UPPER('Villa Lugano - Barrio 15')
	WHEN sede LIKE 'Rodrigo Bueno' THEN UPPER('Barrio Rodrigo Bueno')
	WHEN sede LIKE 'Club Copelo' THEN UPPER('Club Copello')
	WHEN sede LIKE 'Ricchiardelli' THEN UPPER('Ricciardelli')
	ELSE UPPER(sede)
	END nombre_formateado,
	
	CAST(sede AS VARCHAR) nombre_old,
	
	CAST('' AS VARCHAR) clave_tipo,
	CAST('' AS VARCHAR) tipo,
	CAST('' AS VARCHAR) calle,
	CAST('' AS VARCHAR) numero,
	CAST('' AS VARCHAR) clave_localidad,
	CAST('' AS VARCHAR) localidad,
	CAST('' AS VARCHAR) codigo_postal,
	
	CASE 
	WHEN sede in ('Bariro Padre Mugica (Ex 31)', 'Mugica', 'Barrio Rodrigo Bueno', 
				'Rodrigo Bueno', 'Sum Rivadavia', 'Virtual')  THEN 'Comuna 1'
	WHEN sede in ('21-24- CEMAR', 'CEMAR 21 24', 'Comedor Los Pochitos', 
				'La boca', 'MDHH', 'Nido B 20', 'Zavaleta')  THEN 'Comuna 4'
	WHEN sede in ('Barrio Charrua', 'Barrio Rivadavia II', 'Ricchiardelli', 
				'Ricciardelli')  THEN 'Comuna 7'
	WHEN sede in ('Barrio 15', 'Villa Lugano- Barrio 15', 'Barrio 20', 'Club Copelo', 
				'Club Copello', 'Nido Soldati', 'NIDO Soldati', 'Piedrabuena',
				'Sabina Olmos', 'Soldati')  THEN 'Comuna 8'		
	WHEN sede in ('Cildañez', 'Lacarra', 'Mataderos')  THEN 'Comuna 9'				
	WHEN sede in ('Barrio Saavedra')  THEN 'Comuna 12'
	WHEN sede in ('Barrio Fraga', 'Fraga')  THEN 'Comuna 15'
	END comuna
		
FROM "caba-piba-raw-zone-db"."crm_sociolaboral_op_oportunidades_formacion"
WHERE sede IS NOT NULL AND TRIM(sede) NOT LIKE ''
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13
-- Segun GCBA Moodle tiene como establecimiento el programa Codo a Codo dependiente del Ministerio de Educación
-- https://buenosaires.gob.ar/educacion/codocodo/el-programa
UNION	
SELECT
'MOODLE' base_origen,
'PROGRAMACODOACODO' codigo,	
'202010' cue,
'PROGRAMA CODO A CODO' nombre_formateado,
'PROGRAMA CODO A CODO' nombre_old,
'3' clave_tipo,
'Otros' tipo,
'Carlos H. Perette y Calle 10' calle,
CAST('' AS VARCHAR) numero,
'2104001' clave_localidad,
'Ciudad de Buenos Aires' localidad,
'1107' codigo_postal,
'Comuna 1' comuna

-- 2.-- Se crea tabla con domicilios estandarizados
-- DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.tbp_typ_tmp_establecimientos_domicilios_estandarizados;
CREATE TABLE "caba-piba-staging-zone-db".tbp_typ_tmp_establecimientos_domicilios_estandarizados AS
WITH ECN1 AS (
SELECT
base_origen,
codigo,
cue,
nombre_formateado,
nombre_old,
tipo,
clave_tipo,
clave_localidad,
calle AS nombre_calle_origen,
CASE
     --Patrón: Campos que contienen la leyenda 'calle'
    WHEN regexp_like(calle,'y calle|y Calle') THEN UPPER(CONCAT(regexp_extract(calle,'(y\s)(CALLE|calle|Calle)(\s?\d?\d?\d?)',2),' ',regexp_extract(calle,'(y\s)(CALLE|calle|Calle)(\s?\d?\d?\d?)',3)))
    WHEN regexp_like(calle,'CALLE|calle|Calle') THEN UPPER(CONCAT(regexp_extract(calle,'(CALLE|calle|Calle)(\s?\d?\d?\d?\s?[A-Za-z]+\s?\d?\d?\d?\d?)',1),' ',regexp_extract(calle,'(CALLE|calle|Calle)(\s?\d?\d?\d?\s?[A-Za-z]+\s?\d?\d?\d?\d?)',2))) 
    --Patrón: Campos con solo datos de manzanas
    WHEN regexp_like(calle,'MANZANA|manzana|Manzana') THEN UPPER(CONCAT(regexp_extract(calle,'(MANZANA|manzana|Manzana)(\s[0-9]+)',1),' ',regexp_extract(calle,'(MANZANA|manzana|Manzana)(\s[0-9]+)',2)))
    --Patrón:Campos con numero + nombre (ejemplo: 24 de noviembre)
    WHEN regexp_like(calle,'[0-9]+\s\w\w\s[a-zA-Z]+') THEN UPPER(regexp_extract(calle,'[0-9]+\s\w\w\s[a-zA-Z]+'))
    --Patrón:Campos que hagan referencia a avenidas
    WHEN regexp_like(calle,'AV|AVDA|AVENIDA|Av|Avda|Avenida') THEN UPPER(regexp_extract(replace(replace(replace(calle,'Avda.','Av.'),'Avenida','Av.'),'Avda','Av.'),'(Av.?\s?[a-zA-ZÁáÉéÍíÓóÚúÑñÄäËëÏïÖöÜü.?]+\s?[a-zA-ZÁáÉéÍíÓóÚúÑñÄäËëÏïÖöÜü.?]+\s?[a-zA-ZÁáÉéÍíÓóÚúÑñÄäËëÏïÖöÜü.?]+\s?)',1))
    ELSE UPPER(calle)
END nombre_calle_estandarizada1,
numero AS numero_calle_origen,
localidad,
codigo_postal,
comuna
FROM "caba-piba-staging-zone-db".tbp_typ_tmp_establecimientos_1
),
ECN2 AS (
SELECT
ECN1.base_origen,
ECN1.codigo,
ECN1.cue,
ECN1.nombre_formateado,
ECN1.nombre_old,
ECN1.tipo,
ECN1.clave_tipo,
ECN1.clave_localidad,
ECN1.nombre_calle_origen,
CASE
    --Patrón:Campos que hagan referencia a avenidas
    WHEN regexp_like(UPPER(ECN1.nombre_calle_origen),'[a-zA-ZÁáÉéÍíÓóÚúÑñÄäËëÏïÖöÜü,?.?]+\s?[A-ZÁáÉéÍíÓóÚúÑñÄäËëÏïÖöÜü,?.?]+\s?[a-zA-ZÁáÉéÍíÓóÚúÑñÄäËëÏïÖöÜü,?.?]+\s?\s?AV.\s?[0-9]+') THEN UPPER(CONCAT('AV. ',regexp_extract(replace(replace(replace(UPPER(ECN1.nombre_calle_origen),'AVDA.','AV.'),'AVENIDA','Av.'),'AVDA','Av.'),'([a-zA-ZÁáÉéÍíÓóÚúÑñÄäËëÏïÖöÜü,?.?]+\s?[a-zA-ZÁáÉéÍíÓóÚúÑñÄäËëÏïÖöÜü,?.?]+\s?[a-zA-ZÁáÉéÍíÓóÚúÑñÄäËëÏïÖöÜü,?.?]+\s?)(\s?AV.)(\s?[0-9]+)',1)))
    WHEN regexp_like(ECN1.nombre_calle_origen,'Av.?\s?[a-zA-ZÁáÉéÍíÓóÚúÑñÄäËëÏïÖöÜü.?]+\s?[a-zA-ZÁáÉéÍíÓóÚúÑñÄäËëÏïÖöÜü.?]+\s?[a-zA-ZÁáÉéÍíÓóÚúÑñÄäËëÏïÖöÜü.?]+\s?[a-zA-ZÁáÉéÍíÓóÚúÑñÄäËëÏïÖöÜü.?]+\s?[a-zA-ZÁáÉéÍíÓóÚúÑñÄäËëÏïÖöÜü.?]+\s?[0-9]+') THEN UPPER(regexp_extract(replace(replace(replace(ECN1.nombre_calle_origen,'Avda.','Av.'),'Avenida','Av.'),'Avda','Av.'),'(Av.?\s?[a-zA-ZÁáÉéÍíÓóÚúÑñÄäËëÏïÖöÜü.?]+\s?[a-zA-ZÁáÉéÍíÓóÚúÑñÄäËëÏïÖöÜü.?]+\s?[a-zA-ZÁáÉéÍíÓóÚúÑñÄäËëÏïÖöÜü.?]+\s?[a-zA-ZÁáÉéÍíÓóÚúÑñÄäËëÏïÖöÜü.?]+\s?[a-zA-ZÁáÉéÍíÓóÚúÑñÄäËëÏïÖöÜü.?]+)(\s?[0-9]+)',1))
    WHEN regexp_like(ECN1.nombre_calle_origen,'AV|AVDA|AVENIDA|Av|Avda|Avenida') AND ECN1.nombre_calle_origen LIKE '% y %' THEN UPPER(regexp_extract(replace(replace(replace(ECN1.nombre_calle_origen,'Avda.','Av.'),'Avenida','Av.'),'Avda','Av.'),'Av.?\s?[a-zA-ZÁáÉéÍíÓóÚúÑñÄäËëÏïÖöÜü.?]+\s?[a-zA-ZÁáÉéÍíÓóÚúÑñÄäËëÏïÖöÜü.?]+\s?[a-zA-ZÁáÉéÍíÓóÚúÑñÄäËëÏïÖöÜü.?]+\s?[a-zA-ZÁáÉéÍíÓóÚúÑñÄäËëÏïÖöÜü.?]+'))
    --Patrón: Campos con nombre (con puntos) + numero
    WHEN UPPER(ECN1.nombre_calle_origen) LIKE '%HUMBERTO%' THEN 'HUMBERTO 1°'
    WHEN regexp_like(ECN1.nombre_calle_estandarizada1,'[a-zA-ZÁáÉéÍíÓóÚúÑñÄäËëÏïÖöÜü.?\°?\º?]+\s?[a-zA-ZÁáÉéÍíÓóÚúÑñÄäËëÏïÖöÜü.?°?]+\s?\,?\.?\s?[a-zA-ZÁáÉéÍíÓóÚúÑñÄäËëÏïÖöÜü.?\°?\º?]+\s?\,?\.?\s?[a-zA-ZÁáÉéÍíÓóÚúÑñÄäËëÏïÖöÜü.?\°?\º?]+\s?[0-9]+') THEN UPPER(regexp_extract(replace(replace(replace(ECN1.nombre_calle_estandarizada1,'Avda.','Av.'),'Avenida','Av.'),'Avda','Av.'),'([a-zA-ZÁáÉéÍíÓóÚúÑñÄäËëÏïÖöÜü.?°?]+\s?[a-zA-ZÁáÉéÍíÓóÚúÑñÄäËëÏïÖöÜü.?\°?\º?]+\s?\,?\.?\s?[a-zA-ZÁáÉéÍíÓóÚúÑñÄäËëÏïÖöÜü.?\°?\º?]+\s?\,?\.?\s?[a-zA-ZÁáÉéÍíÓóÚúÑñÄäËëÏïÖöÜü.?\°?\º?]+)(\s?[0-9]+)',1))
    --Patrón: Campos que hagan referencia a una misma direcciones con diferente expresion
    WHEN UPPER(ECN1.nombre_calle_origen) LIKE '%PERETTE%' THEN UPPER(CONCAT('CARLOS H. PERETTE',' ',regexp_extract(UPPER(ECN1.nombre_calle_origen),'(PERETTE)(\s?Y\sCALLE\s?\d?\d?)',2)))
    ELSE ECN1.nombre_calle_estandarizada1 
END nombre_calle_estandarizada2,
ECN1.numero_calle_origen,
ECN1.localidad,
ECN1.codigo_postal,
ECN1.comuna
FROM ECN1
),
ECN3 AS (
SELECT
ECN2.base_origen,
ECN2.codigo,
ECN2.cue,
ECN2.nombre_formateado,
ECN2.nombre_old,
ECN2.tipo,
ECN2.clave_tipo,
ECN2.clave_localidad,
ECN2.nombre_calle_origen,
ECN2.nombre_calle_estandarizada2,
CASE
    --Patrón: Campos que hagan referencia a una misma direcciones con diferente expresion
    WHEN ECN2.nombre_calle_estandarizada2 LIKE '%BEIRÓ%' THEN 'AV. FRANCISCO BEIRÓ'
    WHEN ECN2.nombre_calle_estandarizada2 LIKE '%RIVADAVIA%' THEN 'AV. RIVADAVIA'
    WHEN ECN2.nombre_calle_estandarizada2 LIKE '%SANTA FE%' THEN 'AV. SANTA FE'
    --Ajustes
    WHEN ECN2.nombre_calle_estandarizada2 LIKE '%Nº%' OR ECN2.nombre_calle_estandarizada2 LIKE '%N°%' THEN replace(replace(ECN2.nombre_calle_estandarizada2,'Nº',''),'N°','')
    WHEN UPPER(ECN2.nombre_calle_origen) LIKE '% Y %' AND ECN2.nombre_calle_estandarizada2 LIKE '%MÓDULO%' THEN UPPER(regexp_extract(replace(replace(replace(ECN2.nombre_calle_origen,'Avda.','Av.'),'Avenida','Av.'),'Avda','Av.'),'([a-zA-ZÁáÉéÍíÓóÚúÑñÄäËëÏïÖöÜü.°?\s?]+)(\-[a-zA-ZÁáÉéÍíÓóÚúÑñÄäËëÏïÖöÜü0-9?\,?\s?]+)',1))
    WHEN UPPER(ECN2.nombre_calle_origen) LIKE '% Y %' AND ECN2.nombre_calle_estandarizada2 LIKE '%. MZ%'  THEN UPPER(regexp_extract(replace(replace(replace(ECN2.nombre_calle_origen,'Avda.','Av.'),'Avenida','Av.'),'Avda','Av.'),'([a-zA-ZÁáÉéÍíÓóÚúÑñÄäËëÏïÖöÜü.°?\s?]+)(\.[a-zA-ZÁáÉéÍíÓóÚúÑñÄäËëÏïÖöÜü0-9?\,?\s?]+)',1))
    WHEN ECN2.nombre_calle_estandarizada2 LIKE 'LATITUD' THEN 'S/D'
    --Patrón: Campos con nombre (con doble espacio) + numero
    WHEN ECN2.nombre_calle_origen LIKE '%  %' THEN UPPER(regexp_extract(ECN2.nombre_calle_origen,'([a-zA-ZÁáÉéÍíÓóÚúÑñÄäËëÏïÖöÜü.?°?]+\s?[a-zA-ZÁáÉéÍíÓóÚúÑñÄäËëÏïÖöÜü.?\°?\º?]+)(\s?\s?[0-9]+)',1))
    ELSE ECN2.nombre_calle_estandarizada2
END nombre_calle_estandarizada3,
ECN2.numero_calle_origen,
ECN2.localidad,
ECN2.codigo_postal,
ECN2.comuna
FROM ECN2
),
-- Se realizan ajustes finales a la estandarización de nombres de calles y se estandarizan los numeros de calles
ECN4 AS (
SELECT
ECN3.base_origen,
ECN3.codigo,
ECN3.cue,
ECN3.nombre_formateado,
ECN3.nombre_old,
ECN3.tipo,
ECN3.clave_tipo,
ECN3.clave_localidad,
ECN3.nombre_calle_origen,
CASE
    --Patrón: Solo numeros
    WHEN regexp_like(ECN3.nombre_calle_estandarizada3,'[^A-ZÁáÉéÍíÓóÚúÑñÄäËëÏïÖöÜü.?\°?\º?\s?\,?]+') AND ECN3.nombre_calle_estandarizada3 NOT LIKE '%MANZANA%' AND ECN3.nombre_calle_estandarizada3 NOT LIKE '%HUMBERTO%' AND ECN3.nombre_calle_estandarizada3 NOT LIKE '%DE%' AND ECN3.nombre_calle_estandarizada3 NOT LIKE '%CALLE%' AND ECN3.nombre_calle_estandarizada3 NOT LIKE '%S/D%' THEN 'S/D'
    WHEN LENGTH(ECN3.nombre_calle_estandarizada3) = 0 OR ECN3.nombre_calle_estandarizada3 IS NULL THEN 'S/D'
    --Ajuste final
    WHEN ECN3.nombre_calle_estandarizada3 LIKE '%  %' THEN  replace(ECN3.nombre_calle_estandarizada3,'  ',' ')
    ELSE ECN3.nombre_calle_estandarizada3
END nombre_calle_estandarizada,
ECN3.numero_calle_origen,
CASE
    --Patrón: Campos que contengan mas de 1 altura (ejemplo 1234/5678)
    WHEN ECN3.numero_calle_origen LIKE '%/%' AND ECN3.numero_calle_origen NOT LIKE 'S/D' THEN regexp_extract(ECN3.numero_calle_origen,'([a-zA-ZÁáÉéÍíÓóÚúÑñÄäËëÏïÖöÜü\°?\º?\s?\,?.?]+)([0-9]+)(\/\s?[0-9]+)',2)
    --Patrón: Campos que contengan la leyenda "CASA" o "N°"
    WHEN regexp_like(UPPER(ECN3.numero_calle_origen),'CASA\s?[0-9]+') THEN regexp_extract(UPPER(ECN3.numero_calle_origen),'(CASA)(\s?[0-9]+)',2)
    WHEN regexp_like(UPPER(ECN3.numero_calle_origen),'C\s?[0-9]+') THEN regexp_extract(UPPER(ECN3.numero_calle_origen),'(C)(\s?[0-9]+)',2)
    WHEN regexp_like(UPPER(ECN3.numero_calle_origen),'N°\s?[0-9]+') THEN regexp_extract(UPPER(ECN3.numero_calle_origen),'(N°)(\s?[0-9]+)',2)
    --Patrón: Campos con solo datos de manzana
    WHEN UPPER(ECN3.numero_calle_origen) LIKE '%MANZANA%' THEN replace(ECN3.nombre_calle_estandarizada3,'  ',' ')
    --Patrón:Campos con numero + nombre (ejemplo: 24 de noviembre)
    WHEN regexp_like(ECN3.numero_calle_origen,'[0-9]+\s\w\w\s[a-zA-Z]+\s?[0-9]+') THEN UPPER(regexp_extract(ECN3.numero_calle_origen,'([0-9]+\s\w\w\s[a-zA-Z]+)(\s?[0-9]+)',2))
    ELSE UPPER(ECN3.numero_calle_origen)
END numero_calle_estandarizada1,
ECN3.localidad,
ECN3.codigo_postal,
ECN3.comuna
FROM ECN3
),
ECN5 AS (
SELECT
ECN4.base_origen,
ECN4.codigo,
ECN4.cue,
ECN4.nombre_formateado,
ECN4.nombre_old,
ECN4.tipo,
ECN4.clave_tipo,
ECN4.clave_localidad,
ECN4.nombre_calle_origen,
ECN4.nombre_calle_estandarizada,
ECN4.numero_calle_origen,
ECN4.numero_calle_estandarizada1,
CASE
    --Patrón:Campos que hagan referencia a avenidas
    WHEN ECN4.numero_calle_estandarizada1 LIKE '%AV. %' OR ECN4.numero_calle_estandarizada1 LIKE '%AVDA. %' THEN UPPER(regexp_extract(ECN4.numero_calle_estandarizada1,'(AV.?\s?[A-ZÁáÉéÍíÓóÚúÑñÄäËëÏïÖöÜü.?]+\s?[A-ZÁáÉéÍíÓóÚúÑñÄäËëÏïÖöÜü.?]+\s?[A-ZÁáÉéÍíÓóÚúÑñÄäËëÏïÖöÜü.?]+\s?[A-ZÁáÉéÍíÓóÚúÑñÄäËëÏïÖöÜü.?]+)(\s?\s?[0-9]+)',2))
    WHEN ECN4.nombre_calle_estandarizada LIKE '%AV. %' THEN UPPER(regexp_extract(UPPER(ECN4.numero_calle_origen),'(AV.?\s?[A-ZÁáÉéÍíÓóÚúÑñÄäËëÏïÖöÜü.?]+\s?[a-zA-ZÁáÉéÍíÓóÚúÑñÄäËëÏïÖöÜü.?]+\s?[A-ZÁáÉéÍíÓóÚúÑñÄäËëÏïÖöÜü.?]+)(\s?[0-9]+)',2))
    ELSE ECN4.numero_calle_estandarizada1
END numero_calle_estandarizada2,
ECN4.localidad,
ECN4.codigo_postal,
ECN4.comuna
FROM ECN4
),
ECN6 AS (
SELECT
ECN5.base_origen,
ECN5.codigo,
ECN5.cue,
ECN5.nombre_formateado,
ECN5.nombre_old,
ECN5.tipo,
ECN5.clave_tipo,
ECN5.clave_localidad,
ECN5.nombre_calle_origen,
ECN5.nombre_calle_estandarizada,
ECN5.numero_calle_origen,
ECN5.numero_calle_estandarizada1,
ECN5.numero_calle_estandarizada2,
CASE
    --Patrón:Campos que hagan referencia a avenidas
    WHEN UPPER(ECN5.nombre_calle_origen) LIKE '%LATITUD%' THEN 'S/D'
    WHEN regexp_like(ECN5.numero_calle_estandarizada1,'[a-zA-ZÁáÉéÍíÓóÚúÑñÄäËëÏïÖöÜü,?.?]+\s?[A-ZÁáÉéÍíÓóÚúÑñÄäËëÏïÖöÜü,?.?]+\s?[a-zA-ZÁáÉéÍíÓóÚúÑñÄäËëÏïÖöÜü,?.?]+\s?\s?AV.\s?[0-9]+') THEN regexp_extract(ECN5.numero_calle_estandarizada1,'([a-zA-ZÁáÉéÍíÓóÚúÑñÄäËëÏïÖöÜü,?.?]+\s?[a-zA-ZÁáÉéÍíÓóÚúÑñÄäËëÏïÖöÜü,?.?]+\s?[a-zA-ZÁáÉéÍíÓóÚúÑñÄäËëÏïÖöÜü,?.?]+\s?)(\s?AV.)(\s?[0-9]+)',3)
    WHEN regexp_like(ECN5.numero_calle_estandarizada1,'AV.?\s?[A-ZÁáÉéÍíÓóÚúÑñÄäËëÏïÖöÜü.?]+\s?[A-ZÁáÉéÍíÓóÚúÑñÄäËëÏïÖöÜü.?]+\s?[A-ZÁáÉéÍíÓóÚúÑñÄäËëÏïÖöÜü.?]+\s?[A-ZÁáÉéÍíÓóÚúÑñÄäËëÏïÖöÜü.?]+\s?[A-ZÁáÉéÍíÓóÚúÑñÄäËëÏïÖöÜü.?]+\s?[0-9]+') THEN regexp_extract(ECN5.numero_calle_estandarizada1,'(AV.?\s?[A-ZÁáÉéÍíÓóÚúÑñÄäËëÏïÖöÜü.?]+\s?[A-ZÁáÉéÍíÓóÚúÑñÄäËëÏïÖöÜü.?]+\s?[A-ZÁáÉéÍíÓóÚúÑñÄäËëÏïÖöÜü.?]+\s?[A-ZÁáÉéÍíÓóÚúÑñÄäËëÏïÖöÜü.?]+\s?[A-ZÁáÉéÍíÓóÚúÑñÄäËëÏïÖöÜü.?]+)(\s?[0-9]+)',2)
    --Patrón: Campos con nombre (con puntos) + numero
    WHEN ECN5.nombre_calle_estandarizada LIKE '%HUMBERTO%' THEN regexp_extract(ECN5.numero_calle_estandarizada2,'([A-ZÁáÉéÍíÓóÚúÑñÄäËëÏïÖöÜü.?]+)(\s?[0-9]+\°?\º?\s?)([0-9]+)',3)
    WHEN regexp_like(ECN5.numero_calle_estandarizada2,'[a-zA-ZÁáÉéÍíÓóÚúÑñÄäËëÏïÖöÜü.?°?]+\s?\s?[a-zA-ZÁáÉéÍíÓóÚúÑñÄäËëÏïÖöÜü.?\°?\º?]+\s?\,?\.?\s?\s?[a-zA-ZÁáÉéÍíÓóÚúÑñÄäËëÏïÖöÜü.?\°?\º?]+\s?\s?[0-9]+') THEN regexp_extract(ECN5.numero_calle_estandarizada2,'([a-zA-ZÁáÉéÍíÓóÚúÑñÄäËëÏïÖöÜü.?°?]+\s?\s?[a-zA-ZÁáÉéÍíÓóÚúÑñÄäËëÏïÖöÜü.?\°?\º?]+\s?\,?\.?\s?\s?[a-zA-ZÁáÉéÍíÓóÚúÑñÄäËëÏïÖöÜü.?\°?\º?]+\s?\s?)([0-9]+)',2)
    WHEN regexp_like(ECN5.numero_calle_estandarizada1,'[^A-ZÁáÉéÍíÓóÚúÑñÄäËëÏïÖöÜü.?\°?\º?\s?\,?]+') AND ECN5.numero_calle_estandarizada1 NOT LIKE '%S/D%' THEN ECN5.numero_calle_estandarizada1
    WHEN LENGTH(ECN5.numero_calle_estandarizada1) > 0 AND ECN5.numero_calle_estandarizada2 IS NULL THEN ECN5.numero_calle_estandarizada1
    ELSE ECN5.numero_calle_estandarizada2
END numero_calle_estandarizada3,
ECN5.localidad,
ECN5.codigo_postal,
ECN5.comuna
FROM ECN5
),
ECN7 AS (
SELECT
ECN6.base_origen,
ECN6.codigo,
ECN6.cue,
ECN6.nombre_formateado,
ECN6.nombre_old,
ECN6.tipo,
ECN6.clave_tipo,
ECN6.clave_localidad,
ECN6.nombre_calle_origen,
ECN6.nombre_calle_estandarizada,
ECN6.numero_calle_origen,
ECN6.numero_calle_estandarizada1,
ECN6.numero_calle_estandarizada2,
ECN6.numero_calle_estandarizada3,
CASE
    --Ajustes adicionales
    WHEN ECN6.numero_calle_estandarizada3 LIKE '%º%' OR ECN6.numero_calle_estandarizada3 LIKE '%°%' THEN regexp_extract(ECN6.numero_calle_estandarizada3,'([0-9]+)(\s?\d?\°?\º?\s?)([A-ZÁáÉéÍíÓóÚúÑñÄäËëÏïÖöÜü.?\s?\,?]+)',1)
    WHEN LENGTH(ECN6.numero_calle_estandarizada1) > 0 AND ECN6.numero_calle_estandarizada3 IS NULL AND ECN6.nombre_calle_estandarizada LIKE '%HUMBERTO%' THEN regexp_extract(ECN6.numero_calle_estandarizada1,'([0-9]+)(\s?\d?\°?\º?\s?)',1)
    WHEN regexp_like(ECN6.numero_calle_estandarizada3,'[^0-9\s?]+') AND ECN6.numero_calle_estandarizada3 NOT LIKE '%S/D%' THEN replace(regexp_replace(ECN6.numero_calle_estandarizada3,'[^0-9\s?]+',''),' ','')
    ELSE replace(ECN6.numero_calle_estandarizada3,' ','')
END numero_calle_estandarizada4,
ECN6.localidad,
ECN6.codigo_postal,
ECN6.comuna
FROM ECN6
),
ECN8 AS (
SELECT
ECN7.base_origen,
ECN7.codigo,
ECN7.cue,
ECN7.nombre_formateado,
ECN7.nombre_old,
ECN7.tipo,
ECN7.clave_tipo,
ECN7.clave_localidad,
ECN7.nombre_calle_origen,
ECN7.nombre_calle_estandarizada,
ECN7.numero_calle_origen,
ECN7.numero_calle_estandarizada1,
ECN7.numero_calle_estandarizada3,
ECN7.numero_calle_estandarizada4,
CASE
    --Ajutse final
    WHEN LENGTH(ECN7.numero_calle_estandarizada4) = 0 OR ECN7.numero_calle_estandarizada4 IS NULL THEN 'S/D'
    ELSE ECN7.numero_calle_estandarizada4
END numero_calle_estandarizada,
ECN7.localidad,
ECN7.codigo_postal,
ECN7.comuna
FROM ECN7
),
ECNF AS (
SELECT
ECN8.base_origen,
ECN8.codigo,
ECN8.cue,
ECN8.nombre_formateado,
ECN8.nombre_old,
ECN8.tipo,
ECN8.clave_tipo,
ECN8.clave_localidad,
ECN8.nombre_calle_origen,
ECN8.numero_calle_origen,
ECN8.nombre_calle_estandarizada,
ECN8.numero_calle_estandarizada,
ECN8.localidad,
ECN8.codigo_postal,
ECN8.comuna
FROM ECN8
GROUP BY
ECN8.base_origen,
ECN8.codigo,
ECN8.cue,
ECN8.nombre_formateado,
ECN8.nombre_old,
ECN8.tipo,
ECN8.clave_tipo,
ECN8.clave_localidad,
ECN8.nombre_calle_origen,
ECN8.numero_calle_origen,
ECN8.nombre_calle_estandarizada,
ECN8.numero_calle_estandarizada,
ECN8.localidad,
ECN8.codigo_postal,
ECN8.comuna
)
SELECT ECNF.*
FROM ECNF;

-- 3.-- Se crea tabla tbp_typ_tmp_establecimientos cruzando tabla de domicilios estandarizados con 
-- informacion proveniente de:
-- # https://www.argentina.gob.ar/educacion/evaluacion-e-informacion-educativa/padron-oficial-de-establecimientos-educativos
-- # https://data.educacion.gob.ar/
-- # https://data.buenosaires.gob.ar/dataset/establecimientos-educativos
-- esta tabla será la ultima tabla tmp antes de crear la tabla consume
-- DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_tmp_establecimientos`;
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_tmp_establecimientos" AS
SELECT e.base_origen,
	e.codigo,
	CASE 
	WHEN te.cue IS NOT NULL AND length(te.cue)=6 THEN te.cue
	WHEN e.cue IS NOT NULL AND length(e.cue)=6 THEN e.cue
	WHEN e.cue IS NOT NULL AND length(e.cue)=8 THEN substr(e.cue,1,6)
	END cue,
	e.nombre_formateado,
	e.nombre_old,
	-- nombre del listado oficial de establecimientos
	te.nombre as nombre_data,
	e.tipo,
	e.clave_tipo,
	e.nombre_calle_estandarizada calle,
	e.numero_calle_estandarizada numero,
	Upper(te."domicilio_establecimiento_fuente_data_bs") dom_completo,
	-- todos los establecimientos son de CABA
	'CABA' localidad,
	e.clave_localidad,
	coalesce(te.cp, e.codigo_postal) codigo_postal,
	coalesce(te.departamento, e.comuna) comuna,
	-- SE GENERA UNA COLUMNA DE ORDEN DE DUPLICADOS, SIENDO EL MAS COINCIDENTE EL QUE TIENE EL NOMBRE DE ESTABLECIMIENTO
	-- MAS PARECIDO Y UN NUMERO DE ANEXO MENOR
	ROW_NUMBER() OVER(
		PARTITION BY(
			Concat(
				e."nombre_calle_estandarizada",
				' ',
				e."numero_calle_estandarizada"
			)
		)
		ORDER BY (
				(
					Cast(
						Greatest(
							Length(e."nombre_old"),
							Length(Upper(te."nombre"))
						) AS DOUBLE
					) - Cast(
						Levenshtein_distance(
							Upper(e."nombre_old"),
							Upper(te."nombre")
						) AS DOUBLE
					)
				) / Cast(
					Greatest(
						Length(e."nombre_old"),
						Length(Upper(te."nombre"))
					) AS DOUBLE
				)
			) DESC,
			CAST(te.anexo AS INT) ASC
	) AS "orden_duplicado"
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_establecimientos_domicilios_estandarizados" e
	-- SE HACE JOIN POR SIMILITUD DE DIRECCIONES >= 80%
	LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_tmp_data_efectores" te ON (
		(
			(
				Cast(
					Greatest(
						Length(
							Concat(
								e."nombre_calle_estandarizada",
								' ',
								e."numero_calle_estandarizada"
							)
						),
						Length(
							Upper(te."domicilio_establecimiento_fuente_data_bs")
						)
					) AS DOUBLE
				) - Cast(
					Levenshtein_distance(
						Upper(
							Concat(
								e."nombre_calle_estandarizada",
								' ',
								e."numero_calle_estandarizada"
							)
						),
						Upper(te."domicilio_establecimiento_fuente_data_bs")
					) AS DOUBLE
				)
			) / Cast(
				Greatest(
					Length(
						Concat(
							e."nombre_calle_estandarizada",
							' ',
							e."numero_calle_estandarizada"
						)
					),
					Length(
						Upper(te."domicilio_establecimiento_fuente_data_bs")
					)
				) AS DOUBLE
			)
		) >= 0.8
	)