-- Crear tabla def de puestos

--<sql>--
DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_def_puestos`;
--</sql>--

--<sql>--
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_def_puestos" AS
 SELECT ciuo_88_code as puesto_id,
 	ciuo_88_titulo_sp as descripcion
 FROM "caba-piba-raw-zone-db"."tbp_typ_tmp_match_nomenclador_puestos_ciuo_08_ciuo_88"
UNION
SELECT  codigo,
 	descripcion 
FROM "caba-piba-raw-zone-db"."tbp_typ_tmp_puesto_desempeniado_afip"
-- se agrega el codigo 3139 porque esta en la OIT pero no en el archivo ingestado
WHERE codigo='3139'
--</sql>--	

--<sql>--
DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_def_puestos_con_niveles`;
--</sql>--

--<sql>--	
 CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_def_puestos_con_niveles" AS
 WITH titulos_1_digito AS (
 	SELECT ciuo_08_code,
 		ciuo_08_titulo_sp
 	FROM "caba-piba-raw-zone-db"."tbp_typ_tmp_nomenclador_puestos_ciuo_08"
 	WHERE LENGTH(CAST(ciuo_08_code AS VARCHAR)) = 1
 ),
 titulos_2_digito AS (
 	SELECT ciuo_08_code,
 		ciuo_08_titulo_sp
 	FROM "caba-piba-raw-zone-db"."tbp_typ_tmp_nomenclador_puestos_ciuo_08"
 	WHERE LENGTH(CAST(ciuo_08_code AS VARCHAR)) = 2
 ),
 titulos_3_digito AS (
 	SELECT ciuo_08_code,
 		ciuo_08_titulo_sp
 	FROM "caba-piba-raw-zone-db"."tbp_typ_tmp_nomenclador_puestos_ciuo_08"
 	WHERE LENGTH(CAST(ciuo_08_code AS VARCHAR)) = 3
 ),
 nomenclador AS (
  SELECT 
	ciuo_88_code AS puesto_id,
	ciuo_88_code AS ciuo_08_code,
 	ciuo_88_titulo_sp AS ciuo_08_titulo_sp
 FROM "caba-piba-raw-zone-db"."tbp_typ_tmp_match_nomenclador_puestos_ciuo_08_ciuo_88"
UNION
SELECT  
	codigo,
 	codigo AS ciuo_08_code,
	descripcion AS ciuo_08_titulo_sp
FROM "caba-piba-raw-zone-db"."tbp_typ_tmp_puesto_desempeniado_afip"
-- se agrega el codigo 3139 porque esta en la OIT pero no en el archivo ingestado
WHERE codigo='3139'
 )
 SELECT m.puesto_id,
 	m.ciuo_08_code,
 	m.ciuo_08_titulo_sp,
 	t1.ciuo_08_code ciuo_08_1_digitos_code,
 	t1.ciuo_08_titulo_sp ciuo_08_1_digitos_titulo_sp,
 	t2.ciuo_08_code ciuo_08_2_digitos_code,
 	t2.ciuo_08_titulo_sp ciuo_08_2_digitos_titulo_sp,
 	t3.ciuo_08_code ciuo_08_3_digitos_code,
 	t3.ciuo_08_titulo_sp ciuo_08_3_digitos_titulo_sp
 FROM nomenclador m
 LEFT JOIN titulos_1_digito t1 ON (CAST(t1.ciuo_08_code AS VARCHAR)=substr(m.ciuo_08_code,1,1))
 LEFT JOIN titulos_2_digito t2 ON (CAST(t2.ciuo_08_code AS VARCHAR)=substr(m.ciuo_08_code,1,2))
 LEFT JOIN titulos_3_digito t3 ON (CAST(t3.ciuo_08_code AS VARCHAR)=substr(m.ciuo_08_code,1,3))
 WHERE m.puesto_id IS NOT NULL AND m.puesto_id != ''
 --</sql>--