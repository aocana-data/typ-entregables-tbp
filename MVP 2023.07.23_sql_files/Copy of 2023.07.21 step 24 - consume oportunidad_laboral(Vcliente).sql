-- 1.-- Crear la tabla def OPORTUNIDAD_LABORAL CRMEMPLEO, CRMSL, PORTALEMPLEO, 
-- CAMPOS REQUERIDOS EN TABLA DEF SEGUN MODELO (Oferta Laboral, Prácticas Formativas (Pasantías)):
-- Código (1+)
-- Descripción
-- Estado => ABIERTO, CANCELADO, CERRADO
-- Apto discapacitados => S, N
-- Vacantes
-- Modalidad de Trabajo => RELACION DE DEPENDENCIA, CONTRATIO, PASANTIA, AD HONOREM
-- Edad Mínima
-- Edad Máxima
-- Vacantes Cubiertas
-- Tipo de Puesto
-- Turno de Trabajo => MAÑANA, MAÑANA-TARDE, MAÑANA-TARDE-NOCHE, TARDE, TARDE-NOCHE, NOCHE
-- Grado de Estudio => SECUNDARIO, TERCIARIO, UNIVERSITARIO, OTROS
-- Duracion Practica formativa
-- Sector Productivo => ABASTECIMIENTO Y LOGISTICA, ADMINISTRACION, CONTABILIDAD Y FINANZAS,ATENCION AL CLIENTE, CALL CENTER Y TELEMARKETING,ADUANA Y COMERCIO EXTERIOR, COMERCIAL, VENTAS Y NEGOCIOS, GASTRONOMIA, HOTELERIA Y TURISMO, INGENIERIAS , LIMPIEZA Y MANTENIMIENTO (SIN EDIFICIOS), OFICIOS Y OTROS, PRODUCCION Y MANUFACTURA (SIN TEXTIL, ELECTRONICA Y AUTOMOTRIZ), SALUD, MEDICINA, FARMACIA Y ASISTENCIA SOCIAL, SECTOR PUBLICO,MINERIA, ENERGIA, PETROLEO, AGUA Y GAS
-- Nota: la tabla deberá estar relacionada con la entidad "Registro laboral formal" si tomo el empleo 
-- y con la entidad "Programa"

--<sql>--
DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_def_oportunidad_laboral`;
--</sql>--

--<sql>--
DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_def_oportunidad_laboral_idiomas`;
--</sql>--

--<sql>--
DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_def_oportunidad_laboral_conocimientos`;
--</sql>--

--<sql>--
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_def_oportunidad_laboral" AS
WITH 
empresas_validas AS (
	SELECT UPPER(organizacion_empleadora) AS empresa,
		ARRAY_JOIN(ARRAY_SORT(SPLIT(UPPER(organizacion_empleadora), ' ')), ' ') as empresa_ordenada,
		length(organizacion_empleadora) longitud,
		id AS id_old,
		base_origen,
		organizacion_empleadora_cuit
	FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_oportunidad_laboral"
	GROUP BY organizacion_empleadora, id, base_origen, organizacion_empleadora_cuit
),
organizaciones AS (
	SELECT id_organizacion, cuit,
		UPPER(COALESCE(razon_social_new, razon_social_old)) AS razon_social,
		ARRAY_JOIN(ARRAY_SORT(SPLIT(UPPER(COALESCE(razon_social_new, razon_social_old)), ' ')), ' ') as razon_social_ordenada,
		length(COALESCE(razon_social_new, razon_social_old)) longitud
	FROM "caba-piba-staging-zone-db"."tbp_typ_def_organizaciones"
),
ol_organizaciones AS ( 
SELECT 
	el.empresa,
	el.id_old,
	el.base_origen, 
	org.cuit,
	org.razon_social,
	org.id_organizacion,
	1 AS "orden_duplicado"
FROM empresas_validas el
	JOIN organizaciones org ON (
		org.cuit=el.organizacion_empleadora_cuit
	)
UNION
SELECT 
	el.empresa,
	el.id_old,
	el.base_origen, 
	org.cuit,
	org.razon_social,
	org.id_organizacion,
	ROW_NUMBER() OVER(
		PARTITION BY
		el.id_old
		ORDER BY el.organizacion_empleadora_cuit, (
				(
					CAST(greatest(el.longitud, org.longitud) AS DOUBLE) - CAST(
						levenshtein_distance(el.empresa_ordenada, org.razon_social_ordenada) AS DOUBLE
					)
				) / CAST(greatest(el.longitud, org.longitud) AS DOUBLE)
			) desc
	) AS "orden_duplicado"
FROM empresas_validas el
	JOIN organizaciones org ON (
		(
			(
				CAST(greatest(el.longitud, org.longitud) AS DOUBLE) - CAST(
					levenshtein_distance(el.empresa, org.razon_social) AS DOUBLE
				)
			) / CAST(greatest(el.longitud, org.longitud) AS DOUBLE)
		) >= 0.99
		OR
		(
			(
				CAST(greatest(el.longitud, org.longitud) AS DOUBLE) - CAST(
					levenshtein_distance(el.empresa_ordenada, org.razon_social_ordenada) AS DOUBLE
				)
			) / CAST(greatest(el.longitud, org.longitud) AS DOUBLE)
		) >= 0.99
	) LEFT JOIN organizaciones on (el.organizacion_empleadora_cuit = organizaciones.cuit)
	WHERE organizaciones.cuit IS NULL

)
SELECT
	row_number() OVER () AS id_oportunidad_laboral,
	ol.base_origen,
	ol.id id_old,
	ol.descripcion,
	ol.fecha_publicacion,
	p.id_programa,
	ol.programa,
	UPPER(ol.estado) estado,
	ol.apto_discapacitado,
	ol.vacantes,
	ol.modalidad_de_trabajo,
	ol.edad_minima,
	ol.edad_maxima,
	ol.genero_requerido,
	ol.experiencia_requerida,
	ol.grado_de_estudio,
	ol.vacantes_cubiertas,
	ol.tipo_de_puesto,
	ol.turno_trabajo,
	ol.duracion_practica_formativa,
	ol.sector_productivo,
	org.razon_social as organizacion,
	org.id_organizacion,
	ol.informatica,
	ol.internet,
	ol.conocimiento_especifico_requerido
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_oportunidad_laboral" ol
LEFT JOIN ol_organizaciones org ON (ol.id=org.id_old AND ol.base_origen=org.base_origen AND org.orden_duplicado=1)
JOIN "caba-piba-staging-zone-db"."tbp_typ_def_programa" p ON (UPPER(p.nombre_programa)=ol.programa)
GROUP BY
	ol.base_origen,
	ol.id,
	ol.descripcion,
	ol.fecha_publicacion,
	p.id_programa,
	ol.programa,
	UPPER(ol.estado),
	ol.apto_discapacitado,
	ol.vacantes,
	ol.modalidad_de_trabajo,
	ol.edad_minima,
	ol.edad_maxima,
	ol.genero_requerido,
	ol.experiencia_requerida,
	ol.grado_de_estudio,
	ol.vacantes_cubiertas,
	ol.tipo_de_puesto,
	ol.turno_trabajo,
	ol.duracion_practica_formativa,
	ol.sector_productivo,
	org.razon_social,
	org.id_organizacion,
	ol.informatica,
	ol.internet,
	ol.conocimiento_especifico_requerido
--</sql>--

--<sql>--
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_def_oportunidad_laboral_idiomas" AS
WITH 
oportunidad_laboral_idiomas AS (
	SELECT
		row_number() OVER () AS id_oportunidad_laboral_idiomas,
		ol.id_oportunidad_laboral,
		oli.idioma_requerido
	FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_oportunidad_laboral_idiomas" oli
	JOIN "caba-piba-staging-zone-db"."tbp_typ_def_oportunidad_laboral" ol ON (ol.id_old=oli.oportunidad_laboral_id AND ol.base_origen = oli.base_origen)
	)
SELECT *
FROM oportunidad_laboral_idiomas
--</sql>--

--<sql>--
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_def_oportunidad_laboral_conocimientos" AS
WITH 
oportunidad_laboral_conocimientos AS (
	SELECT
		row_number() OVER () AS id_oportunidad_laboral_conocimientos,
		--olc.informatica_conocimiento_id,
		--ol.id_oportunidad_laboral,
		--olc.informatica_id,
		--olc.conocimiento_id,
		olc.tipo_conocimiento,
		olc.descripcion_conocimiento
	FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_oportunidad_laboral_conocimientos" olc
	JOIN "caba-piba-staging-zone-db"."tbp_typ_def_oportunidad_laboral" ol ON (ol.id_old=olc.oportunidad_laboral_id AND ol.base_origen = olc.base_origen)
	)
SELECT *
FROM oportunidad_laboral_conocimientos
--</sql>--