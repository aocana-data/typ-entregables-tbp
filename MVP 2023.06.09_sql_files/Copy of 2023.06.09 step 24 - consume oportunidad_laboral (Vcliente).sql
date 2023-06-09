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
	el.organizacion_empleadora_cuit,
	org.razon_social,
	org.id_organizacion,
	ROW_NUMBER() OVER(
		PARTITION BY 
		el.empresa_ordenada,
		org.razon_social_ordenada,
		el.id_old,
		el.base_origen 
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
		OR
			org.cuit=el.organizacion_empleadora_cuit
	)
)
SELECT
	row_number() OVER () AS oportunidad_laboral_id,
	ol.base_origen,
	ol.id oportunidad_laboral_id_old,
	ol.descripcion,
	ol.fecha_publicacion,
	ol.programa,
	ol.estado,
	ol.apto_discapacitado,
	ol.vacantes,
	ol.modalidad_de_trabajo,
	ol.edad_minima,
	ol.edad_maxima,
	ol.genero_requerido,
	ol.experiencia_requerida,
	ol.idioma_requerido,
	ol.grado_de_estudio,
	ol.vacantes_cubiertas,
	ol.tipo_de_puesto,
	ol.turno_trabajo,
	ol.duracion_practica_formativa,
	ol.sector_productivo,
	ol.organizacion_empleadora as organizacion,
	org.id_organizacion
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_oportunidad_laboral" ol
LEFT JOIN ol_organizaciones org ON (ol.id=org.id_old AND ol.base_origen=org.base_origen AND org.orden_duplicado=1)
GROUP BY 
	ol.base_origen,
	ol.id,
	ol.descripcion,
	ol.fecha_publicacion,
	ol.programa,
	ol.estado,
	ol.apto_discapacitado,
	ol.vacantes,
	ol.modalidad_de_trabajo,
	ol.edad_minima,
	ol.edad_maxima,
	ol.genero_requerido,
	ol.experiencia_requerida,
	ol.idioma_requerido,
	ol.grado_de_estudio,
	ol.vacantes_cubiertas,
	ol.tipo_de_puesto,
	ol.turno_trabajo,
	ol.duracion_practica_formativa,
	ol.sector_productivo,
	ol.organizacion_empleadora,
	org.id_organizacion
--</sql>--