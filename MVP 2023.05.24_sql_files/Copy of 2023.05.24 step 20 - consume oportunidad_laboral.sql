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

-- --<sql>--DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_def_oportunidad_laboral`;--</sql>--
--<sql>--
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_def_oportunidad_laboral" AS
SELECT
	row_number() OVER () AS oportunidad_laboral_id,
	base_origen,
	id oportunidad_laboral_id_old,
	descripcion,
	fecha_publicacion,
	programa,
	estado,
	apto_discapacitado,
	vacantes,
	modalidad_de_trabajo,
	edad_minima,
	edad_maxima,
	vacantes_cubiertas,
	tipo_de_puesto,
	turno_trabajo,
	grado_de_estudio,
	duracion_practica_formativa,
	sector_productivo
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_oportunidad_laboral" 
GROUP BY 
	base_origen,
	id,
	descripcion,
	fecha_publicacion,
	programa,
	estado,
	apto_discapacitado,
	vacantes,
	modalidad_de_trabajo,
	edad_minima,
	edad_maxima,
	vacantes_cubiertas,
	tipo_de_puesto,
	turno_trabajo,
	grado_de_estudio,
	duracion_practica_formativa,
	sector_productivo
--</sql>--