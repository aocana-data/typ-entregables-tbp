-- DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_def_programa`;

-- 19 programas
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_def_programa" AS
SELECT p.id programa_id,
       p.ministerio_id ministerio_id,
       p.reparticion_id reparticion_id,
       p.codigo codigo_programa,
       p.nombre nombre_programa,
       p.tipo_programa,
	   CASE WHEN UPPER(p.trayectoria_educativa_bbdd) IN ('SIU','MOODLE','SIENFO','GOET') THEN UPPER(p.trayectoria_educativa_bbdd)
			WHEN UPPER(p.trayectoria_educativa_bbdd) LIKE 'CRM SOCIOLABORAL%' THEN 'CRMSL'
			ELSE UPPER(p.trayectoria_educativa_bbdd) END base_origen,
	   CASE WHEN p.integrable = 1 THEN 'S' ELSE 'N' END integrable,
	   p.duracion_estimada,
	   p.fecha_inscripcion,
	   CASE WHEN p.activo = 1 THEN 'S' ELSE 'N' END activo,
       m.nombre nombre_ministerio,
       r.nombre nombre_reparticion
FROM "caba-piba-raw-zone-db"."api_asi_programa" p
INNER JOIN "caba-piba-raw-zone-db"."api_asi_ministerio" m ON (m.id = p.ministerio_id)
LEFT JOIN "caba-piba-raw-zone-db"."api_asi_reparticion" r ON (p.ministerio_id = r.ministerio_id AND p.reparticion_id = r.id)
