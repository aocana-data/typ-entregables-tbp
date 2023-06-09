-- 1.-- Crear tabla def de organizaciones
--<sql>--
DROP TABLE IF EXISTS `caba-piba-staging-zone-db`.`tbp_typ_def_organizaciones`;
--</sql>--

--<sql>--
CREATE TABLE "caba-piba-staging-zone-db"."tbp_typ_def_organizaciones" AS
SELECT
row_number() OVER () AS id_organizacion,
cuit,
razon_social_old,
razon_social_new,
estado,
ente_gubernamental
FROM "caba-piba-staging-zone-db"."tbp_typ_tmp_organizaciones"
--</sql>--