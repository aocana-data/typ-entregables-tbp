--El resultado de la consulta es 0 tuplas, por lo cual todas las areas de interes conectadas a los trabajos declarados en los cvs se encuentran ingestadas
SELECT DISTINCT cvs.id, mia_j.name
FROM "caba-piba-raw-zone-db"."portal_empleo_curriculum_vitaes" cvs
LEFT JOIN "caba-piba-raw-zone-db"."portal_empleo_jobs" j
    ON cvs.id = j.curriculum_id
LEFT JOIN "caba-piba-raw-zone-db"."portal_empleo_mtr_interest_areas" mia_j
    ON j.area_id = mia_j.id
WHERE cvs.id IS NOT NULL AND mia_j.name IS NOT NULL
AND CAST(cvs.id AS VARCHAR) || '-' ||mia_j.name NOT IN
(SELECT
    DISTINCT cvs.id_old || '-' || j_mia_name.descripcion
FROM "caba-piba-staging-zone-db"."tbp_typ_def_curriculum" cvs
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_cv_experiencia_laboral" exp
    ON cvs.id_curriculum = exp.id_curriculum
LEFT JOIN "caba-piba-staging-zone-db"."tbp_typ_def_areas_de_interes" j_mia_name
    ON exp.id_areas_de_interes = j_mia_name.id_areas_de_interes
    WHERE cvs.id_old || '-' || j_mia_name.descripcion IS NOT NULL)
-- se agrega esta condicion porque no todos los cvs de la capa pasan a la tabla def ya que se eliminan duplicados
AND CAST(cvs.id AS VARCHAR) IN (SELECT id_old FROM "caba-piba-staging-zone-db"."tbp_typ_def_curriculum")


--El resultado de la consulta es 0 tuplas, por lo cual todas los sectores productivos conectados a los trabajos declarados en los cvs se encuentran ingestados
SELECT 
    *
FROM "caba-piba-raw-zone-db"."portal_empleo_curriculum_vitaes" cvs
LEFT JOIN "caba-piba-raw-zone-db"."portal_empleo_jobs" j 
    ON cvs.id = j.curriculum_id
LEFT JOIN "caba-piba-raw-zone-db"."portal_empleo_mtr_industry_sectors" mis_j
    ON j.industry_id = mis_j.id
WHERE NOT EXISTS (SELECT def_cvs.id_old, j_mis_name.id_origen FROM "caba-piba-staging-zone-db"."tbp_typ_def_curriculum" def_cvs LEFT JOIN
        "caba-piba-staging-zone-db"."tbp_typ_def_cv_experiencia_laboral" exp 
        ON def_cvs.id_curriculum = exp.id_curriculum LEFT JOIN
        "caba-piba-staging-zone-db"."tbp_typ_def_sector_productivo" j_mis_name 
        ON exp.id_sector_productivo = j_mis_name.id_sector_productivo
        WHERE def_cvs.id_old = CAST(cvs.id AS VARCHAR) AND
        (j_mis_name.id_origen LIKE '%,' || CAST(mis_j.id AS VARCHAR) OR j_mis_name.id_origen LIKE CAST(mis_j.id AS VARCHAR) ||',%' OR j_mis_name.id_origen LIKE '%,' || CAST(mis_j.id AS VARCHAR) ||',%' OR j_mis_name.id_origen = CAST(mis_j.id AS VARCHAR))) AND
            mis_j.name IS NOT NULL AND
            mis_j.id IS NOT NULL AND
        -- se agrega esta condicion porque no todos los cvs de la capa pasan a la tabla def ya que se eliminan duplicados
            CAST(cvs.id AS VARCHAR) IN (SELECT id_old FROM "caba-piba-staging-zone-db"."tbp_typ_def_curriculum")