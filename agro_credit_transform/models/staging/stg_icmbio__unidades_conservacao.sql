{{ config(
    materialized='view',
    schema='agro_esg_staging'
) }}

WITH source_data AS (
    SELECT * FROM {{ source('raw_data', 'icmbio_unidades_conservacao') }}
    WHERE Cnuc IS NOT NULL -- O Código Nacional de UC é a chave primária
),

renamed_and_filtered AS (
    SELECT
        file_hash,
        ingested_at,
        
        -- IDENTIFICAÇÃO
        CAST(Cnuc AS STRING) as uc_id,
        TRIM(UPPER(CAST(NomeUC AS STRING))) as uc_name,
        CAST(SiglaCateg AS STRING) as category,
        CAST(GrupoUC AS STRING) as group_name,
        CAST(EsferaAdm AS STRING) as administration_sphere,
        CAST(BiomaIBGE AS STRING) as biome,
        
        -- DADOS TÉCNICOS
        CAST(CriacaoAno AS STRING) as creation_year,
        SAFE_CAST(AreaHaAlb AS FLOAT64) as area_ha,

        -- GEOMETRIA
        SAFE.ST_GEOGFROMTEXT(geom, make_valid => TRUE) as geometry

    FROM source_data
),

deduplicated AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY uc_id 
            ORDER BY ingested_at DESC
        ) as row_num
    FROM renamed_and_filtered
)

SELECT * EXCEPT(row_num) 
FROM deduplicated 
WHERE row_num = 1
  AND geometry IS NOT NULL