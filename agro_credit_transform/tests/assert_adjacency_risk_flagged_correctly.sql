WITH mart AS (
    SELECT property_id, final_eligibility_status, geometry, car_bbox FROM {{ ref('fct_compliance_risk') }}
),
puramente_elegiveis AS (
    SELECT * FROM mart WHERE final_eligibility_status = 'ELIGIBLE'
),
propriedades_bloqueadas AS (
    SELECT * FROM mart WHERE final_eligibility_status LIKE 'NOT ELIGIBLE%'
)
SELECT e.property_id FROM puramente_elegiveis e
INNER JOIN propriedades_bloqueadas b 
    ON e.car_bbox.xmin <= b.car_bbox.xmax 
       AND e.car_bbox.xmax >= b.car_bbox.xmin 
       AND e.car_bbox.ymin <= b.car_bbox.ymax 
       AND e.car_bbox.ymax >= b.car_bbox.ymin
WHERE ST_INTERSECTS(e.geometry, b.geometry)
  AND ST_AREA(ST_INTERSECTION(e.geometry, b.geometry)) > 1