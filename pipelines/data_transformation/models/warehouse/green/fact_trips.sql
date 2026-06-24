SELECT
    d.year,
    d.month_name,
    dt.period_of_day,
    l_pu.borough        AS pickup_borough,
    l_do.borough        AS dropoff_borough,
    v.company_name      AS vendor,
    p.payment_type_name AS payment_method,
    r.ratecode_name     AS rate_type,
    t.trip_type_name    AS trip_type,

    COUNT(*)                         AS total_trips,
    ROUND(AVG(trip_distance), 2)     AS avg_distance_miles,
    ROUND(AVG(trip_duration_minutes), 2) AS avg_duration_min,
    ROUND(SUM(total_amt), 2)         AS total_revenue,
    ROUND(AVG(comparation_total_amt), 2) AS avg_fare_diff

FROM fact_trips f
JOIN dim_date         d    ON f.date_id         = d.date_id
JOIN dim_time         dt   ON f.pickup_time_id  = dt.time_id
JOIN dim_location     l_pu ON f.pu_location_id  = l_pu.location_id
JOIN dim_location     l_do ON f.do_location_id  = l_do.location_id
JOIN dim_vendor       v    ON f.vendor_id        = v.vendor_id
JOIN dim_payment_type p    ON f.payment_type_id  = p.payment_type_id
JOIN dim_ratecode     r    ON f.ratecode_id      = r.ratecode_id
JOIN dim_trip_type    t    ON f.trip_type_id     = t.trip_type_id
GROUP BY 1,2,3,4,5,6,7,8,9
ORDER BY total_trips DESC