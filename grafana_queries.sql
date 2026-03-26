-- Comparison between Stores profit (required the variable multi_store in Grafana that basically let u select the store u are interested in comparing)
SELECT 
    window_start AS time, 
    sum(ck_net_profit) AS profit,
    store AS metric
FROM retail_stats.checkout_analytics
WHERE $__timeFilter(window_start) and store IN ($multi_store)
GROUP BY window_start, store
ORDER BY time

-- Profits and costs over time for a given store (required the variable unique_store in Grafana that let u select just one store u are interested in analyzing)
SELECT 
    window_start AS time, 
    ck_net_profit AS net_profit, 
    -ck_costs AS cost,
    avg(ck_net_profit) OVER () AS average_profit,
    store
FROM retail_stats.checkout_analytics
WHERE $__timeFilter(window_start) AND store= '$unique_store'               
ORDER BY time

-- Sales over the given checkouts
SELECT 
    window_start AS time, 
    sum(ck_net_profit) AS profit,
    store AS metric
FROM retail_stats.checkout_analytics
WHERE $__timeFilter(window_start) and store IN ($multi_store)
GROUP BY window_start, store
ORDER BY time

-- Return rate over the given checkouts
