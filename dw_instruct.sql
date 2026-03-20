CREATE DATABASE IF NOT EXISTS retail_stats


CREATE TABLE retail_stats.checkout_analytics
(
    store String,
    checkout String,
    window_start DateTime,
    window_end DateTime,
    region String,
    loc_type String,
    square_footage Float64,
    checkout_type String,
    checkout_department String,
    ck_net_profit Float64,
    ck_net_revenue Float64,
    ck_net_theoretic_revenue Float64,
    ck_net_costs Float64,
    ck_total_sales Int32,
    ck_total_return Int32,
    total_discount Float64,
    ck_return_rate Float64,
    ck_net_margin Float64
)
ENGINE = MergeTree()
ORDER BY (window_start, store, checkout)


