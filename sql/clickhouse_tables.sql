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
    ck_costs Float64,
    ck_total_sales Int32,
    ck_total_return Int32,
    ck_discount Float64,
    ck_return_rate Float64,
    ck_net_margin Float64
)
ENGINE = MergeTree()
ORDER BY (window_start, store, checkout)



CREATE TABLE retail_stats.payment_analytics
(
    store String,
    checkout String,
    payment String,
    window_start DateTime,
    window_end DateTime,
    receipt_number Int32,
)
ENGINE = MergeTree()
ORDER BY (window_start, store, checkout, payment)



CREATE TABLE retail_stats.article_analytics
(
    category String,
    model String,
    sex String,
    store String,
    window_start DateTime,
    window_end DateTime,
    supplier String,
    sold_articles Int32,
    net_profit_articles Float64,
    returned_articles Int32,
    return_rate Float64,
)
ENGINE = MergeTree()
ORDER BY (window_start, category, model, sex, store)

CREATE TABLE retail_stats.daily_data
(
    category String,
    model String,
    sex String,
    supplier String,
    store String,
    region String,
    loc_type String,
    square_footage String,
    day_of_week String,
    sold_articles Int32,
    net_profit Float64,
    returned_articles Int32,
    costs Float64,
    return_rate Float64,
    net_margin Float64,
    date Date
)
ENGINE = MergeTree()
ORDER BY (date, category, model, sex, store)

delete from retail_stats.article_analytics where 1=1;
delete from retail_stats.checkout_analytics where 1=1;
delete from retail_stats.payment_analytics where 1=1;
delete from retail_stats.daily_data where 1=1;
