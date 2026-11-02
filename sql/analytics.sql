-- 1. Events per hour by type (uses event_time, not arrival time)
SELECT date_trunc('hour', event_time) AS hour,
       event_type,
       COUNT(*) AS events
FROM events
GROUP BY 1, 2
ORDER BY 1, 2;

-- 2. Conversion funnel (distinct users reaching each step)
WITH funnel AS (
    SELECT
        COUNT(DISTINCT user_id) FILTER (WHERE event_type = 'view')        AS viewers,
        COUNT(DISTINCT user_id) FILTER (WHERE event_type = 'add_to_cart') AS carters,
        COUNT(DISTINCT user_id) FILTER (WHERE event_type = 'purchase')    AS buyers
    FROM events
)
SELECT viewers, carters, buyers,
       ROUND(100.0 * carters / NULLIF(viewers, 0), 1) AS view_to_cart_pct,
       ROUND(100.0 * buyers  / NULLIF(carters, 0), 1) AS cart_to_buy_pct
FROM funnel;

-- 3. Top 5 products by purchases, with rank
SELECT product_id,
       COUNT(*) AS purchases,
       RANK() OVER (ORDER BY COUNT(*) DESC) AS rnk
FROM events
WHERE event_type = 'purchase'
GROUP BY product_id
ORDER BY purchases DESC
LIMIT 5;

-- 4. Late-arriving events (happened 10+ minutes before they were ingested)
SELECT COUNT(*) AS late_events,
       ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM events), 1) AS pct_of_total
FROM events
WHERE ingested_at - event_time > INTERVAL '10 minutes';