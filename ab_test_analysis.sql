WITH session_info AS (

SELECT
    s.date,
    s.ga_session_id,
    sp.country,
    sp.device,
    sp.continent,
    sp.channel,
    ab.test,
    ab.test_group
FROM `DA.ab_test` AS ab
JOIN `DA.session` AS s
    ON ab.ga_session_id = s.ga_session_id
JOIN `DA.session_params` AS sp
    ON sp.ga_session_id = ab.ga_session_id
),

session_with_order AS (

SELECT
    session_info.date,
    session_info.ga_session_id,
    session_info.country,
    session_info.device,
    session_info.continent,
    session_info.channel,
    session_info.test,
    session_info.test_group,
    count(distinct o.ga_session_id) AS session_with_orders
FROM `DA.order` AS o
JOIN session_info
    ON o.ga_session_id = session_info.ga_session_id
GROUP BY
    session_info.date,
    session_info.ga_session_id,
    session_info.country,
    session_info.device,
    session_info.continent,
    session_info.channel,
    session_info.test,
    session_info.test_group
),

events AS (

SELECT
    session_info.date,
    session_info.ga_session_id,
    session_info.country,
    session_info.device,
    session_info.continent,
    session_info.channel,
    session_info.test,
    session_info.test_group,
    ep.event_name,
    count(distinct ep.ga_session_id) AS event_count
FROM `DA.event_params` AS ep
JOIN session_info
    ON ep.ga_session_id = session_info.ga_session_id
GROUP BY
    session_info.date,
    session_info.ga_session_id,
    session_info.country,
    session_info.device,
    session_info.continent,
    session_info.channel,
    session_info.test,
    session_info.test_group,
    ep.event_name
),

sessions AS (

SELECT
    session_info.date,
    session_info.ga_session_id,
    session_info.country,
    session_info.device,
    session_info.continent,
    session_info.channel,
    session_info.test,
    session_info.test_group,
    1 AS session_cnt
FROM session_info
),

account AS (

SELECT
    session_info.date,
    session_info.ga_session_id,
    session_info.country,
    session_info.device,
    session_info.continent,
    session_info.channel,
    session_info.test,
    session_info.test_group,
    count(distinct acs.ga_session_id) AS new_account_cnt
FROM `DA.account_session` AS acs
JOIN session_info
    ON acs.ga_session_id = session_info.ga_session_id
GROUP BY
    session_info.date,
    session_info.ga_session_id,
    session_info.country,
    session_info.device,
    session_info.continent,
    session_info.channel,
    session_info.test,
    session_info.test_group
)
SELECT
    session_with_order.date,
    session_with_order.ga_session_id,
    session_with_order.country,
    session_with_order.device,
    session_with_order.continent,
    session_with_order.channel,
    session_with_order.test,
    session_with_order.test_group,
    'session with orders' AS event_name,
    session_with_order.session_with_orders AS value
FROM session_with_order

UNION ALL

SELECT
    events.date,
    events.ga_session_id,
    events.country,
    events.device,
    events.continent,
    events.channel,
    events.test,
    events.test_group,
    events.event_name,
    events.event_count AS value
FROM events

UNION ALL

SELECT
    sessions.date,
    sessions.ga_session_id,
    sessions.country,
    sessions.device,
    sessions.continent,
    sessions.channel,
    sessions.test,
    sessions.test_group,
    'sessions' AS event_name,
    sessions.session_cnt AS value
FROM sessions

UNION ALL

SELECT
    account.date,
    account.ga_session_id,
    account.country,
    account.device,
    account.continent,
    account.channel,
    account.test,
    account.test_group,
    'new accounts' AS event_name,
    account.new_account_cnt AS value
FROM account
