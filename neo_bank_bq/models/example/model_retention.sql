WITH events AS (
  SELECT user_id,
    transaction_id as event_name,
    created_date as occurred_at
  FROM `database`.transactions
  WHERE transactions_type in ('CARD_PAYMENT', 'TOPUP', 'ATM')
),
users AS (
  SELECT user_id,
    created_date AS activated_at
  FROM `database`.users
),
last_occurred AS (
  SELECT MAX(occurred_at) as last_occurred_at,
    user_id
  from events
  group by user_id
),
active_users AS (
  SELECT DATE_TRUNC(u.activated_at, MONTH) AS signup_date,
    DATE_DIFF(
      CAST(e.last_occurred_at as DATE),
      CAST(u.activated_at as DATE),
      MONTH
    ) + 1 AS user_period,
    COUNT(DISTINCT e.user_id) AS nb_active_users
  from users u
    JOIN last_occurred e ON e.user_id = u.user_id
    AND e.last_occurred_at >= u.activated_at
  GROUP BY 1,
    2
),
retained_users AS (
  SELECT *,
    SUM(nb_active_users) OVER (PARTITION BY signup_date) AS nb_new_users,
    SUM(nb_active_users) OVER (PARTITION BY signup_date) - SUM(nb_active_users) OVER (
      PARTITION BY signup_date
      ORDER BY user_period
    ) AS nb_retained_users,
    FROM active_users
)
SELECT *,
  MAX(nb_retained_users / nb_new_users) OVER (
    PARTITION BY user_period,
    signup_date
    ORDER by user_period
  ) * 100 AS retention_rate
FROM retained_users
ORDER BY signup_date,
  user_period
