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
days_between AS (
  Select e.user_id,
    event_name,
    occurred_at,
    DATE_DIFF(
      LEAD(e.occurred_at) OVER (
        PARTITION BY e.user_id
        ORDER BY e.occurred_at
      ),
      e.occurred_at,
      DAY
    ) AS days_before_next_event,
    from events e
    join users u on u.user_id = e.user_id
),
max_days_users AS (
  Select u.user_id,
    MAX(days_before_next_event) as max_days_between_events
  from users u
    join days_between db on u.user_id = db.user_id
  group by 1
)
select PERCENTILE_CONT(max_days_between_events, 0.5 IGNORE NULLS) OVER () as median
from max_days_users
LIMIT 1
