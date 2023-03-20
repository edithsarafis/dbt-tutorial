WITH users AS (
  SELECT user_id,
    created_date as activated_at,
    birth_year,
    user_settings_crypto_unlocked,
    plan,
    attributes_notifications_marketing_push,
    attributes_notifications_marketing_email,
    num_contacts,
    num_referrals,
    num_successful_referrals
  FROM `database`.users
),
events AS (
  SELECT user_id,
    transaction_id as event_id,
    transactions_type as event_name,
    created_date as occurred_at
  FROM `database`.transactions
  WHERE transactions_type in ('CARD_PAYMENT', 'EXCHANGE', 'TOPUP', 'ATM')
),
active_users AS (
  SELECT u.user_id,
    u.activated_at,
    MAX(e.occurred_at) as last_transaction_date,
    DATE_DIFF(
      CAST(
        (
          SELECT MAX(created_date) as last_date
          FROM `database`.transactions
        ) as DATE
      ),
      CAST(MAX(e.occurred_at) as DATE),
      MONTH
    ) as seniority_months,
    CASE
      WHEN DATE_DIFF(
        CAST(
          (
            SELECT MAX(created_date) as last_date
            FROM `database`.transactions
          ) as DATE
        ),
        CAST(MAX(e.occurred_at) as DATE),
        MONTH
      ) > 1 THEN 0
      ELSE 1
    END AS IsActive,
    FROM users u
    JOIN events e ON e.user_id = u.user_id
  GROUP BY 1,
    2
),
user_activities AS(
  SELECT u.user_id as temp_user_id,
    COUNT(
      CASE
        WHEN e.occurred_at > DATE_SUB(au.last_transaction_date, INTERVAL 30 DAY) THEN e.user_id
        ELSE NULL
      END
    ) AS last_month_transactions,
    count(e.event_id) as number_transactions,
    max(seniority_months) as seniority_months,
    max(IsActive) as IsActive
  FROM active_users au
    join users u on au.user_id = u.user_id
    join events e on e.user_id = u.user_id
  group by temp_user_id
)
SELECT *
from users
  join user_activities ua on ua.temp_user_id = users.user_id
