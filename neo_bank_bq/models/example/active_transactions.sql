WITH events AS (
  SELECT user_id,
    transaction_id as event_name,
    created_date as occurred_at,
    transactions_type
  FROM `database`.transactions
)
Select SUM(
    CASE
      WHEN transactions_type in ('CARD_PAYMENT', 'TOPUP', 'ATM') THEN 1
      ELSE 0
    END
  ) / COUNT(*) as percent_active_transactions
from events
