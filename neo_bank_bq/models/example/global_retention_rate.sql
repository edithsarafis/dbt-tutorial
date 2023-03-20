With metrics_users AS (
  SELECT user_id,
IsActive from {{ref('metrics_users')}}
)
select sum(IsActive)/count(user_id) as global_retention_rate from metrics_users
