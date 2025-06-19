CREATE VIEW `views.v_emails_sent_by_month` AS

SELECT
  sent_month
  , id_account
  , message_cnt_month / message_cnt * 100 as sent_msg_percent_from_this_month
  , first_sent_date
  , last_sent_date
FROM
(
  SELECT DISTINCT
    date(
      extract(YEAR FROM date_add(s.date, interval es.sent_date DAY)),
      extract(MONTH FROM date_add(s.date, interval es.sent_date DAY)),
      1
    ) as sent_month
    , es.id_account
    , count(es.id_message) over(partition by date_trunc(date_add(s.date, interval es.sent_date DAY), MONTH)) as message_cnt
    , count(es.id_message) over(partition by es.id_account, date_trunc(date_add(s.date, interval es.sent_date DAY), MONTH)) as message_cnt_month
    , min(date_add(s.date, interval es.sent_date DAY)) over(partition by es.id_account) as first_sent_date
    , max(date_add(s.date, interval es.sent_date DAY)) over(partition by es.id_account) as last_sent_date
  FROM `email_sent` es
  JOIN `account` ac
  ON ac.id = es.id_account
  JOIN `account_session` acs
  ON ac.id = acs.account_id
  JOIN `session` s
  ON s.ga_session_id = acs.ga_session_id
)
ORDER BY 1 DESC

SELECT *
FROM `views.v_emails_sent_by_month`