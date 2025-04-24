# Data_project
--Необхідно зібрати дані, які допоможуть аналізувати динаміку створення акаунтів, активність користувачів за листами (відправлення, відкриття, переходи), а також оцінювати поведінку в категоріях, таких як інтервал відправлення, верифікація акаунтів і статус підписки. 
WITH
  account_metrics AS --витягуємо дані та проводимо обчислення стосовно account
  (
  SELECT
    s.date ,
    country,
    a.send_interval,
    a.is_verified,
    a.is_unsubscribed,
    COUNT(distinct a.id) AS account_cnt,
     CAST(0 AS INT64) AS sent_msg,
     CAST(0 AS INT64) AS open_msg,
     CAST(0 AS INT64) AS visit_msg
  FROM
    `DA.account`a
  LEFT JOIN`DA.account_session` acs ON acs.account_id = a.id
  LEFT JOIN `DA.session` s ON acs.ga_session_id = s.ga_session_id
  JOIN `DA.session_params` sp ON sp.ga_session_id = s.ga_session_id
  GROUP BY
    s.date,country,
    a.send_interval,
    a.is_verified,
    a.is_unsubscribed
    ),
    email_metrics AS  --витягуємо дані та проводимо обчислення стосовно message
  (
  SELECT
    DATE_ADD(s.date, INTERVAL ems.sent_date DAY) AS sent_date,
    country,
    a.send_interval,
    a.is_verified,
    a.is_unsubscribed,
    COUNT(distinct ems.id_message) AS sent_msg,
    COUNT(distinct eo.id_message) AS open_msg,
    COUNT(distinct ev.id_message) AS visit_msg,
    CAST(0 AS INT64) AS account_cnt,
  FROM
    `DA.email_sent` ems
  JOIN `DA.account_session` acs ON ems.id_account = acs.account_id
  JOIN `DA.account` a ON a.id = acs.account_id
  JOIN `DA.session` s ON acs.ga_session_id = s.ga_session_id
  JOIN `DA.session_params` sp ON s.ga_session_id = sp.ga_session_id
  LEFT JOIN `DA.email_open` eo ON ems.id_message = eo.id_message
  LEFT JOIN `DA.email_visit` ev ON ems.id_message = ev.id_message
  GROUP BY
    DATE_ADD(s.date, INTERVAL ems.sent_date DAY) ,
    country,  a.send_interval,
    a.is_verified,
    a.is_unsubscribed
    ),
     union_metrics as -- поєднуємо дані з таблиць про account та message для відображення загальних даних
    (
      SELECT
    date,
    country,
    sent_msg,
    open_msg,
    visit_msg,
    account_cnt,
    send_interval,
    is_verified,
    is_unsubscribed
    FROM account_metrics
    UNION ALL
    SELECT
    sent_date as date,
    country,
    sent_msg,
    open_msg,
    visit_msg,
    account_cnt,
    send_interval,
    is_verified,
    is_unsubscribed
    from  email_metrics
    ),
    total_metrics as --прораховуємо метрики для визначення загальної кількості створених підписників та відправлених листів по кожній з країн
    (
         SELECT
  country,
  SUM (sent_msg)  as total_country_sent_cnt,
  SUM (account_cnt)  as total_country_account_cnt
  FROM union_metrics
  Group by country
   ),
  rank_total_metrics as -- прораховуємо метрики для визначення рангу країни за загальною кількістю підписників та відправлених листів по кожній з країн, сюди ж додаю метрики по загальній кількості
  (
SELECT country,
total_country_sent_cnt,
total_country_account_cnt,
DENSE_RANK() OVER ( ORDER BY  total_country_sent_cnt DESC) AS rank_total_country_sent_cnt,
DENSE_RANK() OVER ( ORDER BY total_country_account_cnt DESC) AS rank_total_country_account_cnt
from total_metrics
)
 
    SELECT -- виводимо необхідні показники з об'єднаних таблиць account та message, за допомогою join додаю сюди також додаткові дані по загальним сумам та рангам
    date,
    union_metrics.country,
    send_interval ,
    is_verified ,
    is_unsubscribed,
     SUM (account_cnt) as account_cnt,
     SUM (sent_msg) as sent_msg,
     SUM (open_msg) as open_msg,
     SUM (visit_msg) as visit_msg,
    total_country_account_cnt,
    total_country_sent_cnt,
    rank_total_country_account_cnt,
    rank_total_country_sent_cnt
    From union_metrics
    JOIN   rank_total_metrics ON union_metrics.country =  rank_total_metrics.country
  WHERE rank_total_country_sent_cnt <= 10 or rank_total_country_account_cnt <=10
   GROUP BY date,
union_metrics.country,
send_interval,
is_verified,
is_unsubscribed,
total_country_account_cnt,
total_country_sent_cnt,
rank_total_country_account_cnt,
rank_total_country_sent_cnt
ORDER BY rank_total_country_account_cnt
