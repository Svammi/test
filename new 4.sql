тестовый текст
WITH vals AS
(
   SELECT * FROM UNNEST(ARRAY['planpackage','help2start','toelama','facebook','google_external','direct_internal', 'direct_external','google_internal','facebook_internal','facebook_external','elama','direct','adwords','google','vkontakte','targetMailRu','yadisplay','yaspravochnik','calltouch','yanavigator','calltracking','yagla','begun','yabayan','yamarket', 'report_subscription', 'lingvo_subscription', 'paid_care_service', 'adwords_external_account', 'direct_external_account', 'facebook_external_account', 'tiktok_ads', 'once_service', 'internal_account', 'yandex_zen','business','programmatic_betweenx']) AS value
),

money_subquery AS -- подзапрос с данными по деньгам в нужном формате
(
   SELECT user_id, period, unit, user_locale, value, value_type, value_all_systems, non_commission, currency, money, price, turnover, dm_turnover
   FROM
   (
      SELECT user_id, period, unit, user_locale, value, value_type,value_all_systems, non_commission, 'RUB' AS currency, 
      money_rub AS money, price_rub AS price, turnover_rub AS turnover, dm_turnover_rub AS dm_turnover
      FROM `linear-theater-116812.eLama.money`
   )
   UNION ALL
   (
      SELECT user_id, period, unit, user_locale, value, value_type, value_all_systems,non_commission, 'USD' AS currency, 
      money_usd AS money, price_usd AS price, turnover_usd AS turnover, dm_turnover_usd AS dm_turnover
      FROM `linear-theater-116812.eLama.money`
   )
   UNION ALL
   (
      SELECT user_id, period, unit, user_locale, value, value_type, value_all_systems, non_commission, 'BRL' AS currency, 
      money_brl AS money, price_brl AS price, turnover_brl AS turnover, NULL AS dm_turnover
      FROM `linear-theater-116812.eLama.money`
   )
   UNION ALL
   (
      SELECT user_id, period, unit, user_locale, value, value_type, value_all_systems, non_commission, 'KZT' AS currency, 
      money_kzt AS money, price_kzt AS price, turnover_kzt AS turnover, NULL AS dm_turnover
      FROM `linear-theater-116812.eLama.money`
   )
   -- открутки и переводы по всем юнитам
    UNION ALL

   (
      SELECT user_id, period, 'ALL' AS unit, user_locale, value, value_type,value_all_systems, non_commission, 'RUB' AS currency, 
      money_rub AS money, price_rub AS price, turnover_rub AS turnover, dm_turnover_rub AS dm_turnover
      FROM `linear-theater-116812.eLama.money`
   )
   UNION ALL
   (
      SELECT user_id, period, 'ALL' AS unit, user_locale, value, value_type, value_all_systems,non_commission, 'USD' AS currency, 
      money_usd AS money, price_usd AS price, turnover_usd AS turnover, dm_turnover_usd AS dm_turnover
      FROM `linear-theater-116812.eLama.money`
   )
   UNION ALL
   (
      SELECT user_id, period, 'ALL' AS unit, user_locale, value, value_type, value_all_systems, non_commission, 'BRL' AS currency, 
      money_brl AS money, price_brl AS price, turnover_brl AS turnover, NULL AS dm_turnover
      FROM `linear-theater-116812.eLama.money`
   )
   UNION ALL
   (
      SELECT user_id, period, 'ALL' AS unit, user_locale, value, value_type, value_all_systems, non_commission, 'KZT' AS currency, 
      money_kzt AS money, price_kzt AS price, turnover_kzt AS turnover, NULL AS dm_turnover
      FROM `linear-theater-116812.eLama.money`
   )
   UNION ALL -- открутки и переводы партнеров
   (
      SELECT agency_owner_group_id  AS user_id, period, 'Partners*' AS unit, agency_owner_user_locale AS user_locale, value, value_type, value_all_systems,non_commission, 'RUB' AS currency, 
      money_rub AS money, price_rub AS price, turnover_rub AS turnover, dm_turnover_rub AS dm_turnover
      FROM `linear-theater-116812.eLama.money`
      WHERE unit IN ('SubAgency')
   )
   UNION ALL
   (
      SELECT agency_owner_group_id  AS user_id, period, 'Partners*' AS  unit, agency_owner_user_locale AS user_locale, value, value_type,value_all_systems, non_commission, 'USD' AS currency, 
      money_usd AS money, price_usd AS price, turnover_usd AS turnover, dm_turnover_usd AS dm_turnover
      FROM `linear-theater-116812.eLama.money`
      WHERE unit IN ('SubAgency')
   )
   UNION ALL
   (
      SELECT agency_owner_group_id  AS user_id, period, 'Partners*' AS  unit, agency_owner_user_locale AS user_locale, value, value_type, value_all_systems,non_commission, 'BRL' AS currency, 
      money_brl AS money, price_brl AS price, turnover_brl AS turnover, NULL AS dm_turnover
      FROM `linear-theater-116812.eLama.money`
      WHERE unit IN ('SubAgency')
   )
   UNION ALL
   (
      SELECT agency_owner_group_id  AS user_id, period, 'Partners*' AS  unit, agency_owner_user_locale AS user_locale, value, value_type, value_all_systems, non_commission, 'KZT' AS currency, 
      money_kzt AS money, price_kzt AS price, turnover_kzt AS turnover, NULL AS dm_turnover
      FROM `linear-theater-116812.eLama.money`
      WHERE unit IN ('SubAgency')
   )
)



SELECT  l.period, l.unit, l.user_locale, l.value, l.value_type, l.currency, IF(l.non_commission=true, 'Non-commissional', 'Commissional') AS non_commission , 
  total_money, active_users, activations, registrations, avg, percentile_50, percentile_25, percentile_75, percentile_90,  order_n
FROM -- здесь создается список к которому JOIN все значения 
(
SELECT REGEXP_EXTRACT(CAST(date as string), r'.{7}') as period, unit, currency, value, value_type,  non_commission, user_locale 
   FROM  UNNEST(GENERATE_DATE_ARRAY('2015-10-01', '2021-05-30', INTERVAL 30 DAY)) AS date
   CROSS JOIN (SELECT unit FROM UNNEST(ARRAY['ALL','SelfService','SubAgency','Agency', 'Partners*']) AS unit)
   CROSS JOIN (SELECT user_locale FROM UNNEST(ARRAY['RUS','INT','BRA','KAZ','INT_ext']) AS user_locale)
   CROSS JOIN (
      SELECT value.value AS value FROM (
         SELECT * FROM vals UNION ALL ( SELECT value FROM UNNEST(ARRAY['all_systems','Analytical Revenue', 'Turnover', 'DmTurnover']) AS value)
      ) AS value )
   CROSS JOIN (SELECT value_type FROM UNNEST(ARRAY['Revenue&Turnover', 'transactions', 'statistics']) AS  value_type)
   CROSS JOIN (SELECT non_commission FROM UNNEST(ARRAY[true, false]) AS  non_commission)
   CROSS JOIN (SELECT currency FROM UNNEST(ARRAY['RUB','USD','BRL','KZT']) AS currency)
   GROUP BY period, unit, currency, user_locale, value, value_type,  non_commission
) AS l


-- здесь считаем деньги 
LEFT JOIN
(
   SELECT period,  unit, user_locale, value, value_type,  non_commission, currency,  price AS total_money
   FROM
   (  SELECT period,  unit, user_locale, IF (value IN ('google', 'adwords'), 'google', value) AS  value, value_type,  non_commission, currency,  SUM(price) AS price
      FROM money_subquery
      WHERE period>='2015-10' AND value IN (SELECT * FROM vals)
      GROUP BY period, unit, user_locale, non_commission, currency, value, value_type
   )
   UNION ALL 
   (  
      SELECT period,  unit, user_locale, 'Analytical Revenue' AS value, 'Revenue&Turnover' as value_type, non_commission, currency,  
      SUM(money) AS price
      FROM money_subquery
      WHERE period>='2015-10'
      GROUP BY period, unit, user_locale, non_commission, currency, value, value_type
   )
      UNION ALL 
   (  
      SELECT period,  unit, user_locale, 'all_systems' AS value, 'transactions' as value_type,  false as non_commission, currency,  
      SUM(price) AS price
      FROM money_subquery
      WHERE period>='2015-10' AND value_all_systems=TRUE 
      GROUP BY period, unit, user_locale, non_commission, currency, value, value_type
   )
    
    UNION ALL 
   (  
      SELECT period,  unit, user_locale, 'Turnover' as value, 'Revenue&Turnover' as value_type,  non_commission, currency,  
      SUM(turnover) AS price
      FROM money_subquery
      WHERE period>='2015-10'AND turnover IS NOT NULL
      GROUP BY period, unit, user_locale, non_commission, currency, value, value_type
   )
    UNION ALL 
   (  
      SELECT period,  unit, user_locale, 'DmTurnover' as value, 'Revenue&Turnover' as value_type,  non_commission, currency,  
      SUM(dm_turnover) AS price
      FROM money_subquery
      WHERE period>='2015-10'AND dm_turnover IS NOT NULL
      GROUP BY period, unit, user_locale, non_commission, currency, value, value_type
   )

   
)as r on (l.period=r.period AND l.unit=r.unit AND l.user_locale=r.user_locale AND l.value=r.value AND l.currency=r.currency AND l.non_commission=r.non_commission AND l.value_type=r.value_type)

-- активации 
LEFT JOIN
(
   SELECT period, unit, user_locale, IF (value IN ('google', 'adwords'), 'google', value) AS  value, value_type, false AS non_commission,  'RUB' AS currency, COUNT(DISTINCT user_id) AS activations
   FROM 
   (
       SELECT user_id, value, value_type, unit, user_locale, period
       FROM
       (
           SELECT user_id, 'Analytical Revenue' as value, 'Revenue&Turnover' as value_type, unit, user_locale, MIN(period) as period
           FROM money_subquery
           WHERE money IS NOT NULL AND currency = 'RUB'
           GROUP BY user_id, unit, value, value_type, user_locale 
       )
       UNION ALL 
       (
           SELECT user_id, 'all_systems' as value, 'transactions' as value_type, unit, user_locale, MIN(period) as period
           FROM money_subquery
           WHERE value_all_systems=TRUE 
           GROUP BY user_id, unit, value, value_type, user_locale 
       )

       UNION ALL 
       (
           SELECT user_id, value, value_type, unit, user_locale, MIN(period) as period
           FROM money_subquery
           WHERE value IN (SELECT * FROM vals) 
           GROUP BY user_id, unit, value, value_type, user_locale 
       )

       UNION ALL 
       (
           SELECT user_id, 'Turnover' AS value,  'Revenue&Turnover' as  value_type, unit, user_locale, MIN(period) as period
           FROM money_subquery
           WHERE turnover IS NOT NULL AND currency = 'USD'
           GROUP BY user_id, unit, value, value_type, user_locale 
       )
      UNION ALL 
       (
           SELECT user_id, 'DmTurnover' AS value,  'Revenue&Turnover' as  value_type, unit, user_locale, MIN(period) as period
           FROM money_subquery
           WHERE dm_turnover IS NOT NULL AND currency = 'RUB'
           GROUP BY user_id, unit, value, value_type, user_locale 
       )

   )
   WHERE period>='2015-10-01' 
   GROUP BY  period, value, unit, user_locale, value_type
)AS t ON (t.period=l.period AND t.unit=l.unit AND t.user_locale=l.user_locale AND t.value=l.value AND t.currency=l.currency AND t.non_commission=l.non_commission AND t.value_type=l.value_type)

LEFT JOIN -- регистрации
(


   SELECT month_registration as period, unit, user_locale, 'Analytical Revenue' AS value, 'Revenue&Turnover' AS value_type, true AS non_commission, 'RUB' AS currency,  
   COUNT(user_id) as registrations
   FROM `linear-theater-116812.eLama.registrations2`
   WHERE reg=TRUE AND period>'2015-09'
   GROUP BY period, unit, user_locale, value_type

   UNION ALL
   (
      SELECT month_registration as period, 'ALL' AS unit, user_locale, 'Analytical Revenue' AS value, 'Revenue&Turnover' AS value_type, true AS non_commission, 'RUB' AS currency, COUNT(user_id) as registrations
      FROM `linear-theater-116812.eLama.registrations2`
      WHERE reg=TRUE AND period>'2015-09'
      GROUP BY period, unit, user_locale, value_type
   )
  UNION ALL
  (
      SELECT SUBSTR(STRING(agency_group_created),1,7) as period, 'Partners*' AS unit, owner_now_user_locale AS user_locale, 'Analytical Revenue' AS value, 'Revenue&Turnover' AS value_type, true AS non_commission, 'RUB' AS currency, COUNT(agency_group_id) as registrations
      FROM `linear-theater-116812.elama.agency_user_group3` created_at
      GROUP BY period, unit, user_locale, value_type
   )


) AS e ON (e.period=l.period AND e.unit=l.unit AND e.user_locale=l.user_locale AND e.value=l.value AND e.currency=l.currency AND e.value_type=l.value_type AND e.non_commission=l.non_commission)

-- активные пользователи
LEFT JOIN 
(
   
   SELECT  period, unit, user_locale, IF (value IN ('google', 'adwords'), 'google', value) AS  value, value_type, false AS non_commission, 'RUB' AS currency,  
   COUNT(DISTINCT user_id) AS active_users
   FROM
   (  
      (
         SELECT period, unit, user_locale, value, value_type, user_id
         FROM money_subquery
         WHERE  value IN (SELECT * FROM vals)
      )
      UNION ALL
      (
         SELECT period, unit, user_locale, 'Analytical Revenue' AS value, 'Revenue&Turnover' AS value_type, user_id
         FROM money_subquery
         WHERE money IS NOT NULL AND currency = 'USD'
      )

      UNION ALL
      (
         SELECT period, unit, user_locale, 'all_systems' AS value, 'transactions' AS value_type, user_id
         FROM money_subquery
         WHERE value_all_systems=TRUE
      )

      UNION ALL
      (
         SELECT period, unit, user_locale, 'Turnover' AS value, 'Revenue&Turnover' AS value_type, user_id
         FROM money_subquery
         WHERE turnover IS NOT NULL AND currency = 'USD'
      )   
       UNION ALL
      (
         SELECT period, unit, user_locale, 'DmTurnover' AS value, 'Revenue&Turnover' AS value_type, user_id
         FROM money_subquery
         WHERE dm_turnover IS NOT NULL AND currency = 'USD'
      )
  
   )
   GROUP BY period, unit, user_locale, value, currency, value_type
   

)AS o ON o.period=l.period AND o.unit=l.unit AND o.user_locale=l.user_locale AND o.value=l.value 
AND o.currency=l.currency AND o.non_commission=l.non_commission AND o.value_type=l.value_type


-- среднее
LEFT JOIN
(
   SELECT period, unit, user_locale, value, value_type, false AS non_commission, currency, CAST(AVG(price) AS INT64) AS avg
   FROM
   (
      SELECT user_id, period,  unit, user_locale, value, value_type,  currency,  price
      FROM
      (  SELECT user_id, period,  unit, user_locale, IF (value IN ('google', 'adwords'), 'google', value) AS  value, value_type, currency,  SUM(price) AS price
         FROM money_subquery
         WHERE value IN (SELECT * FROM vals)
         GROUP BY user_id, period, unit, user_locale, value, value_type, currency
         HAVING price IS NOT NULL AND price!=0
      )

      UNION ALL 
      (  
         SELECT user_id, period,  unit, user_locale, 'Analytical Revenue' AS value, 'Revenue&Turnover' as value_type, currency, SUM(money) AS price
         FROM money_subquery
         GROUP BY user_id, period, unit, user_locale, value, value_type, currency
         HAVING price IS NOT NULL AND price!=0
      )
      
      UNION ALL 
      (  
         SELECT user_id, period,  unit, user_locale, 'all_systems' AS value, 'transactions' as value_type,  currency,  SUM(price) AS price
         FROM money_subquery
         WHERE value_all_systems=TRUE 
         GROUP BY user_id, period, unit, user_locale, value, value_type, currency
         HAVING price IS NOT NULL AND price!=0
      )
       
       UNION ALL 
      (  
         SELECT user_id, period,  unit, user_locale, 'Turnover' as value, 'Revenue&Turnover' as value_type,  currency,  SUM(turnover) AS price
         FROM money_subquery
         WHERE  turnover IS NOT NULL
         GROUP BY user_id, period, unit, user_locale, value, value_type, currency
         HAVING price IS NOT NULL AND price!=0
      )
       UNION ALL 
      (  
         SELECT user_id, period,  unit, user_locale, 'DmTurnover' as value, 'Revenue&Turnover' as value_type,  currency,  SUM(dm_turnover) AS price
         FROM money_subquery
         WHERE  dm_turnover IS NOT NULL
         GROUP BY user_id, period, unit, user_locale, value, value_type, currency
         HAVING price IS NOT NULL AND price!=0
      )
   )
   GROUP BY period, unit, user_locale, value, value_type,  currency, non_commission 
   
) AS m ON (m.period=l.period AND m.unit=l.unit AND m.user_locale=l.user_locale AND m.value=l.value AND m.currency=l.currency AND m.non_commission=l.non_commission AND m.value_type=l.value_type) 

-- медиана
LEFT JOIN 
(
   SELECT 
      period, unit, user_locale, value, value_type,  false AS non_commission,  currency, 
      MIN(percentile_50) as percentile_50, -- нужна любая агр функция
      MIN(percentile_25) as percentile_25,
      MIN(percentile_90) as percentile_90,
      MIN(percentile_75) as percentile_75

   FROM
   (
      SELECT period, unit, value, value_type, user_locale, currency, 
         PERCENTILE_CONT(price, 0.25) OVER(PARTITION BY period, value, value_type, user_locale, unit, currency) AS percentile_25,
         PERCENTILE_CONT(price, 0.50) OVER(PARTITION BY period, value, value_type, user_locale, unit, currency) AS percentile_50,
         PERCENTILE_CONT(price, 0.75) OVER(PARTITION BY period, value, value_type, user_locale, unit, currency) AS percentile_75,
         PERCENTILE_CONT(price, 0.90) OVER(PARTITION BY period, value, value_type, user_locale, unit, currency) AS percentile_90
      FROM
      (
         SELECT user_id, period,  unit, user_locale, value, value_type,  currency,  price
         FROM
         (  SELECT user_id, period,  unit, user_locale, IF (value IN ('google', 'adwords'), 'google', value) AS  value, value_type, currency,  SUM(price) AS price
             FROM money_subquery
             WHERE value IN (SELECT * FROM vals)
             GROUP BY user_id, period, unit, user_locale, value, value_type, currency
             HAVING price IS NOT NULL AND price!=0
         )

         UNION ALL 
         (  
             SELECT user_id, period,  unit, user_locale, 'Analytical Revenue' AS value, 'Revenue&Turnover' as value_type, currency, SUM(money) AS price
             FROM money_subquery
             GROUP BY user_id, period, unit, user_locale, value, value_type, currency
             HAVING price IS NOT NULL AND price!=0
         )
         
             UNION ALL 
         (  
             SELECT user_id, period,  unit, user_locale, 'all_systems' AS value, 'transactions' as value_type,  currency,  SUM(price) AS price
             FROM money_subquery
             WHERE value_all_systems=TRUE 
             GROUP BY user_id, period, unit, user_locale, value, value_type, currency
             HAVING price IS NOT NULL AND price!=0
         )
             
             UNION ALL 
         (  
             SELECT user_id, period,  unit, user_locale, 'Turnover' as value, 'Revenue&Turnover' as value_type,  currency,  SUM(turnover) AS price
             FROM money_subquery
             WHERE  turnover IS NOT NULL
             GROUP BY user_id, period, unit, user_locale, value, value_type, currency
             HAVING price IS NOT NULL AND price!=0
         )
         UNION ALL 
         (  
             SELECT user_id, period,  unit, user_locale, 'DmTurnover' as value, 'Revenue&Turnover' as value_type,  currency,  SUM(dm_turnover) AS price
             FROM money_subquery
             WHERE  dm_turnover IS NOT NULL
             GROUP BY user_id, period, unit, user_locale, value, value_type, currency
             HAVING price IS NOT NULL AND price!=0
         )
      )
   )
   GROUP BY period, unit, user_locale, value,  currency,  non_commission, value_type
   
) AS w ON (w.period=l.period AND w.unit=l.unit AND w.user_locale=l.user_locale AND w.value=l.value AND w.currency=l.currency AND w.non_commission=l.non_commission AND w.value_type=l.value_type)

LEFT JOIN -- нужно для сортировки в PBI
(
   SELECT 'Analytical Revenue' as n_value, 10 as order_n
   UNION ALL (SELECT 'Turnover' as n_value, 20 as order_n)
   UNION ALL (SELECT 'DmTurnover' as n_value, 25 as order_n)
   UNION ALL (SELECT 'all_systems' as n_value, 30 as order_n)
   UNION ALL (SELECT 'google_external' as n_value, 40 as order_n)
   UNION ALL (SELECT 'google_internal' as n_value, 50 as order_n)
   UNION ALL (SELECT 'direct_internal' as n_value, 60 as order_n)
   UNION ALL (SELECT 'direct_external' as n_value, 70 as order_n)
   UNION ALL (SELECT 'facebook_external' as n_value, 80 as order_n)
   UNION ALL (SELECT 'facebook_internal' as n_value, 90 as order_n)
   UNION ALL (SELECT 'elama' as n_value, 100 as order_n)
   UNION ALL (SELECT 'planpackage' as n_value, 110 as order_n)
   UNION ALL (SELECT 'paid_care_service' as n_value, 120 as order_n)
   UNION ALL (SELECT 'report_subscription' as n_value, 130 as order_n)
   UNION ALL (SELECT 'lingvo_subscription' as n_value, 140 as order_n)   
   UNION ALL (SELECT 'adwords_external_account' as n_value, 150 as order_n)      
   UNION ALL (SELECT 'direct_external_account' as n_value, 160 as order_n)
   UNION ALL (SELECT 'facebook_external_account' as n_value, 170 as order_n)
   UNION ALL (SELECT 'internal_account' as n_value,171  as order_n)
   UNION ALL (SELECT 'business' as n_value,172  as order_n)
   UNION ALL (SELECT 'toelama' as n_value, 180 as order_n)
   UNION ALL (SELECT 'once_service' as n_value, 190 as order_n)
   UNION ALL (SELECT 'help2start' as n_value, 200 as order_n)
   UNION ALL (SELECT 'direct' as n_value, 210 as order_n)
   UNION ALL (SELECT 'google' as n_value, 220 as order_n)
   UNION ALL (SELECT 'facebook' as n_value, 230 as order_n)
   UNION ALL (SELECT 'tiktok_ads' as n_value, 240 as order_n)   
   UNION ALL (SELECT 'vkontakte' as n_value, 250 as order_n)
   UNION ALL (SELECT 'targetMailRu' as n_value, 260 as order_n)
   UNION ALL (SELECT 'yadisplay' as n_value, 270 as order_n)
   UNION ALL (SELECT 'yaspravochnik' as n_value, 280 as order_n)
   UNION ALL (SELECT 'calltouch' as n_value, 290 as order_n)
   UNION ALL (SELECT 'yanavigator' as n_value, 300 as order_n)
   UNION ALL (SELECT 'calltracking' as n_value, 310 as order_n)
   UNION ALL (SELECT 'yagla' as n_value, 320 as order_n)
   UNION ALL (SELECT 'begun' as n_value, 330 as order_n)
   UNION ALL (SELECT 'yabayan' as n_value, 340 as order_n)
   UNION ALL (SELECT 'yamarket' as n_value, 350  as order_n)
   UNION ALL (SELECT 'yandex_zen' as n_value, 360  as order_n)
   UNION ALL (SELECT 'programmatic_betweenx' as n_value, 370  as order_n)
) AS orders ON n_value=l.value



WHERE total_money IS NOT NULL OR registrations IS NOT NULL OR activations IS NOT NULL

ORDER BY period, unit, user_locale, value, non_commission, currency