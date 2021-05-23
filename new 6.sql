#standardSQL
WITH 
vals AS
(
   SELECT * FROM UNNEST(ARRAY['planpackage','help2start','toelama','facebook','google_external','direct_internal', 'direct_external',
   'google_internal','facebook_internal','facebook_external','elama','direct','adwords','google','vkontakte','targetMailRu','yadisplay',
   'yaspravochnik','calltouch','yanavigator','calltracking','yagla','begun','yabayan','yamarket', 'report_subscription', 'lingvo_subscription', 
   'paid_care_service', 'adwords_external_account', 'direct_external_account', 'facebook_external_account', 'tiktok_ads', 'once_service', 
   'internal_account', 'yandex_zen','business','programmatic_betweenx']) AS value
),

money_subquery AS -- подзапрос с данными по деньгам в нужном формате
(
   SELECT user_id, period, week, date, unit, user_locale, IF (value IN ('google', 'adwords'), 'google', value) AS  value, value_all_systems,value_type,
         price_rub, price_usd, price_kzt, price_brl, 
         money_rub, money_usd, money_kzt, money_brl,
         turnover_rub, turnover_usd, turnover_kzt, turnover_brl,
         dm_turnover_rub, dm_turnover_usd
      FROM 
      (
         SELECT user_id, period, week, CAST(date_payed AS DATE) AS date, unit, user_locale, value, value_all_systems,value_type,
         price_rub, price_usd, price_kzt, price_brl, 
         money_rub, money_usd, money_kzt, money_brl,
         turnover_rub, turnover_usd, turnover_kzt, turnover_brl,
         dm_turnover_rub, dm_turnover_usd
         FROM `linear-theater-116812.eLama.money`
         
      )    
      UNION ALL
      (
         SELECT user_id, period,  week, CAST(date_payed AS DATE) AS date,  'ALL' AS unit, user_locale, value, value_all_systems,value_type,
         price_rub, price_usd, price_kzt, price_brl, 
         money_rub, money_usd, money_kzt, money_brl,
         turnover_rub, turnover_usd, turnover_kzt, turnover_brl,
         dm_turnover_rub, dm_turnover_usd
         FROM `linear-theater-116812.eLama.money`
        
      )        
      UNION ALL -- здесь открутки и переводы по партнерам
      (
         SELECT agency_owner_group_id  AS user_id, period, week, CAST(date_payed AS DATE) AS date, 'Partners*' as unit,  agency_owner_user_locale as user_locale, 
         value, value_all_systems,value_type,
         price_rub, price_usd, price_kzt, price_brl, 
         money_rub, money_usd, money_kzt, money_brl,
         turnover_rub, turnover_usd, turnover_kzt, turnover_brl,
         dm_turnover_rub, dm_turnover_usd
         FROM `linear-theater-116812.eLama.money`
         WHERE  unit IN ('SubAgency') 
         
      )

)
 
SELECT l.period,  l.week, CAST(l.date AS STRING) AS date, l.unit, l.user_locale, l.value, 
--order_n, 
total_money_rub, total_money_usd, total_money_kzt, total_money_brl, 
activations, 
registrations, 
active_users,
CASE 
   WHEN l.value = 'Analytical Revenue' THEN 10
   WHEN l.value = 'Turnover' THEN 20
   WHEN l.value = 'DmTurnover' THEN 25
   WHEN l.value = 'all_systems' THEN 30
   WHEN l.value =  'google_external' THEN 40
   WHEN l.value =  'google_internal' THEN 50
   WHEN l.value = 'direct_internal' THEN 60
   WHEN l.value =  'direct_external' THEN 70 
   WHEN l.value =  'facebook_external' THEN 80
   WHEN l.value =  'facebook_internal' THEN 90 
   WHEN l.value =  'elama' THEN 100
   WHEN l.value =  'planpackage' THEN 110
   WHEN l.value =  'paid_care_service' THEN 120 
   WHEN l.value =  'report_subscription' THEN 130 
   WHEN l.value =  'lingvo_subscription' THEN 140
   WHEN l.value =  'adwords_external_account' THEN 150 
   WHEN l.value =  'direct_external_account'THEN 160 
   WHEN l.value =  'facebook_external_account' THEN 170
   WHEN l.value =  'internal_account' THEN 171
   WHEN l.value =  'business' THEN 172 
   WHEN l.value =  'toelama'THEN  180
   WHEN l.value =  'once_service'THEN 190 
   WHEN l.value =  'help2start' THEN 200
   WHEN l.value =  'direct' THEN 210 
   WHEN l.value =  'google' THEN 220
   WHEN l.value =  'facebook' THEN 230 
   WHEN l.value =  'tiktok_ads' THEN  240 
   WHEN l.value =  'vkontakte' THEN 250 
   WHEN l.value =  'targetMailRu' THEN 260 
   WHEN l.value =  'yadisplay' THEN 270 
   WHEN l.value =  'yaspravochnik' THEN 280 
   WHEN l.value =  'calltouch' THEN 290
   WHEN l.value =  'yanavigator' THEN 300
   WHEN l.value =  'calltracking' THEN 310
   WHEN l.value =  'yagla' THEN 320
   WHEN l.value =  'begun' THEN 330
   WHEN l.value =  'yabayan' THEN 340
   WHEN l.value =  'yamarket' THEN 350
   WHEN l.value =  'yandex_zen' THEN 360
   WHEN l.value =  'programmatic_betweenx' THEN 370
   END order_n
FROM
(
   SELECT REGEXP_EXTRACT(CAST(date as string), r'.{7}') as period, date, 
      CASE
         WHEN EXTRACT(DAYOFWEEK FROM  date) IN (3,4,5,6,7) THEN CAST(DATE_ADD(date, INTERVAL -(EXTRACT(DAYOFWEEK FROM  date)-2) DAY) AS STRING) 
         WHEN EXTRACT(DAYOFWEEK FROM  date)=1 THEN CAST(DATE_ADD(date, INTERVAL  -6 DAY) AS STRING)
         ELSE CAST(date AS STRING)
      END AS week, 
      unit, user_locale, value 
   FROM  UNNEST(GENERATE_DATE_ARRAY('2018-01-01', '2021-05-30', INTERVAL 1 DAY)) AS date
   CROSS JOIN (SELECT unit FROM UNNEST(ARRAY['ALL','SelfService','SubAgency','Agency', 'Partners*']) AS unit)
   CROSS JOIN (SELECT user_locale FROM UNNEST(ARRAY['RUS','INT','BRA','KAZ','INT_ext']) AS user_locale)
   CROSS JOIN (SELECT value.value AS value FROM (
      SELECT * FROM vals UNION ALL ( SELECT value FROM UNNEST(ARRAY['all_systems','Analytical Revenue'/*, 'Turnover'*/]) AS value)
   ) AS value )
   GROUP BY period, week, date,  unit, user_locale, value
) as l

LEFT JOIN -- здесь считаем деньги 
(
   SELECT date, unit, user_locale, IF (value IN ('google', 'adwords'), 'google', value) AS value, 
   SUM(money_rub) AS total_money_rub, SUM(money_usd) AS total_money_usd, SUM(money_kzt) AS total_money_kzt, SUM(money_brl) AS total_money_brl
   FROM 
   (
    (
        SELECT user_id, period, week, date, unit, user_locale, value, 
        price_rub AS money_rub, price_usd AS money_usd, price_kzt AS money_kzt, price_brl AS money_brl, 
        FROM money_subquery
        WHERE value IN (SELECT * FROM vals)
    )
    
    UNION ALL 
    (
        SELECT user_id, period, week, date, unit, user_locale,  'Analytical Revenue' AS value, 
        money_rub, money_usd, money_kzt, money_brl,
        FROM money_subquery
    )
    UNION ALL 
    (
        SELECT user_id, period, week, date, unit, user_locale,  'all_systems'  AS value, 
        price_rub AS money_rub, price_usd AS money_usd, price_kzt AS money_kzt, price_brl AS money_brl, 
        FROM money_subquery
        WHERE value_all_systems=TRUE       
    )
    UNION ALL 
    (
        SELECT user_id, period, week, date, unit, user_locale,  'Turnover' AS value, 
        turnover_rub AS money_rub, turnover_usd AS money_usd, turnover_kzt AS money_kzt, turnover_brl AS money_brl,
        FROM money_subquery
        WHERE turnover_rub IS NOT NULL
    )
    UNION ALL 
    (
        SELECT user_id, period, week, date, unit, user_locale,  'DmTurnover' AS value, 
        dm_turnover_rub AS money_rub, dm_turnover_usd AS money_usd, NULL AS money_kzt, NULL AS money_brl
        FROM money_subquery
        WHERE dm_turnover_rub IS NOT NULL
    )
    )
   WHERE date>CAST(DATE_ADD(CURRENT_DATE(), INTERVAL -365 DAY) AS DATE)
   GROUP BY  date, unit, user_locale, value
) as r on (l.date=r.date  AND l.unit=r.unit AND l.user_locale=r.user_locale AND l.value=r.value )   


LEFT JOIN -- регистрации
(


   SELECT month_registration AS period, week_registration AS week, CAST(date_registration AS DATE)  AS date, unit, user_locale, 'Analytical Revenue' AS value,  COUNT(user_id) as registrations
   FROM `linear-theater-116812.eLama.registrations2`
   WHERE reg=TRUE AND date_registration>TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL -365 DAY)
   GROUP BY period, week, date, unit, user_locale, value
   
   UNION ALL
   (
      SELECT month_registration AS period, week_registration AS week, CAST(date_registration  AS DATE) AS date,  'ALL' AS unit, user_locale, 'Analytical Revenue' AS value,  COUNT(user_id) as registrations
      FROM `linear-theater-116812.eLama.registrations2`
      WHERE reg=TRUE AND date_registration>TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL -365 DAY)
      GROUP BY period, week, date, unit, user_locale
   )
  UNION ALL
  (
      SELECT  
      SUBSTR(STRING(agency_group_created),1,7) AS period, 
      CASE
         WHEN EXTRACT(DAYOFWEEK FROM  agency_group_created) IN (3,4,5,6,7) THEN CAST(DATE_ADD(CAST(agency_group_created AS DATE), INTERVAL -(EXTRACT(DAYOFWEEK FROM  agency_group_created)-2) DAY) AS STRING) 
         WHEN EXTRACT(DAYOFWEEK FROM  agency_group_created)=1 THEN CAST(DATE_ADD(CAST(agency_group_created AS DATE), INTERVAL  -6 DAY) AS STRING)
         ELSE CAST(CAST(agency_group_created AS DATE) AS STRING)
      END AS week,
      CAST(agency_group_created AS DATE) AS date, 
      'Partners*' AS unit, owner_now_user_locale AS user_locale, 'Analytical Revenue' AS value,  COUNT(agency_group_id) as registrations
      FROM `linear-theater-116812.elama.agency_user_group3` 
      WHERE agency_group_created>TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL -365 DAY)
      GROUP BY period, week, date, unit, user_locale
   )



) AS e ON ( e.date=l.date AND e.unit=l.unit AND e.user_locale=l.user_locale AND e.value=l.value )


LEFT JOIN -- активации 
(
   SELECT REGEXP_EXTRACT(CAST(date_payed as string), r'.{7}') AS period, 
      CASE
         WHEN EXTRACT(DAYOFWEEK FROM  date_payed) IN (3,4,5,6,7) THEN CAST(DATE_ADD(CAST(date_payed AS DATE), INTERVAL -(EXTRACT(DAYOFWEEK FROM  date_payed)-2) DAY) AS STRING) 
         WHEN EXTRACT(DAYOFWEEK FROM  date_payed)=1 THEN CAST(DATE_ADD(CAST(date_payed AS DATE), INTERVAL  -6 DAY) AS STRING)
         ELSE CAST(CAST(date_payed AS DATE) AS STRING)
      END AS week,
      CAST(date_payed as DATE) AS date,
   unit, user_locale, IF (value IN ('google', 'adwords'), 'google', value) AS value,  COUNT(DISTINCT user_id) AS activations
   FROM 
   (
      (
         SELECT user_id, 'Analytical Revenue' as value,  unit, user_locale, MIN(date) as date_payed
         FROM money_subquery
         WHERE money_rub IS NOT NULL
         GROUP BY user_id, unit, value,  user_locale   
      )
      UNION ALL
      (
         SELECT user_id, 'all_systems' as value,  unit, user_locale, MIN(date) as date_payed
         FROM money_subquery
         WHERE value_all_systems=TRUE
         GROUP BY user_id, unit, value,  user_locale
      )
      UNION ALL
      (
         SELECT user_id, value,  unit, user_locale, MIN(date) as date_payed
         FROM money_subquery
         WHERE value IN (SELECT * FROM vals)
         GROUP BY user_id, unit, value,  user_locale
      )
     
      UNION ALL
      (
         SELECT user_id, 'Turnover' AS value, unit, user_locale, MIN(date) as date_payed
         FROM money_subquery
         WHERE turnover_rub IS NOT NULL
         GROUP BY user_id, unit, value,  user_locale
      )
        UNION ALL
      (
         SELECT user_id, 'DmTurnover' AS value, unit, user_locale, MIN(date) as date_payed
         FROM money_subquery
         WHERE dm_turnover_rub IS NOT NULL
         GROUP BY user_id, unit, value,  user_locale
      )
      

  )
   
    WHERE date_payed>=DATE_ADD(CURRENT_DATE(), INTERVAL -365 DAY)
    GROUP BY  period, week, date, value, unit, user_locale

) AS t ON ( t.date=l.date AND t.unit=l.unit AND t.user_locale=l.user_locale AND t.value=l.value)


LEFT JOIN -- active users 
(
   SELECT  date, unit, user_locale, IF (value IN ('google', 'adwords'), 'google', value) AS value, value_type, false AS non_commission, 'RUB' AS currency,  COUNT(DISTINCT user_id) AS active_users
   FROM
   (
      SELECT  date, unit, user_locale, value, value_type, user_id
      FROM money_subquery
      WHERE  value IN (SELECT * FROM vals)

   UNION ALL
   (
      SELECT date, unit, user_locale, 'Analytical Revenue' AS value, 'Revenue&Turnover' AS value_type, user_id
      FROM money_subquery
      WHERE money_usd IS NOT NULL
   )

   UNION ALL
   (
      SELECT date, unit, user_locale, 'all_systems' AS value, 'transactions' AS value_type, user_id
      FROM money_subquery
      WHERE value_all_systems=TRUE
   )
   
   UNION ALL
   (
       SELECT date, unit, user_locale, 'Turnover' AS value, 'Revenue&Turnover' AS value_type, user_id
       FROM money_subquery
       WHERE turnover_rub IS NOT NULL
   )   
    UNION ALL
   (
       SELECT date, unit, user_locale, 'DmTurnover' AS value, 'Revenue&Turnover' AS value_type, user_id
       FROM money_subquery
       WHERE dm_turnover_rub IS NOT NULL
   )
   
)
 
GROUP BY date, unit, user_locale, value, currency, value_type

) AS u ON ( u.date=l.date AND u.unit=l.unit AND u.user_locale=l.user_locale AND u.value=l.value)



WHERE total_money_rub IS NOT NULL OR registrations IS NOT NULL OR activations IS NOT NULL
order by date desc,	unit,	user_locale,	value