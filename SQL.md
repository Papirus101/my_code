# Запрос на агрегацию данных и заполнение таблицы
```
INSERT INTO {db_name}.{agg_categories}
SELECT event_date,
       category_key,
       total_orders      as orders,
       total_price       as orders_rub,
       count_vendor_code as count_product,
       count_sell_product,
       competition   as competition,
       redemption,
       median        as median,
       sku,
       avg_price
FROM (
      SELECT event_date,
             -- Категория
             arrayJoin(dictGet('{vendor_data_dict}', 'category_keys', vendor_code)) as category_key,
             groupArray(price_discounted)                                           as array_price,
             arrayReverse(arraySort(x -> x, array_price))                           as array_price_sort,
             -- SKU
             uniq(vendor_code)                                                      as sku,
             -- Средневзвешенная цена
             avgWeighted(price_discounted, sum_orders)                              as avg_weight_price,
             round(multiIf(isFinite(avg_weight_price) = 0, 0, avg_weight_price))    as avg_price,
             -- Общее число продаж
             sum(sum_orders)                                                        as total_orders,
             -- Кол-во товаров в категории
             count(vendor_code)                                                     as count_vendor_code,
             -- Сумма продаж в категории
             sum(price_discounted * sum_orders)                                     as total_price,
             -- Количество проданных товаров
             countIf(vendor_code, sum_orders > 0)                                   as count_sell_product,
             -- Медиана заказов
             quantileIf(0.5)(sum_orders, sum_orders > 0)                            as median,
             -- Доминирование лидера
             arrayCumSum(array_price_sort)                                          as cum_sum_orders,
             total_price / 100 * 80                                                 as percent,
             arrayFilter(x -> x <= percent, cum_sum_orders)                         as cum_sum_orders_80,
             length(cum_sum_orders_80)                                              as count_product_80,
             round((count_product_80 / sku) * 100)                                  as competition,
             -- Выкупаемость
             round((count_sell_product / sku) * 100)                                as redemption
      FROM (
            SELECT *
            FROM {db_name}.{main_agg}
            WHERE event_date = '{date}'
            ORDER BY sum_orders DESC
               )
      GROUP BY event_date, category_key
         )
```