# DRF
### view на пост запрос
```
class AnalysisCategoriesApiView(GenericAPIView):
    permission_classes = (IsActivePeriodPermission,)
    serializer_class = ReportQuerySerializer
    queryset = []

    def post(self, request, *args, **kwargs):
        serializer = self.serializer_class(data=request.data, context={'request': request})
        serializer.is_valid(raise_exception=True)
        report = AnalysisCategoriesReport(serializer.data)
        data = report.get_result()

        if 'error' in data:
            return Response(data, status=status.HTTP_404_NOT_FOUND)

        return Response(data, status=status.HTTP_200_OK)
```
### Класс с бизнес логикой
```
class AnalysisCategoriesReport(Report):
    def __init__(self, request_data, **kwargs):
        super().__init__(request_data, **kwargs)

    def get_sql(self):
        if not self.request_data.get('category_keys', False):
            SQL = queries.sql_analysis_categories_root.get(self.site_name)
            category_key = None
        else:
            SQL = queries.sql_analysis_categories.get(self.site_name)
            category_key = self.request_data.get('category_keys')[0]
        if SQL is None:
            return None
        SQL = SQL.format(
            categories_hierarchical=self.db_config.get('categories_hierarchical'),
            agg_categories=self.db_config.get('agg_categories'),
            categories_dict=self.db_config.get('categories_dict'),
            db_name=self.db_config.get('db_name'),
            categories_table_name=self.db_config.get('categories_table_name'),
            category_key=category_key
        )
        return SQL

    def get_data(self):
        SQL = self.get_sql()
        data = self.clickhouse.query(SQL, format_type='JSON', data_only=True, no_cache=False)
        return data

    def prepare_data(self):
        data = self.get_data()
        if len(data) == 0:
            return {'error': 'no data for the period'}
        for category in data:
            for cat in category:
                if isinstance(category[cat], str) and category[cat].isdigit():
                    category[cat] = int(category[cat])
        return data

    def get_result(self):
        return self.prepare_data()
```

### SQL запрос ( clickhouse )
```
SELECT category_key,
       category_name,
       sum_orders,
       count_product,
       count_sell_product,
       competition,
       redemption,
       median,
       sku,
       avg_weight,
       trend,
       has_children
FROM (
      SELECT filtered_key                                                        as category_key,
             dictGet('{categories_dict}', 'sorting_key', category_key)           as position,
             dictGet('{categories_dict}', 'name', category_key)                  as category_name,
             dictGet('{categories_dict}', 'has_children', category_key)          as has_children,
             groupArray(orders_rub)                                              as price_array,
             sum(orders)                                                         as sum_orders,
             sum(count_product)                                                  as count_product,
             sum(count_sell_product)                                             as count_sell_product,
             round(argMax(competition, event_date))                              as competition,
             round(avg(redemption))                                              as redemption,
             round(avg(median))                                                  as median,
             argMax(sku, event_date)                                             as sku,
             round(avg(avg_weight_price))                                        as avg_weight,
             flatten(price_array)                                                as group_sales,
             tupleElement(
                     arrayReduce('simpleLinearRegression', groupArray(toUInt64(toDateTime(event_date))), group_sales),
                     1)                                                          as percent_category,
             round(multiIf(isFinite(percent_category) = 0, 0, percent_category)) as trend
      FROM (SELECT category_key
                 , arrayReverse(dictGetHierarchy('{categories_hierarchical}',
                                                 toUInt64(category_key)))        as all_categories
                 , arrayElement(
                  all_categories, indexOf(all_categories, {category_key}) + 1
              )                                                                  as filtered_key
                 , orders
                 , event_date
                 , orders_rub
                 , count_product
                 , count_sell_product
                 , redemption
                 , competition
                 , median
                 , sku
                 , avg_weight_price
            FROM {db_name}.{agg_categories}
            WHERE event_date BETWEEN today() - 30 AND today()
              AND category_key IN (
                SELECT toUInt64(key) as key
                FROM (
                      SELECT key,
                             dictGetHierarchy('{categories_hierarchical}', toUInt64(key)) as all_category
                      FROM {db_name}.{categories_table_name}
                      WHERE has_children = 0
                        AND hasAny(all_category, [{category_key}])
                        AND event_date BETWEEN today() - 30 AND today()
                         ))
            ORDER BY event_date DESC
               )
      GROUP BY filtered_key)
```