class SQLQueryGenerator():

    def generate_get_count(self, sql_query: str):
        return f"SELECT COUNT(*) AS num_results FROM ( {sql_query} ) AS subquery;"

    def generate_get_sorted_publs(self, values: list[int]):
        select_string = 'SELECT publs.id AS "publication_id", publs.title AS "title"' + ", "
        select_substrings = ', '.join([f'd{i}.factor_id AS "factor_id_{i}", dnv{
            i}.variant AS "term_name_{i}", d{i}.value AS "value{i}"' for i in range(len(values))]) + ", "
        total_string = ' + '.join([f"d{i}.value" for i in range(len(values))]
                                  ) + " " 'AS "total_value"' + " "
        from_string = "FROM deltas AS d0\n"
        publs_join = "JOIN publications AS publs ON d0.publication_id = publs.id" + "\n"
        deltas_join_substrings = "\n".join([f'INNER JOIN deltas AS d{i} ON d0.publication_id = d{
            i}.publication_id AND d{i}.factor_id = {values[i]}' for i in range(1, len(values))]) + "\n"
        factors_names_substrings = "\n".join([f'INNER JOIN factor_name_variants AS dnv{
            i} ON d{i}.factor_id = dnv{i}.factor_id' for i in range(0, len(values))]) + "\n"
        where_string = f"WHERE d0.factor_id = {values[0]}" + "\n"
        group_by = "GROUP BY publs.id" + ", "
        group_by_substrings = ', '.join([f'd{i}.factor_id, dnv{
            i}.variant, d{i}.value' for i in range(len(values))]) + "\n"
        order_by = 'ORDER BY "total_value" DESC' + "\n"
        # limit = f'LIMIT {10 * len(values)};'
        limit = f'LIMIT 200'

        query_string = select_string + select_substrings + total_string + \
            from_string + publs_join + deltas_join_substrings + \
            factors_names_substrings + where_string + \
            group_by + group_by_substrings + order_by + limit

        query_string_for_count = select_string + select_substrings + total_string + \
            from_string + publs_join + deltas_join_substrings + \
            factors_names_substrings + where_string + \
            group_by + group_by_substrings + order_by
        count_query = self.generate_get_count(query_string_for_count)

        return query_string, count_query

    def generate_get_sorted_publs_from_string(self, values: list[int]):
        select_string = 'SELECT publs.id AS "publication_id", publs.title AS "title"' + ", "
        select_substrings = ', '.join([f'd{i}.factor_id AS "factor_id_{i}", dnv{
            i}.variant AS "term_name_{i}", d{i}.value AS "value{i}"' for i in range(len(values))]) + ", "
        total_string = ' + '.join([f"d{i}.value" for i in range(len(values))]
                                  ) + " " 'AS "total_value"' + " "
        from_string = "FROM deltas AS d0\n"
        publs_join = "JOIN publications AS publs ON d0.publication_id = publs.id" + "\n"
        deltas_join_substrings = "\n".join([f'INNER JOIN deltas AS d{i} ON d0.publication_id = d{
            i}.publication_id' for i in range(1, len(values))]) + "\n"
        factors_names_substrings = "\n".join([f'INNER JOIN factor_name_variants AS dnv{
            i} ON d{i}.factor_id = dnv{i}.factor_id' for i in range(0, len(values))]) + "\n"
        where_string = "WHERE" + \
            " " + 'AND '.join(
                [f"dnv{i}.variant = '{values[i]}'" for i in range(len(values))]) + "\n"
        group_by = "GROUP BY publs.id" + ", "
        group_by_substrings = ', '.join([f'd{i}.factor_id, dnv{
            i}.variant, d{i}.value' for i in range(len(values))]) + "\n"
        order_by = 'ORDER BY "total_value" DESC' + "\n"
        # limit = f'LIMIT {10 * len(values)};'
        limit = f'LIMIT {200}' + '\n'
        offset = f'OFFSET {0}'

        query_string_for_count = select_string + select_substrings + total_string + \
            from_string + publs_join + deltas_join_substrings + \
            factors_names_substrings + where_string + \
            group_by + group_by_substrings + order_by

        query_string = query_string_for_count + limit
        count_query = self.generate_get_count(query_string_for_count)
        return query_string, count_query
