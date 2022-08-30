class SqlQueries:

    """
    - Write Insert queries for all the data tables to be worked on
    """

    fact_table_insert = ("""
        INSERT INTO table (id_col, datetime_col, col1, col2, col3, col4, col5, col6, col7, col8, col9)
        SELECT
            md5(table.id_col || datetime_col)
            table.
        
        """)