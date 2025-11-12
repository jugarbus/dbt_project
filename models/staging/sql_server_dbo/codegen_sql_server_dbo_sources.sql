-- Para generar de forma automáticas los sources con sus campos para rellenar (te hace la estructura)

{{
    codegen.generate_source(
        schema_name = 'sql_server_dbo',
        database_name = 'ALUMNO20_DEV_BRONZE_DB',
        table_names = ['orders','order_items', 'users'],
        generate_columns = True,
        include_descriptions=True,
        include_data_types=True,
        name='desarrollo',
        include_database=True,
        include_schema=True
        )
}}