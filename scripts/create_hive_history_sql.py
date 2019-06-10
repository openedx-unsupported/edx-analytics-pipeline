# S3 path to the data that's dumped from MySQL daily and loaded into Vertica.
BASE_S3_REPLICA_LOCATION = 's3://edx-analytics-data/prod/warehouse/import_mysql_to_vertica'

# S3 path to a scratch location where to store history data.
BASE_S3_SCRATCH_LOCATION = 's3://edx-analytics-scratch/juliasq/create_history'

# Mapping of tables to their origin database and their column names.
TABLES_TO_COLUMNS = {
    'voucher_couponvouchers_vouchers': {
        'ida': 'ecommerce',
        'columns': (
            'couponvouchers_id',
            'voucher_id',
        ),
    },
    'course_metadata_course_authoring_organizations': {
        'ida': 'discovery',
        'columns': (
            'course_id',
            'organization_id',
            'sort_value',
        )
    },
    'course_metadata_program_authoring_organizations': {
        'ida': 'discovery',
        'columns': (
            'course_id',
            'organization_id',
            'sort_value',
        )
    },
    'course_metadata_program_courses': {
        'ida': 'discovery',
        'columns': (
            'program_id',
            'course_id',
            'sort_value',
        )
    },
    'course_metadata_program_excluded_course_runs': {
        'ida': 'discovery',
        'columns': (
            'program_id',
            'courserun_id',
        )
    },
    'course_metadata_programtype_applicable_seat_types': {
        'ida': 'discovery',
        'columns': (
            'programtype_id',
            'seattype_id',
        )
    },
    'student_courseenrollment': {
        'ida': 'wwc',
        'columns': (
            'user_id',
            'course_id',
            'created',
            'is_active',
            'mode'
        )
    },
    'waffle_flag': {
        'ida': 'wwc',
        'columns': (
            'name', 'everyone', 'percent', 'testing', 'superusers', 'staff', 'authenticated', 'languages', 'rollout', 'note', 'created', 'modified'
        )
    }
}


# String format template used to create the SQL for each history run.
HIVE_HISTORY_SQL = """
  CREATE EXTERNAL TABLE `{table_name}` (
    id INT,
    {column_defs}
  )
  PARTITIONED BY (`dt` STRING)
  ROW FORMAT DELIMITED
  LOCATION '{base_s3_replica_location}/{application}/{table_name}';

  MSCK REPAIR TABLE {table_name};

  CREATE EXTERNAL TABLE `{table_name}_myhistory` (
    id INT,
    history_id STRING,
    history_date STRING,
    history_type STRING,
    {column_defs},
    {history_column_defs}
  )
  ROW FORMAT DELIMITED
  LOCATION '{base_s3_scratch_location}/{application}/{table_name}_myhistory';

  INSERT INTO TABLE `{table_name}_myhistory`
    SELECT
        id,
        'NNULLL' AS history_id,
        dt AS history_date,
        IF(t.lag_dt = 'NNULLL', '+', '~') AS history_type,
        {columns},
        {nulled_history_columns}
    FROM (
        SELECT
            COALESCE(id, 'NNULLL') AS id,
            COALESCE(LAG(id) OVER w, 'NNULLL') AS lag_id,
            COALESCE(dt, 'NNULLL') AS dt,
            COALESCE(LAG(dt) OVER w, 'NNULLL') AS lag_dt,
            {coalesced_columns_with_lag}

         FROM {table_name}
         WINDOW w AS (PARTITION BY id ORDER BY dt)
      ) t
    WHERE
        t.id <> t.lag_id OR
        {column_lag_comparisons};

  SELECT
    history_date,
    COUNT(*) AS date_count
  FROM {table_name}_myhistory
  WHERE history_type <> '+'
  GROUP BY history_date
  ORDER BY date_count DESC
  LIMIT 30;
"""

# Loop over each table and output a SQL file to use Hive to find how many updated rows exist over time for each table.
for table, table_cfg in TABLES_TO_COLUMNS.items():
    column_defs = ', '.join(['{} STRING'.format(col) for col in table_cfg['columns']])
    history_column_defs = ', '.join(['history_{} STRING'.format(col) for col in table_cfg['columns']])
    columns = ', '.join(table_cfg['columns'])
    nulled_history_columns = ', '.join(["'NNULLL' AS history_{}".format(col) for col in table_cfg['columns']])
    coalesced_columns_with_lag = ', '.join(
        ["COALESCE({col}, 'NNULLL') AS {col}, COALESCE(LAG({col}) OVER w, 'NNULLL') AS lag_{col}".format(col=col) for col in table_cfg['columns']]
    )
    column_lag_comparisons = ' OR '.join(
        ['t.{col} <> t.lag_{col}'.format(col=col) for col in table_cfg['columns']]
    )
    with open('{}_history.sql'.format(table), 'w') as outfile:
        outfile.write(
            HIVE_HISTORY_SQL.format(
                table_name = table,
                column_defs = column_defs,
                base_s3_replica_location = BASE_S3_REPLICA_LOCATION,
                base_s3_scratch_location = BASE_S3_SCRATCH_LOCATION,
                application = table_cfg['ida'],
                history_column_defs = history_column_defs,
                columns = columns,
                nulled_history_columns = nulled_history_columns,
                coalesced_columns_with_lag = coalesced_columns_with_lag,
                column_lag_comparisons = column_lag_comparisons
            )
        )


