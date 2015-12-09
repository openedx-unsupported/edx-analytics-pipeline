
class HiveService(object):

    def __init__(self, task, config, database_name):
        self.task = task
        self.config = config
        self.database_name = database_name

    def reset(self):
        return self.execute(
            "DROP DATABASE IF EXISTS {0} CASCADE; CREATE DATABASE {0}".format(self.database_name),
            explicit_db=False
        )

    def execute(self, statement, explicit_db=True):
        db_parameter = ' --database ' + self.database_name if explicit_db else ''
        return self.task.launch([
            '--user', self.config['connection_user'],
            '--sudo-user', self.config['hive_user'],
            '--shell', ". $HOME/.bashrc && hive --service cli{db} -e \"{stmt}\"".format(
                db=db_parameter,
                stmt=statement
            ),
        ])

    def execute_many(self, statements):
        return self.execute(';'.join(statements))

    def drop_table(self, table_name):
        return self.drop_table_many([table_name])

    def drop_table_many(self, table_names):
        return self.execute_many(['DROP TABLE IF EXISTS ' + name for name in table_names])
