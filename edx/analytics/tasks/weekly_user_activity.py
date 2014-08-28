
"""
s3://edx-analytics-scratch/calendar/dt=2014-08-28/calendar_table.txt
"""

class ImportCalendarTable(ImportIntoHiveTableTask):
	property
    def table_name(self):
        return 'calendar'

    @property
    def columns(self):
        return [
            ('date', 'STRING')
        ]

    @property
    def table_location(self):
    	directory = "s3://edx-analytics-scratch/"
        return url_path_join(directory, table_name)

    @property
    def table_format(self):
        """Provides structure of Hive external table data."""
        return "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'"



class WeeklyActivityToHiveTask(EventLogSelectionDownstreamMixin,
        MapReduceJobTaskMixin,
        ImportIntoHiveTableTask):

	@property
    def table_name(self):
        return 'weekly_user_activity'

    @property
    def columns(self):
        return [
            ('date', 'STRING'),
            ('course_id', 'STRING'),
            ('category', 'STRING'),
            ('count', 'INT')
        ]

    @property
    def table_location(self):
        output_name = 'weekly-user-activity/'
        return url_path_join(self.warehouse_path, output_name)

    @property
    def table_format(self):
        """Provides structure of Hive external table data."""
        return "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'"

    @property
    def partition(self):
        """Provides name of Hive database table partition.
        This overrides the default method to partition on an interval instead of a single date """
        # The Luigi hive code expects partitions to be defined by dictionaries.
        return {'interval': str(self.interval)}

	def requires(self):
		return ImportDailyUserActivityToHiveTask(
			source=self.source,
	        interval=self.interval,
	        pattern=self.pattern,
	        n_reduce_tasks=self.n_reduce_tasks,
	        warehouse_path=self.warehouse_path,
	        overwrite=self.overwrite
    	), ImportCalendarTable()




first: group all by category, date,


# what is the parameter that does 180?

# still have to join with calendar table?

FROM (
    SELECT course_id, category, date, date_sub(from_unixtime(date), 7) AS 7
    FROM daily_user_activity
) y

select y.date, y.course_id, y.category, count(distinct x.user_id)
from daily_user_activity x
join y on x.course_id = y.course_id, y.category = x.category
where x.date > y.180_days_ago

group by y.course_id, y.date, y.category;



from table z
	join

GROUP BY

select

date course_id category count



select a.date, b.course_id, b.category, b.username, b.count
from calendar_table
left outer join daily_user_activity
on calendar_table.date =

select y.date, y.course_id, y.category, count(distinct username)
from daily_user_activity_calendar
over (order by date rows between current row and 7 preceding)
group by y.course_id, y.calendar