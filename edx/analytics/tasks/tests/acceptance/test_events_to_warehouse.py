"""
End-to-end test of the preliminary event-loading infrastructure for loading event logs into Vertica.
"""
import os
import logging
import datetime

import pandas

from edx.analytics.tasks.tests.acceptance import AcceptanceTestCase
from edx.analytics.tasks.url import url_path_join

log = logging.getLogger(__name__)


class BaseEventsToWarehouseAcceptanceTest(AcceptanceTestCase):
    """Base class for the end-to-end test of event loading tasks."""

    INPUT_FILE = 'events_to_warehouse_acceptance_tracking.log.gz'
    CANON_INPUT_FILE = 'part-00000-canonicalized'

    def setUp(self):
        super(BaseEventsToWarehouseAcceptanceTest, self).setUp()

        assert 'oddjob_jar' in self.config

        self.oddjob_jar = self.config['oddjob_jar']

        self.upload_data()
        self.upload_tracking_log(self.INPUT_FILE, datetime.date(2014, 8, 21))

    def upload_data(self):
        """Puts the test course catalog where the processing task would look for it, bypassing calling the actual API"""
        src = os.path.join(self.data_dir, 'input', self.INPUT_FILE)
        src2 = os.path.join(self.data_dir, 'input', self.CANON_INPUT_FILE)
        src3 = os.path.join(self.data_dir, 'input', '_SUCCESS')
        dst = url_path_join(self.warehouse_path, 'src', self.INPUT_FILE)
        dst2 = url_path_join(self.warehouse_path, 'tracking-logs', self.INPUT_FILE)
        dst3 = url_path_join(self.warehouse_path, 'events', 'dt=2014-08-21', self.CANON_INPUT_FILE)
        dst4 = url_path_join(self.warehouse_path, 'events', 'dt=2014-08-21', '_SUCCESS')

        # Upload mocked results of the API call
        self.s3_client.put(src, dst)
        self.s3_client.put(src, dst2)
        # self.s3_client.put(src2, dst3)
        # self.s3_client.put(src3, dst4)


class EventsToFlexTableAcceptanceTest(BaseEventsToWarehouseAcceptanceTest):
    """End-to-end test of pulling the event data into Vertica flex tables."""

    def test_events_to_warehouse_flex_table(self):
        """Tests the workflow for the course subjects, end to end."""

        self.task.launch([
            'VerticaEventLoadingWorkflow',
            '--credentials', self.vertica.vertica_creds_url,
            '--interval', '2014-08-20-2014-08-25',
            '--use-flex', '--remove-implicit',
            '--overwrite',
            '--n-reduce-tasks', '1'
        ])

        self.validate_output()

        # Drop the table afterwards so that the next test starts with a clean table.
        with self.vertica.cursor() as cursor:
            cursor.execute("DROP TABLE IF EXISTS {schema}.event_logs".format(schema=self.vertica.schema_name))

    def validate_output(self):
        """Validates the output, comparing it to a csv of all the expected events in this name."""
        with self.vertica.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM {schema}.event_logs;"
                           .format(schema=self.vertica.schema_name))
            total_count = cursor.fetchone()[0]
            print "FOUND ", total_count, " ROWS"
            expected_total_count = 3
            # self.assertEqual(total_count, expected_total_count)

            # Verify that all the subject data loaded into Vertica is correct.
            expected_output_csv = os.path.join(self.data_dir, 'output',
                                               'events_to_warehouse_acceptance_flex_events.csv')
            expected = pandas.read_csv(expected_output_csv)

            # cursor.execute("SELECT \"agent.type\", \"agent.device_name\", \"agent.os\", \"agent.browser\", "
            #                "\"agent.touch_capable\", event_type, event_source, host, ip,"
            #                "page, \"time\", username, \"context.course_id\", \"context.org_id\", "
            #                "\"context.user_id\", \"context.path\" FROM {schema}.event_logs;"
            #                .format(schema=self.vertica.schema_name))
            cursor.execute("SELECT * FROM {schema}.event_logs;".format(schema=self.vertica.schema_name))
            events = cursor.fetchall()

            print events

            cleaned_events = [event[1:] for event in events]

            events_df = pandas.DataFrame(cleaned_events, ['agent.type', 'agent.device_name', 'agent.os', 'agent.browser',
                                                  'agent.touch_capable', 'event_type', 'event_source', 'host', 'ip',
                                                  'page', 'time', 'username', 'context.course_id', 'context.org_id',
                                                  'context.user_id', 'context.path'])

            print "OBSERVED:"
            print events
            print "EXPECTED:"
            print expected

            try:
                self.assertTrue(all(events_df == expected))
            except ValueError:
                self.fail("Expected and returned data frames have different shapes or labels.")
