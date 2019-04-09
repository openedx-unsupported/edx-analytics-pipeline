"""Group events by institution and export them for research purposes"""

import gzip
import logging
from collections import defaultdict

import gnupg
import luigi.date_interval
import yaml

import edx.analytics.tasks.util.opaque_key_util as opaque_key_util
from edx.analytics.tasks.common.mapreduce import MultiOutputMapReduceJobTask
from edx.analytics.tasks.common.pathutil import EventLogSelectionMixin
from edx.analytics.tasks.util import eventlog
from edx.analytics.tasks.util.encrypt import make_encrypted_file
from edx.analytics.tasks.util.url import ExternalURL, get_target_from_url, url_path_join

log = logging.getLogger(__name__)


class EventExportTask(EventLogSelectionMixin, MultiOutputMapReduceJobTask):
    """
    Group events by institution and export them for research purposes.

    """

    output_root = luigi.Parameter(
        config_path={'section': 'event-export', 'name': 'output_root'},
        description='Directory to store the output in.',
    )
    config = luigi.Parameter(
        config_path={'section': 'event-export', 'name': 'config'},
        description='A URL to a YAML file that contains the list of organizations and servers to export events for.',
    )
    org_id = luigi.ListParameter(
        default=[],
        description='A list of organizations to process data for. If provided, only these organizations will be '
        'processed.  Otherwise, all valid organizations will be processed.',
    )
    gpg_key_dir = luigi.Parameter(
        config_path={'section': 'event-export', 'name': 'gpg_key_dir'},
    )
    gpg_master_key = luigi.Parameter(
        config_path={'section': 'event-export', 'name': 'gpg_master_key'},
    )
    environment = luigi.Parameter(
        config_path={'section': 'event-export', 'name': 'environment'},
        description='A single string that describe the single environment that generated the events.',
    )
    required_path_text = luigi.Parameter(
        config_path={'section': 'event-export', 'name': 'required_path_text'},
    )

    def requires_local(self):
        return ExternalURL(url=self.config)

    def extra_modules(self):
        return [gnupg, yaml]

    def init_local(self):
        super(EventExportTask, self).init_local()

        with self.input_local().open() as config_input:
            config_data = yaml.load(config_input)
            organizations = config_data['organizations']

        # If org_ids are specified, restrict the processed files to that set of primary org ids.
        if self.org_id:
            organizations = {k: v for k, v in organizations.iteritems() if k in self.org_id}

        self.recipients_for_org_id = {}
        self.courses_for_org_id = {}
        self.primary_org_ids_for_org_id = defaultdict(list)
        self.org_id_whitelist = set()

        for org_id, org_config in organizations.iteritems():
            aliases = [org_id] + org_config.get('other_names', [])

            self.recipients_for_org_id[org_id] = set(org_config.get('recipients', []))
            if self.gpg_master_key is not None:
                self.recipients_for_org_id[org_id].add(self.gpg_master_key)

            self.courses_for_org_id[org_id] = org_config.get('courses')

            for alias in aliases:
                self.org_id_whitelist.add(alias)
                self.primary_org_ids_for_org_id[alias].append(org_id)

        log.debug('Using org_id whitelist ["%s"]', '", "'.join(self.org_id_whitelist))

    def mapper(self, line):
        event, date_string = self.get_event_and_date_string(line) or (None, None)
        if event is None:
            return

        if not self.is_valid_input_file():
            return

        org_id = self.get_org_id(event)
        if org_id not in self.org_id_whitelist:
            log.debug('Unrecognized organization: org_id=%s', org_id or '')
            return

        # Do not export events that have been explicitly flagged as not being for export.
        # Any event without an '_export' key will be sent as part of the export by default,
        # and likewise any event without a falsey value. The preferred value to not export is 'false'.
        event_data = eventlog.get_event_data(event)
        if event_data and str(event_data.get('_export', 'true')).lower() in ('n', 'f', '0', 'false', 'no'):
            return

        # Check to see if the org_id is one that should be grouped with other org_ids.
        org_ids = self.primary_org_ids_for_org_id[org_id]

        for key_org_id in org_ids:
            key = (date_string, key_org_id)

            # Include only requested courses
            requested_courses = self.courses_for_org_id.get(key_org_id)
            if requested_courses and eventlog.get_course_id(event, from_url=True) not in requested_courses:
                continue

            # Enforce a standard encoding for the parts of the key. Without this a part of the key
            # might appear differently in the key string when it is coerced to a string by luigi. For example,
            # if the same org_id appears in two different records, one as a str() type and the other a
            # unicode() then without this change they would appear as u'FooX' and 'FooX' in the final key
            # string. Although python doesn't care about this difference, hadoop does, and will bucket the
            # values separately. Which is not what we want.
            yield tuple([value.encode('utf8') for value in key]), line.strip()

    def get_event_time(self, event):
        # Some events may emitted and stored for quite some time before actually being entered into the tracking logs.
        # The primary cause of this is mobile devices that go offline for a significant period of time. They will store
        # events locally and then when connectivity is restored transmit them to the server. We log the time that they
        # were received by the server and use that to batch them into exports since it is much simpler than trying to
        # inject them into past exports.
        try:
            return event['context']['received_at']
        except KeyError:
            return super(EventExportTask, self).get_event_time(event)

    def is_valid_input_file(self):
        """
        Return True iff the contents of the input file being processed should be included in the export.

        """
        return self.required_path_text in self.get_map_input_file()

    def output_path_for_key(self, key):
        date, org_id = key
        year = str(date).split("-")[0]

        # This is the structure currently produced by the existing tracking log export script
        return url_path_join(
            self.output_root,
            org_id.lower(),
            self.environment,
            "events",
            year,
            '{org}-{site}-events-{date}.log.gz.gpg'.format(
                org=org_id.lower(),
                site=self.environment,
                date=date,

            )
        )

    def event_export_counter(self, counter_title, incr_value=1):
        """
        A shorthand hadoop counter incrementer that organizes counters under a common title.
        """
        self.incr_counter("Event Export", counter_title, incr_value)

    def multi_output_reducer(self, key, values, output_file):
        """
        Write values to the appropriate file as determined by the key.
        Write to the encrypted file by streaming through gzip, which compresses before encrypting
        """
        _date_string, org_id = key
        recipients = self.recipients_for_org_id[org_id]
        log.info('Encryption recipients: %s', str(recipients))

        def report_progress(num_bytes):
            """Update hadoop counters as the file is written"""
            self.event_export_counter(counter_title='Bytes Written to Output', incr_value=num_bytes)

        key_file_targets = [get_target_from_url(url_path_join(self.gpg_key_dir, recipient)) for recipient in recipients]
        try:
            with make_encrypted_file(output_file, key_file_targets, progress=report_progress,
                                     hadoop_counter_incr_func=self.event_export_counter) as encrypted_output_file:
                outfile = gzip.GzipFile(mode='wb', fileobj=encrypted_output_file)
                try:
                    for value in values:
                        outfile.write(value.strip())
                        outfile.write('\n')
                        # WARNING: This line ensures that Hadoop knows that our process is not sitting in an infinite
                        # loop.  Do not remove it.
                        self.event_export_counter(counter_title='Raw Bytes Written', incr_value=(len(value) + 1))
                finally:
                    outfile.close()
        except IOError as err:
            log.error("Error encountered while encrypting and gzipping Organization: %s file: %s Exception: %s",
                      org_id, key_file_targets, err)
            # This counter is set when there is an error during the generation of the encryption file for an
            # organization for any reason, including encryption errors related to an expired GPG key.
            self.event_export_counter(counter_title="{} org with Errors".format(org_id), incr_value=1)

    def get_org_id(self, event):
        """
        Attempt to determine the organization that is associated with this particular event.

        This method may return incorrect results, so a white list of
        valid organization names is used to filter out the noise.

        None is returned if no org information is found in the item.
        """

        try:
            # Different behavior based on type of event source.
            if event['event_source'] == 'server':
                return self._parse_server_event(event)
            elif event['event_source'] == 'browser':
                return self._parse_browser_event(event)
            elif event['event_source'] == 'mobile':
                return self._parse_mobile_event(event)
            else:
                # TODO: Handle other event source values (e.g. task).
                return None
        except Exception:  # pylint: disable=broad-except
            log.exception('Unable to determine organization for event: %s', unicode(event).encode('utf8'))

        return None

    def _parse_server_event(self, event):
        # Always check context first for server events.
        org_id = event.get('context', {}).get('org_id')
        if org_id:
            return org_id

        # Try to infer the institution from the event data
        evt_type = event['event_type']
        if '/courses/' in evt_type:
            course_key = opaque_key_util.get_course_key_from_url(evt_type)
            if course_key and '/' not in unicode(course_key):
                return course_key.org
            else:
                # It doesn't matter if we found a good deprecated key.
                # We need to provide backwards-compatibility.
                return get_slash_value(evt_type, 2)
        elif '/' in evt_type:
            return None
        else:
            # Specific server logging. One-off parser for each type.
            # Survey of logs showed 4 event types:
            # reset_problem, save_problem_check,
            # save_problem_check_fail, save_problem_fail.  All
            # four of these have a problem_id, which for legacy events
            # we could extract from.  For newer events, we assume this
            # won't be needed, because context will be present.
            try:
                return get_slash_value(event['event']['problem_id'], 2)
            except Exception:  # pylint: disable=broad-except
                return None

        return None

    def _parse_browser_event(self, event):
        # TODO: Note that for browser events we are not using the org_id from the context.

        page = event['page']
        if 'courses' in page:
            # This is different than the original algorithm in that it assumes
            # the page contains a valid coursename.  The original code
            # merely looked for what followed "http[s]://<host>/courses/"
            # (and also hoped there were no extra slashes or different content).
            course_key = opaque_key_util.get_course_key_from_url(page)
            if course_key and '/' not in unicode(course_key):
                return course_key.org
            else:
                # It doesn't matter if we found a good deprecated key.
                # We need to provide backwards-compatibility.
                return get_slash_value(page, 4)

        return None

    def _parse_mobile_event(self, event):
        org_id = event.get('context', {}).get('org_id')
        if org_id:
            return org_id

        return None


def get_slash_value(input_value, index):
    """Return index value after splitting input on slashes."""
    try:
        return input_value.split('/')[index]
    except IndexError:
        return None
