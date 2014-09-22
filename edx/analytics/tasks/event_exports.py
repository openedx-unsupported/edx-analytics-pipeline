"""Group events by institution and export them for research purposes"""

import logging
import os

import gnupg
import luigi
import luigi.date_interval
import yaml
import gzip

from edx.analytics.tasks.encrypt import make_encrypted_file
from edx.analytics.tasks.mapreduce import MultiOutputMapReduceJobTask
from edx.analytics.tasks.pathutil import EventLogSelectionMixin
from edx.analytics.tasks.url import url_path_join, ExternalURL, get_target_from_url
import edx.analytics.tasks.util.opaque_key_util as opaque_key_util

log = logging.getLogger(__name__)


class EventExportTask(EventLogSelectionMixin, MultiOutputMapReduceJobTask):
    """
    Group events by institution and export them for research purposes.

    Parameters:
        output_root: Directory to store the output in.
        config: A URL to a YAML file that contains the list of organizations and servers to export events for.
        org_id: A list of organizations to process data for. If provided, only these organizations will be processed.
            Otherwise, all valid organizations will be processed.
        environment: A single string that describe the single environment that generated the events.
        interval: The range of dates to export logs for.

        The following are defined in EventLogSelectionMixin:
        source: A URL to a path that contains log files that contain the events.
        pattern: A regex with a named capture group for the date that approximates the date that the events within were
            emitted. Note that the search interval is expanded, so events don't have to be in exactly the right file
            in order for them to be processed.

    """

    output_root = luigi.Parameter(
        default_from_config={'section': 'event-export', 'name': 'output_root'}
    )
    config = luigi.Parameter(
        default_from_config={'section': 'event-export', 'name': 'config'}
    )
    org_id = luigi.Parameter(is_list=True, default=[])

    gpg_key_dir = luigi.Parameter(
        default_from_config={'section': 'event-export', 'name': 'gpg_key_dir'}
    )
    gpg_master_key = luigi.Parameter(
        default_from_config={'section': 'event-export', 'name': 'gpg_master_key'}
    )
    environment = luigi.Parameter(
        default_from_config={'section': 'event-export', 'name': 'environment'}
    )

    required_path_text = luigi.Parameter(
        default_from_config={'section': 'event-export', 'name': 'required_path_text'}
    )

    def requires_local(self):
        return ExternalURL(url=self.config)

    def extra_modules(self):
        return [gnupg, yaml]

    def init_local(self):
        super(EventExportTask, self).init_local()

        with self.input_local().open() as config_input:
            config_data = yaml.load(config_input)
            self.organizations = config_data['organizations']

        # Map org_ids to recipient names, taking in to account org_id aliases. For example, if an org_id Foo is also
        # known as FooX then two entries will appear in this dictionary ('Foo', 'recipient@foo.org') and
        # ('FooX', 'recipient@foo.org'). Note that both aliases map to the same recipient.
        self.recipient_for_org_id = {}
        self.primary_org_id_for_org_id = {}
        for org_id, org_config in self.organizations.iteritems():
            recipient = org_config['recipient']
            self.recipient_for_org_id[org_id] = recipient
            self.primary_org_id_for_org_id[org_id] = org_id
            for alias in org_config.get('other_names', []):
                self.recipient_for_org_id[alias] = recipient
                self.primary_org_id_for_org_id[alias] = org_id

        self.org_id_whitelist = set(self.recipient_for_org_id.keys())

        # If org_ids are specified, restrict the processed files to that set.
        if len(self.org_id) > 0:
            self.org_id_whitelist.intersection_update(self.org_id)

        log.debug('Using org_id whitelist ["%s"]', '", "'.join(self.org_id_whitelist))

    def mapper(self, line):
        value = self.get_event_and_date_string(line)
        if value is None:
            return
        event, date_string = value

        org_id = self.get_org_id(event)
        if org_id not in self.org_id_whitelist:
            log.debug('Unrecognized organization: org_id=%s', org_id or '')
            return
        # Check to see if the org_id is one that should be grouped with other org_ids.
        org_id = self.primary_org_id_for_org_id[org_id]

        if not self.is_valid_input_file():
            return

        key = (date_string, org_id)
        # Enforce a standard encoding for the parts of the key. Without this a part of the key might appear differently
        # in the key string when it is coerced to a string by luigi. For example, if the same org_id appears in two
        # different records, one as a str() type and the other a unicode() then without this change they would appear as
        # u'FooX' and 'FooX' in the final key string. Although python doesn't care about this difference, hadoop does,
        # and will bucket the values separately. Which is not what we want.
        yield tuple([value.encode('utf8') for value in key]), line.strip()

    def is_valid_input_file(self):
        """
        Return True iff the contents of the input file being processed should be included in the export.

        """
        try:
            # Hadoop sets an environment variable with the full URL of the input file. This url will be something like:
            # s3://bucket/root/host1/tracking.log.gz. In this example, assume self.source is "s3://bucket/root".
            return self.required_path_text in os.environ['map_input_file']
        except KeyError:
            log.warn('map_input_file not defined in os.environ, unable to determine input file path')
            return False

    def output_path_for_key(self, key):
        date, org_id = key
        year = str(date).split("-")[0]

        # remap site name from prod to edx
        site = "edx" if self.environment == 'prod' else self.environment

        # This is the structure currently produced by the existing tracking log export script
        return url_path_join(
            self.output_root,
            org_id.lower(),
            site,
            "events",
            year,
            '{org}-{site}-events-{date}.log.gz.gpg'.format(
                org=org_id.lower(),
                site=site,
                date=date,

            )
        )

    def multi_output_reducer(self, key, values, output_file):
        """
        Write values to the appropriate file as determined by the key.
        Write to the encrypted file by streaming through gzip, which compresses before encrypting
        """
        _date_string, org_id = key
        recipients = self._get_recipients(org_id)

        key_file_targets = [get_target_from_url(url_path_join(self.gpg_key_dir, recipient)) for recipient in recipients]
        with make_encrypted_file(output_file, key_file_targets) as encrypted_output_file:
            outfile = gzip.GzipFile(mode='wb', fileobj=encrypted_output_file)
            try:
                for value in values:
                    outfile.write(value.strip())
                    outfile.write('\n')
            finally:
                outfile.close()

    def _get_recipients(self, org_id):
        """Get the correct recipients for the specified organization."""
        recipients = [self.recipient_for_org_id[org_id]]
        if self.gpg_master_key is not None:
            recipients.append(self.gpg_master_key)
        return recipients

    def get_org_id(self, item):
        """
        Attempt to determine the organization that is associated with this particular event.

        This method may return incorrect results, so a white list of
        valid organization names is used to filter out the noise.

        None is returned if no org information is found in the item.
        """
        def get_slash_value(input_value, index):
            """Return index value after splitting input on slashes."""
            try:
                return input_value.split('/')[index]
            except IndexError:
                return None

        try:
            # Different behavior based on type of event source.
            if item['event_source'] == 'server':
                # Always check context first for server events.
                org_id = item.get('context', {}).get('org_id')
                if org_id:
                    return org_id

                # Try to infer the institution from the event data
                evt_type = item['event_type']
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
                        return get_slash_value(item['event']['problem_id'], 2)
                    except Exception:  # pylint: disable=broad-except
                        return None
            elif item['event_source'] == 'browser':
                # Note that the context of browser events is ignored.
                page = item['page']
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
            else:
                # TODO: Handle other event source values (e.g. task or mobile).
                return None

        except Exception:  # pylint: disable=broad-except
            log.exception('Unable to determine institution for event: %s', unicode(item).encode('utf8'))

        return None
