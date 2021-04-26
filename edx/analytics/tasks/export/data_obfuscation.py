"""Obfuscate course state files by removing and stubbing user information."""
import csv
import json
import logging
import os
import shutil
import tarfile
import tempfile
import xml.etree.ElementTree

import cjson
import luigi
import yaml

import edx.analytics.tasks.util.opaque_key_util as opaque_key_util
from edx.analytics.tasks.common.pathutil import PathSetTask
from edx.analytics.tasks.util.file_util import copy_file_to_file, read_config_file
from edx.analytics.tasks.util.obfuscate_util import (
    ObfuscatorDownstreamMixin, ObfuscatorMixin, backslash_decode_value, backslash_encode_value
)
from edx.analytics.tasks.util.tempdir import make_temp_directory
from edx.analytics.tasks.util.url import ExternalURL, get_target_from_url, url_path_join

log = logging.getLogger(__name__)


class BaseObfuscateDumpTask(ObfuscatorMixin, luigi.Task):
    """
    Base class for obfuscation of state files.
    """

    course = luigi.Parameter()
    output_directory = luigi.Parameter()
    data_directory = luigi.Parameter()

    def output(self):
        if len(self.input()['data']) == 0:
            raise IOError("Course File '{filename}' not found for course '{course}'".format(
                filename=self.file_pattern, course=self.course
            ))
        output_filename = os.path.basename(self.input()['data'][0].path)
        return get_target_from_url(url_path_join(self.output_directory, output_filename))

    def requires(self):
        base_reqs = {
            # We want to process files that are zero-length.
            'data': PathSetTask([self.data_directory], [self.file_pattern], include_zero_length=True)
        }
        base_reqs.update(self.user_info_requirements())
        return base_reqs

    @property
    def file_pattern(self):
        """Provides the file pattern for input file."""
        raise NotImplementedError

    @property
    def file_input_target(self):
        """Get filename of source for copying."""
        return self.input()['data'][0]


class ObfuscateSqlDumpTask(BaseObfuscateDumpTask):
    """Task to obfuscate an sql file."""

    def run(self):
        with self.output().open('w') as output_file:
            with self.input()['data'][0].open('r') as input_file:
                writer = csv.writer(output_file, dialect='mysqlexport')
                reader = csv.reader(input_file, dialect='mysqlexport')

                headers = next(reader, None)

                if headers:
                    writer.writerow(headers)

                for row in reader:
                    # Found an empty line at the bottom of test file
                    if row:
                        filtered_row = self.filter_row(row)
                        writer.writerow(filtered_row)

    def filter_row(self, row):
        """Function to deitentify a particular row."""
        raise NotImplementedError


class ObfuscateAuthUserProfileTask(ObfuscateSqlDumpTask):
    """Task to obfuscate auth_userprofile dump file."""

    @property
    def file_pattern(self):
        return '*-auth_userprofile-*'

    def filter_row(self, row):
        row[1] = self.remap_id(row[1])  # user_id
        row[2] = ''  # name
        row[3] = ''  # language
        row[4] = ''  # location
        row[5] = ''  # meta
        row[6] = ''  # courseware
        row[8] = ''  # mailing_address
        row[13] = ''  # city
        row[14] = ''  # bio

        return row


class ObfuscateAuthUserTask(ObfuscateSqlDumpTask):
    """Task to obfuscate auth_user dump file."""

    @property
    def file_pattern(self):
        return '*-auth_user-*'

    def filter_row(self, row):
        user_id = row[0]
        row[0] = self.remap_id(user_id)  # id
        row[1] = self.generate_obfuscated_username_from_user_id(user_id)
        row[2] = ''  # first_name
        row[3] = ''  # last_name
        row[4] = ''  # email
        row[5] = ''  # passwords

        for i in xrange(11, len(row)):
            row[i] = ''

        return row


class ObfuscateStudentCourseEnrollmentTask(ObfuscateSqlDumpTask):
    """Task to obfuscate student_courseenrollment dump file."""

    @property
    def file_pattern(self):
        return '*-student_courseenrollment-*'

    def filter_row(self, row):
        row[1] = self.remap_id(row[1])  # user_id
        return row


class ObfuscateUserApiUserCourseTagTask(ObfuscateSqlDumpTask):
    """Task to obfuscate user_api_usercoursetag dump file."""

    @property
    def file_pattern(self):
        return '*-user_api_usercoursetag-*'

    def filter_row(self, row):
        row[1] = self.remap_id(row[1])  # user_id
        return row


class ObfuscateStudentLanguageProficiencyTask(ObfuscateSqlDumpTask):
    """Task to obfuscate student_languageproficiency dump file."""

    @property
    def file_pattern(self):
        return '*-student_languageproficiency-*'

    def filter_row(self, row):
        return row


class ObfuscateCoursewareStudentModule(ObfuscateSqlDumpTask):
    """Task to obfuscate courseware_studentmodule dump file."""

    @property
    def file_pattern(self):
        return '*-courseware_studentmodule-*'

    def filter_row(self, row):
        user_id = row[3]
        user_info = {'user_id': [user_id, ]}
        try:
            user_id = int(user_id)
            entry = self.user_by_id[user_id]
            if 'username' in entry:
                user_info['username'] = [entry['username'], ]
            if 'name' in entry:
                user_info['name'] = [entry['name'], ]
        except KeyError:
            log.error("Unable to find CWSM user_id: %r in the user_by_id map of size %s", user_id, len(self.user_by_id))

        row[3] = self.remap_id(user_id)  # student_id

        # Courseware_studentmodule is not processed with the other SQL tables, so it
        # is not escaped in the same way.  In particular, we will not decode and encode it,
        # but merely transform double backslashes.
        state_str = row[4].replace('\\\\', '\\')
        try:
            if state_str == 'NULL':
                updated_state_dict = {}
            else:
                state_dict = cjson.decode(state_str, all_unicode=True)
                # Traverse the dictionary, looking for entries that need to be scrubbed.
                updated_state_dict = self.obfuscator.obfuscate_structure(state_dict, u"state", user_info)
        except Exception:   # pylint:  disable=broad-except
            log.exception(u"Unable to parse state as JSON for record %s: type = %s, state = %r",
                          row[0], type(state_str), state_str)
            updated_state_dict = {}

        if updated_state_dict is not None:
            # Can't reset values, so update original fields.
            updated_state = cjson.encode(updated_state_dict).replace('\\', '\\\\')
            row[4] = updated_state
            if self.obfuscator.is_logging_enabled():
                log.info(u"Obfuscated state for user_id '%s' module_id '%s'", user_id, row[2])

        return row


class ObfuscateCertificatesGeneratedCertificate(ObfuscateSqlDumpTask):
    """Task to obfuscate certificates_generatedcertificate dump file."""

    @property
    def file_pattern(self):
        return '*-certificates_generatedcertificate-*'

    def filter_row(self, row):
        row[1] = self.remap_id(row[1])  # user_id
        row[2] = ''  # download_url
        row[5] = ''  # key
        row[8] = ''  # verify_uuid
        row[9] = ''  # download_uuid
        row[10] = ''  # name
        row[13] = ''  # error_reason
        return row


class ObfuscateTeamsTask(ObfuscateSqlDumpTask):
    """Task to obfuscate teams dump file."""

    @property
    def file_pattern(self):
        return '*-teams-*'

    def filter_row(self, row):
        return row


class ObfuscateTeamsMembershipTask(ObfuscateSqlDumpTask):
    """Task to obfuscate teams_membership dump file."""

    @property
    def file_pattern(self):
        return '*-teams_membership-*'

    def filter_row(self, row):
        row[1] = self.remap_id(row[1])  # user_id
        return row


class ObfuscateVerificationStatusTask(ObfuscateSqlDumpTask):
    """Task to obfuscate verify_student_verificationstatus dump file."""

    @property
    def file_pattern(self):
        return '*-verify_student_verificationstatus-*'

    def filter_row(self, row):
        row[4] = self.remap_id(row[4])  # user_id
        return row


class ObfuscateWikiArticleTask(ObfuscateSqlDumpTask):
    """Task to obfuscate wiki_article dump file."""

    @property
    def file_pattern(self):
        return '*-wiki_article-*'

    def filter_row(self, row):
        # Removing these just to be safe.
        row[4] = ''  # owner_id
        row[5] = ''  # group_id
        return row


class ObfuscateWikiArticleRevisionTask(ObfuscateSqlDumpTask):
    """Task to obfuscate wiki_articlerevision dump file."""

    @property
    def file_pattern(self):
        return '*-wiki_articlerevision-*'

    def filter_row(self, row):
        user_id = row[5]
        user_info = {}
        if user_id != 'NULL':
            user_id = int(user_id)
            user_info['user_id'] = [user_id, ]
            try:
                entry = self.user_by_id[user_id]
                if 'username' in entry:
                    user_info['username'] = [entry['username'], ]
                if 'name' in entry:
                    user_info['name'] = [entry['name'], ]
            except KeyError:
                log.error("Unable to find wiki user_id: %s in the user_by_id map", user_id)

        row[2] = ''  # user_message
        row[3] = ''  # automatic_log
        row[4] = ''  # ip_address
        # For user_id, preserve 'NULL' value if present.
        if user_id != 'NULL':
            row[5] = self.remap_id(user_id)

        wiki_content = backslash_decode_value(row[12].decode('utf8'))
        cleaned_content = self.obfuscator.obfuscate_text(wiki_content, user_info)
        row[12] = backslash_encode_value(cleaned_content).encode('utf8')

        return row


class XBlockConfigMixin(object):
    """Reads the xblock obfuscation configuration and provides convenient accessors for that configuration"""

    xblock_obfuscation_config = luigi.Parameter(
        config_path={'section': 'obfuscation', 'name': 'xblock_obfuscation_config'}
    )

    def requires(self):
        """Require the external config if we are not using the default one"""
        reqs = super(XBlockConfigMixin, self).requires()
        if os.path.basename(self.xblock_obfuscation_config) != self.xblock_obfuscation_config:
            reqs['xblock_config'] = ExternalURL(self.xblock_obfuscation_config)
        return reqs

    @property
    def xblock_config(self):
        """The complete structure as read from the config file"""
        if not hasattr(self, '_xblock_config'):
            with read_config_file(self.xblock_obfuscation_config) as xblock_config_file:
                self._xblock_config = yaml.load(xblock_config_file)

        return self._xblock_config

    def _get_field_list(self, block_type, list_type):
        """For a particular block get the whitelist or blacklist of fields"""
        if list_type not in ('whitelist', 'blacklist'):
            raise ValueError('list_type must either be "whitelist" or "blacklist"')

        if not hasattr(self, '_field_lists'):
            self._field_lists = {}
            for category, block_config in self.xblock_config['xblocks'].iteritems():
                self._field_lists[category] = {
                    'whitelist': frozenset(block_config.get('fields', []) + self.xblock_config.get('fields', [])),
                    'blacklist': frozenset(
                        block_config.get('exclude_fields', []) + self.xblock_config.get('exclude_fields', [])
                    ),
                }
        return self._field_lists.get(block_type, {}).get(list_type, frozenset())

    def remove_fields_from_dict(self, block_type, dict_to_modify):
        """
        Using the blacklists and whitelists, remove any potentially sensitive fields from the dictionary.

        This modifies the dictionary in place. It is intended to be used on structures pulled from JSON files, XML files
        etc that enumerate fields on XBlocks.
        """
        to_remove = frozenset(dict_to_modify.keys()) - self._get_field_list(block_type, 'whitelist')
        for attribute in to_remove:
            dict_to_modify.pop(attribute)
            if attribute in self._get_field_list(block_type, 'blacklist'):
                log.debug('Removed blacklisted attribute "%s" from "%s"', attribute, block_type)
            else:
                log.warning('Removed unknown attribute "%s" from "%s"', attribute, block_type)

        return to_remove


class CourseStructureTask(XBlockConfigMixin, BaseObfuscateDumpTask):
    """Task to copy course_structure dump file, removing any unknown elements and fields."""

    @property
    def file_pattern(self):
        return '*-course_structure-*'

    def run(self):
        with self.output().open('w') as output_file:
            with self.input()['data'][0].open('r') as input_file:
                course_structure = json.load(input_file)
                for block_structure in course_structure.values():
                    category = block_structure.get('category')
                    metadata = block_structure.get('metadata', {})
                    if category in self.xblock_config['xblocks']:
                        removed_fields = self.remove_fields_from_dict(category, metadata)
                    else:
                        removed_fields = set(metadata.keys())
                        for key in removed_fields:
                            del metadata[key]
                        if category in self.xblock_config.get('exclude_xblocks', set()):
                            log.debug('Removed metadata from blacklisted block "%s"', category)
                        else:
                            log.warning('Removed metadata from unknown block "%s"', category)

                    if len(removed_fields) > 0:
                        block_structure['redacted_metadata'] = list(removed_fields)

                json.dump(course_structure, output_file)


class CourseContentTask(XBlockConfigMixin, BaseObfuscateDumpTask):
    """Task to copy course dump file, removing any unknown elements and fields."""

    XML_PACKAGE_REF_ATTRIBUTE = 'url_name'

    @property
    def file_pattern(self):
        return '*-course-*'

    def run(self):
        with self.output().open('w') as output_file:
            with self.input()['data'][0].open('r') as input_file:
                with make_temp_directory(prefix='obfuscate-course.') as tmp_directory:
                    with tempfile.TemporaryFile() as temp_input_file:
                        # We cannot seek in HDFS streams, so copy the file to the local disk before extracting
                        copy_file_to_file(input_file, temp_input_file)
                        temp_input_file.flush()
                        temp_input_file.seek(0)

                        with tarfile.open(mode='r:gz', fileobj=temp_input_file) as course_archive:
                            course_archive.extractall(tmp_directory)

                        course_dir = os.listdir(tmp_directory)[0]
                        root_dir = os.path.join(tmp_directory, course_dir)

                        self.clean_drafts(root_dir)

                        course_package_ref = self.read_course_package_ref(root_dir)

                        policy_file_path = os.path.join(root_dir, 'policies', course_package_ref, 'policy.json')
                        self.clean_course_policy(course_package_ref, policy_file_path)

                        self.clean_xml_files(root_dir)

                        with tarfile.open(mode='w:gz', fileobj=output_file) as output_archive_file:
                            output_archive_file.add(tmp_directory, arcname='')

    def clean_drafts(self, root_dir):
        """
        Remove any draft blocks from the course.

        These are "works in progress" that are likely not useful for research but may contain sensitive information.
        """

        drafts_path = os.path.join(root_dir, 'drafts')
        if os.path.exists(drafts_path):
            log.debug('Removing all drafts')
            shutil.rmtree(drafts_path)

    def read_course_package_ref(self, root_dir):
        """The course url_name is used in file names and policy keys etc, we need to read it from the metadata."""
        course_metadata_et = xml.etree.ElementTree.parse(os.path.join(root_dir, 'course.xml'))
        course_metadata = course_metadata_et.getroot()
        course_package_ref = course_metadata.attrib.get(self.XML_PACKAGE_REF_ATTRIBUTE)
        return course_package_ref

    def clean_course_policy(self, course_package_ref, policy_file_path):
        """The policy file contains many of the course-level fields; we need to scrub those just like we do the XML."""
        log.debug('Cleaning course policy file "%s"', policy_file_path)
        with open(policy_file_path, 'r') as policy_file:
            policy = json.load(policy_file)
            course_root_policy = policy.get('course/' + course_package_ref, {})
            removed_fields = self.remove_fields_from_dict('course', course_root_policy)
            course_root_policy['redacted_attributes'] = list(removed_fields)
        with open(policy_file_path, 'w') as policy_file:
            json.dump(policy, policy_file)

    def clean_xml_files(self, root_dir):
        """Find all of the XML files in the package and remove any unrecognized or known sensitive fields from them."""
        log.debug('Cleaning XML files')
        xml_file_paths = [target.path for target in PathSetTask([root_dir], ['*.xml']).output()]
        for xml_file_path in xml_file_paths:
            document = xml.etree.ElementTree.parse(xml_file_path)
            element = document.getroot()
            self.clean_element(element)
            document.write(xml_file_path)

    def clean_element(self, element):
        """
        Given an XML element, remove any unknown attributes, children etc.

        Here we err on the side of safety, removing anything that is not explicitly in the whitelist or that seems like
        it could possibly contain sensitive information.
        """
        block_config = self.xblock_config['xblocks'].get(element.tag)
        removed_fields = set()
        removed_children = set()
        if block_config:
            removed_fields = self.remove_fields_from_dict(element.tag, element.attrib)

            # The logic here is a bit tricky, a sub-element can either be a field of the current block or a child block.
            # We just assume everything is a child block unless we are told otherwise by the configuration. Fields that
            # can appear as sub-elements need to be declared in the field whitelist. Note that some wonkiness is
            # possible if a field name happens to conflict with a block name. This could result in us thinking a child
            # block is actually a field and ignoring it.
            #
            # We "clean" a sub-element if and only if:
            # 1) The current block does not have the property has_child_xblocks=False. This is used to prevent traversal of
            #    sub-elements for blocks that can have arbitrary children (like raw HTML). Note that by default we
            #    traverse the sub-elements, we only disable this behavior if we have explicitly set this field to false.
            # 2) The sub-element name does not appear in the field list for the current block.
            declared_has_child_xblocks = 'has_child_xblocks' in block_config and block_config['has_child_xblocks']
            if block_config.get('has_child_xblocks', True):
                for child_element in element:
                    if child_element.tag not in block_config.get('fields', []):
                        if not declared_has_child_xblocks:
                            log.warning(
                                'Found a block with non-field children that did not '
                                'have has_child_xblocks=True: %s',
                                element.tag
                            )
                        self.clean_element(child_element)
        else:
            if element.tag in self.xblock_config.get('exclude_xblocks', set()):
                log.debug('Blacklisted element "%s", removing all attributes and children', element.tag)
            else:
                log.warning('Unrecognized element "%s", removing all attributes and children', element.tag)

            for attribute in element.attrib.keys():
                # This attribute is used to refer to an external definition of the element in a separate file. We
                # preserve these so that it's clear what file belongs to this element. Note that the file will also
                # have all of the relevant data stripped from it, so this is of limited utility, but it at least
                # explains the reason for the file's existence.
                if attribute != self.XML_PACKAGE_REF_ATTRIBUTE:
                    element.attrib.pop(attribute)
                    removed_fields.add(attribute)

            # Make a copy of the list of children before modifying it
            for child_element in list(element):
                removed_children.add(child_element.tag)
                element.remove(child_element)

        if len(removed_fields) > 0:
            element.set("redacted_attributes", ",".join(sorted(removed_fields)))

        if len(removed_children) > 0:
            element.set("redacted_children", ",".join(sorted(removed_children)))


class ObfuscateMongoDumpsTask(BaseObfuscateDumpTask):
    """Task to obfuscate mongo dump file."""

    @property
    def file_pattern(self):
        return '*mongo*'

    def run(self):
        with self.output().open('w') as output_file:
            with self.input()['data'][0].open('r') as input_file:
                for line in input_file:
                    row = json.loads(line)
                    filtered_row = self.filter_row(row)
                    output_file.write(json.dumps(filtered_row, ensure_ascii=False).encode('utf-8'))
                    output_file.write('\n')

    def filter_row(self, row):
        """Replace/remove sensitive information."""
        try:
            author_id = int(row['author_id'])
        except ValueError:
            log.error("Encountered non-integer value for author_id (%s) in forums data.  Username = '%s'",
                      row.get('author_id'), row.get('author_username'))
            author_id = None

        user_info = {}
        if author_id is not None:
            # Gather user_info.
            user_info['user_id'] = [author_id, ]
            user_info['username'] = [row.get('author_username'), ]
            try:
                entry = self.user_by_id[author_id]
                if 'name' in entry:
                    user_info['name'] = [entry['name'], ]
                # While we're at it, perform a sanity check on username.
                decoded_username = row.get('author_username', '').decode('utf8')
                if decoded_username != entry['username']:
                    log.error("author_username '%s' for author_id: %s does not match cached value '%s'",
                              decoded_username, author_id, entry['username'])
            except KeyError:
                log.error("Unable to find author_id: %s in the user_by_id map", author_id)

            # Remap author values, if possible.
            row['author_id'] = str(self.remap_id(author_id))
            row['author_username'] = self.generate_obfuscated_username_from_user_id(author_id)

        # Clean the body of the forum post.
        body = row['body']
        row['body'] = self.obfuscator.obfuscate_text(body, user_info)

        # Also clean the title, if present, since it also contains username and fullname matches.
        if 'title' in row:
            title = row['title']
            row['title'] = self.obfuscator.obfuscate_text(title, user_info)

        # Remap user_id values that are stored in lists.
        if 'votes' in row:
            votes = row['votes']
            if 'down' in votes and len(votes['down']) > 0:
                votes['down'] = [str(self.remap_id(user_id)) for user_id in votes['down']]
            if 'up' in votes and len(votes['up']) > 0:
                votes['up'] = [str(self.remap_id(user_id)) for user_id in votes['up']]

        if 'abuse_flaggers' in row and len(row['abuse_flaggers']) > 0:
            row['abuse_flaggers'] = [str(self.remap_id(user_id)) for user_id in row['abuse_flaggers']]
        if 'historical_abuse_flaggers' in row and len(row['historical_abuse_flaggers']) > 0:
            row['historical_abuse_flaggers'] = [
                str(self.remap_id(user_id)) for user_id in row['historical_abuse_flaggers']
            ]
        if 'endorsement' in row and row['endorsement'] and 'user_id' in row['endorsement']:
            user_id = row['endorsement']['user_id']
            if user_id is not None:
                row['endorsement']['user_id'] = str(self.remap_id(user_id))

        return row


class ObfuscatedCourseDumpTask(ObfuscatorDownstreamMixin, luigi.WrapperTask):
    """Wrapper task to obfuscate data for a particular course."""

    course = luigi.Parameter()
    dump_root = luigi.Parameter()
    output_root = luigi.Parameter()

    def __init__(self, *args, **kwargs):
        super(ObfuscatedCourseDumpTask, self).__init__(*args, **kwargs)

        filename_safe_course_id = opaque_key_util.get_filename_safe_course_id(self.course)
        dump_path = url_path_join(self.dump_root, filename_safe_course_id, 'state')
        auth_userprofile_targets = PathSetTask([dump_path], ['*auth_userprofile*']).output()
        # TODO: Refactor out this logic of getting latest file. Right now we expect a date, so we use that
        dates = [target.path.rsplit('/', 2)[-2] for target in auth_userprofile_targets]
        # TODO: Make the date a parameter that defaults to the most recent, but allows the user to override?
        # This should return an error if no data is found, rather than getting a cryptic 'index out of range' error.
        if len(dates) == 0:
            raise Exception('Missing auth_userprofile data file in {}'.format(dump_path))
        latest_date = sorted(dates)[-1]
        self.data_directory = url_path_join(self.dump_root, filename_safe_course_id, 'state', latest_date)
        self.output_directory = url_path_join(self.output_root, filename_safe_course_id, 'state', latest_date)

    def requires(self):
        kwargs = {
            'course': self.course,
            'output_directory': self.output_directory,
            'data_directory': self.data_directory,
            'entities': self.entities,
            'log_context': self.log_context,
            'auth_user_path': self.auth_user_path,
            'auth_userprofile_path': self.auth_userprofile_path,
        }
        yield (
            ObfuscateAuthUserTask(**kwargs),
            ObfuscateAuthUserProfileTask(**kwargs),
            ObfuscateStudentCourseEnrollmentTask(**kwargs),
            ObfuscateUserApiUserCourseTagTask(**kwargs),
            ObfuscateStudentLanguageProficiencyTask(**kwargs),
            # TODO: decide if CWSM should be optional, and if so, how to implement that.
            ObfuscateCoursewareStudentModule(**kwargs),
            ObfuscateCertificatesGeneratedCertificate(**kwargs),
            ObfuscateTeamsTask(**kwargs),
            ObfuscateTeamsMembershipTask(**kwargs),
            ObfuscateVerificationStatusTask(**kwargs),
            ObfuscateWikiArticleTask(**kwargs),
            ObfuscateWikiArticleRevisionTask(**kwargs),
            CourseContentTask(**kwargs),
            CourseStructureTask(**kwargs),
            ObfuscateMongoDumpsTask(**kwargs),
        )


class DataObfuscationTask(ObfuscatorDownstreamMixin, luigi.WrapperTask):
    """Wrapper task for data obfuscation development."""

    course = luigi.ListParameter()
    dump_root = luigi.Parameter()
    output_root = luigi.Parameter(
        config_path={'section': 'obfuscation', 'name': 'output_root'}
    )

    def requires(self):
        for course in self.course:   # pylint: disable=not-an-iterable
            kwargs = {
                'dump_root': self.dump_root,
                'course': course,
                'output_root': self.output_root,
                'entities': self.entities,
                'log_context': self.log_context,
                'auth_user_path': self.auth_user_path,
                'auth_userprofile_path': self.auth_userprofile_path,
            }
            yield ObfuscatedCourseDumpTask(**kwargs)
