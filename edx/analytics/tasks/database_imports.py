"""
Import data from external RDBMS databases into Hive.
"""
import datetime
import logging
import textwrap
import re

import luigi
from luigi.hive import HiveQueryTask, HivePartitionTarget

from edx.analytics.tasks.sqoop import SqoopImportFromMysql
from edx.analytics.tasks.url import url_path_join, ExternalURL, get_target_from_url
from edx.analytics.tasks.util.overwrite import OverwriteOutputMixin
from edx.analytics.tasks.util.hive import hive_database_name, hive_decimal_type, WarehouseMixin
from edx.analytics.tasks.mysql_load import MysqlInsertTaskMixin, CredentialFileMysqlTarget
from edx.analytics.tasks.util.record import Record, StringField, FloatField, DateTimeField, IntegerField, BooleanField
from edx.analytics.tasks.vertica_load import VerticaCopyTask, VerticaCopyTaskMixin


log = logging.getLogger(__name__)


class DatabaseImportMixin(object):
    """
    Provides general parameters needed for accessing RDBMS databases.

    Example Credentials File::

        {
            "host": "db.example.com",
            "port": "3306",
            "username": "exampleuser",
            "password": "example password"
        }
    """
    destination = luigi.Parameter(
        config_path={'section': 'database-import', 'name': 'destination'},
        description='The directory to write the output files to.'
    )
    credentials = luigi.Parameter(
        config_path={'section': 'database-import', 'name': 'credentials'},
        description='Path to the external access credentials file.',
    )
    database = luigi.Parameter(
        default_from_config={'section': 'database-import', 'name': 'database'}
    )
    import_date = luigi.DateParameter(
        default=None,
        description='Date to assign to Hive partition.  Default is today\'s date, UTC.',
    )
    num_mappers = luigi.Parameter(
        default=None,
        significant=False,
        description='The number of map tasks to ask Sqoop to use.',
    )
    verbose = luigi.BooleanParameter(
        default=False,
        significant=False,
        description='Print more information while working.',
    )

    def __init__(self, *args, **kwargs):
        super(DatabaseImportMixin, self).__init__(*args, **kwargs)

        if not self.import_date:
            self.import_date = datetime.datetime.utcnow().date()


class ImportIntoHiveTableTask(OverwriteOutputMixin, HiveQueryTask):
    """
    Abstract class to import data into a Hive table.

    Requires four properties and a requires() method to be defined.
    """

    def query(self):
        # TODO: Figure out how to clean up old data. This just cleans
        # out old metastore info, and doesn't actually remove the table
        # data.

        # Ensure there is exactly one available partition in the
        # table. Don't keep historical partitions since we don't want
        # to commit to taking snapshots at any regular interval. They
        # will happen when/if they need to happen.  Table snapshots
        # should *not* be used for analyzing trends, instead we should
        # rely on events or database tables that keep historical
        # information.
        query_format = textwrap.dedent("""
            USE {database_name};
            DROP TABLE IF EXISTS {table_name};
            CREATE EXTERNAL TABLE {table_name} (
                {col_spec}
            )
            PARTITIONED BY (dt STRING)
            {table_format}
            LOCATION '{location}';
            ALTER TABLE {table_name} ADD PARTITION (dt = '{partition_date}');
        """)

        query = query_format.format(
            database_name=hive_database_name(),
            table_name=self.table_name,
            col_spec=','.join([' '.join(c) for c in self.columns]),
            location=self.table_location,
            table_format=self.table_format,
            partition_date=self.partition_date,
        )

        log.debug('Executing hive query: %s', query)

        # Mark the output as having been removed, even though
        # that doesn't technically happen until the query has been
        # executed (and in particular that the 'DROP TABLE' is executed).
        log.info("Marking existing output as having been removed for task %s", str(self))
        self.attempted_removal = True

        return query

    @property
    def partition(self):
        """Provides name of Hive database table partition."""
        # The Luigi hive code expects partitions to be defined by dictionaries.
        return {'dt': self.partition_date}

    @property
    def partition_location(self):
        """Provides location of Hive database table's partition data."""
        # The actual folder name where the data is stored is expected to be in the format <key>=<value>
        partition_name = '='.join(self.partition.items()[0])
        # Make sure that input path ends with a slash, to indicate a directory.
        # (This is necessary for S3 paths that are output from Hadoop jobs.)
        return url_path_join(self.table_location, partition_name + '/')

    @property
    def table_name(self):
        """Provides name of Hive database table."""
        raise NotImplementedError

    @property
    def table_format(self):
        """Provides format of Hive database table's data."""
        raise NotImplementedError

    @property
    def table_location(self):
        """Provides root location of Hive database table's data."""
        raise NotImplementedError

    @property
    def partition_date(self):
        """Provides value to use in constructing the partition name of Hive database table."""
        raise NotImplementedError

    @property
    def columns(self):
        """
        Provides definition of columns in Hive.

        This should define a list of (name, definition) tuples, where
        the definition defines the Hive type to use. For example,
        ('first_name', 'STRING').

        """
        raise NotImplementedError

    def output(self):
        return HivePartitionTarget(
            self.table_name, self.partition, database=hive_database_name(), fail_missing_table=False
        )


class ImportMysqlToHiveTableTask(DatabaseImportMixin, ImportIntoHiveTableTask):
    """
    Dumps data from an RDBMS table, and imports into Hive.

    Requires override of `table_name` and `columns` properties.
    """

    @property
    def table_location(self):
        return url_path_join(self.destination, self.table_name)

    @property
    def table_format(self):
        # Use default of hive built-in format.
        return ""

    @property
    def partition_date(self):
        # Partition date is provided by DatabaseImportMixin.
        return self.import_date.isoformat()

    def requires(self):
        return SqoopImportFromMysql(
            table_name=self.table_name,
            # TODO: We may want to make the explicit passing in of columns optional as it prevents a direct transfer.
            # Make sure delimiters and nulls etc. still work after removal.
            columns=[c[0] for c in self.columns],
            destination=self.partition_location,
            credentials=self.credentials,
            num_mappers=self.num_mappers,
            verbose=self.verbose,
            overwrite=self.overwrite,
            database=self.database,
            # Hive expects NULL to be represented by the string "\N" in the data. You have to pass in "\\N" to sqoop
            # since it uses that string directly in the generated Java code, so "\\N" actually looks like "\N" to the
            # Java code. In order to get "\\N" onto the command line we have to use another set of escapes to tell the
            # python code to pass through the "\" character.
            null_string='\\\\N',
            # It's unclear why, but this setting prevents us from correctly substituting nulls with \N.
            mysql_delimiters=False,
            # This is a string that is interpreted as an octal number, so it is equivalent to the character Ctrl-A
            # (0x01). This is the default separator for fields in Hive.
            fields_terminated_by='\x01',
            # Replace delimiters with a single space if they appear in the data. This prevents the import of malformed
            # records. Hive does not support escape characters or other reasonable workarounds to this problem.
            delimiter_replacement=' ',
        )

    def output(self):
        return get_target_from_url(self.partition_location.rstrip('/') + '/')


class MysqlTableSchemaTask(OverwriteOutputMixin, DatabaseImportMixin, luigi.Task):

    import_table = luigi.Parameter()

    def requires(self):
        return {
            'credentials': ExternalURL(self.credentials),
        }

    def output(self):
        url_with_filename = url_path_join(self.warehouse_path, "sql_schema", "{0}.csv".format(self.import_table))
        return get_target_from_url(url_with_filename)

    def run(self):
        mysql_target = CredentialFileMysqlTarget(
            credentials_target=self.input()['credentials'],
            database_name=self.database,
            table=self.import_table,
            update_id=self.task_id
        )
        connection = mysql_target.connect()

        try:
            cursor = connection.cursor()
            cursor.execute("describe {0}".format(self.import_table))
            column_info = cursor.fetchall()
            with self.output().open('w') as output_file:
                for column in column_info:
                    field = column[0]
                    field_type = column[1]
                    field_null = column[2]
                    output_file.write('\t'.join((field, field_type, field_null)))
                    output_file.write('\n')
        except:
            connection.rollback()
            raise
        finally:
            connection.close()


# class TestTask(WarehouseMixin, DatabaseImportMixin, luigi.Task):
#
#     import_table = luigi.Parameter()
#     overwrite = luigi.BooleanParameter()
#
#     def __init__(self, *args, **kwargs):
#         super(TestTask, self).__init__(*args, **kwargs)
#         self.record = type(self.import_table, (Record,), {})
#
#     def requires(self):
#         return {
#             'credentials': ExternalURL(self.credentials),
#             'import_task': self.import_task,
#         }
#
#     @property
#     def import_task(self):
#         return SqoopImportFromMysql(
#             table_name=self.import_table,
#             # TODO: We may want to make the explicit passing in of columns optional as it prevents a direct transfer.
#             # Make sure delimiters and nulls etc. still work after removal.
#             destination=self.hive_partition_path(self.import_table, self.import_date.isoformat()),
#             credentials=self.credentials,
#             num_mappers=self.num_mappers,
#             verbose=self.verbose,
#             overwrite=self.overwrite,
#             database=self.database,
#             # Hive expects NULL to be represented by the string "\N" in the data. You have to pass in "\\N" to sqoop
#             # since it uses that string directly in the generated Java code, so "\\N" actually looks like "\N" to the
#             # Java code. In order to get "\\N" onto the command line we have to use another set of escapes to tell the
#             # python code to pass through the "\" character.
#             #null_string='\\\\N',
#             # It's unclear why, but this setting prevents us from correctly substituting nulls with \N.
#             mysql_delimiters=True,
#             # This is a string that is interpreted as an octal number, so it is equivalent to the character Ctrl-A
#             # (0x01). This is the default separator for fields in Hive.
#             #fields_terminated_by='\x01',
#             # Replace delimiters with a single space if they appear in the data. This prevents the import of malformed
#             # records. Hive does not support escape characters or other reasonable workarounds to this problem.
#             #delimiter_replacement=' ',
#         )
#
#     def output(self):
#         return get_target_from_url(self.hive_partition_path(self.import_table, self.import_date.isoformat()))
#
#
#     def run(self):
#         mysql_target = CredentialFileMysqlTarget(
#             credentials_target=self.input()['credentials'],
#             database_name=self.database,
#             table=self.import_table,
#             update_id=self.task_id
#         )
#         connection = mysql_target.connect()
#
#         try:
#             cursor = connection.cursor()
#             cursor.execute("describe {0}".format(self.import_table))
#             column_info = cursor.fetchall()
#             for column in column_info:
#                 column_name = column[0]
#                 column_type_info = column[1]
#                 column_nullable = column[2]
#                 nullable = True# if column_nullable == 'YES' else False
#
#                 match = re.search("(.*)\((\d*)\)", column_type_info)
#                 if match:
#                     column_type = match.group(1)
#                     column_length = match.group(2)
#                 else:
#                     column_type = column_type_info
#
#                 if column_type == 'int' or column_type == 'smallint':
#                     setattr(self.record, column_name, IntegerField(nullable=nullable))
#                 elif column_type == 'tinyint':
#                     setattr(self.record, column_name, BooleanField(nullable=nullable))
#                 elif column_type == 'varchar' or column_type == 'char':
#                     setattr(self.record, column_name, StringField(nullable=nullable, length=column_length))
#                 elif column_type == 'datetime' or column_type == 'date':
#                     setattr(self.record, column_name, DateTimeField(nullable=nullable))
#                 elif column_type == 'longtext':
#                     setattr(self.record, column_name, StringField(nullable=nullable, length=65000))
#         except:
#             connection.rollback()
#             raise
#         finally:
#             connection.close()


class LoadMysqlToVerticaTableTask(WarehouseMixin, VerticaCopyTask):

    import_table = luigi.Parameter()
    import_date = luigi.DateParameter(
        default=datetime.datetime.utcnow().date(),
        description='Date to assign to Hive partition.  Default is today\'s date, UTC.',
    )

    def requires(self):
        if self.required_tasks is None:
            self.required_tasks = {
                'credentials': ExternalURL(url=self.credentials),
                'insert_source': self.insert_source_task,
                'mysql_schema_task': self.mysql_schema_task,
            }
        return self.required_tasks

    def vertica_compliant_schema(self):
        schema = []
        with self.input()['mysql_schema_task'].open('r') as schema_file:
            for line in schema_file:
                field_name, field_type, field_null = line.split('\t')
                
                if field_type == 'longtext':
                    field_type == 'LONG VARCHAR'
                elif field_type == 'double':
                    field_type == 'DOUBLE PRECISION'

                schema.append((field_name, field_type))
        log.debug(schema)
        return schema

    @property
    def copy_delimiter(self):
        """The delimiter in the data to be copied.  Default is tab (\t)"""
        return "','"

    @property
    def copy_null_sequence(self):
        """The null sequence in the data to be copied.  Default is Hive NULL (\\N)"""
        return "'NULL'"

    @property
    def insert_source_task(self):
        return SqoopImportFromMysql(
            table_name=self.import_table,
            # TODO: We may want to make the explicit passing in of columns optional as it prevents a direct transfer.
            # Make sure delimiters and nulls etc. still work after removal.
            destination=self.hive_partition_path(self.import_table, self.import_date.isoformat()),
            overwrite=self.overwrite,
            # Hive expects NULL to be represented by the string "\N" in the data. You have to pass in "\\N" to sqoop
            # since it uses that string directly in the generated Java code, so "\\N" actually looks like "\N" to the
            # Java code. In order to get "\\N" onto the command line we have to use another set of escapes to tell the
            # python code to pass through the "\" character.
            #null_string='\\\\N',
            # It's unclear why, but this setting prevents us from correctly substituting nulls with \N.
            mysql_delimiters=True,
            # This is a string that is interpreted as an octal number, so it is equivalent to the character Ctrl-A
            # (0x01). This is the default separator for fields in Hive.
            #fields_terminated_by='\x01',
            # Replace delimiters with a single space if they appear in the data. This prevents the import of malformed
            # records. Hive does not support escape characters or other reasonable workarounds to this problem.
            #delimiter_replacement=' ',
        )

    @property
    def mysql_schema_task(self):
        return MysqlTableSchemaTask(
            import_table=self.import_table,
            overwrite=self.overwrite,
        )

    @property
    def default_columns(self):
        return []

    @property
    def table(self):
        return self.import_table

    @property
    def columns(self):
        return self.vertica_compliant_schema()

    @property
    def auto_primary_key(self):
        return None


class VerticaLoadWrapperTask(WarehouseMixin, VerticaCopyTaskMixin, luigi.WrapperTask):

    tables = luigi.Parameter(
        is_list=True,
        default=("activity", "activity_receiving_users", "answer", "api_admin_apiaccessconfig", "api_admin_apiaccessrequest", "api_admin_historicalapiaccessrequest", "askbot_activityauditstatus", "askbot_anonymousanswer", "askbot_anonymousquestion", "askbot_badgedata", "askbot_emailfeedsetting", "askbot_markedtag", "askbot_post", "askbot_postrevision", "askbot_questionview", "askbot_thread", "askbot_thread_followed_by", "askbot_thread_tags", "assessment_aiclassifier", "assessment_aiclassifierset", "assessment_aigradingworkflow", "assessment_aitrainingworkflow", "assessment_aitrainingworkflow_training_examples", "assessment_assessment", "assessment_assessmentfeedback", "assessment_assessmentfeedback_assessments", "assessment_assessmentfeedback_options", "assessment_assessmentfeedbackoption", "assessment_assessmentpart", "assessment_criterion", "assessment_criterionoption", "assessment_peerworkflow", "assessment_peerworkflowitem", "assessment_rubric", "assessment_staffworkflow", "assessment_studenttrainingworkflow", "assessment_studenttrainingworkflowitem", "assessment_trainingexample", "assessment_trainingexample_options_selected", "auth_group", "auth_group_permissions", "auth_message", "auth_permission", "auth_registration", "auth_user", "auth_user_groups", "auth_user_groups_copy", "auth_user_groups_copy_post_ok", "auth_user_user_permissions", "auth_userprofile", "award", "badges_badgeassertion", "badges_badgeclass", "badges_coursecompleteimageconfiguration", "badges_courseeventbadgesconfiguration", "bookmarks_bookmark", "bookmarks_xblockcache", "branding_brandingapiconfig", "branding_brandinginfoconfig", "bulk_email_bulkemailflag", "bulk_email_cohorttarget", "bulk_email_courseauthorization", "bulk_email_courseemail", "bulk_email_courseemail_targets", "bulk_email_courseemailtemplate", "bulk_email_optout", "bulk_email_target", "catalog_catalogintegration", "ccx_ccxfieldoverride", "ccx_ccxfuturemembership", "ccx_ccxmembership", "ccx_customcourseforedx", "ccxcon_ccxcon", "celery_taskmeta", "celery_tasksetmeta", "certificates_certificategenerationconfiguration", "certificates_certificategenerationcoursesetting", "certificates_certificategenerationhistory", "certificates_certificatehtmlviewconfiguration", "certificates_certificateinvalidation", "certificates_certificatetemplate", "certificates_certificatetemplateasset", "certificates_certificatewhitelist", "certificates_examplecertificate", "certificates_examplecertificateset", "certificates_generatedcertificate", "comment", "commerce_commerceconfiguration", "contentserver_cdnuseragentsconfig", "contentserver_courseassetcachettlconfig", "contentstore_pushnotificationconfig", "contentstore_videouploadconfig", "cors_csrf_xdomainproxyconfiguration", "corsheaders_corsmodel", "course_action_state_coursererunstate", "course_creators_coursecreator", "course_groups_cohortmembership", "course_groups_coursecohort", "course_groups_coursecohortssettings", "course_groups_courseusergroup", "course_groups_courseusergroup_users", "course_groups_courseusergrouppartitiongroup", "course_modes_coursemode", "course_modes_coursemodeexpirationconfig", "course_modes_coursemodesarchive", "course_overviews_courseoverview", "course_overviews_courseoverviewimageconfig", "course_overviews_courseoverviewimageset", "course_overviews_courseoverviewtab", "course_structures_coursestructure", "coursetalk_coursetalkwidgetconfiguration", "courseware_offlinecomputedgrade", "courseware_offlinecomputedgradelog", "courseware_studentfieldoverride", "courseware_xmodulestudentinfofield", "courseware_xmodulestudentprefsfield", "courseware_xmoduleuserstatesummaryfield", "credentials_credentialsapiconfig", "credit_creditconfig", "credit_creditcourse", "credit_crediteligibility", "credit_creditprovider", "credit_creditrequest", "credit_creditrequirement", "credit_creditrequirementstatus", "credit_historicalcreditrequest", "credit_historicalcreditrequirementstatus", "dark_lang_darklangconfig", "django_admin_log", "django_comment_client_permission", "django_comment_client_permission_roles", "django_comment_client_role", "django_comment_client_role_users", "django_comment_common_forumsconfig", "django_content_type", "django_migrations", "django_openid_auth_association", "django_openid_auth_nonce", "django_openid_auth_useropenid", "django_openid_auth_useropenid_removed", "django_redirect", "django_session", "django_site", "djcelery_crontabschedule", "djcelery_intervalschedule", "djcelery_periodictask", "djcelery_periodictasks", "djcelery_taskstate", "djcelery_workerstate", "djkombu_message", "djkombu_queue", "edxval_coursevideo", "edxval_encodedvideo", "edxval_profile", "edxval_subtitle", "edxval_video", "email_marketing_emailmarketingconfiguration", "embargo_country", "embargo_countryaccessrule", "embargo_courseaccessrulehistory", "embargo_embargoedcourse", "embargo_embargoedstate", "embargo_ipfilter", "embargo_restrictedcourse", "enterprise_enterprisecustomer", "enterprise_enterprisecustomeruser", "enterprise_historicalenterprisecustomer", "external_auth_externalauthmap", "favorite_question", "foldit_puzzlecomplete", "foldit_score", "followit_followuser", "grades_coursepersistentgradesflag", "grades_persistentcoursegrade", "grades_persistentgradesenabledflag", "grades_persistentsubsectiongrade", "grades_visibleblocks", "instructor_task_instructortask", "licenses_coursesoftware", "licenses_userlicense", "linkedin_linkedin", "livesettings_longsetting", "livesettings_setting", "lms_xblock_xblockasidesconfig", "mentoring_answer", "microsite_configuration_historicalmicrositeorganizationmapping", "microsite_configuration_historicalmicrositetemplate", "microsite_configuration_microsite", "microsite_configuration_micrositehistory", "microsite_configuration_micrositeorganizationmapping", "microsite_configuration_micrositetemplate", "milestones_coursecontentmilestone", "milestones_coursemilestone", "milestones_milestone", "milestones_milestonerelationshiptype", "milestones_usermilestone", "mobile_api_appversionconfig", "mobile_api_mobileapiconfig", "notes_note", "notifications_articlesubscription", "notify_notification", "notify_notificationtype", "notify_settings", "notify_subscription", "organizations_organization", "organizations_organizationcourse", "problem_builder_answer", "proctoring_proctoredexam", "proctoring_proctoredexamreviewpolicy", "proctoring_proctoredexamreviewpolicyhistory", "proctoring_proctoredexamsoftwaresecurereview", "proctoring_proctoredexamsoftwaresecurereviewhistory", "proctoring_proctoredexamstudentallowance", "proctoring_proctoredexamstudentallowancehistory", "proctoring_proctoredexamstudentattempt", "proctoring_proctoredexamstudentattemptcomment", "proctoring_proctoredexamstudentattempthistory", "programs_programsapiconfig", "psychometrics_psychometricdata", "question", "repute", "robots_rule", "robots_rule_allowed", "robots_rule_disallowed", "robots_rule_sites", "robots_url", "rss_proxy_whitelistedrssurl", "self_paced_selfpacedconfiguration", "shoppingcart_certificateitem", "shoppingcart_coupon", "shoppingcart_couponredemption", "shoppingcart_courseregcodeitem", "shoppingcart_courseregcodeitemannotation", "shoppingcart_courseregistrationcode", "shoppingcart_courseregistrationcodeinvoiceitem", "shoppingcart_donation", "shoppingcart_donationconfiguration", "shoppingcart_invoice", "shoppingcart_invoicehistory", "shoppingcart_invoiceitem", "shoppingcart_invoicetransaction", "shoppingcart_order", "shoppingcart_orderitem", "shoppingcart_paidcourseregistration", "shoppingcart_paidcourseregistrationannotation", "shoppingcart_registrationcoderedemption", "simplewiki_article", "simplewiki_article_related", "simplewiki_articleattachment", "simplewiki_namespace", "simplewiki_permission", "simplewiki_permission_can_read", "simplewiki_permission_can_write", "simplewiki_revision", "site_configuration_siteconfiguration", "site_configuration_siteconfigurationhistory", "social_auth_association", "social_auth_code", "social_auth_nonce", "social_auth_usersocialauth", "splash_splashconfig", "static_replace_assetbaseurlconfig", "static_replace_assetexcludedextensionsconfig", "status_coursemessage", "status_globalstatusmessage", "student_anonymoususerid", "student_anonymoususerid_temp_archive_cale", "student_courseaccessrole", "student_courseaccessrole_copy", "student_courseenrollment", "student_courseenrollmentallowed", "student_courseenrollmentattribute", "student_dashboardconfiguration", "student_enrollmentrefundconfiguration", "student_entranceexamconfiguration", "student_historicalcourseenrollment", "student_languageproficiency", "student_linkedinaddtoprofileconfiguration", "student_loginfailures", "student_logoutviewconfiguration", "student_manualenrollmentaudit", "student_passwordhistory", "student_pendingemailchange", "student_pendingnamechange", "student_registrationcookieconfiguration", "student_userattribute", "student_usersignupsource", "student_userstanding", "student_usertestgroup", "student_usertestgroup_users", "submissions_score", "submissions_scoreannotation", "submissions_scoresummary", "submissions_studentitem", "submissions_submission", "survey_surveyanswer", "survey_surveyform", "tag", "tagging_tagavailablevalues", "tagging_tagcategories", "teams_courseteam", "teams_courseteammembership", "theming_sitetheme", "third_party_auth_ltiproviderconfig", "third_party_auth_oauth2providerconfig", "third_party_auth_providerapipermissions", "third_party_auth_samlconfiguration", "third_party_auth_samlproviderconfig", "third_party_auth_samlproviderdata", "thumbnail_kvstore", "track_trackinglog", "user_api_usercoursetag", "user_api_userorgtag", "user_api_userpreference", "util_ratelimitconfiguration", "verified_track_content_verifiedtrackcohortedcourse", "verify_student_historicalverificationdeadline", "verify_student_icrvstatusemailsconfiguration", "verify_student_incoursereverificationconfiguration", "verify_student_skippedreverification", "verify_student_softwaresecurephotoverification", "verify_student_verificationcheckpoint", "verify_student_verificationcheckpoint_photo_verification", "verify_student_verificationdeadline", "verify_student_verificationstatus", "vote", "waffle_flag", "waffle_flag_groups", "waffle_flag_users", "waffle_sample", "waffle_switch", "wiki_article", "wiki_articleforobject", "wiki_articleplugin", "wiki_articlerevision", "wiki_attachment", "wiki_attachmentrevision", "wiki_image", "wiki_imagerevision", "wiki_reusableplugin", "wiki_reusableplugin_articles", "wiki_revisionplugin", "wiki_revisionpluginrevision", "wiki_simpleplugin", "wiki_urlpath", "workflow_assessmentworkflow", "workflow_assessmentworkflowcancellation", "workflow_assessmentworkflowstep", "xblock_config_studioconfig", "xblock_django_xblockconfiguration", "xblock_django_xblockstudioconfiguration", "xblock_django_xblockstudioconfigurationflag"),
    )

    def requires(self):
        for table in self.tables:
            yield LoadMysqlToVerticaTableTask(
                warehouse_path=self.warehouse_path,
                schema=self.schema,
                import_table=table
            )

class ImportStudentCourseEnrollmentTask(ImportMysqlToHiveTableTask):
    """Imports course enrollment information from an external LMS DB to a destination directory."""

    @property
    def table_name(self):
        return 'student_courseenrollment'

    @property
    def columns(self):
        return [
            ('id', 'INT'),
            ('user_id', 'INT'),
            ('course_id', 'STRING'),
            ('created', 'TIMESTAMP'),
            ('is_active', 'BOOLEAN'),
            ('mode', 'STRING'),
        ]


class ImportAuthUserTask(ImportMysqlToHiveTableTask):

    """Imports user information from an external LMS DB to a destination directory."""

    @property
    def table_name(self):
        return 'auth_user'

    @property
    def columns(self):
        # Fields not included are 'password', 'first_name' and 'last_name'.
        # In our LMS, the latter two are always empty.
        return [
            ('id', 'INT'),
            ('username', 'STRING'),
            ('last_login', 'TIMESTAMP'),
            ('date_joined', 'TIMESTAMP'),
            ('is_active', 'BOOLEAN'),
            ('is_superuser', 'BOOLEAN'),
            ('is_staff', 'BOOLEAN'),
            ('email', 'STRING'),
        ]


class ImportAuthUserProfileTask(ImportMysqlToHiveTableTask):
    """
    Imports user demographic information from an external LMS DB to both a
    destination directory and a HIVE metastore.

    """
    @property
    def table_name(self):
        return 'auth_userprofile'

    @property
    def columns(self):
        return [
            ('user_id', 'INT'),
            ('name', 'STRING'),
            ('gender', 'STRING'),
            ('year_of_birth', 'INT'),
            ('level_of_education', 'STRING'),
            ('language', 'STRING'),
            ('location', 'STRING'),
            ('mailing_address', 'STRING'),
            ('city', 'STRING'),
            ('country', 'STRING'),
            ('goals', 'STRING'),
        ]


class ImportCourseUserGroupTask(ImportMysqlToHiveTableTask):
    """
    Imports course cohort information from an external LMS DB to both a
    destination directory and a HIVE metastore.

    """
    @property
    def table_name(self):
        return 'course_groups_courseusergroup'

    @property
    def columns(self):
        return [
            ('id', 'INT'),
            ('name', 'STRING'),
            ('course_id', 'STRING'),
            ('group_type', 'STRING'),
        ]


class ImportCourseUserGroupUsersTask(ImportMysqlToHiveTableTask):
    """
    Imports user cohort information from an external LMS DB to both a
    destination directory and a HIVE metastore.

    """
    @property
    def table_name(self):
        return 'course_groups_courseusergroup_users'

    @property
    def columns(self):
        return [
            ('courseusergroup_id', 'INT'),
            ('user_id', 'INT'),
        ]


class ImportShoppingCartOrder(ImportMysqlToHiveTableTask):
    """
    Imports orders from an external LMS DB shopping cart table to both a
    destination directory and a HIVE metastore.

    """
    @property
    def table_name(self):
        return 'shoppingcart_order'

    @property
    def columns(self):
        return [
            ('id', 'INT'),
            ('user_id', 'INT'),
            ('currency', 'STRING'),
            ('status', 'STRING'),
            ('purchase_time', 'TIMESTAMP'),
            ('bill_to_first', 'STRING'),
            ('bill_to_last', 'STRING'),
            ('bill_to_street1', 'STRING'),
            ('bill_to_street2', 'STRING'),
            ('bill_to_city', 'STRING'),
            ('bill_to_state', 'STRING'),
            ('bill_to_postalcode', 'STRING'),
            ('bill_to_country', 'STRING'),
            ('bill_to_ccnum', 'STRING'),
            ('bill_to_cardtype', 'STRING'),
            ('processor_reply_dump', 'STRING'),
            ('refunded_time', 'STRING'),
            ('company_name', 'STRING'),
            ('company_contact_name', 'STRING'),
            ('company_contact_email', 'STRING'),
            ('recipient_name', 'STRING'),
            ('recipient_email', 'STRING'),
            ('customer_reference_number', 'STRING'),
            ('order_type', 'STRING'),
        ]


class ImportShoppingCartOrderItem(ImportMysqlToHiveTableTask):
    """
    Imports individual order items from an external LMS DB shopping cart table to both a
    destination directory and a HIVE metastore.

    """
    @property
    def table_name(self):
        return 'shoppingcart_orderitem'

    @property
    def columns(self):
        return [
            ('id', 'INT'),
            ('order_id', 'INT'),
            ('user_id', 'INT'),
            ('status', 'STRING'),
            ('qty', 'int'),
            ('unit_cost', hive_decimal_type(12, 2)),
            ('line_desc', 'STRING'),
            ('currency', 'STRING'),
            ('fulfilled_time', 'TIMESTAMP'),
            ('report_comments', 'STRING'),
            ('refund_requested_time', 'TIMESTAMP'),
            ('service_fee', hive_decimal_type(12, 2)),
            ('list_price', hive_decimal_type(12, 2)),
            ('created', 'TIMESTAMP'),
            ('modified', 'TIMESTAMP'),
        ]


class ImportShoppingCartCertificateItem(ImportMysqlToHiveTableTask):
    """
    Imports certificate items from an external LMS DB shopping cart table to both a
    destination directory and a HIVE metastore.

    """
    @property
    def table_name(self):
        return 'shoppingcart_certificateitem'

    @property
    def columns(self):
        return [
            ('orderitem_ptr_id', 'INT'),
            ('course_id', 'STRING'),
            ('course_enrollment_id', 'INT'),
            ('mode', 'STRING'),
        ]


class ImportShoppingCartPaidCourseRegistration(ImportMysqlToHiveTableTask):
    """
    Imports paid course registrations from an external LMS DB shopping cart table to both a
    destination directory and a HIVE metastore.

    """
    @property
    def table_name(self):
        return 'shoppingcart_paidcourseregistration'

    @property
    def columns(self):
        return [
            ('orderitem_ptr_id', 'INT'),
            ('course_id', 'STRING'),
            ('mode', 'STRING'),
            ('course_enrollment_id', 'INT'),
        ]


class ImportShoppingCartDonation(ImportMysqlToHiveTableTask):
    """
    Imports donations from an external LMS DB shopping cart table to both a
    destination directory and a HIVE metastore.

    """
    @property
    def table_name(self):
        return 'shoppingcart_donation'

    @property
    def columns(self):
        return [
            ('orderitem_ptr_id', 'INT'),
            ('donation_type', 'STRING'),
            ('course_id', 'STRING'),
        ]


class ImportShoppingCartCourseRegistrationCodeItem(ImportMysqlToHiveTableTask):
    """
    Imports course registration codes from an external ecommerce table to both a
    destination directory and a HIVE metastore.

    """
    @property
    def table_name(self):
        return 'shoppingcart_courseregcodeitem'

    @property
    def columns(self):
        return [
            ('orderitem_ptr_id', 'INT'),
            ('course_id', 'STRING'),
            ('mode', 'STRING'),
        ]


class ImportShoppingCartCoupon(ImportMysqlToHiveTableTask):
    """
    Imports coupon definitions from an external LMS DB shopping cart table to both a
    destination directory and a HIVE metastore.

    """
    @property
    def table_name(self):
        return 'shoppingcart_coupon'

    @property
    def columns(self):
        return [
            ('id', 'INT'),
            ('code', 'STRING'),
            ('description', 'STRING'),
            ('course_id', 'STRING'),
            ('percentage_discount', 'INT'),
            ('created_at', 'TIMESTAMP'),
            ('is_active', 'BOOLEAN'),
            ('expiration_date', 'TIMESTAMP'),
            ('created_by_id', 'INT'),
        ]


class ImportShoppingCartCouponRedemption(ImportMysqlToHiveTableTask):
    """
    Imports coupon redeptions from an external LMS DB shopping cart table to both a
    destination directory and a HIVE metastore.

    """
    @property
    def table_name(self):
        return 'shoppingcart_couponredemption'

    @property
    def columns(self):
        return [
            ('id', 'INT'),
            ('coupon_id', 'INT'),
            ('order_id', 'INT'),
            ('user_id', 'INT'),
        ]


class ImportEcommerceUser(ImportMysqlToHiveTableTask):
    """Ecommerce: Users: Imports users from an external ecommerce table to a destination dir."""

    @property
    def table_name(self):
        return 'ecommerce_user'

    @property
    def columns(self):
        return [
            ('id', 'INT'),
            ('username', 'STRING'),
            ('email', 'STRING'),
        ]


class ImportProductCatalog(ImportMysqlToHiveTableTask):
    """
    Ecommerce: Products: Imports product catalog from an external ecommerce table to both a
    destination directory and a HIVE metastore.

    """
    @property
    def table_name(self):
        return 'catalogue_product'

    @property
    def columns(self):
        return [
            ('id', 'INT'),
            ('structure', 'STRING'),
            ('upc', 'STRING'),
            ('title', 'STRING'),
            ('slug', 'STRING'),
            ('description', 'STRING'),
            ('rating', 'STRING'),
            ('date_created', 'TIMESTAMP'),
            ('date_updated', 'TIMESTAMP'),
            ('is_discountable', 'STRING'),
            ('parent_id', 'INT'),
            ('product_class_id', 'INT'),
            ('course_id', 'STRING'),
            ('expires', 'TIMESTAMP'),
        ]


class ImportProductCatalogClass(ImportMysqlToHiveTableTask):
    """Ecommerce: Products: Imports product catalog classes from an external ecommerce table to a destination dir."""

    @property
    def table_name(self):
        return 'catalogue_productclass'

    @property
    def columns(self):
        return [
            ('id', 'INT'),
            ('name', 'STRING'),
            ('slug', 'STRING'),
            ('requires_shipping', 'TINYINT'),
            ('track_stock', 'TINYINT'),
        ]


class ImportProductCatalogAttributes(ImportMysqlToHiveTableTask):
    """
    Ecommerce: Products: Imports product catalog attributes from an external ecommerce table to both a
    destination directory and a HIVE metastore.

    """
    @property
    def table_name(self):
        return 'catalogue_productattribute'

    @property
    def columns(self):
        return [
            ('id', 'INT'),
            ('name', 'STRING'),
            ('code', 'STRING'),
            ('type', 'STRING'),
            ('required', 'INT'),
            ('option_group_id', 'INT'),
            ('product_class_id', 'INT'),
        ]


class ImportProductCatalogAttributeValues(ImportMysqlToHiveTableTask):
    """
    Ecommerce: Products: Imports product catalog attribute values from an external ecommerce table to both a
    destination directory and a HIVE metastore.

    """
    @property
    def table_name(self):
        return 'catalogue_productattributevalue'

    @property
    def columns(self):
        return [
            ('id', 'INT'),
            ('value_text', 'STRING'),
            ('value_integer', 'INT'),
            ('value_boolean', 'BOOLEAN'),
            ('value_float', 'STRING'),
            ('value_richtext', 'STRING'),
            ('value_date', 'TIMESTAMP'),
            ('value_file', 'STRING'),
            ('value_image', 'STRING'),
            ('entity_object_id', 'INT'),
            ('attribute_id', 'INT'),
            ('entity_content_type_id', 'INT'),
            ('product_id', 'INT'),
            ('value_option_id', 'INT'),
        ]


class ImportCurrentRefundRefundLineState(ImportMysqlToHiveTableTask):
    """
    Ecommerce: Current: Imports current refund line items from an ecommerce table to both a
    destination directory and a HIVE metastore.

    """
    @property
    def table_name(self):
        return 'refund_refundline'

    @property
    def columns(self):
        return [
            ('id', 'INT'),
            ('line_credit_excl_tax', hive_decimal_type(12, 2)),
            ('quantity', 'INT'),
            ('status', 'STRING'),
            ('order_line_id', 'INT'),
            ('refund_id', 'INT'),
            ('created', 'TIMESTAMP'),
            ('modified', 'TIMESTAMP'),
        ]


class ImportCurrentOrderState(ImportMysqlToHiveTableTask):
    """
    Ecommerce Current: Imports current orders from an ecommerce table to both a
    destination directory and a HIVE metastore.

    """
    @property
    def table_name(self):
        return 'order_order'

    @property
    def columns(self):
        return [
            ('id', 'INT'),
            ('number', 'STRING'),
            ('currency', 'STRING'),
            ('total_incl_tax', hive_decimal_type(12, 2)),
            ('total_excl_tax', hive_decimal_type(12, 2)),
            ('shipping_incl_tax', hive_decimal_type(12, 2)),
            ('shipping_excl_tax', hive_decimal_type(12, 2)),
            ('shipping_method', 'STRING'),
            ('shipping_code', 'STRING'),
            ('status', 'STRING'),
            ('guest_email', 'STRING'),
            ('date_placed', 'TIMESTAMP'),
            ('basket_id', 'INT'),
            ('billing_address_id', 'INT'),
            ('shipping_address_id', 'INT'),
            ('site_id', 'INT'),
            ('user_id', 'INT'),
        ]


class ImportCurrentOrderLineState(ImportMysqlToHiveTableTask):
    """
    Ecommerce: Current: Imports current order line items from an ecommerce table to a
    destination directory and a HIVE metastore.

    """
    @property
    def table_name(self):
        return 'order_line'

    @property
    def columns(self):
        return [
            ('id', 'INT'),
            ('partner_name', 'STRING'),
            ('partner_sku', 'STRING'),
            ('partner_line_reference', 'STRING'),
            ('partner_line_notes', 'STRING'),
            ('title', 'STRING'),
            ('upc', 'STRING'),
            ('quantity', 'INT'),
            ('line_price_incl_tax', hive_decimal_type(12, 2)),
            ('line_price_excl_tax', hive_decimal_type(12, 2)),
            ('line_price_before_discounts_incl_tax', hive_decimal_type(12, 2)),
            ('line_price_before_discounts_excl_tax', hive_decimal_type(12, 2)),
            ('unit_cost_price', hive_decimal_type(12, 2)),
            ('unit_price_incl_tax', hive_decimal_type(12, 2)),
            ('unit_price_excl_tax', hive_decimal_type(12, 2)),
            ('unit_retail_price', hive_decimal_type(12, 2)),
            ('status', 'STRING'),
            ('est_dispatch_date', 'TIMESTAMP'),
            ('order_id', 'INT'),
            ('partner_id', 'INT'),
            ('product_id', 'INT'),
            ('stockrecord_id', 'INT'),
        ]


class ImportCurrentOrderDiscountState(ImportMysqlToHiveTableTask):
    """
    Ecommerce: Current: Imports current order discount records from an ecommerce table to a
    destination directory and a HIVE metastore.

    """
    @property
    def table_name(self):
        return 'order_orderdiscount'

    @property
    def columns(self):
        return [
            ('id', 'INT'),
            ('category', 'STRING'),
            ('offer_id', 'INT'),
            ('offer_name', 'STRING'),
            ('voucher_id', 'INT'),
            ('voucher_code', 'STRING'),
            ('frequency', 'INT'),
            ('amount', hive_decimal_type(12, 2)),
            ('message', 'STRING'),
            ('order_id', 'INT'),
        ]


class ImportCouponVoucherIndirectionState(ImportMysqlToHiveTableTask):
    """
    Ecommerce: Current: Imports the voucher_couponvouchers table from the ecommerce database to
    a destination directory and a HIVE metastore.

    This table is just an extra layer of indirection in the source schema design and is required
    to translate a 'couponvouchers_id' into a coupon id.
    Coupons are represented as products in the product table, which is imported separately.
    A coupon can have many voucher codes associated with it.
    """
    @property
    def table_name(self):
        return 'voucher_couponvouchers'

    @property
    def columns(self):
        return [
            ('id', 'INT'),
            ('coupon_id', 'INT'),
        ]


class ImportCouponVoucherState(ImportMysqlToHiveTableTask):
    """
    Ecommerce: Current: Imports the voucher_couponvouchers_vouchers table from the ecommerce
    database to a destination directory and a HIVE metastore.

    A coupon can have many voucher codes associated with it. This table associates voucher IDs
    with 'couponvouchers_id's, which are stored in the voucher_couponvouchers table and
    have a 1:1 relationship to coupon IDs.
    """
    @property
    def table_name(self):
        return 'voucher_couponvouchers_vouchers'

    @property
    def columns(self):
        return [
            ('id', 'INT'),
            ('couponvouchers_id', 'INT'),
            ('voucher_id', 'INT'),
        ]


class ImportEcommercePartner(ImportMysqlToHiveTableTask):
    """
    Ecommerce: Current: Imports Partner information from an ecommerce table to a
    destination directory and a HIVE metastore.
    """

    @property
    def table_name(self):
        return 'partner_partner'

    @property
    def columns(self):
        return [
            ('id', 'INT'),
            ('code', 'STRING'),
            ('name', 'STRING'),
            ('short_code', 'STRING'),
        ]


class ImportCourseModeTask(ImportMysqlToHiveTableTask):
    """
    Course Information: Imports course_modes table to both a destination directory and a HIVE metastore.

    """
    @property
    def table_name(self):
        return 'course_modes_coursemode'

    @property
    def columns(self):
        return [
            ('id', 'INT'),
            ('course_id', 'STRING'),
            ('mode_slug', 'STRING'),
            ('mode_display_name', 'STRING'),
            ('min_price', 'INT'),
            ('suggested_prices', 'STRING'),
            ('currency', 'STRING'),
            ('expiration_date', 'TIMESTAMP'),
            ('expiration_datetime', 'TIMESTAMP'),
            ('description', 'STRING'),
            ('sku', 'STRING'),
        ]


class ImportGeneratedCertificatesTask(ImportMysqlToHiveTableTask):

    @property
    def table_name(self):
        return 'certificates_generatedcertificate'

    @property
    def columns(self):
        return [
            ('user_id', 'INT'),
            ('course_id', 'STRING'),
            ('grade', 'STRING'),
            ('status', 'STRING'),
            ('mode', 'STRING'),
            ('created_date', 'TIMESTAMP'),
            ('modified_date', 'TIMESTAMP'),
        ]


class ImportAllDatabaseTablesTask(DatabaseImportMixin, OverwriteOutputMixin, luigi.WrapperTask):
    """Imports a set of database tables from an external LMS RDBMS."""

    def requires(self):
        kwargs = {
            'destination': self.destination,
            'credentials': self.credentials,
            'num_mappers': self.num_mappers,
            'verbose': self.verbose,
            'import_date': self.import_date,
            'overwrite': self.overwrite,
        }
        yield (
            ImportStudentCourseEnrollmentTask(**kwargs),
            ImportAuthUserTask(**kwargs),
            ImportAuthUserProfileTask(**kwargs),
        )

    def output(self):
        return [task.output() for task in self.requires()]
