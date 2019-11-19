"""
Loads Google Analytics permissions info into the warehouse.
"""

from __future__ import absolute_import

import datetime
import json
import logging

import luigi
from apiclient.discovery import build
from google.oauth2 import service_account

from edx.analytics.tasks.common.vertica_load import VerticaCopyTask, VerticaCopyTaskMixin
from edx.analytics.tasks.util.hive import WarehouseMixin
from edx.analytics.tasks.util.overwrite import OverwriteOutputMixin
from edx.analytics.tasks.util.record import BooleanField, DateTimeField, IntegerField, Record, StringField
from edx.analytics.tasks.util.url import ExternalURL, get_target_from_url, url_path_join


class PullGoogleAnalyticsDataMixin(WarehouseMixin, OverwriteOutputMixin):
    ga_credentials = luigi.Parameter()
    date = luigi.DateParameter(default=datetime.datetime.utcnow().date())


class PullGoogleAnalyticsDataBaseTask(PullGoogleAnalyticsDataMixin, luigi.Task):
    """Process the information from Google Analytics Management API and write it to a tsv."""

    def requires(self):
        return {
            'credentials': ExternalURL(url=self.ga_credentials),
        }

    def create_management_api_service(self):
        with self.input()['credentials'].open('r') as credentials_file:
            json_creds = json.load(credentials_file)
            scopes = [
                'https://www.googleapis.com/auth/analytics.manage.users.readonly',
                'https://www.googleapis.com/auth/analytics.readonly'
            ]
            credentials = service_account.Credentials.from_service_account_info(json_creds, scopes=scopes)
            service = build('analytics', 'v3', credentials=credentials)

        return service

    def run(self):
        raise NotImplementedError

    @property
    def table_name(self):
        raise NotImplementedError

    def output(self):
        return get_target_from_url(
            url_path_join(
                self.hive_partition_path(self.table_name, partition_value=self.date), '{0}.tsv'.format(self.table_name)
            )
        )

    def create_permission_record(self, record_class, id_attr, id_value, user_link_info):
        kwargs = {
            id_attr: id_value,
            'user_id': user_link_info['userRef']['id'],
            'user_email': user_link_info['userRef']['email'],
            'has_manage_users': 'MANAGE_USERS' in user_link_info['permissions']['effective'],
            'has_edit': 'EDIT' in user_link_info['permissions']['effective'],
            'has_collaborate': 'COLLABORATE' in user_link_info['permissions']['effective'],
            'has_read_and_analyze': 'READ_AND_ANALYZE' in user_link_info['permissions']['effective'],
            'manage_users_set': 'MANAGE_USERS' in user_link_info['permissions']['local'],
            'edit_set': 'EDIT' in user_link_info['permissions']['local'],
            'collaborate_set': 'COLLABORATE' in user_link_info['permissions']['local'],
            'read_and_analyze_set': 'READ_AND_ANALYZE' in user_link_info['permissions']['local']
        }
        return record_class(**kwargs)


class AccountRecord(Record):
    """Represents a google analytics account."""
    account_id = IntegerField(description='Google Analytics Account ID', nullable=False)
    account_name = StringField(description='Google Analytics Account Name', nullable=False, length=200)
    created = DateTimeField(description='Time the account was created.', nullable=False)
    updated = DateTimeField(description='Time the account was last modified.', nullable=False)


class PullGoogleAnalyticsAccountData(PullGoogleAnalyticsDataBaseTask):
    """Pulls in Google Analytics account data through the API and writes it to a TSV."""

    def run(self):
        self.remove_output_on_overwrite()
        service = self.create_management_api_service()
        with self.output().open('w') as output_file:
            accounts_response = service.management().accounts().list().execute()
            for account in accounts_response.get('items', []):
                record = AccountRecord(
                    account_id=int(account.get('id')),
                    account_name=account.get('name'),
                    created=DateTimeField().deserialize_from_string(account.get('created')),
                    updated=DateTimeField().deserialize_from_string(account.get('updated'))
                )
                output_file.write(record.to_separated_values(sep=u'\t'))
                output_file.write('\n')

    @property
    def table_name(self):
        return 'ga_accounts'


class LoadGoogleAnalyticsAccountsToWarehouse(PullGoogleAnalyticsDataMixin, VerticaCopyTask):
    """Loads account data into the warehouse."""

    @property
    def insert_source_task(self):
        return PullGoogleAnalyticsAccountData(
            date=self.date,
            ga_credentials=self.ga_credentials,
        )

    @property
    def table(self):
        return 'ga_accounts'

    @property
    def auto_primary_key(self):
        return None

    @property
    def default_columns(self):
        return None

    @property
    def columns(self):
        return AccountRecord.get_sql_schema()


class PropertyRecord(Record):
    """Represents a Google Analytics Web Property."""
    account_id = IntegerField(description='Account ID to which this web property belongs.', nullable=False)
    property_id = StringField(description='Web property ID of the form UA-XXXXX-YY.', nullable=False, length=20)
    property_name = StringField(description='Name of this web property.', nullable=False, length=200)
    website_url = StringField(description='Website url for this web property.', nullable=True, length=255)
    created = DateTimeField(description='Time this web property was created.', nullable=False)
    updated = DateTimeField(description='Time this web property was last modified.', nullable=False)


class PullGoogleAnalyticsPropertyData(PullGoogleAnalyticsDataBaseTask):
    """Pulls in Google Analytics web properties data through the API and writes it to a TSV."""

    def run(self):
        self.remove_output_on_overwrite()
        service = self.create_management_api_service()
        with self.output().open('w') as output_file:
            properties_response = service.management().webproperties().list(accountId='~all').execute()
            for property in properties_response.get('items', []):
                record = PropertyRecord(
                    account_id=int(property.get('accountId')),
                    property_id=property.get('id'),
                    property_name=property.get('name'),
                    website_url=property.get('websiteUrl'),
                    created=DateTimeField().deserialize_from_string(property.get('created')),
                    updated=DateTimeField().deserialize_from_string(property.get('updated'))
                )
                output_file.write(record.to_separated_values(sep=u'\t'))
                output_file.write('\n')

    @property
    def table_name(self):
        return 'ga_properties'


class LoadGoogleAnalyticsPropertiesToWarehouse(PullGoogleAnalyticsDataMixin, VerticaCopyTask):
    """Loads web properties data into the warehouse."""

    @property
    def insert_source_task(self):
        return PullGoogleAnalyticsPropertyData(
            date=self.date,
            ga_credentials=self.ga_credentials,
        )

    @property
    def table(self):
        return 'ga_properties'

    @property
    def auto_primary_key(self):
        return None

    @property
    def default_columns(self):
        return None

    @property
    def columns(self):
        return PropertyRecord.get_sql_schema()


class ProfileRecord(Record):
    """Represents a Google Analytics View (profile)."""
    property_id = StringField(description='Web property ID to which this view (profile) belongs.', nullable=False, length=20)
    profile_id = IntegerField(description='View (Profile) ID.', nullable=False)
    profile_name = StringField(description='Name of this view (profile).', nullable=False, length=200)
    profile_type = StringField(description='View (Profile) type. WEB or APP.', nullable=False, length=10)
    created = DateTimeField(description='Time this view (profile) was created.', nullable=False)
    updated = DateTimeField(description='Time this view (profile) was last modified.', nullable=False)


class PullGoogleAnalyticsProfileData(PullGoogleAnalyticsDataBaseTask):
    """Pulls in Google Analytics profiles data through the API and writes it to a TSV."""

    def run(self):
        self.remove_output_on_overwrite()
        service = self.create_management_api_service()
        with self.output().open('w') as output_file:
            profiles_response = service.management().profiles().list(accountId='~all', webPropertyId='~all').execute()
            for profile in profiles_response.get('items', []):
                record = ProfileRecord(
                    property_id=profile.get('webPropertyId'),
                    profile_id=int(profile.get('id')),
                    profile_name=profile.get('name'),
                    profile_type=profile.get('type'),
                    created=DateTimeField().deserialize_from_string(profile.get('created')),
                    updated=DateTimeField().deserialize_from_string(profile.get('updated'))
                )
                output_file.write(record.to_separated_values(sep=u'\t'))
                output_file.write('\n')

    @property
    def table_name(self):
        return 'ga_profiles'


class LoadGoogleAnalyticsProfilesToWarehouse(PullGoogleAnalyticsDataMixin, VerticaCopyTask):
    """Loads profiles data into the warehouse."""

    @property
    def insert_source_task(self):
        return PullGoogleAnalyticsProfileData(
            date=self.date,
            ga_credentials=self.ga_credentials,
        )

    @property
    def table(self):
        return 'ga_profiles'

    @property
    def auto_primary_key(self):
        return None

    @property
    def default_columns(self):
        return None

    @property
    def columns(self):
        return ProfileRecord.get_sql_schema()


class IdForAcountPermission(object):
    account_id = IntegerField(description='Account ID.', nullable=False)


class IdForPropertyPermission(object):
    property_id = StringField(description='Web property ID.', nullable=False, length=20)


class IdForProfilePermission(object):
    profile_id = IntegerField(description='View (profile) ID.', nullable=False)


class PermissionRecordBase(Record):
    user_id = StringField(description='User ID.', nullable=False, length=30)
    user_email = StringField(description='Email of this user.', nullable=False, length=100)
    has_manage_users = BooleanField(description='Whether the user has MANAGE_USERS permission.', nullable=False)
    has_edit = BooleanField(description='Whether the user has EDIT permission.', nullable=False)
    has_collaborate = BooleanField(description='Whether the user has COLLABORATE permission.', nullable=False)
    has_read_and_analyze = BooleanField(description='Whether the user has READ_AND_ANALYZE permission.', nullable=False)
    manage_users_set = BooleanField(description='Whether the user has been assigned MANAGE_USERS permission.', nullable=False)
    edit_set = BooleanField(description='Whether the user has been assigned EDIT permission.', nullable=False)
    collaborate_set = BooleanField(description='Whether the user has been assigned COLLABORATE permission.', nullable=False)
    read_and_analyze_set = BooleanField(description='Whether the user has been assigned READ_AND_ANALYZE permission.', nullable=False)


class AccountPermissionRecord(IdForAcountPermission, PermissionRecordBase):
    """Represents Permissions granted at account level."""
    pass


class PropertyPermissionRecord(IdForPropertyPermission, PermissionRecordBase):
    """Represents Permissions granted at property level."""
    pass


class ProfilePermissionRecord(IdForProfilePermission, PermissionRecordBase):
    """Represents Permissions granted at view (profile) level."""
    pass


class PullGoogleAnalyticsAccountUserLinks(PullGoogleAnalyticsDataBaseTask):
    """Pulls in Google Analytics account permissions data through the API and writes it to a TSV."""

    def run(self):
        self.remove_output_on_overwrite()
        ga_account_data_target = yield PullGoogleAnalyticsAccountData(date=self.date, ga_credentials=self.ga_credentials)
        with ga_account_data_target.open('r') as account_data:
            account_ids = [x.split('\t')[0] for x in account_data.readlines()]

        service = self.create_management_api_service()
        with self.output().open('w') as output_file:
            for account_id in account_ids:
                account_links = service.management().accountUserLinks().list(accountId=account_id).execute()
                for account_user_link in account_links.get('items'):
                    account_id = int(account_user_link['entity']['accountRef']['id'])
                    record = self.create_permission_record(
                        record_class=AccountPermissionRecord,
                        id_attr='account_id',
                        id_value=account_id,
                        user_link_info=account_user_link
                    )
                    output_file.write(record.to_separated_values(sep=u'\t'))
                    output_file.write('\n')

    @property
    def table_name(self):
        return 'ga_account_permissions'


class LoadGoogleAnalyticsAccountPermissionsToWarehouse(PullGoogleAnalyticsDataMixin, VerticaCopyTask):
    """Loads account permissions data into the warehouse."""

    @property
    def insert_source_task(self):
        return PullGoogleAnalyticsAccountUserLinks(
            date=self.date,
            ga_credentials=self.ga_credentials,
        )

    @property
    def table(self):
        return 'ga_account_permissions'

    @property
    def auto_primary_key(self):
        return None

    @property
    def default_columns(self):
        return None

    @property
    def columns(self):
        return AccountPermissionRecord.get_sql_schema()


class PullGoogleAnalyticsPropertyUserLinks(PullGoogleAnalyticsDataBaseTask):
    """Pulls in Google Analytics property permissions data through the API and writes it to a TSV."""

    def run(self):
        self.remove_output_on_overwrite()
        ga_account_data_target = yield PullGoogleAnalyticsAccountData(date=self.date, ga_credentials=self.ga_credentials)
        with ga_account_data_target.open('r') as account_data:
            account_ids = [x.split('\t')[0] for x in account_data.readlines()]

        service = self.create_management_api_service()
        with self.output().open('w') as output_file:
            for account_id in account_ids:
                start_index = 1
                while True:
                    property_links = service.management().webpropertyUserLinks().list(
                        accountId=account_id,
                        webPropertyId='~all',
                        start_index=start_index
                    ).execute()
                    for property_user_link in property_links.get('items'):
                        property_id = property_user_link['entity']['webPropertyRef']['id']
                        record = self.create_permission_record(
                            record_class=PropertyPermissionRecord,
                            id_attr='property_id',
                            id_value=property_id,
                            user_link_info=property_user_link
                        )
                        output_file.write(record.to_separated_values(sep=u'\t'))
                        output_file.write('\n')
                    start_index += property_links.get('itemsPerPage')
                    if start_index > property_links.get('totalResults'):
                        break

    @property
    def table_name(self):
        return 'ga_property_permissions'


class LoadGoogleAnalyticsPropertyPermissionsToWarehouse(PullGoogleAnalyticsDataMixin, VerticaCopyTask):
    """Loads property permissions data into the warehouse."""

    @property
    def insert_source_task(self):
        return PullGoogleAnalyticsPropertyUserLinks(
            date=self.date,
            ga_credentials=self.ga_credentials,
        )

    @property
    def table(self):
        return 'ga_property_permissions'

    @property
    def auto_primary_key(self):
        return None

    @property
    def default_columns(self):
        return None

    @property
    def columns(self):
        return PropertyPermissionRecord.get_sql_schema()


class PullGoogleAnalyticsProfileUserLinks(PullGoogleAnalyticsDataBaseTask):
    """Pulls in Google Analytics profile permissions data through the API and writes it to a TSV."""

    def run(self):
        self.remove_output_on_overwrite()
        ga_account_data_target = yield PullGoogleAnalyticsAccountData(date=self.date, ga_credentials=self.ga_credentials)
        with ga_account_data_target.open('r') as account_data:
            account_ids = [x.split('\t')[0] for x in account_data.readlines()]

        service = self.create_management_api_service()
        with self.output().open('w') as output_file:
            for account_id in account_ids:
                start_index = 1
                while True:
                    profile_links = service.management().profileUserLinks().list(
                        accountId=account_id,
                        webPropertyId='~all',
                        profileId='~all',
                        start_index=start_index
                    ).execute()
                    for profile_user_link in profile_links.get('items'):
                        profile_id = int(profile_user_link['entity']['profileRef']['id'])
                        record = self.create_permission_record(
                            record_class=ProfilePermissionRecord,
                            id_attr='profile_id',
                            id_value=profile_id,
                            user_link_info=profile_user_link
                        )
                        output_file.write(record.to_separated_values(sep=u'\t'))
                        output_file.write('\n')
                    start_index += profile_links.get('itemsPerPage')
                    if start_index > profile_links.get('totalResults'):
                        break

    @property
    def table_name(self):
        return 'ga_profile_permissions'


class LoadGoogleAnalyticsProfilePermissionsToWarehouse(PullGoogleAnalyticsDataMixin, VerticaCopyTask):
    """Loads profile permissions data into the warehouse."""

    @property
    def insert_source_task(self):
        return PullGoogleAnalyticsProfileUserLinks(
            date=self.date,
            ga_credentials=self.ga_credentials,
        )

    @property
    def table(self):
        return 'ga_profile_permissions'

    @property
    def auto_primary_key(self):
        return None

    @property
    def default_columns(self):
        return None

    @property
    def columns(self):
        return ProfilePermissionRecord.get_sql_schema()


class LoadGoogleAnalyticsPermissionsWorkflow(PullGoogleAnalyticsDataMixin, VerticaCopyTaskMixin, luigi.WrapperTask):
    """Workflow for loading Google Analytics permissions data into the data warehouse."""

    def requires(self):
        kwargs = {
            'date': self.date,
            'ga_credentials': self.ga_credentials,
            'schema': self.schema,
            'overwrite': self.overwrite,
        }
        yield LoadGoogleAnalyticsAccountsToWarehouse(**kwargs)
        yield LoadGoogleAnalyticsPropertiesToWarehouse(**kwargs)
        yield LoadGoogleAnalyticsProfilesToWarehouse(**kwargs)
        yield LoadGoogleAnalyticsAccountPermissionsToWarehouse(**kwargs)
        yield LoadGoogleAnalyticsPropertyPermissionsToWarehouse(**kwargs)
        yield LoadGoogleAnalyticsProfilePermissionsToWarehouse(**kwargs)

    def complete(self):
        # OverwriteOutputMixin changes the complete() method behavior, so we override it.
        return all(r.complete() for r in luigi.task.flatten(self.requires()))
