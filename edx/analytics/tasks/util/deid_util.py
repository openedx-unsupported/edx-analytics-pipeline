import luigi

from edx.analytics.tasks.url import ExternalURL

IMPLICIT_EVENT_TYPE_PATTERNS = [
    r"/courses/\(course_id\)/jump_to_id/",
    r"/courses/\(course_id\)/courseware/",
    r"/courses/\(course_id\)/info/?$",
    r"/courses/\(course_id\)/progress/?$",
    r"/courses/\(course_id\)/course_wiki/?$",
    r"/courses/\(course_id\)/about/?$",
    r"/courses/\(course_id\)/teams/?$",
    r"/courses/\(course_id\)/[a-fA-F\d]{32}/?$",
    r"/courses/\(course_id\)/?$",
    r"/courses/\(course_id\)/pdfbook/\d+(/chapter/\d+(/\d+)?)?/?$",
    r"/courses/\(course_id\)/wiki((?!/_).)*$",
    r"/courses/\(course_id\)/discussion/(threads|comments)",
    r"/courses/\(course_id\)/discussion/(upload|users|forum/?)$",
    r"/courses/\(course_id\)/discussion/[\w\-.]+/threads/create$",
    r"/courses/\(course_id\)/discussion/forum/[\w\-.]+/(inline|search|threads)$",
    r"/courses/\(course_id\)/discussion/forum/[\w\-.]+/threads/\w+$",
]


_user_by_id = None
_user_by_username = None


class UserInfoDownstreamMixin(object):

    auth_user_path = luigi.Parameter()
    auth_userprofile_path = luigi.Parameter()


class UserInfoMixin(UserInfoDownstreamMixin):

    def user_info_requirements(self):
        return {
            'auth_user': ExternalURL(self.auth_user_path.rstrip('/') + '/'),
            'auth_userprofile': ExternalURL(self.auth_userprofile_path.rstrip('/') + '/')
        }

    @property
    def user_by_id(self):
        self._intialize_user_info()
        return _user_by_id

    @property
    def user_by_username(self):
        self._intialize_user_info()
        return _user_by_username

    def _intialize_user_info(self):
        global _user_by_id
        global _user_by_username

        if _user_by_id is None:
            _user_by_id = {}
            _user_by_username = {}

            input_targets = {k: v.output() for k, v in self.user_info_requirements().items()}
            with input_targets['auth_user'].open('r') as auth_user_file:
                for line in auth_user_file:
                    split_line = line.rstrip('\r\n').split('\x01')
                    try:
                        user_id = int(split_line[0])
                    except ValueError:
                        continue
                    username = split_line[1]
                    _user_by_id[user_id] = {'username': username, 'user_id': user_id}
                    # Point to the same object so that we can just store two pointers to the data instead of two
                    # copies of the data
                    _user_by_username[username] = _user_by_id[user_id]

            with input_targets['auth_userprofile'].open('r') as auth_user_profile_file:
                for line in auth_user_profile_file:
                    split_line = line.rstrip('\r\n').split('\x01')
                    try:
                        user_id = int(split_line[0])
                    except ValueError:
                        continue
                    name = split_line[1]
                    try:
                        _user_by_id[user_id]['name'] = name
                    except KeyError:
                        pass
