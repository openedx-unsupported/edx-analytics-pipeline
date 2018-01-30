"""
End to end test of tags distribution.
"""

import datetime

from edx.analytics.tasks.tests.acceptance import AcceptanceTestCase, as_list_param
from edx.analytics.tasks.util.url import url_path_join


class TagsDistributionAcceptanceTest(AcceptanceTestCase):
    """Acceptance tests for Tags Distribution Task -> MySQL"""

    INPUT_FILE = 'tags_dist_acceptance_tracking.log'

    def test_base(self):
        self.upload_tracking_log(self.INPUT_FILE, datetime.date(2015, 8, 1))
        self.execute_sql_fixture_file('load_auth_userprofile.sql')

        self.task.launch([
            'TagsDistributionWorkflow',
            '--source', as_list_param(self.test_src),
            '--interval', '2010-01-01-2020-01-01',
            '--n-reduce-tasks', str(self.NUM_REDUCERS),
            '--output-root', url_path_join(self.test_out, 'tags_dist_acceptance', ''),
            '--database', self.export_db.database_name
        ])

        self.validate_base()

    def validate_base(self):
        """
        Method to validate task results
        """
        with self.export_db.cursor() as cursor:
            sql = 'SELECT course_id, org_id, module_id, tag_name, tag_value, total_submissions, correct_submissions ' \
                  'FROM tags_distribution'
            cursor.execute(sql)
            results = cursor.fetchall()

        self.assertItemsEqual(results, [
            ('course-v1:edX+E930+2014_T2', 'edX',
             'block-v1:edX+E930+2014_T2+type@problem+block@a4d38fc85c83ace7e0fd6d48d6398e16',
             'difficulty', 'Medium', 10, 3),
            ('course-v1:edX+E930+2014_T2', 'edX',
             'block-v1:edX+E930+2014_T2+type@problem+block@8e984b587e5391af139838aded9d8748',
             'difficulty', 'Hard', 2, 0),
            ('course-v1:edX+E931+2014_T2', 'edX',
             'block-v1:edX+E931+2014_T2+type@problem+block@1942cad42cf93df04e924bb9cfbe0c4c',
             'difficulty', 'Medium', 4, 2),
            ('course-v1:edX+E931+2014_T2', 'edX',
             'block-v1:edX+E931+2014_T2+type@problem+block@c417f140ca09ad44e3645886ab84bd9f',
             'difficulty', 'Medium', 7, 4),
            ('course-v1:edX+E929+2014_T2', 'edX',
             'block-v1:edX+E929+2014_T2+type@problem+block@ec44920729dd6c4a4267bbebcec2bb49',
             'difficulty', 'Medium', 9, 5),
            ('course-v1:edX+E931+2014_T2', 'edX',
             'block-v1:edX+E931+2014_T2+type@problem+block@1173afcc141d3adc4aeec9da61171075',
             'learning_outcome', 'Learned nothing', 9, 5),
            ('course-v1:edX+E929+2014_T2', 'edX',
             'block-v1:edX+E929+2014_T2+type@problem+block@236b8c618c8b213d5e4ae27bc41821ae',
             'learning_outcome', 'Learned a few things', 10, 2),
            ('course-v1:edX+E931+2014_T2', 'edX',
             'block-v1:edX+E931+2014_T2+type@problem+block@91c2765bec7b14b6199ea7cf10c155cf',
             'learning_outcome', 'Learned everything', 10, 3),
            ('course-v1:edX+E930+2014_T2', 'edX',
             'block-v1:edX+E930+2014_T2+type@problem+block@a4d38fc85c83ace7e0fd6d48d6398e16',
             'learning_outcome', 'Learned nothing', 10, 3),
            ('course-v1:edX+E930+2014_T2', 'edX',
             'block-v1:edX+E930+2014_T2+type@problem+block@c3d12293b899f2391da08a1a16a6eb7b',
             'difficulty', 'Hard', 6, 1),
            ('course-v1:edX+E929+2014_T2', 'edX',
             'block-v1:edX+E929+2014_T2+type@problem+block@08783e85ddae5a58a590bd3f828b4d6d',
             'learning_outcome', 'Learned everything', 9, 2),
            ('course-v1:edX+E930+2014_T2', 'edX',
             'block-v1:edX+E930+2014_T2+type@problem+block@c3d12293b899f2391da08a1a16a6eb7b',
             'learning_outcome', 'Learned nothing', 6, 1),
            ('course-v1:edX+E931+2014_T2', 'edX',
             'block-v1:edX+E931+2014_T2+type@problem+block@1942cad42cf93df04e924bb9cfbe0c4c',
             'learning_outcome', 'Learned a few things', 4, 2),
            ('course-v1:edX+E929+2014_T2', 'edX',
             'block-v1:edX+E929+2014_T2+type@problem+block@08783e85ddae5a58a590bd3f828b4d6d',
             'difficulty', 'Medium', 9, 2),
            ('course-v1:edX+E930+2014_T2', 'edX',
             'block-v1:edX+E930+2014_T2+type@problem+block@325af68222c277c757418041ae59993c',
             'difficulty', 'Hard', 4, 2),
            ('course-v1:edX+E930+2014_T2', 'edX',
             'block-v1:edX+E930+2014_T2+type@problem+block@f21b06e4a7379093749341c007a4c090',
             'learning_outcome', 'Learned a few things', 5, 0),
            ('course-v1:edX+E929+2014_T2', 'edX',
             'block-v1:edX+E929+2014_T2+type@problem+block@fed6c06e3d6c938c42df71c68675763b',
             'difficulty', 'Medium', 3, 2),
            ('course-v1:edX+E931+2014_T2', 'edX',
             'block-v1:edX+E931+2014_T2+type@problem+block@e711a09faece5b49e836ce5211d512e7',
             'difficulty', 'Medium', 9, 4),
            ('course-v1:edX+E930+2014_T2', 'edX',
             'block-v1:edX+E930+2014_T2+type@problem+block@8e984b587e5391af139838aded9d8748',
             'learning_outcome', 'Learned nothing', 2, 0),
            ('course-v1:edX+E929+2014_T2', 'edX',
             'block-v1:edX+E929+2014_T2+type@problem+block@fbe2b0266347569d559ddfb62af77fe5',
             'difficulty', 'Medium', 3, 0),
            ('course-v1:edX+E930+2014_T2', 'edX',
             'block-v1:edX+E930+2014_T2+type@problem+block@f21b06e4a7379093749341c007a4c090',
             'difficulty', 'Easy', 5, 0),
            ('course-v1:edX+E931+2014_T2', 'edX',
             'block-v1:edX+E931+2014_T2+type@problem+block@1173afcc141d3adc4aeec9da61171075',
             'difficulty', 'Medium', 9, 5),
            ('course-v1:edX+E931+2014_T2', 'edX',
             'block-v1:edX+E931+2014_T2+type@problem+block@91c2765bec7b14b6199ea7cf10c155cf',
             'difficulty', 'Hard', 10, 3),
            ('course-v1:edX+E931+2014_T2', 'edX',
             'block-v1:edX+E931+2014_T2+type@problem+block@c417f140ca09ad44e3645886ab84bd9f',
             'learning_outcome', 'Learned a few things', 7, 4),
            ('course-v1:edX+E929+2014_T2', 'edX',
             'block-v1:edX+E929+2014_T2+type@problem+block@236b8c618c8b213d5e4ae27bc41821ae',
             'difficulty', 'Easy', 10, 2),
            ('course-v1:edX+E929+2014_T2', 'edX',
             'block-v1:edX+E929+2014_T2+type@problem+block@fbe2b0266347569d559ddfb62af77fe5',
             'learning_outcome', 'Learned everything', 3, 0),
            ('course-v1:edX+E930+2014_T2', 'edX',
             'block-v1:edX+E930+2014_T2+type@problem+block@325af68222c277c757418041ae59993c',
             'learning_outcome', 'Learned nothing', 4, 2),
            ('course-v1:edX+E931+2014_T2', 'edX',
             'block-v1:edX+E931+2014_T2+type@problem+block@e711a09faece5b49e836ce5211d512e7',
             'learning_outcome', 'Learned everything', 9, 4),
            ('course-v1:edX+E929+2014_T2', 'edX',
             'block-v1:edX+E929+2014_T2+type@problem+block@fed6c06e3d6c938c42df71c68675763b',
             'learning_outcome', 'Learned a few things', 3, 2),
            ('course-v1:edX+E929+2014_T2', 'edX',
             'block-v1:edX+E929+2014_T2+type@problem+block@ec44920729dd6c4a4267bbebcec2bb49',
             'learning_outcome', 'Learned everything', 9, 5),
        ])
