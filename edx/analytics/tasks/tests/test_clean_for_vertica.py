"""Test the MapReduce task for cleaning event data prior to bulkloading into Vertica."""
import json
import StringIO
import hashlib
import os
import tempfile
import shutil
import math

from mock import Mock, call
from opaque_keys.edx.locator import CourseLocator

from edx.analytics.tasks.answer_dist import (
    ProblemCheckEventMixin,
    AnswerDistributionPerCourseMixin,
    AnswerDistributionOneFilePerCourseTask,
    try_str_to_float,
)
from edx.analytics.tasks.tests import unittest
from edx.analytics.tasks.tests.config import with_luigi_config, OPTION_REMOVED
from edx.analytics.tasks.tests.map_reduce_mixins import MapperTestMixin, ReducerTestMixin
from edx.analytics.tasks.tests.opaque_key_mixins import InitializeOpaqueKeysMixin, InitializeLegacyKeysMixin
