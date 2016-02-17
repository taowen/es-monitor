import unittest
import datetime

from executors.translators import filter_translator


class TestEvalInterval(unittest.TestCase):
    def test_day(self):
        self.assertEqual(datetime.timedelta(days=1), filter_translator.eval_interval('1 DAY'))

    def test_days(self):
        self.assertEqual(datetime.timedelta(days=2), filter_translator.eval_interval('2 DAYS'))

    def test_hour(self):
        self.assertEqual(datetime.timedelta(hours=1), filter_translator.eval_interval('1 hour'))

    def test_hours(self):
        self.assertEqual(datetime.timedelta(hours=2), filter_translator.eval_interval('2 hours'))

    def test_minute(self):
        self.assertEqual(datetime.timedelta(minutes=1), filter_translator.eval_interval('1 minute'))

    def test_minutes(self):
        self.assertEqual(datetime.timedelta(minutes=2), filter_translator.eval_interval('2 minutes'))

    def test_second(self):
        self.assertEqual(datetime.timedelta(seconds=1), filter_translator.eval_interval('1 second'))

    def test_seconds(self):
        self.assertEqual(datetime.timedelta(seconds=2), filter_translator.eval_interval('2 seconds'))

    def test_1_day_2_hour_3_minute_4_second(self):
        self.assertEqual(
            datetime.timedelta(days=1, hours=2, minutes=3, seconds=4),
            filter_translator.eval_interval('1 DAY 2 HOURS 3 MINUTES 4 SECONDS'))

    def test_invalid_character(self):
        try:
            filter_translator.eval_interval('1 DAY 2 HOURD 3 MINUTE')
        except:
            return
        self.fail('should fail')
