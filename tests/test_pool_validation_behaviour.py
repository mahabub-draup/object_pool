import unittest
import time
from object_pool import ObjectPool


class ObjectPoolCreationTest(unittest.TestCase):

    def setUp(self):
        class Browser:

            def __init__(self):
                self.browser = self.__class__.__create_connection()

            @staticmethod
            def __create_connection():
                obj = "connection_object"
                return obj

            def call_me(self):
                return True

        self.klass = Browser

    def test_with_non_expiry(self):
        self.pool = ObjectPool(self.klass, max_members=1, expires=0)

        with self.pool.get() as (item, item_stats):
            t = item_stats['created_at']

        time.sleep(13)

        with self.pool.get() as (item1, item_stats1):
            t1 = item_stats1['created_at']

        self.assertEqual(item, item1)

    def test_with_expire_true_pre_check(self):
        self.pool = ObjectPool(self.klass, max_members=1, expires=10, pre_check=True, post_check=False)

        with self.pool.get() as (item, item_stats):
            t = item_stats['created_at']

        time.sleep(13)

        with self.pool.get() as (item1, item_stats1):
            t1 = item_stats1['created_at']

        self.assertNotEqual(item, item1)

    def test_with_expire_true_post_check(self):
        self.pool = ObjectPool(self.klass, max_members=1, expires=10)

        with self.pool.get() as (item, item_stats):
            t = item_stats['created_at']

        time.sleep(13)

        with self.pool.get() as (item1, item_stats1):
            t1 = item_stats1['created_at']

        with self.pool.get() as (item2, item_stats2):
            t2 = item_stats2['created_at']

        if item1 is item:
            self.assertNotEqual(item2, item)
        else:
            self.assertEqual(item1, item)

    def test_with_expire_false_pre_check(self):
        self.pool = ObjectPool(self.klass, max_members=1, expires=10)

        with self.pool.get() as (item, item_stats):
            t = item_stats['created_at']

        time.sleep(13)

        with self.pool.get() as (item1, item_stats1):
            t1 = item_stats1['created_at']

        self.assertEqual(item1, item)

    def test_with_expire_false_post_check(self):
        self.pool = ObjectPool(self.klass, max_members=1, expires=10, post_check=False, pre_check=True)

        with self.pool.get() as (item, item_stats):
            t = item_stats['created_at']

        time.sleep(13)

        with self.pool.get() as (item1, item_stats1):
            t1 = item_stats1['created_at']

        self.assertNotEqual(item1, item)

    def test_multiple_pool_creation(self):

        class Browser1:

            def __init__(self):
                self.browser = self.__class__.__create_connection()

            @staticmethod
            def __create_connection():
                obj = "connection_object"
                return obj

            def call_me(self):
                return False

        self.pool = ObjectPool(self.klass, max_members=2)
        dpool = ObjectPool(Browser1, max_members=3)

        size = self.pool.get_pool_size()
        dsize = dpool.get_pool_size()

        self.assertNotEqual(self.pool, dpool)
        dpool.destroy()

    def test_multiple_pool_invocation(self):

        class Browser1:

            def __init__(self):
                self.browser = self.__class__.__create_connection()

            @staticmethod
            def __create_connection():
                obj = "connection_object"
                return obj

            def call_me(self):
                return False

        self.pool = ObjectPool(self.klass, max_members=2)
        pool = self.pool
        dpool = ObjectPool(Browser1, max_members=3)

        with pool.get() as (item, item_stats):
            t = item.call_me()

        with dpool.get() as (item1, item_stats1):
            t1 = item1.call_me()

        self.assertNotEqual(t1, t)

    def tearDown(self):
        self.pool.destroy()
        self.klass = None
