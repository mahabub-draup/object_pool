import unittest
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

    def test_with_default_methods(self):
        self.pool = ObjectPool(self.klass)
        self.assertIsNotNone(self.pool)

    def test_with_force_option(self):
        self.pool = ObjectPool(self.klass)
        pool = self.pool

        pool1 = ObjectPool(self.klass, force=True)
        self.assertNotEqual(pool, pool1)

        pool1.destroy()

    def test_with_lazy_option(self):
        self.pool = ObjectPool(self.klass, lazy=True)
        pool = self.pool
        size = pool.get_pool_size()
        self.assertEqual(size, 0)

    def test_creation_with_lazy_option(self):
        self.pool = ObjectPool(self.klass, lazy=True)
        pool = self.pool

        with pool.get() as (item, item_stats):
            pass

        size = pool.get_pool_size()

        self.assertEqual(size, 1)

    def tearDown(self):
        self.pool.destroy()
        self.klass = None
