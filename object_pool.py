import datetime
from queue import Queue


class SingletonMetaPoolRegistry(type):
    """
    This is singleton registry class (metaclass) for object pool
    """

    __pool_registry = {}

    @classmethod
    def _remove_pool(cls, klass):
        """
        removes pool from the registry and deletes for GC.

        :param klass: class for which pool is created
        :return: None
        """

        rm = cls.__pool_registry.pop(klass.__name__, None)
        del rm

    @classmethod
    def _pool_exists(cls, base_klass):
        """
        This method checks if the pool exists in the registry

        :param base_klass: class for which pool will be created
        :return: boolean
        """

        pool_registered = base_klass.__name__ in cls.__pool_registry
        return pool_registered

    def __call__(cls, base_klass, max_members=10, max_reusable=20, expires=30,
                 create_func=None, check_func=None, cleanup_func=None, lazy=False, force=False,
                 pre_check=False, post_check=True):

        if force:
            cls._remove_pool(base_klass)

        pool_registered = cls._pool_exists(base_klass)

        if pool_registered:
            return cls.__pool_registry[base_klass.__name__]

        klass = super().__call__(base_klass, max_members, max_reusable, expires,
                                 create_func, check_func, cleanup_func,
                                 lazy, force, pre_check, post_check)
        cls.__pool_registry[base_klass.__name__] = klass
        return klass


class ObjectPool(metaclass=SingletonMetaPoolRegistry):
    """
    This is singleton object pool class. It creates pool, checks expiry and validation of the pool member.
    """

    def __init__(self, klass, max_members=10, max_reusable=20, expires=600,
                 create_func=None, check_func=None, cleanup_func=None, lazy=False, force=False,
                 pre_check=False, post_check=True):
        """
        creates pool with given configuration

        :param base_klass: class for which pool will be created
        :param max_members: max members in the pool
        :param max_reusable: max no. of times member can be reused. Once this exceeds,
                             this member will be destroyed and new member will be created.
        :param expires: pool member will be expired in seconds
        :param create_func: custom create function for the object.  If the function is not provided,
                            instance will be created in a standard way.
        :param check_func: custom check function for the object validation. This will be additional validation
                           from expiration and max_reusable.
        :param lazy: by default, pool members are created when initiated.
                     lazy option will skip member creation on init and will create
                     when the pool item is requested.
        :param force: by default pool is created once and called subsequently.
                      With force option, you can recreate the pool with new configuration.
        :param pre_check: by default, pool member expiration checked after member is being used.
                          With this option, member validation is performed before requesting the memeber.
        :param post_check: pool member expiration checked after member is being used.
        :return: member and member stats
        """

        klass_create = getattr(klass, 'create', None)
        klass_check_invalid = getattr(klass, 'check_invalid', None)
        klass_cleanup = getattr(klass, 'cleanup', None)

        self.__pool = Queue()

        self.klass = klass
        self.max_pool_object = max_members
        self.max_reusable_count = max_reusable
        self.expire_in_secs = expires
        self.pre_check = pre_check
        self.post_check = post_check

        self.__create_func = create_func or klass_create or None
        self.__check_func = check_func or klass_check_invalid or None
        self.__cleanup_func = cleanup_func or klass_cleanup or None

        if not self.max_pool_object >= 0:
            raise Exception("max_members should not be negative number.")

        if self.max_pool_object == 0:
            print(f'{klass.__name__} Pool will have unlimited members.')

        if self.expire_in_secs == 0:
            print(f'{klass.__name__} Pool members does not expire.')

        if not lazy:
            self.__create_pool()
        else:
            print(f'pool items will be created on request.')

        print(f'{self.get_pool_size()} pool items are created.')

    def get_pool_size(self):
        """
        gets the pool(queue) size
        :return: integer
        """

        return self.__pool.qsize()

    def get(self):
        """
        gets the contextmanager object for pool fetching

        :return: pool member
        """
        return self.__class__.Executor(self)

    def get_member(self):
        """
        creates pool member if the pool is empty and check is the member is valid to provide to client.

        :return: member, member stats
        """
        pool_size = self.get_pool_size()

        if pool_size == 0:
            obj = self.__create_new_instance()
            obj_stats = self._get_default_stats()
        else:
            obj, obj_stats = self.__pool.get()
            if self.pre_check:
                obj, obj_stats = self.__check_and_get_member(obj, obj_stats)

        return obj, obj_stats

    def queue_member(self, member, member_stats):
        """
        Once client release the member, this method puts back to the pool to re-use.

        :param member: pool member
        :param member_stats: member stats
        """

        if self.post_check:
            member, member_stats = self.__check_and_get_member(member, member_stats)

        self.__pool.put((member, member_stats))

    @staticmethod
    def pool_exists(klass):
        """
        checks if the pool is created already.

        :param klass:
        :return:
        """
        return SingletonMetaPoolRegistry._pool_exists(klass)

    def destroy(self):
        """
        removes class from the pool registry and clean up is being performed.

        :param klass: class for which pool will be created
        """

        klass = self.klass
        while True:
            if self.get_pool_size() == 0:
                break
            else:
                member, stats = self.__pool.get()
                self.__member_cleanup(member, stats)

        SingletonMetaPoolRegistry._remove_pool(klass)

    def _internal_invalid_check(self, **member_stats):
        """
        checks for the valid member by validating max reusable count, expiration
        and custom validation function
        :param member_stats: stats on the member
        :return: boolean - valid or non valid
        """

        created_at = member_stats.get('created_at', None)
        count = member_stats.get('count', None)

        expires_at = self._get_expiry_time(created_at)

        if self.max_reusable_count != 0 and self.max_reusable_count < count:
            print("Member expired by usage count.")
            return True

        if self.expire_in_secs != 0 and expires_at < datetime.datetime.now():
            print("Member expired by usage time.")
            return True

        return False

    def _get_expiry_time(self, created_at):
        """
        provides expiring time

        :param created_at: datetime object
        :return: datetime - expiring time
        """

        if created_at:
            expires_at = created_at + datetime.timedelta(seconds=self.expire_in_secs)
        else:
            expires_at = datetime.datetime.now()

        return expires_at

    def _get_default_stats(self, new=True):
        """
        provide default stats data for the member.

        :param new: flag to indicate new memeber
        :return: dict - memeber stats
        """

        member_stats = {
            'count': 0,
            'new': new,
            'created_at': datetime.datetime.now(),
            'last_used': datetime.datetime.now()
        }
        return member_stats

    def __create_pool(self):
        """
        create pool upto max members to put into the queue.
        """

        for i in range(self.max_pool_object):
            obj = self.__create_new_instance()
            obj_stats = self._get_default_stats()
            self.__pool.put((obj, obj_stats))

    def __create_new_instance(self):
        """
        creates new instance if the custom function is provided or creates instance of the given class
        :return: class instance
        """

        return self.__create_func() if callable(self.__create_func) else self.klass()

    def __check_and_get_member(self, member, member_stats):
        """
        this method validates the member and creates new if the member becomes invalid.
        if the member is valid, it updates stats.

        :param member: client given class instance as pool member
        :param member_stats: pool member stats
        :return: member, member_stats
        """

        invalid_member = self.__check_func(member, **member_stats) if callable(self.__check_func) else False
        invalid_member_internal = self._internal_invalid_check(**member_stats)

        if invalid_member or invalid_member_internal:
            member, member_stats = self.__cleanup_and_get_member(member, member_stats)
        else:
            member_stats = self.__update_member_stats(member_stats)

        return member, member_stats

    def __cleanup_and_get_member(self, member, member_stats):
        """
        creates new member and stats

        :param member: client given class instance as pool member
        :param member_stats: pool member stats
        :return: member, member_stats
        """
        self.__member_cleanup(member, member_stats)
        member = self.__create_new_instance()
        member_stats = self._get_default_stats(new=False)
        return member, member_stats

    def __member_cleanup(self, member, member_stats):
        """
        calls cleanup function if that is provided while creating pool.

        :param member: client given class instance as pool member
        :param member_stats: pool member stats
        """

        if callable(self.__cleanup_func):
            self.__cleanup_func(member, **member_stats)

    def __update_member_stats(self, member_stats):
        """
        updates the stats of the valid member after it is being used.

        :param member_stats: pool member stats
        :return: member_stats
        """

        member_stats['count'] = member_stats['count'] + 1
        member_stats['new'] = False
        member_stats['last_used'] = datetime.datetime.now()
        return member_stats

    class Executor:
        """
        This is context manager for ObjectPool class to fetch pool members
        """

        def __init__(self, klass):
            self.__pool = klass
            self.member, self.member_stats = None, None

        def __enter__(self):
            self.member, self.member_stats = self.__pool.get_member()
            return self.member, self.member_stats

        def __exit__(self, exc_type, exc_val, exc_tb):
            self.__pool.queue_member(self.member, self.member_stats)
