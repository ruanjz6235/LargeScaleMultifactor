
from framework.database import DatabasePool

from common.log import logger


def _empty_function(*args, **kwargs):
    pass


class ResourceInjector:

    def __init__(self, creator, destroyer=None, **kwargs):
        self.creator = creator
        self.destroyer = destroyer if destroyer else _empty_function
        self.config_dict = kwargs if kwargs else {}

    def config(self, **kwargs):
        kwargs = kwargs if kwargs else {}
        self.config_dict.update(kwargs)

    def get_resource(self):
        try:
            if isinstance(self.creator, DatabasePool) and self.creator._pool:
                return self.creator.get_connection()

            return self.creator(**self.config_dict)
        except Exception as e:
            logger.error('failed to create resource with creator %s and config %s due to error %s',
                      self.creator, self.config_dict, e)
            logger.exception(e)
            raise e

    def destroy_resource(self, resource=None):
        logger.info('destroy resource %s', resource)
        if resource:
            return self.destroyer(resource)

        return self.destroyer()

    def __call__(self, target, *args, **kwargs):
        injector = self

        def decorator_maker(receiver_constructor):

            origin_init = receiver_constructor.__init__

            def __init__(self, *args, **kwargs):
                origin_init(self, *args, **kwargs)
                setattr(self, target, injector.get_resource())

            receiver_constructor.__init__ = __init__

            def __getstate__(self):
                state = self.__dict__.copy()
                if target in state:
                    del state[target]
                return state

            receiver_constructor.__getstate__ = __getstate__

            return receiver_constructor

        return decorator_maker
