import time
import logging
import numpy as np
from functools import wraps

logger = logging.getLogger(__name__)


def runtime(func):
    """
    Calculation of function runtime.
    :param func: function apply calculation runtime to.
    :return: function wrapper.
    """
    @wraps(func)
    def wrap(*args, **kwargs):
        s_time = time.time()
        try:
            return func(*args, **kwargs)
        finally:
            final_time = np.round(time.time() - s_time, 0)
            minutes = np.fix(final_time // 60).astype(int)
            seconds = np.round(final_time % 60).astype(int)
            if all([minutes > 0, seconds > 0]):
                logger.info("'%s' completed in %s min. %s sec." % (wrap.__name__, str(minutes), str(seconds)))
    return wrap
