from relay_mesos import log


def catch(func, exception_sender):
    """Closure that calls given func.  If an error is raised, send it somewhere

    `func` function to call
    `exception_sender` a writable end of a multiprocessing.Pipe
    """
    def f(*args, **kwargs):
        try:
            func(*args, **kwargs)
        except Exception as e:
            log.exception(e)
            exception_sender.send(e)
    return f
