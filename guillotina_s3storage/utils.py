def aretriable(count=3, reraise=True, on_retry_exhausted=None):

    def decorator(func):
        async def wrapped(*args, **kwargs):
            retried = 0
            while retried < count:
                try:
                    return await func(*args, **kwargs)
                except:
                    if retried >= count:
                        if on_retry_exhausted is not None:
                            on_retry_exhausted(*args, **kwargs)
                        if reraise:
                            raise
        return wrapped
    return decorator
