import pytest

from upstash_redis import Redis


@pytest.fixture(autouse=True)
def flush_db(redis: Redis):
    redis.flushdb()

def test_pipeline(redis: Redis):

    pipeline = redis.pipeline()

    pipeline.incr("rocket")
    pipeline.incr("rocket")
    pipeline.incr("space")
    pipeline.incr("rocket")
    pipeline.incr("space")
    pipeline.incr("rocket")

    pipeline.get("rocket")
    pipeline.get("space")
    pipeline.get("marine")

    res = pipeline.exec()
    assert res == [1, 2, 1, 3, 2, 4, "4", "2", None]

def test_multi(redis: Redis):

    pipeline = redis.multi()

    pipeline.incr("rocket")
    pipeline.incr("rocket")
    pipeline.incr("space")
    pipeline.incr("rocket")
    pipeline.incr("space")
    pipeline.incr("rocket")

    pipeline.get("rocket")
    pipeline.get("space")
    pipeline.get("marine")

    res = pipeline.exec()
    assert res == [1, 2, 1, 3, 2, 4, "4", "2", None]

def test_raises(redis: Redis):

    pipeline = redis.pipeline()
    with pytest.raises(NotImplementedError):
        pipeline.pipeline()
    with pytest.raises(NotImplementedError):
        pipeline.multi()

    multi = redis.multi()
    with pytest.raises(NotImplementedError):
        multi.pipeline()
    with pytest.raises(NotImplementedError):
        multi.multi()
