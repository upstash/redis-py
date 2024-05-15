import pytest

from upstash_redis import Redis


@pytest.fixture(autouse=True)
def flush_db(redis: Redis):
    redis.delete("rocket", "space", "marine")

def test_pipeline(redis: Redis):

    pipeline = redis.pipeline()

    pipeline.incr("rocket")
    pipeline.incr("rocket")
    pipeline.incr("space")
    pipeline.incr("rocket")
    pipeline.incr("space")
    pipeline.incr("rocket")

    pipeline.get("rocket").get("space").get("marine")

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

def test_context_manager_usage(redis: Redis):

    with redis.pipeline() as pipeline:
        pipeline.incr("rocket")
        pipeline.incr("rocket")
        pipeline.incr("space")
        pipeline.incr("rocket")
        pipeline.incr("space")
        pipeline.incr("rocket")
        result = pipeline.exec()

        # add a command to the pipeline which will be
        # removed from the pipeline when we exit the context
        pipeline.set("foo", "bar")

    assert result == [1, 2, 1, 3, 2, 4]
    assert len(pipeline._command_stack) == 0 # pipeline is empty

    # redis still works after pipeline is done
    result = redis.get("rocket")
    assert result == "4"

    get_pipeline = redis.pipeline()
    get_pipeline.get("rocket")
    get_pipeline.get("space")
    get_pipeline.get("marine")

    res = get_pipeline.exec()
    assert res == ["4", "2", None]

def test_context_manager_raise(redis: Redis):
    """
    Check that exceptions in context aren't silently ignored

    This can happen if we return something in __exit__ method
    """
    with pytest.raises(Exception):
        with redis.pipeline() as pipeline:
            pipeline.incr("rocket")
            raise Exception("test")

def test_run_pipeline_twice(redis: Redis):
    """
    Runs a pipeline twice
    """
    pipeline = redis.pipeline()
    pipeline.incr("bird")
    result = pipeline.exec()
    assert result == [1]

    pipeline.incrby("bird", 2)
    result = pipeline.exec()
    assert result == [3]
