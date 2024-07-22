from upstash_redis import Redis


def test_should_update_sync_token_on_basic_request(redis: Redis):
    redis = Redis.from_env(read_your_writes=True)

    initial_token = redis._upstash_sync_token

    redis.set("key", "value")

    updated_token = redis._upstash_sync_token

    assert initial_token != updated_token


def test_should_update_sync_token_on_pipeline(redis: Redis):
    redis = Redis.from_env()

    initial_token = redis._upstash_sync_token

    pipeline = redis.pipeline()

    pipeline.set("key", "value")
    pipeline.set("key2", "value2")

    pipeline.exec()

    updated_token = redis._upstash_sync_token

    assert initial_token != updated_token


def test_updates_after_successful_lua_script_call(redis):
    s = """
    redis.call('SET', 'mykey', 'myvalue')
    return 1
    """
    initial_sync = redis._upstash_sync_token
    redis.eval(s, keys=[], args=[])

    updated_sync = redis._upstash_sync_token
    assert updated_sync != initial_sync


def test_should_not_update_sync_state_with_opt_out_ryw():
    redis = Redis.from_env(read_your_writes=False)
    initial_sync = redis._upstash_sync_token
    redis.set("key", "value")
    updated_sync = redis._upstash_sync_token
    assert updated_sync == initial_sync


def test_should_update_sync_state_with_default_behavior():
    redis = Redis.from_env()
    initial_sync = redis._upstash_sync_token
    redis.set("key", "value")
    updated_sync = redis._upstash_sync_token
    assert updated_sync != initial_sync
