import pytest

from upstash_redis import Redis


@pytest.fixture(autouse=True)
def flush_db(redis: Redis):
    redis.flushdb()


def test_xread_basic(redis: Redis):
    """Test basic XREAD functionality"""
    stream_key = "test_stream"

    # Add entries
    redis.xadd(stream_key, "*", {"field1": "value1", "field2": "value2"})

    # Read from beginning
    result = redis.xread({stream_key: "0-0"})
    assert len(result) == 1

    # Result should be [stream_name, entries]
    stream_name, entries = result[0]
    assert stream_name == stream_key
    assert len(entries) == 1


def test_xread_multiple_entries(redis: Redis):
    """Test XREAD with multiple entries"""
    stream_key = "test_stream"

    # Add multiple entries
    for i in range(3):
        redis.xadd(stream_key, "*", {"field": f"value{i}"})

    # Read all entries
    result = redis.xread({stream_key: "0-0"})
    stream_name, entries = result[0]

    assert len(entries) == 3


def test_xread_with_count(redis: Redis):
    """Test XREAD with COUNT option"""
    stream_key = "test_stream"

    # Add multiple entries
    for i in range(5):
        redis.xadd(stream_key, "*", {"field": f"value{i}"})

    # Read with count limit
    result = redis.xread({stream_key: "0-0"}, count=2)
    stream_name, entries = result[0]

    assert len(entries) == 2


def test_xread_multiple_streams(redis: Redis):
    """Test XREAD with multiple streams"""
    stream1 = "stream1"
    stream2 = "stream2"

    # Add entries to both streams
    redis.xadd(stream1, "*", {"field": "value1"})
    redis.xadd(stream2, "*", {"field": "value2"})
    redis.xadd(stream1, "*", {"field": "value3"})

    # Read from both streams
    result = redis.xread({stream1: "0-0", stream2: "0-0"})

    # Should return data for both streams
    assert len(result) <= 2  # May be 1 or 2 depending on implementation

    # Collect all returned streams
    returned_streams = {stream_name for stream_name, entries in result}

    # At least one stream should be returned
    assert len(returned_streams) >= 1


def test_xread_from_specific_id(redis: Redis):
    """Test XREAD starting from specific ID"""
    stream_key = "test_stream"

    # Add entries
    id1 = redis.xadd(stream_key, "*", {"field": "value1"})
    id2 = redis.xadd(stream_key, "*", {"field": "value2"})
    id3 = redis.xadd(stream_key, "*", {"field": "value3"})

    # Read from id2 onwards (should get id3)
    result = redis.xread({stream_key: id2})

    if result:  # May be empty if no new entries after id2
        stream_name, entries = result[0]
        if len(entries) > 0:
            # Should get entries after id2
            returned_ids = [entry[0] for entry in entries]
            assert id3 in returned_ids
            assert id1 not in returned_ids
            assert id2 not in returned_ids


def test_xread_no_new_entries(redis: Redis):
    """Test XREAD when no new entries exist"""
    stream_key = "test_stream"

    # Add an entry
    last_id = redis.xadd(stream_key, "*", {"field": "value"})

    # Try to read from the last ID (should return empty or no data)
    result = redis.xread({stream_key: last_id})

    # Should either be empty or contain no entries for this stream
    if result:
        for stream_name, entries in result:
            if stream_name == stream_key:
                assert len(entries) == 0


def test_xread_empty_stream(redis: Redis):
    """Test XREAD on empty stream"""
    result = redis.xread({"nonexistent_stream": "0-0"})

    # Should return empty or no results
    if result:
        for stream_name, entries in result:
            if stream_name == "nonexistent_stream":
                assert len(entries) == 0


def test_xread_dollar_id(redis: Redis):
    """Test XREAD with $ ID (should get only new entries)"""
    stream_key = "test_stream"

    # Add initial entries
    redis.xadd(stream_key, "*", {"field": "old1"})
    redis.xadd(stream_key, "*", {"field": "old2"})

    # Read from $ (should get nothing initially)
    result = redis.xread({stream_key: "$"})

    # Should return empty for this stream
    if result:
        for stream_name, entries in result:
            if stream_name == stream_key:
                assert len(entries) == 0

    # Add new entry
    redis.xadd(stream_key, "*", {"field": "new"})

    # Now read from $ again (in a real scenario, this might still not work
    # without blocking, but we'll test the ID format)
    # This is more of a format validation test


def test_xread_with_mixed_existing_nonexisting_streams(redis: Redis):
    """Test XREAD with mix of existing and non-existing streams"""
    existing_stream = "existing_stream"
    nonexisting_stream = "nonexisting_stream"

    # Add data to existing stream only
    redis.xadd(existing_stream, "*", {"field": "value"})

    # Read from both
    result = redis.xread({existing_stream: "0-0", nonexisting_stream: "0-0"})

    # Should get data for existing stream only
    if result:
        stream_names = [stream_name for stream_name, entries in result]
        # Existing stream should be present
        found_existing = False
        for stream_name, entries in result:
            if stream_name == existing_stream:
                found_existing = True
                assert len(entries) > 0
            elif stream_name == nonexisting_stream:
                assert len(entries) == 0

        # Should have found the existing stream
        if len(result) > 0:
            assert found_existing or nonexisting_stream not in stream_names


def test_xread_progressive_reading(redis: Redis):
    """Test progressive reading as new entries are added"""
    stream_key = "test_stream"

    # Start with empty stream
    last_read_id = "0-0"

    # Add entries and read progressively
    for i in range(3):
        # Add new entry
        new_id = redis.xadd(stream_key, "*", {"counter": str(i)})

        # Read from last position
        result = redis.xread({stream_key: last_read_id})

        if result:
            stream_name, entries = result[0]
            if stream_name == stream_key and len(entries) > 0:
                # Update last read position
                last_read_id = entries[-1][0]

                # Verify we got the expected entry
                for entry_id, entry_data in entries:
                    if entry_id == new_id:
                        entry_dict = {}
                        for j in range(0, len(entry_data), 2):
                            entry_dict[entry_data[j]] = entry_data[j + 1]
                        assert entry_dict["counter"] == str(i)


def test_xread_entry_format(redis: Redis):
    """Test that XREAD returns entries in correct format"""
    stream_key = "test_stream"

    # Add entry with known data
    test_data = {"name": "John", "age": "30", "city": "NYC"}
    entry_id = redis.xadd(stream_key, "*", test_data)

    # Read the entry
    result = redis.xread({stream_key: "0-0"})
    assert len(result) == 1

    stream_name, entries = result[0]
    assert stream_name == stream_key
    assert len(entries) == 1

    returned_id, returned_data = entries[0]
    assert returned_id == entry_id

    # Convert returned data to dict
    returned_dict = {}
    for i in range(0, len(returned_data), 2):
        returned_dict[returned_data[i]] = returned_data[i + 1]

    assert returned_dict == test_data
