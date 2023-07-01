# from pytest import mark
# from tests.client import syncRedis
# from tests.execute_on_http import sync_execute_on_http

# def test() -> None:
    
#     assert syncRedis.copy(source="string", destination="copy_destination") is True

#     assert sync_execute_on_http("GET", "copy_destination") == "test"

# def test_with_replace() -> None:
    
#     assert (
#         syncRedis.copy(
#             source="string", destination="string_as_copy_destination", replace=True
#         )
#         is True
#     )

#     assert sync_execute_on_http("GET", "string_as_copy_destination") == "test"


# def test_without_formatting() -> None:
#     syncRedis.format_return = False

#     assert syncRedis.copy(source="string", destination="copy_destination_2") == 1

#     syncRedis.format_return = True

# def test_with_formatting() -> None:
#     syncRedis.format_return = True

#     assert syncRedis.copy(source="string", destination="copy_destination_2") == False