from typing import Union, List, Dict

"""
The raw output returned by some Geo commands, usually the ones that return properties of members.

If no additional properties are requested, the output is a list of strings, each string representing a member.

If coordinates are requested, each member's properties will be represented by a list (the result itself being a list),
where the coordinates themselves will be another one.
"""
GeoMembersReturn = List[Union[str, List[Union[str, List[str]]]]]

"""
The output resulted by formatting "GeoMembersReturn".

Note that this might differ from the "GeoMember" type that represents the initial properties of a geo member.
"""
FormattedGeoMembersReturn = List[Dict[str, Union[str, float, int]]]


# The raw output returned by the Hash commands that return the field-value pairs of a hash.
HashReturn = List[str]

# The output resulted by formatting "HashReturn"
FormattedHashReturn = Dict[str, str]

# The raw output returned by the Sorted Set commands that return the member-score pairs of a sorted set.
SortedSetReturn = List[str]

# The output resulted by formatting "SortedSetReturn"
FormattedSortedSetReturn = Dict[str, float]
