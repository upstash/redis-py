# The raw output returned by some Geo commands, usually the ones that return properties of members.
GeoMembersReturn = list[str]

"""
The output resulted by formatting "GeoMembersReturn".

Note that this might differ from the "GeoMember" type that represents the initial properties of a geo member.
"""
FormattedGeoMembersReturn = list[dict[str, float | int]]


# The raw output returned by the Hash commands that return the field-value pairs of a hash.
HashReturn = list[str]

# The output resulted by formatting "HashReturn"
FormattedHashReturn = dict[str, str]

# The raw output returned by the Sorted Set commands that return the member-score pairs of a sorted set.
SortedSetReturn = list[str]

# The output resulted by formatting "SortedSetReturn"
FormattedSortedSetReturn = dict[str, float]
