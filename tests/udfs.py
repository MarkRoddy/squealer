"""
User Defined Functions needed for testing.

Intended to be imported by Pig, do not run me
directly.
"""


@outputSchema('newstring:chararray')
def concat(string1, string2):
    return string1 + string2
