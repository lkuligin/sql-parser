"""Utils for parser."""

def is_integer(value: str):
    """Check whether a string can be converted to an interger."""
    try:
        int(value)
        return True
    except ValueError:
        return False
