def format_duration(seconds, digits=3):
    """
    Format a duration in seconds using the most appropriate unit.
    Returns (value_str, unit).
    """
    if seconds is None:
        return "-", ""

    try:
        seconds = float(seconds)
    except (TypeError, ValueError):
        return str(seconds), ""

    if seconds >= 1:
        return f"{seconds:.{digits}f}", "s"
    elif seconds >= 1e-3:
        return f"{seconds * 1e3:.{digits}f}", "ms"
    elif seconds >= 1e-6:
        return f"{seconds * 1e6:.{digits}f}", "µs"
    else:
        return f"{seconds * 1e9:.{digits}f}", "ns"
