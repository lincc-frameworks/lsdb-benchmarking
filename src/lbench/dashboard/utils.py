def format_memory(bytes_value, digits=2):
    """
    Format a memory size in bytes using binary units (KiB, MiB, GiB).
    Returns (value_str, unit).
    """
    if bytes_value is None:
        return "-", ""

    try:
        num_bytes = int(bytes_value)
    except (TypeError, ValueError):
        return str(bytes_value), ""

    units = ["B", "KiB", "MiB", "GiB", "TiB"]

    for unit in units:
        if abs(num_bytes) < 1024:
            return f"{num_bytes:.{digits}f}", unit
        num_bytes /= 1024

    return f"{num_bytes:.{digits}f}", units[-1]

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
