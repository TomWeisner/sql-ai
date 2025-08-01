def normalize_and_sort(obj):
    """Recursively normalize and sort keys in dicts, and stringify numbers."""
    if isinstance(obj, dict):
        return {k: normalize_and_sort(obj[k]) for k in sorted(obj)}
    elif isinstance(obj, list):
        return [normalize_and_sort(v) for v in obj]
    elif isinstance(obj, (int, float)):
        return str(obj)
    elif isinstance(obj, str):
        return obj.strip()
    else:
        return obj
