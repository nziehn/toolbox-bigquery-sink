"""
Code adapted from by: https://github.com/dgilland/pydash
"""

import collections


def get(obj, path, default=None):
    current = obj
    for key in path:
        if isinstance(current, list) and isinstance(key, int):
            if key < 0 or key >= len(current):
                return default
            else:
                current = current[key]
                continue
        if current is None or key not in current:
            return default
        current = current[key]

    return current


def in_obj(obj, path):
    if len(path) == 0:
        return False

    if len(path) == 1:
        current = obj
    else:
        current = get(obj, path[:-1])

    key = path[-1]
    if isinstance(obj, list) and isinstance(key, int):
        if 0 <= key < len(obj):
            return True
        else:
            return False

    return key in current


def insert(container, key_path, item):
    """
    >>> insert({}, ['a', '1', '2', 'world'], 'hello')
    {'a': {'1': {'2': {'world': 'hello'}}}}
    """
    if isinstance(container, collections.OrderedDict):
        gen = collections.OrderedDict
        update = lambda i, k, v: i.update({k: v})
    else:
        gen = dict
        update = lambda i, k, v: i.__setitem__(k, v)

    sub_container = container
    for key in key_path[:-1]:
        if isinstance(key, int):
            raise ValueError('No int keys allowed in deep insert')
        if key not in sub_container:
            update(sub_container, key, gen())
        sub_container = sub_container[key]

    update(sub_container, key_path[-1], item)

    return container



if __name__ == '__main__':
    pass