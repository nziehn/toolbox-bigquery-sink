"""
Taken from: https://stackoverflow.com/a/2257449/1816644
"""

import random
import string


def generate_id(size=16, chars=string.ascii_uppercase + string.digits):
    """
    Generate a random id string
    :param size: number of letters in the string
    :param chars: which allowed characters to chose from
    :return: a random string
    """
    return ''.join(random.choice(chars) for _ in range(size))