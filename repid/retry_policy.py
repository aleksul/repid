from datetime import timedelta
from typing import Protocol


class RetryPolicyT(Protocol):
    def __call__(self, retry_number: int = 1) -> timedelta:
        """Calculates backoff for retries.

        Args:
            retry_number (int, optional): what is the number of this retry.
            Always >= 1. Defaults to 1.

        Returns:
            timedelta: time to wait before retring.
        """


def default_retry_policy_factory(
    min_backoff: int = 10,
    max_backoff: int = 86400,
    multiplier: int = 5,
    max_exponent: int = 15,
) -> RetryPolicyT:
    """Creates default (exponential) retry policy.

    Args:
        min_backoff (int, optional): Minimum amount of seconds to delay for. Defaults to 10.
        max_backoff (int, optional): Maximum amount of seconds to delay for. Defaults to 86400.
        multiplier (int, optional): Amount of seconds which will be multiplied. Defaults to 5.
        max_exponent (int, optional): Exponent can't exceed this number. Defaults to 15.

    Returns:
        RetryPolicyT: exponential retry policy.
    """

    def inner(retry_number: int = 1) -> timedelta:
        exponent = min(retry_number, max_exponent)
        backoff = min(multiplier * 2**exponent, max_backoff)
        return timedelta(seconds=max(min_backoff, backoff))

    return inner
