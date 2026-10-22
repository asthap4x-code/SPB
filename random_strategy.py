
import random


class RandomStrategy:
    """Random UP/DOWN signals for testing only."""

    name = "Random Strategy"

    def __init__(self, enabled=True):
        self.enabled = enabled

    def analyze(self, candles=None, *args, **kwargs):
        """
        Returns:
            "UP", "DOWN", or None
        """
        if not self.enabled:
            return None

        if candles is not None and len(candles) < 1:
            return None

        return random.choice(["UP", "DOWN"])

    def get_signal(self, candles=None, *args, **kwargs):
        return self.analyze(candles, *args, **kwargs)


def generate_signal(candles=None):
    """Simple function interface for compatibility."""
    if candles is not None and len(candles) < 1:
        return None

    return random.choice(["UP", "DOWN"])
