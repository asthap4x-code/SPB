# random_strategy.py
import random
from typing import Any, Dict, List


class RandomStrategy:
    def __init__(self):
        self.name = "RANDOM"

    def analyze(self, candles=None) -> Dict[str, Any]:
        if not candles:
            return {
                "signal": "WAIT",
                "strategy": "RANDOM",
                "reason": "No candle data",
            }

        signal = random.choice(["UP", "DOWN"])

        return {
            "signal": signal,
            "strategy": "RANDOM",
            "reason": "Random signal generated",
        }

    def get_signal(self, candles=None) -> str:
        return self.analyze(candles)["signal"]

    def generate_signal(self, candles=None) -> Dict[str, Any]:
        return self.analyze(candles)


def analyze(candles=None) -> Dict[str, Any]:
    return RandomStrategy().analyze(candles)


def get_signal(candles=None) -> str:
    return RandomStrategy().get_signal(candles)


def generate_signal(candles=None) -> Dict[str, Any]:
    return RandomStrategy().generate_signal(candles)
