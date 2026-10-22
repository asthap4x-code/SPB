# stm_strategy.py
# STM PRO BOT - STM Strategy

from typing import Any, Dict, List, Optional


class STMStrategy:
    def __init__(self):
        self.name = "STM"

    @staticmethod
    def _candle_color(candle: Dict[str, Any]) -> Optional[str]:
        try:
            open_price = float(candle["open"])
            close_price = float(candle["close"])

            if close_price > open_price:
                return "GREEN"
            if close_price < open_price:
                return "RED"

            return "DOJI"
        except (KeyError, TypeError, ValueError):
            return None

    def pvip(self, candles: List[Dict[str, Any]]) -> Dict[str, Any]:
        if not candles or len(candles) < 4:
            return {
                "signal": "WAIT",
                "strategy": "PVIP",
                "reason": "Not enough candles",
            }

        colors = [self._candle_color(c) for c in candles]
        last_color = colors[-1]

        if last_color not in ("GREEN", "RED"):
            return {
                "signal": "WAIT",
                "strategy": "PVIP",
                "reason": "Last candle is doji or invalid",
            }

        # Count consecutive candles before the last candle.
        previous_color = colors[-2]
        count = 0

        for color in reversed(colors[:-1]):
            if color != previous_color:
                break
            count += 1

        # Reversal confirmation:
        # consecutive green candles followed by a red candle
        # or consecutive red candles followed by a green candle.
        if previous_color == "GREEN" and last_color == "RED":
            if count >= 3:
                return {
                    "signal": "DOWN",
                    "strategy": "PVIP",
                    "reason": "Green run followed by red candle",
                }

        if previous_color == "RED" and last_color == "GREEN":
            if count >= 3:
                return {
                    "signal": "UP",
                    "strategy": "PVIP",
                    "reason": "Red run followed by green candle",
                }

        return {
            "signal": "WAIT",
            "strategy": "PVIP",
            "reason": "No confirmed setup",
        }

    def pvip2(self, candles: List[Dict[str, Any]]) -> Dict[str, Any]:
        if not candles or len(candles) < 3:
            return {
                "signal": "WAIT",
                "strategy": "PVIP2",
                "reason": "Not enough candles",
            }

        try:
            last = candles[-1]
            previous = candles[-2]

            open_price = float(last["open"])
            close_price = float(last["close"])
            prev_open = float(previous["open"])
            prev_close = float(previous["close"])

            ema_value = last.get("ema10", last.get("ema"))

            if ema_value is None:
                return {
                    "signal": "WAIT",
                    "strategy": "PVIP2",
                    "reason": "EMA value is missing",
                }

            ema_value = float(ema_value)

            # Price crosses/retests EMA and closes back on one side.
            if prev_close < ema_value and close_price > ema_value:
                return {
                    "signal": "UP",
                    "strategy": "PVIP2",
                    "reason": "Price crossed above EMA",
                }

            if prev_close > ema_value and close_price < ema_value:
                return {
                    "signal": "DOWN",
                    "strategy": "PVIP2",
                    "reason": "Price crossed below EMA",
                }

            # Candle direction fallback.
            if close_price > open_price and prev_close < prev_open:
                return {
                    "signal": "UP",
                    "strategy": "PVIP2",
                    "reason": "Bullish candle reversal",
                }

            if close_price < open_price and prev_close > prev_open:
                return {
                    "signal": "DOWN",
                    "strategy": "PVIP2",
                    "reason": "Bearish candle reversal",
                }

        except (KeyError, TypeError, ValueError):
            pass

        return {
            "signal": "WAIT",
            "strategy": "PVIP2",
            "reason": "No confirmed setup",
        }

    def analyze(
        self,
        candles: List[Dict[str, Any]],
        strategy: str = "pvip",
    ) -> Dict[str, Any]:
        strategy = str(strategy).lower().strip()

        if strategy in ("pvip2", "pvip_2", "2"):
            return self.pvip2(candles)

        return self.pvip(candles)

    def get_signal(
        self,
        candles: List[Dict[str, Any]],
        strategy: str = "pvip",
    ) -> str:
        result = self.analyze(candles, strategy)
        return result["signal"]

    def generate_signal(
        self,
        candles: List[Dict[str, Any]],
        strategy: str = "pvip",
    ) -> Dict[str, Any]:
        return self.analyze(candles, strategy)


# Module-level function required by ai_strategy.py
def analyze(
    candles: List[Dict[str, Any]],
    strategy: str = "pvip",
) -> Dict[str, Any]:
    engine = STMStrategy()
    return engine.analyze(candles, strategy)


def stm_pvip(candles: List[Dict[str, Any]]) -> Dict[str, Any]:
    return STMStrategy().pvip(candles)


def stm_pvip2(candles: List[Dict[str, Any]]) -> Dict[str, Any]:
    return STMStrategy().pvip2(candles)
