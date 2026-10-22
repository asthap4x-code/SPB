
from typing import Optional, Dict, Any, List


def candle_color(candle: Dict[str, Any]) -> Optional[str]:
    """Detect candle color from open and close prices."""
    try:
        open_price = float(candle["open"])
        close_price = float(candle["close"])
    except (KeyError, TypeError, ValueError):
        return None

    if close_price > open_price:
        return "GREEN"
    if close_price < open_price:
        return "RED"
    return "DOJI"


def get_value(candle, *keys):
    for key in keys:
        if key in candle and candle[key] is not None:
            try:
                return float(candle[key])
            except (TypeError, ValueError):
                pass
    return None


class STMStrategy:
    """
    STM PVIP:
    - Uptrend: 3-10 green candles, then one red candle -> UP.
    - Downtrend: 3-10 red candles, then one green candle -> DOWN.

    STM PVIP 2:
    - EMA 10 + matching green close / next red open level.
    - Retest and candle body-position confirmation.
    """

    def __init__(
        self,
        min_trend_candles=3,
        max_trend_candles=10,
        body_percent=55,
        max_body_percent=85,
        level_tolerance=0.0002,
    ):
        self.min_trend_candles = min_trend_candles
        self.max_trend_candles = max_trend_candles
        self.body_percent = body_percent
        self.max_body_percent = max_body_percent
        self.level_tolerance = level_tolerance

    # --------------------------------------------------
    # STM PVIP - Trend-following candle pattern
    # --------------------------------------------------
    def pvip(self, candles: List[Dict[str, Any]]) -> Optional[str]:
        """
        candles must be chronological: oldest first, newest last.
        The final candle must be the opposite-color candle.
        Signal is for the NEXT candle.
        """
        if not candles or len(candles) < self.min_trend_candles + 1:
            return None

        last_color = candle_color(candles[-1])

        # Last candle red after a run of 3-10 green candles => UP
        if last_color == "RED":
            count = 0
            for candle in reversed(candles[:-1]):
                if candle_color(candle) != "GREEN":
                    break
                count += 1

            if self.min_trend_candles <= count <= self.max_trend_candles:
                return "UP"

        # Last candle green after a run of 3-10 red candles => DOWN
        elif last_color == "GREEN":
            count = 0
            for candle in reversed(candles[:-1]):
                if candle_color(candle) != "RED":
                    break
                count += 1

            if self.min_trend_candles <= count <= self.max_trend_candles:
                return "DOWN"

        return None

    # --------------------------------------------------
    # STM PVIP 2 - EMA 10 + horizontal level retest
    # --------------------------------------------------
    def pvip2(self, candles: List[Dict[str, Any]]) -> Optional[str]:
        """
        Each candle should contain:
            open, high, low, close, ema10

        Requires a level formed by:
            previous GREEN candle close ~= next RED candle open
        or the reverse arrangement for the bearish level.

        Signal is for the NEXT candle after a confirmed retest.
        """
        if not candles or len(candles) < 3:
            return None

        # Search for the latest valid horizontal level in prior candles.
        level = None
        level_type = None
        level_index = None

        for i in range(len(candles) - 2):
            first = candles[i]
            second = candles[i + 1]

            c1 = candle_color(first)
            c2 = candle_color(second)

            close1 = get_value(first, "close")
            open2 = get_value(second, "open")
            ema1 = get_value(first, "ema10", "ema")
            ema2 = get_value(second, "ema10", "ema")

            if None in (close1, open2, ema1, ema2):
                continue

            # Price equality within a small tolerance.
            if abs(close1 - open2) > self.level_tolerance:
                continue

            # Bullish level: both candles above EMA.
            if (
                c1 == "GREEN"
                and c2 == "RED"
                and close1 > ema1
                and open2 > ema2
            ):
                level = (close1 + open2) / 2
                level_type = "BULLISH"
                level_index = i + 1

            # Bearish level: both candles below EMA.
            elif (
                c1 == "RED"
                and c2 == "GREEN"
                and close1 < ema1
                and open2 < ema2
            ):
                level = (close1 + open2) / 2
                level_type = "BEARISH"
                level_index = i + 1

        if level is None or level_index is None:
            return None

        # Need a later candle that retests the level.
        retest = candles[-1]

        if len(candles) - 1 <= level_index:
            return None

        open_price = get_value(retest, "open")
        close_price = get_value(retest, "close")
        high_price = get_value(retest, "high")
        low_price = get_value(retest, "low")
        ema = get_value(retest, "ema10", "ema")

        if None in (open_price, close_price, high_price, low_price, ema):
            return None

        full_range = high_price - low_price
        body = abs(close_price - open_price)

        if full_range <= 0 or body <= 0:
            return None

        body_ratio = body / full_range * 100

        if not (
            self.body_percent <= body_ratio <= self.max_body_percent
        ):
            return None

        # Retest candle must touch or cross the level.
        touched_level = low_price <= level <= high_price
        if not touched_level:
            return None

        if level_type == "BULLISH":
            # Confirm bullish body and price above EMA.
            body_position = (close_price - level) / body * 100

            if (
                candle_color(retest) == "GREEN"
                and close_price > ema
                and body_position >= self.body_percent
            ):
                return "UP"

        elif level_type == "BEARISH":
            # Confirm bearish body and price below EMA.
            body_position = (level - close_price) / body * 100

            if (
                candle_color(retest) == "RED"
                and close_price < ema
                and body_position >= self.body_percent
            ):
                return "DOWN"

        return None

    # --------------------------------------------------
    # Compatibility methods
    # --------------------------------------------------
    def analyze(self, candles, strategy="pvip"):
        strategy = str(strategy).lower().replace(" ", "")

        if strategy in ("pvip2", "stm pvip2", "stm_pvip2"):
            return self.pvip2(candles)

        return self.pvip(candles)

    def get_signal(self, candles, strategy="pvip"):
        return self.analyze(candles, strategy)


# Convenience functions
def stm_pvip(candles):
    return STMStrategy().pvip(candles)


def stm_pvip2(candles):
    return STMStrategy().pvip2(candles)
