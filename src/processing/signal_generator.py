from pyspark.sql.functions import *
from pyspark.sql.types import *

def generate_trading_signals(df):
    """
    Generate buy/sell signals based on volume and price action analysis.
    
    This function implements a comprehensive signal generation strategy that focuses
    on volume confirmation of price movements rather than traditional indicators.
    """
    
    signals_df = df.withColumn(
        "signal",
        when(
            # Strong Bullish Signal Conditions:
            # 1. Significant price increase (>1%)
            # 2. Volume spike (>50% above average)
            # 3. Price breaks above previous high (breakout)
            (col("price_change_pct") > 1.0) & 
            (col("volume_ratio") > 1.5) &
            (col("close") > col("prev_high")) &
            (col("prev_high").isNotNull()),
            "BUY"
        ).when(
            # Strong Bearish Signal Conditions:
            # 1. Significant price decrease (<-1%)
            # 2. Volume spike (>50% above average)
            # 3. Price breaks below previous low (breakdown)
            (col("price_change_pct") < -1.0) &
            (col("volume_ratio") > 1.5) &
            (col("close") < col("prev_low")) &
            (col("prev_low").isNotNull()),
            "SELL"
        ).when(
            # Bullish Engulfing Pattern with Volume:
            # Current candle closes above previous high
            # Current candle opened below previous close
            # Supported by above-average volume
            (col("close") > col("prev_high")) &
            (col("open") < col("prev_close")) &
            (col("volume_ratio") > 1.2) &
            (col("body_size") > (col("high") - col("low")) * 0.6) &  # Strong body
            (col("prev_high").isNotNull()) &
            (col("prev_close").isNotNull()),
            "BUY"
        ).when(
            # Bearish Engulfing Pattern with Volume:
            # Current candle closes below previous low
            # Current candle opened above previous close
            # Supported by above-average volume
            (col("close") < col("prev_low")) &
            (col("open") > col("prev_close")) &
            (col("volume_ratio") > 1.2) &
            (col("body_size") > (col("high") - col("low")) * 0.6) &  # Strong body
            (col("prev_low").isNotNull()) &
            (col("prev_close").isNotNull()),
            "SELL"
        ).when(
            # Volume Accumulation Signal:
            # Price moving up with consistent high volume
            # Indicates institutional accumulation
            (col("price_change_pct") > 0.5) &
            (col("volume_ratio") > 1.3) &
            (col("high_volume_up") == True),
            "BUY"
        ).when(
            # Volume Distribution Signal:
            # Price moving down with consistent high volume
            # Indicates institutional distribution
            (col("price_change_pct") < -0.5) &
            (col("volume_ratio") > 1.3) &
            (col("high_volume_down") == True),
            "SELL"
        ).otherwise("HOLD")
    ).withColumn(
        # Signal strength based on volume intensity
        "signal_strength",
        when(col("volume_ratio") > 2.5, "VERY_STRONG")
        .when(col("volume_ratio") > 2.0, "STRONG")
        .when(col("volume_ratio") > 1.5, "MODERATE")
        .when(col("volume_ratio") > 1.2, "WEAK")
        .otherwise("VERY_WEAK")
    ).withColumn(
        # Additional context for signal validation
        "signal_context", 
        struct(
            col("volume_ratio").alias("volume_multiple"),
            col("price_change_pct").alias("price_move_pct"),
            col("body_size").alias("candle_body_size"),
            (col("upper_shadow") + col("lower_shadow")).alias("total_shadows"),
            col("volume_spike").alias("volume_anomaly")
        )
    ).withColumn(
        "signal_timestamp", current_timestamp()
    )
    
    # Filter to only return actionable signals (BUY/SELL)
    return signals_df.filter(col("signal").isin(["BUY", "SELL"]))

def add_risk_management_metrics(signals_df):
    """Add risk management metrics to trading signals."""
    
    return signals_df.withColumn(
        # Calculate stop loss level (2% below buy price or 2% above sell price)
        "stop_loss_price",
        when(col("signal") == "BUY", col("close") * 0.98)
        .when(col("signal") == "SELL", col("close") * 1.02)
        .otherwise(None)
    ).withColumn(
        # Calculate take profit level (3% above buy price or 3% below sell price)
        "take_profit_price",
        when(col("signal") == "BUY", col("close") * 1.03)
        .when(col("signal") == "SELL", col("close") * 0.97)
        .otherwise(None)
    ).withColumn(
        # Risk-reward ratio
        "risk_reward_ratio",
        when(col("signal") == "BUY", 
             (col("close") * 1.03 - col("close")) / (col("close") - col("close") * 0.98))
        .when(col("signal") == "SELL",
             (col("close") - col("close") * 0.97) / (col("close") * 1.02 - col("close")))
        .otherwise(None)
    )
