#!/usr/bin/env python3
"""
Windowed Aggregations for Spark Structured Streaming
"""

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, LongType, TimestampType

def compute_rolling_metrics(df):
    """
    Compute rolling metrics for traffic data:
    - 5-minute tumbling window: AVG(traffic_volume), MAX(traffic_volume), COUNT(*)
    - 15-minute sliding window (slide every 5 min): AVG(traffic_volume), AVG(speed_mph)
    - Per-sensor: group by sensor_id + window
    - Add: volume_trend = current_5min_avg - previous_5min_avg
    """
    
    # 5-minute tumbling window
    tumbling_5min = df.groupBy(
        F.col("sensor_id"),
        F.window("timestamp", "5 minutes")
    ).agg(
        F.avg("traffic_volume").alias("avg_volume_5min"),
        F.max("traffic_volume").alias("max_volume_5min"),
        F.count("*").alias("reading_count_5min"),
        F.avg("speed_mph").alias("avg_speed_5min"),
        F.first("road_name").alias("road_name"),
        F.first("direction").alias("direction"),
        F.first("latitude").alias("latitude"),
        F.first("longitude").alias("longitude")
    )
    
    # Add window start/end times
    tumbling_5min = tumbling_5min.withColumn("window_start", F.col("window.start")) \
                                   .withColumn("window_end", F.col("window.end")) \
                                   .withColumn("sensor_id", F.col("sensor_id"))
    
    # 15-minute sliding window (slide every 5 min)
    sliding_15min = df.groupBy(
        F.col("sensor_id"),
        F.window("timestamp", "15 minutes", "5 minutes")
    ).agg(
        F.avg("traffic_volume").alias("avg_volume_15min"),
        F.avg("speed_mph").alias("avg_speed_15min"),
        F.count("*").alias("reading_count_15min"),
        F.first("road_name").alias("road_name"),
        F.first("direction").alias("direction")
    )
    
    sliding_15min = sliding_15min.withColumn("window_start", F.col("window.start")) \
                                      .withColumn("window_end", F.col("window.end")) \
                                      .withColumn("sensor_id", F.col("sensor_id"))
    
    # Calculate volume trend (current vs previous 5-min average)
    # Join current 5-min with previous 5-min to get trend
    trend_window = tumbling_5min.alias("current").join(
        tumbling_5min.alias("previous"),
        (F.col("current.sensor_id") == F.col("previous.sensor_id")) &
        (F.col("current.window_start") == F.lag(F.col("previous.window_start"), 1))
    )
    
    # Calculate trend
    trend_window = trend_window.withColumn(
        "volume_trend",
        F.col("current.avg_volume_5min") - F.col("previous.avg_volume_5min")
    ).withColumn(
        "trend_indicator",
        F.when(F.col("volume_trend") > 0, "increasing")
         .when(F.col("volume_trend") < 0, "decreasing")
         .otherwise("stable")
    )
    
    # Select final columns from trend calculation
    trend_metrics = trend_window.select(
        F.col("current.sensor_id").alias("sensor_id"),
        F.col("current.window_start").alias("window_start"),
        F.col("current.window_end").alias("window_end"),
        F.col("current.avg_volume_5min").alias("avg_volume_5min"),
        F.col("current.max_volume_5min").alias("max_volume_5min"),
        F.col("current.reading_count_5min").alias("reading_count_5min"),
        F.col("current.avg_speed_5min").alias("avg_speed_5min"),
        F.col("volume_trend").alias("volume_trend"),
        F.col("trend_indicator").alias("trend_indicator"),
        F.col("current.road_name").alias("road_name"),
        F.col("current.direction").alias("direction"),
        F.col("current.latitude").alias("latitude"),
        F.col("current.longitude").alias("longitude")
    )
    
    # Use the 5-minute tumbling window metrics with trend
    final_metrics = trend_metrics
    
    # Add congestion level
    final_metrics = final_metrics.withColumn(
        "congestion_level",
        F.when(F.col("avg_volume_5min") > 5000, "severe_congestion")
         .when(F.col("avg_volume_5min") > 3000, "heavy_traffic")
         .when(F.col("avg_volume_5min") > 1500, "moderate_traffic")
         .otherwise("normal")
    )
    
    # Add peak hour condition
    peak_condition = ((F.col("window_start") >= 7) & (F.col("window_start") <= 9)) | \
                   ((F.col("window_start") >= 17) & (F.col("window_start") <= 19))
    
    final_metrics = final_metrics.withColumn("is_peak_hour", peak_condition)
    
    # Add weekend condition
    weekend_condition = F.col("window_start").cast("date").isin([1, 7])  # Sunday=1, Saturday=7
    
    final_metrics = final_metrics.withColumn("is_weekend", weekend_condition)
    
    # Add processing timestamp
    final_metrics = final_metrics.withColumn("processing_timestamp", F.current_timestamp())
    
    return final_metrics

def add_lag_features(df, lag_columns=["traffic_volume", "speed_mph"], lag_periods=[1, 5, 15]):
    """
    Add lag features for anomaly detection
    """
    from pyspark.sql.window import Window
    
    window_spec = Window.partitionBy("sensor_id").orderBy("timestamp")
    
    for lag_col in lag_columns:
        for lag_period in lag_periods:
            df = df.withColumn(
                f"{lag_col}_lag_{lag_period}",
                F.lag(F.col(lag_col), lag_period).over(window_spec)
            )
    
    return df

def calculate_rolling_std(df, column="traffic_volume", window_minutes=15):
    """
    Calculate rolling standard deviation for anomaly detection
    """
    from pyspark.sql.window import Window
    
    window_spec = Window.partitionBy("sensor_id") \
                           .orderBy("timestamp") \
                           .rangeBetween(-window_minutes * 60, 0)  # Convert minutes to seconds
    
    return df.withColumn(
        f"{column}_rolling_std",
        F.stddev(F.col(column)).over(window_spec)
    ).withColumn(
        f"{column}_rolling_avg",
        F.avg(F.col(column)).over(window_spec)
    )

def add_time_features(df):
    """
    Add time-based features for better analysis
    """
    rush_hour_morning = ((F.hour("timestamp") >= 7) & (F.hour("timestamp") <= 9))
    rush_hour_evening = ((F.hour("timestamp") >= 17) & (F.hour("timestamp") <= 19))
    
    return df.withColumn("hour_of_day", F.hour("timestamp")) \
             .withColumn("day_of_week", F.dayofweek("timestamp")) \
             .withColumn("day_of_month", F.dayofmonth("timestamp")) \
             .withColumn("month", F.month("timestamp")) \
             .withColumn("year", F.year("timestamp")) \
             .withColumn("is_weekend", F.dayofweek("timestamp").isin([1, 7])) \
             .withColumn("is_rush_hour", rush_hour_morning | rush_hour_evening)
