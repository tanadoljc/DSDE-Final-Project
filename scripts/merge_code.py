import findspark

spark_url = 'local'

#library

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, round, date_sub, avg
from pyspark.sql import functions as F


spark = SparkSession.builder \
    .master(spark_url) \
    .appName('Merge Code') \
    .getOrCreate()

path = '../used_data.csv'
trained_df = spark.read.csv(path, header=True, inferSchema=True)


"""# Merge Holiday Data"""

holiday_path = '../external_csv/bangkok_holidays.csv'
holiday_df = spark.read.csv(holiday_path, header=True, inferSchema=True)

trained_df = trained_df.withColumn("date", F.to_date("date"))
holiday_df = holiday_df.withColumn("holiday_date", F.to_date("holiday_date"))

# Extract week start (Monday) for main and holiday dates
trained_df = trained_df.withColumn("week_start", F.date_sub("date", F.dayofweek("date") - 2))
holiday_df = holiday_df.withColumn("holiday_week_start", F.date_sub("holiday_date", F.dayofweek("holiday_date") - 2))

# Join on the same week_start
df_joined = trained_df.join(holiday_df, trained_df.week_start == holiday_df.holiday_week_start, how="left")

# If holiday_date is not null, it means that week has a holiday
df_res = df_joined.withColumn("holiday_this_week", F.when(F.col("holiday_date").isNotNull(), True).otherwise(False))

df_res = df_res.drop("holiday_name", "holiday_week_start", "holiday_date", "week_start")


"""# Merge Rain Data"""


df_res = df_res.withColumn("date", to_date(col("date")))
# df_res = df_res.filter(col("date") > "2022-09-01")

df_res = df_res.drop("rounded_lat", "rounded_lon")

df_res = df_res.withColumn("rounded_lat", round(col("lat"), 2)) \
                             .withColumn("rounded_lon", round(col("long"), 2))

# fix lat long already
rain_df = spark.read.csv("../external_csv/rainfall_data.csv", header=True, inferSchema=True)
rain_df = rain_df.withColumn("date", to_date(col("date"))) \
                 .withColumn("rounded_lat", round(col("long"), 2)) \
                 .withColumn("rounded_lon", round(col("lat"), 2)) \
                 .select("date", "rounded_lat", "rounded_lon", "Precipitation (mm)")

df_res = df_res.withColumn("date_start", date_sub(col("date"), 7)) \
                             .withColumn("date_end", col("date"))

rain_df_renamed = rain_df.withColumnRenamed("date", "rain_date")
df_merged = df_res.join(
    rain_df_renamed,
    (df_res.rounded_lat == rain_df_renamed.rounded_lat) &
    (df_res.rounded_lon == rain_df_renamed.rounded_lon) &
    (rain_df_renamed.rain_date >= df_res.date_start) &
    (rain_df_renamed.rain_date <= df_res.date_end),
    how='left'
)


group_by_columns = [df_res[c] for c in df_res.columns if c not in ["date_start", "date_end"]]
df_merged = df_merged.select(
    *group_by_columns,
    col("Precipitation (mm)")
).groupBy(
    *[df_res[c] for c in df_res.columns if c not in ["date_start", "date_end"]]
).agg(
    avg(col("Precipitation (mm)")).alias("avg_precipitation_7days")
)

df_merged = df_merged.fillna({"avg_precipitation_7days": 0})

df_merged = df_merged.withColumn("lat", col("lat")) \
                     .withColumn("long", col("long"))


"""# Merge PM2.5 Data"""

# === Step 1: Load PM2.5 data ===
pm25_df = spark.read.csv("../external_csv/pm25_data.csv", header=True, inferSchema=True)

# Format columns
pm25_df = pm25_df.withColumn("date", to_date(col("date"))) \
                 .withColumn("rounded_lat", round(col("rounded_lat"), 2)) \
                 .withColumn("rounded_lon", round(col("rounded_lon"), 2)) \
                 .select("date", "rounded_lat", "rounded_lon", "pm2_5")

# === Step 3: Join with PM2.5 ===
df_final = df_merged.join(
    pm25_df,
    on=[
        df_merged.date == pm25_df.date,
        df_merged.rounded_lat == pm25_df.rounded_lat,
        df_merged.rounded_lon == pm25_df.rounded_lon
    ],
    how="inner"
).select(
    df_merged["*"],  # all columns from df_merged
    pm25_df["pm2_5"] # only the PM2.5 value from the right table
)

df_final = df_final.fillna({"pm2_5": 0.0})


df_final = df_final.withColumn("avg_precipitation_7days", round(col("avg_precipitation_7days"), 2))

df_final = df_final.withColumn("pm2_5", round(col("pm2_5"), 2))

df_used = df_final.select('year', 'week', 'date', 'lat', 'long', 'subdistrict', 'district', 'province', 'type_array', 'holiday_this_week', 'avg_precipitation_7days', 'pm2_5')

for field in df_used.schema.fields:
    if str(field.dataType).startswith("ArrayType"):
        df_used = df_used.withColumn(field.name, col(field.name).cast("string"))

df_used.coalesce(1).write.csv('used_output', header=True, mode='overwrite')