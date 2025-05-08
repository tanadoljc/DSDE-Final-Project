import findspark

spark_url = 'local'

#library

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, substring, to_date, weekofyear, explode, regexp_replace, split, expr

spark = SparkSession.builder \
    .master(spark_url) \
    .appName('Spark SQL') \
    .getOrCreate()

"""# Feature Engineering"""

path = '../bangkok_traffy.csv'
df = spark.read.csv(path, header=True, inferSchema=True)

#deleted column
del_col = ['photo','photo_after']

df = df.drop(*del_col)

df.printSchema()

df.count()

# at least 5 non-null values
df = df.na.drop(thresh=5)

# deselect the unknown type
df = df.filter(df["type"] != "{}")

df = df.withColumn("date", to_date(substring('timestamp',1,10), "yyyy-MM-dd"))
df = df.withColumn("year", substring("date", 1, 4))
df = df.withColumn("week", weekofyear("date"))

# df.show()

# filter more
df = df.filter(col("date") > "2022-09-01")
df = df.filter(col('subdistrict').isNotNull() & col('district').isNotNull() & col('province').isNotNull())

df.count()

# filter the wrong case
train_col = ['type', 'coords', 'subdistrict', 'district', 'province', 'timestamp']

for c in train_col:
  df = df.filter(~(col(c).startswith("https:")))

# df.count()

df_cleaned = df.withColumn("type", regexp_replace(col("type"), "[{}]", ""))

allowed_types = ['ป้ายจราจร', 'เสียงรบกวน', 'สะพาน', 'PM2.5', 'กีดขวาง', 'ห้องน้ำ', 'ทางเท้า', 'ต้นไม้', 'การเดินทาง', 'คนจรจัด', 'ป้าย', 'ความสะอาด', 'คลอง', 'แสงสว่าง', 'น้ำท่วม', 'ท่อระบายน้ำ', 'ถนน', 'จราจร', 'ความปลอดภัย', 'สายไฟ', 'สัตว์จรจัด']

# สร้าง array literal สำหรับ allowed_types
allowed_types_expr = "array({})".format(",".join([f"'{t}'" for t in allowed_types]))

# สร้าง DataFrame ใหม่โดยแยก field 'type' เป็น array
df_with_array = df_cleaned.withColumn("type_array", split("type", ","))

# ใช้ Spark SQL expression เพื่อตรวจว่า type_array ⊆ allowed_types
filtered_df = df_with_array.filter(
    expr(f"array_except(type_array, {allowed_types_expr}) = array()")
)

# fix lat long already
filtered_df = filtered_df.withColumn("lat", split(col("coords"), ",")[1]) \
       .withColumn("long", split(col("coords"), ",")[0])

for field in filtered_df.schema.fields:
    if str(field.dataType).startswith("ArrayType"):
        filtered_df = filtered_df.withColumn(field.name, col(field.name).cast("string"))

filtered_df.coalesce(1).write.csv('used_data', header=True, mode='overwrite')

filtered_df = filtered_df.select('year', 'week', 'date', 'lat', 'long', 'subdistrict', 'district', 'province', 'type_array')

for field in filtered_df.schema.fields:
    if str(field.dataType).startswith("ArrayType"):
        filtered_df = filtered_df.withColumn(field.name, col(field.name).cast("string"))

filtered_df.coalesce(1).write.csv('used_data', header=True, mode='overwrite')