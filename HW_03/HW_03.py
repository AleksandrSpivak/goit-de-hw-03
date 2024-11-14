from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, round

# Створюємо сесію Spark
spark = SparkSession.builder \
    .appName("HW_03") \
    .getOrCreate()


# 1. Завантаження та читання CSV-файлів
users = spark.read.csv('./users.csv', header=True)
purchases = spark.read.csv('./purchases.csv', header=True)
products = spark.read.csv('./products.csv', header=True)

# Перетворення відповідних колонок на числові типи
users = users.withColumn("age", col("age").cast("integer"))
purchases = purchases.withColumn("quantity", col("quantity").cast("integer"))
products = products.withColumn("price", col("price").cast("float"))

print("Data loaded")
print("users_lenght: ", users.count())
print("purchases_lenght: ", purchases.count())
print("products_lenght: ", products.count())

# 2. Очистка даних від рядків з пропущеними значеннями
users = users.na.drop()
purchases = purchases.na.drop()
products = products.na.drop()

print("\nAfter NA dropped")
print("users_lenght: ", users.count())
print("purchases_lenght: ", purchases.count())
print("products_lenght: ", products.count())

print("\nСтруктура таблиць")
print("users: ", users.columns)
print("purchases: ", purchases.columns)
print("products: ", products.columns)

# 3. Визначення загальної суми покупок за кожною категорією продуктів
purchases_products = purchases.join(products, "product_id")
print("\npurchases_products: ", purchases_products.columns)
purchases_products = purchases_products.withColumn("total_price", col("quantity") * col("price"))
total_sum_by_category = purchases_products.groupBy("category").agg(spark_sum("total_price").alias("total_sum"))
print("Загальна сума покупок за кожною категорією продуктів:")
total_sum_by_category.show()

# 4. Визначення суми покупок за кожною категорією для вікової групи 18-25 років
users_18_25 = users.filter((col("age") >= 18) & (col("age") <= 25))
purchases_users_products = purchases_products.join(users_18_25, "user_id")
print("\npurchases_users_products: ", purchases_users_products.columns)
total_sum_by_category_18_25 = purchases_users_products.groupBy("category").agg(spark_sum("total_price").alias("total_sum_18_25"))
print("Сума покупок за кожною категорією для вікової групи 18-25 років:")
total_sum_by_category_18_25.show()

# 5. Визначення частки покупок за кожною категорією для вікової групи 18-25 років
total_sum_18_25 = total_sum_by_category_18_25.agg(spark_sum("total_sum_18_25")).collect()[0][0]
percentage_by_category_18_25 = total_sum_by_category_18_25.withColumn("percentage_18_25", round((col("total_sum_18_25") / total_sum_18_25) * 100, 2))
print("\nЧастка покупок за кожною категорією для вікової групи 18-25 років (%):")
percentage_by_category_18_25.show()

# 6. Вибір 3 категорій з найвищим відсотком витрат для вікової категорії 18-25 років
top_3_categories_18_25 = percentage_by_category_18_25.orderBy(col("percentage_18_25").desc()).limit(3)
print("\nТоп 3 категорії з найвищим відсотком витрат для вікової категорії 18-25 років:")
top_3_categories_18_25.show()

spark.stop()
