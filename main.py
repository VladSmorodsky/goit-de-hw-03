from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F


def read_df(spark: SparkSession, csv_path: str) -> DataFrame:
    return spark.read.csv(csv_path, header=True, inferSchema=True)


if __name__ == '__main__':
    with SparkSession.builder.appName("MyGoitSparkSandbox").getOrCreate() as spark:
        # Task 1

        users_df = read_df(spark, 'users.csv')
        products_df = read_df(spark, 'products.csv')
        purchases_df = read_df(spark, 'purchases.csv')

        users_df.show(5)
        products_df.show(5)
        purchases_df.show(5)

        # Task 2

        print(
            f'Before cleaning: users={users_df.count()}, products={products_df.count()}, purchases={purchases_df.count()}')

        users_df = users_df.dropna()
        products_df = products_df.dropna()
        purchases_df = purchases_df.dropna()

        print(
            f'After cleaning: users={users_df.count()}, products={products_df.count()}, purchases={purchases_df.count()}')

        # Task 3

        products_df.join(purchases_df, products_df.product_id == purchases_df.product_id, 'inner') \
            .drop(purchases_df.product_id) \
            .groupBy(products_df.category) \
            .agg(F.round(F.sum(F.col('price') * F.col('quantity')), 2).alias('total_amount')) \
            .show()

        # Task 4

        young_age_df = products_df \
            .join(purchases_df, products_df.product_id == purchases_df.product_id, 'inner') \
            .join(users_df, users_df.user_id == purchases_df.user_id, 'inner') \
            .drop(purchases_df.product_id, purchases_df.user_id) \
            .filter(F.col('age').between(18, 25)) \
            .groupBy(products_df.category) \
            .agg(F.round(F.sum(F.col('price') * F.col('quantity')), 2).alias('total_amount'))

        young_age_df.show()

        # Task 5
        total = young_age_df.agg(F.round(F.sum('total_amount'), 2)).first()[0]

        young_age_df = young_age_df.withColumn('share_part', F.round(
            F.col('total_amount') / total * 100, 2))

        young_age_df.show()

        # Task 6
        young_age_df.orderBy(F.col('share_part').desc()).limit(3).show()
