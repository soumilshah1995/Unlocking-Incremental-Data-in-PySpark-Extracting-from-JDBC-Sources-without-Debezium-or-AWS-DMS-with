try:
    import sys
    from pyspark.sql import SparkSession
    from pyspark import SparkConf, SparkContext
    import os
    from datetime import datetime
    from pyspark.sql.functions import from_json, col
    import pyspark.sql.functions as F
    from dataclasses import dataclass, field

except Exception as e:
    pass

# ======================================= Settings ===============================
global spark, jdbc_url, table_name, user, password, pk, updated_at_column_name, driver

SUBMIT_ARGS = "--packages org.postgresql:postgresql:42.5.4 pyspark-shell"
os.environ["PYSPARK_SUBMIT_ARGS"] = SUBMIT_ARGS
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

jdbc_url = 'jdbc:postgresql://localhost:5432/postgres'
table_name = 'public.sales'
user = 'postgres'
password = 'postgres'
pk = "salesid"
updated_at_column_name = "updated_at"
driver = 'org.postgresql.Driver'
spark = SparkSession.builder.appName('IncrementalJDBC').getOrCreate()


# ===============================================================================================


class Parameters:
    def __init__(self):
        self.first_time_read_flag = False
        self.prev_commit = 0
        self.prev_updated_date = "2000-01-01"
        self.insert_data_exists_flag = False
        self.update_data_exists_flag = False


class Checkpoints(Parameters):
    def __init__(self, directory="./checkpoint/max_id", table_name="sales"):
        self.directory = directory
        self.table_name = table_name
        Parameters.__init__(self)

    def read(self):
        if self.is_exists():
            self.prev_commit, self.prev_updated_date, self.table_name = spark.read.csv(self.directory).collect()[0]
            return True
        else:
            self.first_time_read_flag = True
            return False

    def write(self):
        spark_df = spark.createDataFrame(data=[(str(self.prev_commit), str(self.prev_updated_date), self.table_name)],
                              schema=['prev_commit', "prev_updated_date", "table_name"])
        spark_df.write.mode("overwrite").csv(self.directory)

        return True

    def is_exists(self):
        if os.path.exists(self.directory):
            print(f"Checkpoint found ")
            return True
        else:
            print(f"Checkpoint Not found ")
            return False


class QuerySource(object):
    def __init__(self, pk_auto_inc_column_name, updated_at_column_name, check_point_instance):
        self.pk_auto_inc_column_name = pk_auto_inc_column_name
        self.updated_at_column_name = updated_at_column_name
        self.check_point_instance = check_point_instance

    def get_inc_insert(self):
        query = f"SELECT * FROM {self.check_point_instance.table_name} WHERE {self.pk_auto_inc_column_name} > {self.check_point_instance.prev_commit}"

        df = spark.read.format('jdbc').options(
            url=jdbc_url,
            query=query,
            user=user,
            password=password,
            driver=driver
        ).load()
        if df.count() >= 0: self.check_point_instance.insert_data_exists_flag = True
        return df

    def get_inc_update(self):
        query = f"SELECT * FROM {self.check_point_instance.table_name} WHERE {self.updated_at_column_name} > '{self.check_point_instance.prev_updated_date}'"

        df = spark.read.format('jdbc').options(
            url=jdbc_url,
            query=query,
            user=user,
            password=password,
            driver=driver
        ).load()
        if df.count() >= 0: self.check_point_instance.update_data_exists_flag = True
        return df


def main():
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    helper_check_points = Checkpoints(
        directory="./checkpoint/max_id",
        table_name=table_name,
    )
    helper_check_points.read()

    query_instance = QuerySource(
        pk_auto_inc_column_name=pk,
        updated_at_column_name=updated_at_column_name,
        check_point_instance=helper_check_points
    )

    insert_inc_df = query_instance.get_inc_insert()
    update_inc_df = query_instance.get_inc_update()

    if helper_check_points.first_time_read_flag:

        helper_check_points.prev_commit = insert_inc_df.agg({pk: "max", updated_at_column_name: "max"}).collect()[0][0]

        helper_check_points.prev_updated_date = \
        insert_inc_df.agg({pk: "max", updated_at_column_name: "max"}).collect()[0][1]

        helper_check_points.write()

    else:
        current_commit = insert_inc_df.agg({pk: "max", updated_at_column_name: "max"}).collect()[0][0]

        current_updated_date = update_inc_df.agg({pk: "max", updated_at_column_name: "max"}).collect()[0][1]

        if current_commit is not None and helper_check_points.insert_data_exists_flag:
            if current_commit > helper_check_points.prev_commit:
                helper_check_points.prev_commit = current_commit

        if current_updated_date is not None and helper_check_points.update_data_exists_flag:
            if current_updated_date.__str__() > helper_check_points.prev_updated_date.__str__() :
                helper_check_points.prev_updated_date = current_updated_date

    helper_check_points.write()
    result = insert_inc_df.union(update_inc_df)
    print(result.show())


if __name__ == "__main__":
    main()
