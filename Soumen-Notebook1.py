# Databricks notebook source
# MAGIC %scala
# MAGIC import org.apache.spark.sql.functions._
# MAGIC import org.apache.spark.sql.types._
# MAGIC import org.apache.spark.sql.types.TimestampType
# MAGIC
# MAGIC // val schema = StructType(Seq(
# MAGIC //       StructField("paper_id", IntegerType, true),
# MAGIC //       StructField("Title", StringType, true),
# MAGIC //       StructField("author_id", IntegerType, true),
# MAGIC //       StructField("author_name", StringType, true),
# MAGIC //       StructField("journal_name", StringType, true),
# MAGIC //       StructField("journal_id", IntegerType, true),
# MAGIC //       StructField("publication_date", StringType, true),
# MAGIC //       StructField("citation_count", IntegerType, true),
# MAGIC //       StructField("paper_status", StringType, true),
# MAGIC //       StructField("doi", StringType, true),
# MAGIC //       StructField("keywords", StringType, true),
# MAGIC //       StructField("abstract", StringType, true),
# MAGIC //       StructField("reference_count", StringType, true),
# MAGIC //       StructField("research_field", StringType, true)
# MAGIC //     ))
# MAGIC
# MAGIC     val irisSchema = StructType(Seq(
# MAGIC       StructField("sepal_length", FloatType, true),
# MAGIC       StructField("sepal_width", FloatType, true),
# MAGIC       StructField("petal_length", FloatType, true),
# MAGIC       StructField("petal_width", FloatType, true),
# MAGIC       StructField("species", StringType, true)
# MAGIC     ))
# MAGIC
# MAGIC val df = spark.read.schema(irisSchema).option("header", "true").csv("/Volumes/ness_data3_7474647874956712/default/soumen/iris.csv")
# MAGIC df.show()
# MAGIC
# MAGIC df.writeTo("default.iris1").createOrReplace()
# MAGIC
# MAGIC // val table = spark.read.table("default.editorial")
# MAGIC // println(table.count())
# MAGIC
# MAGIC // val data = Seq ( 
# MAGIC //   ("654321", "Nothing", "-22", "Soumen", "None", "-22", "123142423", "0", "Unpublished", "DOI", "None", "Nothing", "1", "Football Ground")
# MAGIC // )
# MAGIC // import spark.implicits._
# MAGIC
# MAGIC // val editorial_updates = spark.createDataFrame(data).toDF("paper_id", "Title", "author_id",
# MAGIC // "author_name", "journal_name", "journal_id", "publication_date", "citation_count", "paper_status", "doi",
# MAGIC // "keywords", "abstract", "reference_count", "research_field"
# MAGIC // )
# MAGIC // editorial_updates.createOrReplaceTempView("editorial_updates")
# MAGIC
# MAGIC // import io.delta.tables.DeltaTable
# MAGIC // val deltaTable = DeltaTable.forName(spark, "default.editorial1")
# MAGIC // val historyDF = deltaTable.history() // Returns a DataFrame with version, timestamp, etc.
# MAGIC // historyDF.show()
# MAGIC
# MAGIC // val totalRows = deltaTable.detail().select("numRecords").first().getLong(0)
# MAGIC // println(totalRows)
# MAGIC
# MAGIC //deltaTable.restoreToVersion(0)
# MAGIC // println(deltaTable.count())
# MAGIC
# MAGIC // deltaTable.as("t")
# MAGIC // .merge(
# MAGIC //   editorial_updates.as("s"),
# MAGIC //   "t.paper_id = s.paper_id"
# MAGIC // ).whenMatched()
# MAGIC //   .updateAll()
# MAGIC //   .whenNotMatched()
# MAGIC //   .insertAll()
# MAGIC //   .execute()
# MAGIC
# MAGIC // val table1 = spark.read.table("default.editorial")
# MAGIC // println(table1.count())
# MAGIC
# MAGIC // deltaTable.updateExpr(
# MAGIC //   "author_name = 'Soumen'",
# MAGIC //   Map("author_id" -> "'-44'")
# MAGIC // )
# MAGIC
# MAGIC // deltaTable.delete(
# MAGIC //   "author_name = 'Soumen'"
# MAGIC // )
# MAGIC
# MAGIC // val table1 = spark.read.table("default.editorial1")
# MAGIC // println(table1.count())
# MAGIC
# MAGIC // val historyDF = deltaTable.history() // Returns a DataFrame with version, timestamp, etc.
# MAGIC // historyDF.show()
# MAGIC // val ed_df = spark.read.table("default.editorial").filter($"author_name" >= "Soumen")
# MAGIC // display(ed_df)

# COMMAND ----------

