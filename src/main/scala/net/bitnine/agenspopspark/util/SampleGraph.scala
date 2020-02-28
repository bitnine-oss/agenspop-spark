package net.bitnine.agenspopspark.util

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType}
import org.graphframes.GraphFrame

object SampleGraph {

	val AGENS_DS:String = "modern"

	// case class ElasticProperty(key: String, `type`:String, value: String)

	val schemaProperty: StructType = new StructType(Array(
			StructField("key", StringType, false),
			StructField("type", StringType, false),
			StructField("value", StringType, false)
		))

	val schemaVertex: StructType = new StructType(Array[StructField](
			StructField("datasource", StringType, false),
			StructField("id", StringType, false),
			StructField("label", StringType, false),
			StructField("properties", new ArrayType(schemaProperty, true), false)
		))

	val schemaEdge: StructType = new StructType(Array[StructField](
			StructField("datasource", StringType, false),
			StructField("id", StringType, false),
			StructField("label", StringType, false),
			StructField("src", StringType, false),
			StructField("dst", StringType, false),
			StructField("properties", new ArrayType(schemaProperty, true), false)
		))

	///////////////////////////////////////////////////////

	def sampleVertices(spark:SparkSession) = {
		val rows = List[Row](
			Row(AGENS_DS, AGENS_DS + "_" + 1, "person", Array(
				Row("name", "java.lang.String", "marko"), Row("age", "java.lang.Integer", "29"), Row("country", "java.lang.String", "USA")
			)),
			Row(AGENS_DS, AGENS_DS + "_" + 2, "person", Array(
				Row("name", "java.lang.String", "vadas"), Row("age", "java.lang.Integer", "27"), Row("country", "java.lang.String", "USA")
			)),
			Row(AGENS_DS, AGENS_DS + "_" + 3, "software", Array(
				Row("name", "java.lang.String", "lop"), Row("lang", "java.lang.String", "java")
			)),
			Row(AGENS_DS, AGENS_DS + "_" + 4, "person", Array(
				Row("name", "java.lang.String", "josh"), Row("age", "java.lang.Integer", "32"), Row("country", "java.lang.String", "France")
			)),
			Row(AGENS_DS, AGENS_DS + "_" + 5, "software", Array(
				Row("name", "java.lang.String", "ripple"), Row("lang", "java.lang.String", "C++")
			)),
			Row(AGENS_DS, AGENS_DS + "_" + 6, "person", Array(
				Row("name", "java.lang.String", "peter"), Row("age", "java.lang.Integer", "29"), Row("country", "java.lang.String", "Germany")
			))
		)
		spark.createDataFrame(
			spark.sparkContext.parallelize(rows),
			schemaVertex
		)
	}

	def sampleEdges(spark:SparkSession) = {
		val rows = List[Row](
			Row(AGENS_DS, AGENS_DS + "_" + 7, "knows", AGENS_DS + "_" + 1, AGENS_DS + "_" + 2, Array(
				Row("weight", "java.lang.Float", "0.5")
			)),
			Row(AGENS_DS, AGENS_DS + "_" + 8, "knows", AGENS_DS + "_" + 1, AGENS_DS + "_" + 4, Array(
				Row("weight", "java.lang.Float", "1.0")
			)),
			Row(AGENS_DS, AGENS_DS + "_" + 9, "created", AGENS_DS + "_" + 1, AGENS_DS + "_" + 3, Array(
				Row("weight", "java.lang.Float", "0.6")
			)),
			Row(AGENS_DS, AGENS_DS + "_" + 10, "created", AGENS_DS + "_" + 4, AGENS_DS + "_" + 5, Array(
				Row("weight", "java.lang.Float", "0.7")
			)),
			Row(AGENS_DS, AGENS_DS + "_" + 11, "created", AGENS_DS + "_" + 4, AGENS_DS + "_" + 3, Array(
				Row("weight", "java.lang.Float", "0.2")
			)),
			Row(AGENS_DS, AGENS_DS + "_" + 12, "created", AGENS_DS + "_" + 6, AGENS_DS + "_" + 3, Array(
				Row("weight", "java.lang.Float", "0.3")
			))
		)
		spark.createDataFrame(
			spark.sparkContext.parallelize(rows),
			schemaEdge
		)
	}

	def sampleGraph(spark:SparkSession) = {
		GraphFrame( sampleVertices(spark), sampleEdges(spark) )
	}

}
