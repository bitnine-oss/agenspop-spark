package net.bitnine.agenspopspark.process

import net.bitnine.agenspopspark.util.{AgensSparkHelper, SampleGraph}
import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions.{col, collect_list, explode, lit}
import org.apache.spark.sql.types.DataTypes.StringType
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, Row, SparkSession}
import org.apache.spark.sql.types.{ArrayType, StructField, StructType}
import org.graphframes.GraphFrame
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component

import scala.collection.JavaConversions._

@Component
class AgensSparkService		// (@Autowired sparkConf:SparkConf)
{
	// val sql:SparkSession = SparkSession.builder.config(sparkConf).getOrCreate

	val name: String = "agenspop-spark"
	val message: String = s"Hello~, $name"

	// **ERROR ==> Unable to generate an encoder for inner class `net.bitnine.agenspopspark.process.AgensSparkService$ElasticProperty` without access to the scope that this class was defined in.
	// **Solution: StructType & Row 로 변경해야 함
	//		사용자 정의 데이터타입은 spark-summit 해서 내장하지 않으면 참조 못함

	// case class ElasticProperty(key:String, `type`:String, value:String)
	// case class ElasticElement(id:String, property:ElasticProperty)
	// case class ElasticVertex(datasource:String, id:String, label:String, properties:Array[ElasticProperty])
	// case class ElasticEdge(datasource:String, id:String, label:String, properties:Array[ElasticProperty], src:String, dst:String)

	// **NOTE : these schema are useless
	val schemaProperty = new StructType(Array(
		StructField("key", StringType, false),
		StructField("type", StringType, false),
		StructField("value", StringType, false)
	))
	val schemaBase = new StructType(Array(
		StructField("id", StringType, false),
		StructField("property", schemaProperty, false)
	))
	val schemaVertex = StructType( Array(
		StructField("timestamp", StringType, false),
		StructField("datasource", StringType, false),
		StructField("id", StringType, false),
		StructField("label", StringType, false),
		StructField("properties", new ArrayType(schemaProperty, true), false)
	))
	val schemaEdge = schemaVertex.
		add( StructField("src", StringType, false)).
		add( StructField("dst", StringType, false))

	//////////////////////////////////////////////////

	def hello = message

	def sampleTest(sql: SparkSession):Dataset[Row] = {

		val g = SampleGraph.sampleGraph(sql)

		println("\nTEST1) vertices :: groupBy(label)")
		g.vertices.groupBy("label").count.show

		println("\n       edges    :: groupBy(label)")
		g.edges.groupBy("label").count.show

		val cnt = g.edges.filter("label = 'knows'").count()
		println(s"\nTEST2) edges :: filter :: count => $cnt")

		println("\nTEST3) graph :: outDegrees")
		g.outDegrees
	}

	//////////////////////////////////////////////////

	def count(g: GraphFrame) = {
		(g.vertices.count(), g.edges.count())
	}

	def flatProperties(dfV:DataFrame) = {
		dfV.
			select(col("id"), explode(col("properties")).as("property")).
			map { r => {
				val row = r.getStruct(1)
				Row(r.getAs[String](0), Row(row.getString(0), row.getString(1), row.getString(2)))
			} }(RowEncoder(schemaBase))
	}

//	def makePropertyDataset(dfV:DataFrame, pName:String, pType:String) = {
//		dfV.map { r =>
//			Row(r.getAs[String](0), Row(pName, pType, r.getAs[Integer](1).toString))
//		}(RowEncoder(schemaBase))
//	}

	def appendProperties(orgDf:Dataset[Row], newDf:Dataset[Row]) = {
		orgDf.union(newDf).
			groupBy("id").agg(collect_list("property").as("properties_gf")).
			withColumnRenamed("id","id_gf")
	}

	def replaceProperties(orgDf:DataFrame, newDf:DataFrame) = {
		orgDf.join(newDf, orgDf.col("id")===newDf.col("id_gf"), "leftouter").
			drop(col("id_gf")).drop(col("properties")).
			withColumnRenamed("properties_gf","properties")
	}


	//////////////////////////////////////////////////
	//
	//	GraphFrame algorithms
	//  https://graphframes.github.io/graphframes/docs/_site/user-guide.html
	//
	//////////////////////////////////////////////////

	//////////////////////////////////////////////////
	// _$$pagerank

	def dropKeys(df:Dataset[Row], keys:java.util.List[String]):Dataset[Row]  = {
		// explode array of array
		var tmpV3 = flatProperties(df)

		// Filter AND chaining
		for(key <- keys) {
			tmpV3 = tmpV3.
				filter(col("property").getField("key")=!=lit(key))
		}

		// STEP4: append new Property DF to Original Property DF AND groupBy to properties
		val tmpV4 = tmpV3.
			groupBy("id").agg(collect_list("property").as("properties_gf")).
			withColumnRenamed("id","id_gf")

		// STEP5: replace properties with new property
		return replaceProperties(df, tmpV4)
	}

	//////////////////////////////////////////////////
	// _$$pagerank

	def pageRank(g: GraphFrame, operName:String):Dataset[Row]  = {
		val pName = "_$$"+operName
		val pType = "java.lang.Double"
		val orgDf = g.vertices

		// STEP1: indregree of vertices
		val tmpGf = g.pageRank.resetProbability(0.15).maxIter(10).run()
		val tmpV1 = tmpGf.vertices.select(col("id"), col("pagerank"))

		// STEP2: create new DataFrame for GraphFrame results
		val tmpV2 = // makePropertyDataset(tmpV1, pName, pType)
			tmpV1.map { r =>
				Row(r.getAs[String](0), Row(pName, pType, r.getAs[Double](1).toString))
			}(RowEncoder(schemaBase))

		// STEP3: explode properties of original DataFrame that excluded property with pName
		val tmpV3 = flatProperties(orgDf).
			filter(col("property").getField("key")=!=lit(pName))

		// STEP4: append new Property DF to Original Property DF AND groupBy to properties
		val tmpV4 = appendProperties(tmpV3, tmpV2)

		// STEP5: replace properties with new property
		return replaceProperties(orgDf, tmpV4)
	}


	//////////////////////////////////////////////////
	// _$$indegree

	def inDegree(g: GraphFrame, operName:String):Dataset[Row]  = {
		val pName = "_$$"+operName
		val pType = "java.lang.Long"
		val orgDf = g.vertices

		// STEP1: indregree of vertices
		val tmpV1 = g.inDegrees

		// STEP2: create new DataFrame for GraphFrame results
		val tmpV2 = // makePropertyDataset(tmpV1, pName, pType)
			tmpV1.select("id", "inDegree").map { r =>
				Row(r.getAs[String](0), Row(pName, pType, r.getAs[Long](1).toString))
			}(RowEncoder(schemaBase))

		// STEP3: explode original DataFrame of properties that excluded property with pName
		val tmpV3 = flatProperties(orgDf).
			filter(col("property").getField("key")=!=lit(pName))

		// STEP4: append new Property DF to Original Property DF AND groupBy to properties
		val tmpV4 = appendProperties(tmpV3, tmpV2)

		// STEP5: replace properties with new property
		return replaceProperties(orgDf, tmpV4)
	}

	//////////////////////////////////////////////////
	// _$$outdegree

	def outDegree(g: GraphFrame, operName:String):Dataset[Row]  = {
		val pName = "_$$"+operName
		val pType = "java.lang.Long"
		val orgDf = g.vertices

		// STEP1: indregree of vertices
		val tmpV1 = g.outDegrees

		// STEP2: create new DataFrame for GraphFrame results
		val tmpV2 = // makePropertyDataset(tmpV1, pName, pType)
			tmpV1.select("id", "outDegree").map { r =>
				Row(r.getAs[String](0), Row(pName, pType, r.getAs[Long](1).toString))
			}(RowEncoder(schemaBase))

		// STEP3: explode original DataFrame of properties that excluded property with pName
		val tmpV3 = flatProperties(orgDf).
			filter(col("property").getField("key")=!=lit(pName))

		// STEP4: append new Property DF to Original Property DF AND groupBy to properties
		val tmpV4 = appendProperties(tmpV3, tmpV2)

		// STEP5: replace properties with new property
		return replaceProperties(orgDf, tmpV4)
	}

	//////////////////////////////////////////////////
	// _$$component : connected (not strong)

	def connComponent(g: GraphFrame, operName:String):Dataset[Row]  = {
		val pName = "_$$"+operName
		val pType = "java.lang.Long"
		val orgDf = g.vertices

		// STEP1: indregree of vertices
		val tmpV1 = g.connectedComponents.run()

		// STEP2: create new DataFrame for GraphFrame results
		val tmpV2 = // makePropertyDataset(tmpV1, pName, pType)
			tmpV1.select("id", "component").map { r =>
				Row(r.getAs[String](0), Row(pName, pType, r.getAs[Long](1).toString))
			}(RowEncoder(schemaBase))

		// STEP3: explode original DataFrame of properties that excluded property with pName
		val tmpV3 = flatProperties(orgDf).
			filter(col("property").getField("key")=!=lit(pName))

		// STEP4: append new Property DF to Original Property DF AND groupBy to properties
		val tmpV4 = appendProperties(tmpV3, tmpV2)

		// STEP5: replace properties with new property
		return replaceProperties(orgDf, tmpV4)
	}

	//////////////////////////////////////////////////
	// _$$scomponent : strong connected

	def sconnComponent(g: GraphFrame, operName:String):Dataset[Row]  = {
		val pName = "_$$"+operName
		val pType = "java.lang.Long"
		val orgDf = g.vertices

		// STEP1: indregree of vertices
		val tmpV1 = g.stronglyConnectedComponents.maxIter(10).run()

		// STEP2: create new DataFrame for GraphFrame results
		val tmpV2 = // makePropertyDataset(tmpV1, pName, pType)
			tmpV1.select("id", "component").map { r =>
				Row(r.getAs[String](0), Row(pName, pType, r.getAs[Long](1).toString))
			}(RowEncoder(schemaBase))

		// STEP3: explode original DataFrame of properties that excluded property with pName
		val tmpV3 = flatProperties(orgDf).
			filter(col("property").getField("key")=!=lit(pName))

		// STEP4: append new Property DF to Original Property DF AND groupBy to properties
		val tmpV4 = appendProperties(tmpV3, tmpV2)

		// STEP5: replace properties with new property
		return replaceProperties(orgDf, tmpV4)
	}

	//////////////////////////////////////////////////
	// pageRank


	//////////////////////////////////////////////////
	// pageRank


	//////////////////////////////////////////////////

}
