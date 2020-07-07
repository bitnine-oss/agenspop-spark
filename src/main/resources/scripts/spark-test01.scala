/*

$ spark-shell --name es-bitnine --master local --executor-memory 4G --driver-memory 4G --packages org.elasticsearch:elasticsearch-spark-20_2.11:7.3.1,graphframes:graphframes:0.7.0-spark2.4-s_2.11

:load /Users/bgmin/Workspaces/agenspop/agenspop-spark/src/main/resources/scripts/spark-test01.scala

*/

import com.fasterxml.jackson.core.`type`.TypeReference
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.{Column, DataFrame, Dataset, Encoders, Row, RowFactory, SaveMode, SparkSession}
import org.apache.spark.sql.types.{ArrayType, DataType, DecimalType, MapType, StructField, StructType}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.graphframes.GraphFrame
import org.apache.spark.sql.types.DataTypes._
import org.apache.spark.sql.functions._

/////////////////////////////////////////////////////////////////

// **NOTE : skip this line!!
val sc = new SparkContext()
val spark = SparkSession.builder().config(sc.getConf).getOrCreate

/////////////////////////////////////////////////////////////////

// checkpoint directory (only Local mode)
// ==> DO NOT in Cluster mode!!
// sc.setCheckpointDir("/Users/bgmin/Temp/checkpoint")


case class ElasticProperty(key:String, `type`:String, value:String)
case class ElasticElement(id:String, property:ElasticProperty)
case class ElasticVertex(timestamp:String, datasource:String, id:String, label:String, properties:Array[ElasticProperty])
case class ElasticEdge(timestamp:String, datasource:String, id:String, label:String, properties:Array[ElasticProperty], src:String, dst:String)

val schemaP = StructType( Array(
	StructField("key", StringType, false),
	StructField("type", StringType, false),
	StructField("value", StringType, false)
))
val encoderP = RowEncoder(schemaP)
val schemaV = StructType( Array(
	StructField("timestamp", StringType, false),
	StructField("datasource", StringType, false),
	StructField("id", StringType, false),
	StructField("label", StringType, false),
	StructField("properties", new ArrayType(schemaP, true), false)
))
val schemaE = schemaV.
	add(StructField("src", StringType, false)).
	add(StructField("dst", StringType, false))

val conf = Map(
	"es.nodes"->"tonyne.iptime.org",
	"es.port"->"39200",
	"es.nodes.wan.only"->"true",
	"es.mapping.id"->"id",
	"es.write.operation"->"upsert",
	"es.index.auto.create"->"true",
	"es.scroll.size"->"10000",
	"es.net.http.auth.user"->"elastic",			// elasticsearch security
	"es.net.http.auth.pass"->"bitnine",			// => curl -u user:password
	"es.mapping.date.rich"->"false"				// for timestamp
)
val resourceV = "agensvertex"
val resourceE = "agensedge"


val datasource = "modern"		// "northwind"
val esQueryV:String = s"""{ "query": { "bool": {
	"filter": { "term": { "datasource": "${datasource}" } }
}}}"""
val esQueryE:String = s"""{ "query": { "bool": {
	"filter": { "term": { "datasource": "${datasource}" } }
}}}"""


//val esQuery:String = s"?q=datasource:${datasource}"
//val esQuery:String = s"""{ "query":{"term":{"datasource":"${datasource}"}} }"""
/*
val excludeLabelsV = Array("supplier","category")
val excludeLabelsE = Array("reports_to","part_of","supplies")

val esQueryV:String = s"""{ "query": { "bool": {
	"filter": { "term": { "datasource": "${datasource}" } }
	, "must_not": { "terms": { "label": ${excludeLabelsV.mkString("[\"","\",\"","\"]")} } }
}}}"""
val esQueryE:String = s"""{ "query": { "bool": {
	"filter": { "term": { "datasource": "${datasource}" } }
	, "must_not": { "terms": { "label": ${excludeLabelsE.mkString("[\"","\",\"","\"]")} } }
}}}"""
*/

/*
val datasource = "modern"
val esQueryV:String = s"?q=datasource:${datasource}"
val esQueryE:String = s"?q=datasource:${datasource}"
*/


val dfV = spark.read.format("es").options(conf).
	option("es.query", esQueryV).
	schema(Encoders.product[ElasticVertex].schema).		// schema(schemaV).
	load(resourceV)
val dfE = spark.read.format("es").options(conf).
	option("es.query", esQueryE).
	schema(Encoders.product[ElasticEdge].schema).		// schema(schemaE).
	load(resourceE)
val gf = GraphFrame(dfV,dfE)

/////////////////////////////////////////////////////////////////
//
// inDegree
//
val pName1:String = "_$$indegree"
val pType1:String = "java.lang.Integer"

// STEP1: explode nested array field about properties
val tmpV11 = dfV.
	select(col("id"), explode(col("properties")).as("property")).
	map { r => {
		val row = r.getStruct(1)
		ElasticElement(r.getAs[String](0), ElasticProperty(row.getString(0), row.getString(1), row.getString(2)))
	} }(Encoders.product[ElasticElement]).
	filter(col("property").getField("key")=!=lit(pName1))

// STEP2: indregree of vertices
val tmpV12:DataFrame = gf.inDegrees

// STEP3: create new DataFrame for GraphFrame results
val tmpV13 = tmpV12.map { r =>
	ElasticElement(r.getAs[String](0), ElasticProperty(pName1, pType1, r.getAs[Integer](1).toString))
}(Encoders.product[ElasticElement])

// STEP4: append new Property DF to Original Property DF AND groupBy to properties
val tmpV14 = tmpV11.union(tmpV13).
	groupBy("id").agg(collect_list("property").as("properties_gf")).
	withColumnRenamed("id","id_gf")

val tmpV15 = dfV.join(tmpV14, dfV.col("id")===tmpV14.col("id_gf"), "leftouter").
	drop(col("id_gf")).drop(col("properties")).
	withColumnRenamed("properties_gf","properties")

// **NOTE: Spark SQL must be SaveMode.Append (default=ErrorIfExists)
tmpV15.write.mode(SaveMode.Append).format("es").options(conf).save(resourceV)

// **TEST : write to ES
// tmpV4.write.mode(SaveMode.Append).format("es").options(conf).save("sparktemp")
// http://27.117.163.21:15619/sparktemp/_search?pretty=true&q=*:*

// **NOTE: 결과 확인
// http://27.117.163.21:15619/elasticvertex/_search?pretty=true&q=datasource:modern

// **NOTE: cluster status = green
// curl -X GET "27.117.163.21:15619/_cat/indices?v"


/////////////////////////////////////////////////////////////////
//
// pageRank
//

val pName2:String = "_$$pagerank"
val pType2:String = "java.lang.Float"

// STEP1: explode nested array field about properties
val tmpV21:Dataset[ElasticElement] = dfV.
	select(col("id"), explode(col("properties")).as("property")).
	map { r => {
		val row = r.getStruct(1)
		ElasticElement(r.getAs[String](0), ElasticProperty(row.getString(0), row.getString(1), row.getString(2)))
	} }(Encoders.product[ElasticElement]).
	filter(col("property").getField("key")=!=lit(pName2))

// STEP2: Run PageRank until convergence to tolerance ".tol(0.01)" (long time)
// ref. https://graphframes.github.io/graphframes/docs/_site/user-guide.html#pagerank
val tmpV22_g:GraphFrame = gf.pageRank.resetProbability(0.15).maxIter(10).run()
val tmpV22 = tmpV22_g.vertices.select(col("id"), col("pagerank"))

// STEP3: create new DataFrame for GraphFrame results
val tmpV23 = tmpV22.map { r =>
	ElasticElement(r.getAs[String](0), ElasticProperty(pName2, pType2, r.getAs[Float](1).toString))
}(Encoders.product[ElasticElement])

// STEP4: append new Property DF to Original Property DF AND groupBy to properties
val tmpV24 = tmpV21.union(tmpV23).
	groupBy("id").agg(collect_list("property").as("properties_gf")).
	withColumnRenamed("id","id_gf")

// **TEST : write to ES
// tmpV4.write.mode(SaveMode.Append).format("es").options(conf).save("sparktemp")
// http://27.117.163.21:15619/sparktemp/_search?pretty=true&q=*:*

val tmpV25 = dfV.join(tmpV24, dfV.col("id")===tmpV24.col("id_gf"), "leftouter").
	drop(col("id_gf")).drop(col("properties")).
	withColumnRenamed("properties_gf","properties")

// **NOTE: Spark SQL must be SaveMode.Append (default=ErrorIfExists)
tmpV25.write.mode(SaveMode.Append).format("es").options(conf).save(resourceV)

// **NOTE: 결과 확인
// http://27.117.163.21:15619/elasticvertex/_search?pretty=true&q=datasource:modern


/////////////////////////////////////////////////////////////////
//
// outDegree
//
val pName3:String = "_$$outdegree"
val pType3:String = "java.lang.Integer"

// STEP1: explode nested array field about properties
val tmpV31 = dfV.
	select(col("id"), explode(col("properties")).as("property")).
	map { r => {
		val row = r.getStruct(1)
		ElasticElement(r.getAs[String](0), ElasticProperty(row.getString(0), row.getString(1), row.getString(2)))
	} }(Encoders.product[ElasticElement]).
	filter(col("property").getField("key")=!=lit(pName3))

// STEP2: indregree of vertices
val tmpV32:DataFrame = gf.outDegrees

// STEP3: create new DataFrame for GraphFrame results
val tmpV33 = tmpV32.map { r =>
	ElasticElement(r.getAs[String](0), ElasticProperty(pName3, pType3, r.getAs[Integer](1).toString))
}(Encoders.product[ElasticElement])

// STEP4: append new Property DF to Original Property DF AND groupBy to properties
val tmpV34 = tmpV31.union(tmpV33).
	groupBy("id").agg(collect_list("property").as("properties_gf")).
	withColumnRenamed("id","id_gf")

val tmpV35 = dfV.join(tmpV34, dfV.col("id")===tmpV34.col("id_gf"), "leftouter").
	drop(col("id_gf")).drop(col("properties")).
	withColumnRenamed("properties_gf","properties")

// **NOTE: Spark SQL must be SaveMode.Append (default=ErrorIfExists)
tmpV35.write.mode(SaveMode.Append).format("es").options(conf).save(resourceV)



/////////////////////////////////////////////////////////////////
//
// Strong Component
//
val pName4:String = "_$$component"
val pType4:String = "java.lang.Long"

// STEP1: explode nested array field about properties
val tmpV41 = dfV.
	select(col("id"), explode(col("properties")).as("property")).
	map { r => {
		val row = r.getStruct(1)
		ElasticElement(r.getAs[String](0), ElasticProperty(row.getString(0), row.getString(1), row.getString(2)))
	} }(Encoders.product[ElasticElement]).
	filter(col("property").getField("key")=!=lit(pName4))

// STEP2: Strongly connected components
//val tmpV42 = gf.stronglyConnectedComponents.maxIter(10).run()
val tmpV42 = gf.connectedComponents.run();
// tmpV42.select("id", "component").show(20, false)

// STEP3: create new DataFrame for GraphFrame results
val tmpV43 = tmpV42.select("id", "component").map { r =>
	ElasticElement(r.getAs[String](0), ElasticProperty(pName4, pType4, r.getAs[Long](1).toString))
}(Encoders.product[ElasticElement])

// STEP4: append new Property DF to Original Property DF AND groupBy to properties
val tmpV44 = tmpV41.union(tmpV43).
	groupBy("id").agg(collect_list("property").as("properties_gf")).
	withColumnRenamed("id","id_gf")

val tmpV45 = dfV.join(tmpV44, dfV.col("id")===tmpV44.col("id_gf"), "leftouter").
	drop(col("id_gf")).drop(col("properties")).
	withColumnRenamed("properties_gf","properties")

// **NOTE: Spark SQL must be SaveMode.Append (default=ErrorIfExists)
tmpV45.write.mode(SaveMode.Append).format("es").options(conf).save(resourceV)

/*
val tmpV42 = gf.connectedComponents.run();

scala> tmpV42.show() ==> component : long type
+----------+--------+--------+--------------------+------------+
|datasource|      id|   label|          properties|   component|
+----------+--------+--------+--------------------+------------+
|    modern|modern_4|  person|[[name, java.lang...|171798691840|
|    modern|modern_6|  person|[[name, java.lang...|171798691840|
|    modern|modern_1|  person|[[name, java.lang...|171798691840|
|    modern|modern_2|  person|[[name, java.lang...|171798691840|
|    modern|modern_3|software|[[name, java.lang...|171798691840|
|    modern|modern_5|software|[[name, java.lang...|171798691840|
+----------+--------+--------+--------------------+------------+

val tmpV42 = gf.stronglyConnectedComponents.maxIter(10).run()

scala> tmpV42.select("id", "component").orderBy("component").show()
+--------+-------------+
|      id|    component|
+--------+-------------+
|modern_6| 171798691840|
|modern_2| 730144440320|
|modern_3| 798863917056|
|modern_1| 927712935936|
|modern_4| 953482739712|
|modern_5|1537598291968|
+--------+-------------+
*/

/*
///////////////////////////////////////////////////////////
//

//val tmpV3:DataFrame = tmpV1.
//	join(tmpV1, v1.col("id_1")===v0.col("id"), "leftouter").
//	drop(col("id_1")).
//	na.fill(0, Seq("inDegree"))

// STEP2: split nested fields
//val tmpV2:DataFrame = tmpV1.
//	withColumn("pkey", col("prop").getItem("key")).
//	withColumn("ptype", col("prop").getItem("type")).
//	withColumn("pvstr", col("prop").getItem("value")).
//	drop(col("prop"))

// STEP2: get tuples about key and type
val plist:List[(String,String)] = tmpV2.groupBy(col("pkey"), col("ptype")).
	agg(count("*").alias("cnt")).
	sort(col("cnt").desc).collect().toList.map(r => (r.getAs[String](0), r.getAs[String](1)))
// ==> List((name,java.lang.String), (country,java.lang.String), (age,java.lang.Integer), (lang,java.lang.String))


val v0:DataFrame = gf.vertices.as("v0")
v0.createOrReplaceTempView("v0")

val v1:DataFrame = gf.inDegrees.as("v1").withColumnRenamed("id","id_1")
v1.createOrReplaceTempView("v1")

val v2:DataFrame = v0.
	join(v1, v1.col("id_1")===v0.col("id"), "leftouter").
	drop(col("id_1")).
	na.fill(0, Seq("inDegree"))

// STEP1: explode nested array field about properties
// val v3:DataFrame = t2.select(col("id"), col("inDegree"), explode(col("properties")).as("prop"))

// val v4 = t3.map { r => ElasticProperty("_indegree","java.lang.Integer",r.getAs[Integer](1).toString) }
// Caused by: java.lang.ClassCastException: scala.collection.mutable.WrappedArray$ofRef cannot be cast to [Lorg.apache.spark.sql.Row;


val schemaVtemp = StructType( Array(
	StructField("datasource", StringType, false),
	StructField("id", StringType, false),
	StructField("label", StringType, false)
))

case class DummyVertex(datasource:String, id:String, label:String)

val t3 = t2.map { r =>
	DummyVertex(r.getAs[String](0), r.getAs[String](1), r.getAs[String](2))
}(Encoders.product[DummyVertex])

//val t3 = t2.map { r => {
//	var props:Array[Row] = r.getAs[Array[Row]](3)
//	val newP:Row = new GenericRowWithSchema(Array("_indegree","java.lang.Integer",r.getAs[Integer](1).toString), schemaP)
//	val newPlist = props :+ newP
//	RowFactory.create(r.getAs[String](0), r.getAs[String](1), r.getAs[String](2))//, props)
//} }(RowEncoder(schemaVtemp))


t1.show()
+--------+--------+
|      id|inDegree|
+--------+--------+
|modern_2|       1|
|modern_3|       3|
|modern_4|       1|
|modern_5|       1|
+--------+--------+
*/

/*
// **ref. joinType
// https://jaceklaskowski.gitbooks.io/mastering-spark-sql/spark-sql-joins.html

// Reference 'id' is ambiguous, could be: t0.id, t1.id.;


// val t2:DataFrame = spark.sql("select t0.datasource, t0.id, t0.label, t0.properties, t1.inDegree as _indegree from t0 left outter join t1 on t0.id == t1.id");
// val t3 = t2.withColumn("_indegree", when(t2.col("_indegree").isNull, lit(0)).otherwise(t2.col("_indegree")))


def createProperty(key:String, value:Any):Row = {
	val schemaProperty = StructType( Array(
		StructField("key", StringType, false),
		StructField("type", StringType, false),
		StructField("value", StringType, false)
	))
	val row:Row = new GenericRowWithSchema(Array(key,value.getClass.getName,value.toString), schemaProperty)
	row
}

val propertyUDF = udf[String, Any, Row](createProperty)


spark.udf.register("createProperty", createProperty)

//scala> createProperty("_pagerank",1.23)
//res52: org.apache.spark.sql.Row = [_pagerank,java.lang.Double,1.23]

val results = gf.pageRank.resetProbability(0.01).maxIter(20).run()
results.vertices.select("id", "pagerank").show()


dfV.foreach{ row => println( row.getString(0) ) }
// ==> OK


def distictList(plist:List[(String,String)]):List[(String,String)] = {
	var res = new ListBuffer[(String,String)]()
	for(tp <- plist){
		val keys = res.map(r => r._1)
		if( !keys.contains(tp._1) ) res += tp
	}
	res.toList
}
val plistUniq = distictList(plist)
// ==> List((name,java.lang.String), (country,java.lang.String), (age,java.lang.Integer), (lang,java.lang.String))

// STEP3: filtering
//val tmpV3:DataFrame = tmpV2.
//	filter(col("pkey")==="lang").
//	filter(col("pvstr")==="java")
//// create View
//tmpV3.createOrReplaceTempView("tmp3")
//tmpV3.withColumn("lang", col("pvstr").cast(IntegerType))


// DataType mapping to Java ==> https://stackoverflow.com/a/49796854/6811653
def ptypecast(ptype: String):DataType = {
	var b:DataType = null
	ptype match {
		case "java.lang.String" => b = StringType
		case "java.lang.Long" => b = LongType
		case "java.lang.Double" => b = DoubleType
		case "java.lang.Float" => b = FloatType
		case "java.lang.Integer" => b = IntegerType
		case "java.lang.Short" => b = ShortType
		case "java.lang.Boolean" => b = BooleanType
		case "java.lang.Byte" => b = ByteType
		case "java.sql.Timestamp" => b = TimestampType
		case "java.sql.Date" => b = DateType
		// case _ => b = StringType
	}
	b
}

// **ERROR!!
// java.lang.UnsupportedOperationException: Schema for type org.apache.spark.sql.types.DataType is not supported
// val castUDF = udf[DataType, String](ptypecast)

// STEP4: get ClassType of value
val ptype:String = tmpV3.groupBy(col("ptype")).
	agg(count("*").alias("cnt")).
	sort(col("cnt").desc).first().getAs[String](0)
val clssPtype = Class.forName(ptype)

// STEP5: convert property value
//val tmpV4 = tmpV2.filter(col("pkey")==="age").
//	withColumn("age",col("pvstr").cast(ptypecast("java.lang.Integer")))
*/


/*
tmpV.rdd.map(r => r(1)).collect.toList.foreach(p => {
	val row = p.asInstanceOf[Row]
	if( row != null ){
		val prop = toTuple(row.getString(0), row.getString(1), row.getString(2))
		if( prop.nonEmpty ) println( prop.get )
	}
})

scala> dfV.select($"id", explode($"properties").alias("prop")).printSchema
root
|-- id: string (nullable = false)
|-- prop: struct (nullable = true)
|    |-- key: string (nullable = false)
|    |-- type: string (nullable = false)
|    |-- value: string (nullable = false)
*/



/*
tmpV.withColumn("pkey", $"prop".map{ x:Row => toTuple(x.getString(0),x.getString(1), x.getString(2)) }).printSchema


val newDf = sc.createDataFrame(df.map(row => Row(row.getInt(0) + SOMETHING, applySomeDef(row.getAs[Double]("y")), df.schema)

val tmpDfV = sc.createDataFrame( dfV.map(row => Row(row.getString(1)), ),  }

val newResult = sc.createDataFrame(results.map(row =>
	Row(row.getString(0), ), df.schema)

val tmpV = results.vertices.withColumn("rowPr", $"pagerank".map { x:Row => createProperty("_pagerank", x) })


tmpPR.select("pagerank").collect.foreach{ x:Row => for( field <- x.schema.fields) println( field.dataType.simpleString ) }



tmpPR.select(explode($"properties")).collect.foreach{ x => println(x.get(0)) }
results.vertices.createOrReplaceTempView("tmpPR")

val tmpPR = spark.sql("SELECT * FROM tmpPR")
tmpPR.show(truncate = false)

// readDF.write.format("es").options(conf).save("sparktemp")

def addValue = udf((array: Array[Row])=> array ++ Array(5))

val tmp1 = tmpPR.withColumn("replaced", $"properties".map{ p:Property => p }: _*)

val p1:Array[Any] = Array("pagerank","java.lang.Float","1.23")
val p1row = new GenericRowWithSchema(p1, schemaP)

tmpPR.select($"id",explode($"properties")).foreach((a,b) => println(a.getClass.getName))
*/

/*
def toAny(`type`:String, value:String) = {
	val mapper = new ObjectMapper() with ScalaObjectMapper
	mapper.registerModule(DefaultScalaModule)
	mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
	try {
		if( `type`.equals("java.lang.String") || `type`.equals("String") ) value
		else {
			val clzz = Class.forName(`type`)
			mapper.readValue(value.toString, clzz)
		}
	}catch {
		case e: Exception => { // e.printStackTrace();
			null; }
	}
}
val tmp1 = toAny("java.lang.String","java")

def toTuple(key:String, `type`:String, value:String):Option[Tuple2[String,Any]] = {
	val convertedValue = toAny(`type`, value)
	if( convertedValue != null ) Some(key, convertedValue) else null
}
val tmp2 = toTuple("lang","java.lang.String","java")


**NOTE: Spark UDF error - Schema for type Any is not supported
**
val udfToAny = udf((key:String, `type`:String, value:String) => {
	val mapper = new ObjectMapper() with ScalaObjectMapper
	mapper.registerModule(DefaultScalaModule)
	mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
	try {
		if( `type`.equals("java.lang.String") || `type`.equals("String") ) (key, value)
		else {
			val clzz = Class.forName(`type`)
			(key, mapper.readValue(value.toString, clzz))
		}
	}catch {
		case e: Exception => { // e.printStackTrace();
			null }
	}
})
*/
