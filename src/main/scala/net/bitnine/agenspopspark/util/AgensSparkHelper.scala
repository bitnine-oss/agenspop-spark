package net.bitnine.agenspopspark.util

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import org.apache.spark.sql.functions.{col, collect_list, explode}
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.DataTypes.{BooleanType, ByteType, DateType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, TimestampType}
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, Row}

object AgensSparkHelper {

	// DataType mapping to Java ==> https://stackoverflow.com/a/49796854/6811653
	def ptypecast(ptype:String):DataType = {
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

}
