package net.bitnine.agenspopspark;

import com.google.common.collect.ImmutableList;
import net.bitnine.agenspopspark.config.properties.ElasticProperties;
import net.bitnine.agenspopspark.config.properties.ProductProperties;
import net.bitnine.agenspopspark.config.properties.SparkProperties;
import net.bitnine.agenspopspark.process.AgensSparkService;
import net.bitnine.agenspopspark.service.ElasticSparkService;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.web.servlet.support.SpringBootServletInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.event.EventListener;
import scala.collection.JavaConverters;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.spark.api.java.function.MapFunction;
import scala.collection.immutable.Map;
import scala.collection.mutable.Seq;

@SpringBootApplication
@EnableConfigurationProperties({ ElasticProperties.class, SparkProperties.class, ProductProperties.class })
public class AgenspopSparkApplication implements ApplicationRunner {

	private static final Logger LOGGER = LoggerFactory.getLogger(AgenspopSparkApplication.class);

	public static void main(String[] args) {
		ConfigurableApplicationContext context = SpringApplication.run(AgenspopSparkApplication.class, args);

		String[] beanNames = context.getBeanNamesForType(SparkConf.class);

		if( beanNames.length > 0 ){
			System.out.println("** BEAN: "+beanNames[0]+"\n==>");
			SparkConf conf = (SparkConf) context.getBean(beanNames[0]);
			System.out.println(Arrays.stream(conf.getAll()).map(t->{
				return t._1()+"="+t._2();
			}).sorted().collect(Collectors.joining("\n")));
			System.out.println("\n==================================\n");

			System.out.println("** spark.job-monitor.url ==> http://<"+conf.get("spark.master")+">:4040/\n");
		}

/*
		ElasticSparkService es = context.getBean(ElasticSparkService.class);
		AgensSparkService as = context.getBean(AgensSparkService.class);

		if( es != null ){
			Dataset<Row> df = es.read("modern", "v");
			df.printSchema();
			df.show();

			// ** To do.
			// 1) createTempView
			// 2) select where property.key contains "lang"
			// 3) ... analyze by GraphFrame
			// 3) create new DataFrame having new property (weight, etc..)
			// 4) save to ES

			Encoder<Row> encoder = es.getEncoder("v");

			Dataset<Row> df2 = df.map(new MapFunction<Row, Row>() {
				@Override
				public Row call(Row row) throws Exception {
//					List<Row> pDst = new ArrayList<>();
//					List<Row> pSrc = row.getList(3);
//					for( Object p :  ){
//						GenericRowWithSchema pSrc = (GenericRowWithSchema) p;
//						pDst.add( RowFactory.create(pSrc.getString(0), pSrc.getBoolean(1), pSrc.getString(2), pSrc.getString(3)) );
//					}
					if( row != null )
						System.out.println("anyNull="+row.anyNull()+", size="+row.size());
					Map<String, Object> values = row.getValuesMap(
							JavaConverters.asScalaIteratorConverter(ImmutableList.of("datasource", "id","label").iterator()).asScala().toSeq()
							);
					System.out.println(values);
					return RowFactory.create(
							row.getAs("datasource")
							, row.getAs("id")
							, row.getAs("label")
					);
			}
			}, encoder);

			df2.printSchema();
			df2.show();

//			Dataset<Row> properties = es.transform(df);
//			Row[] rows = df2.collect();
//			for( Row row : rows ){
//				System.out.println(""+row.getString());
//			}
		}
 */
	}

	@Override
	public void run(ApplicationArguments applicationArguments) throws Exception {
		// for DEBUG
		System.out.println("\n** ready!! \n==================================\n");
	}

}
