package net.bitnine.agenspopspark.service;

import com.google.common.collect.ImmutableMap;
import net.bitnine.agenspopspark.config.properties.ElasticProperties;
import net.bitnine.agenspopspark.process.AgensSparkService;
import net.bitnine.agenspopspark.util.DatasetResult;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.StructField;
import org.elasticsearch.hadoop.util.EsMajorVersion;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import org.graphframes.GraphFrame;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.*;
import java.util.stream.Collectors;

import static org.apache.spark.sql.types.DataTypes.BooleanType;
import static org.apache.spark.sql.types.DataTypes.StringType;

@Service
@Scope("singleton")
public class ElasticSparkService {

    public static enum INDEX {
        VERTEX("v", schemaV),
        EDGE("e", schemaE),
        PROPERTY("p", schemaP);

        final String value;
        final StructType schema;
        final Encoder<Row> encoder;
        String resource;

        INDEX(String value, StructType schema){
            this.value = value;
            this.schema = schema;
            this.encoder = RowEncoder.apply(schema);
        }
        public void setResource(String resource){
            this.resource = resource;
        }

        public static INDEX get(String type){
            return type.equalsIgnoreCase(VERTEX.value) ? VERTEX
                : (type.equalsIgnoreCase(EDGE.value) ? EDGE : PROPERTY);
        }
    }

    // **NOTE: must eqaul the field orders of Dataset to field orders of ES mappings
    public static final StructType schemaP = new StructType(new StructField[]{
        new StructField("key", StringType, false, Metadata.empty()),
        new StructField("type", StringType, false, Metadata.empty()),
        new StructField("value", StringType, false, Metadata.empty())
        });
    public static final StructType schemaV = new StructType(new StructField[]{
        new StructField("datasource", StringType, false, Metadata.empty()),
        new StructField("id", StringType, false, Metadata.empty()),
        new StructField("label", StringType, false, Metadata.empty()),
        new StructField("properties", new ArrayType(schemaP, true), false, Metadata.empty())
        });
    public static final StructType schemaE = schemaV
        .add( new StructField("src", StringType, false, Metadata.empty()) )
        .add( new StructField("dst", StringType, false, Metadata.empty()) );

    private final JavaSparkContext jsc;
    private final SparkSession sql;
    private final ElasticProperties properties;

    @Autowired
    public ElasticSparkService(
            ElasticProperties elasticProperties,
            JavaSparkContext javaSparkContext
    ){
        this.jsc = javaSparkContext;
        this.sql = SparkSession.builder().sparkContext(jsc.sc()).getOrCreate();

        this.properties = elasticProperties;
        INDEX.VERTEX.setResource(properties.getVertexIndex());
        INDEX.EDGE.setResource(properties.getEdgeIndex());
    }

    //////////////////////////////////////////////////////

    public Map<String,String> esConf(){
        Map<String,String> conf = new HashMap<>();
        conf.put("es.nodes", properties.getHost());
        conf.put("es.port", String.valueOf(properties.getPort()));
        conf.put("es.nodes.wan.only", "true");
        conf.put("es.mapping.id", "id");
        conf.put("es.write.operation", "upsert");
        conf.put("es.index.auto.create", "true");
        conf.put("es.scroll.size", "10000");
        conf.put("es.mapping.exclude", "removed,*.present");    // not work about JSON
        return ImmutableMap.copyOf(conf);
    }

    public SparkSession getSql() { return sql; }

/*
    public JavaPairRDD<String, Map<String, Object>> load(String datasource, String type){
        Map<String,String> cfgEsHadoop = new HashMap<>(esConf());
        cfgEsHadoop.put("es.resource", INDEX.get(type).resource);
        cfgEsHadoop.put("es.query", "?q=datasource:"+datasource);

        JavaPairRDD<String, Map<String, Object>> esRDD = JavaEsSpark.esRDD(jsc, cfgEsHadoop);

        System.out.println("** "+datasource+"["+type+"].count = "+esRDD.count());
        return esRDD;
    }
*/

    public void write(Dataset<Row> df, INDEX index){
        df.write().mode(SaveMode.Append).format("es").options(esConf()).save(index.resource);
    }

    public String makeQuery(String datasource, List<String> excludeLabels){
        String query = "{\"query\":{\"bool\":{ "
            +"\"filter\":{\"term\":{\"datasource\":\""+datasource+"\"}}";
        if( excludeLabels != null && excludeLabels.size() > 0 ) {
            String excludeStmt = ", \"must_not\":{\"terms\":{\"label\": "
                    +excludeLabels.stream().collect(Collectors.joining("\",\"", "[\"", "\"]"))
                    +"}}";
            query += excludeStmt;
        }
        query += "}}}";
        System.out.println("**esQuery => "+query);
        return query;
    }

    public Dataset<Row> read(String datasource, INDEX index, List<String> excludeLabels){
        String esQuery = makeQuery(datasource, excludeLabels);
        Dataset<Row> df = sql.read().format("es").options(esConf())
                .option("es.query", esQuery)
                .schema(index.schema)
                .load(index.resource);
        // for DEBUG
        System.out.println("  ==> "+datasource+"["+index.resource+"].count = "+df.count());
        return df;
    }

    public GraphFrame graph(String datasource, List<String> excludeV, List<String> excludeE){
        Dataset<Row> dfV = read(datasource, INDEX.VERTEX, excludeV);
        Dataset<Row> dfE = read(datasource, INDEX.EDGE, excludeE);
        return dfV.count() == 0 ? null : GraphFrame.apply(dfV, dfE);
    }

}
