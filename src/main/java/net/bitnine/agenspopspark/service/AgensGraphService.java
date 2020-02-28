package net.bitnine.agenspopspark.service;

import net.bitnine.agenspopspark.job.AgensSparkJob;
import net.bitnine.agenspopspark.job.SparkJob;
import net.bitnine.agenspopspark.process.AgensSparkService;
import net.bitnine.agenspopspark.util.AgensUtilHelper;
import net.bitnine.agenspopspark.util.DatasetResult;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.graphframes.GraphFrame;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import scala.Tuple2;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static net.bitnine.agenspopspark.util.AgensUtilHelper.convertMap2String;

@Service
@Scope("singleton")
public class AgensGraphService {

    private final transient ElasticSparkService elasticService;
    private final transient AgensSparkService sparkService;
    private final transient SparkSession sql;
    private final JobManagerService jobService;

    @Autowired
    public AgensGraphService(
            ElasticSparkService elasticService,
            AgensSparkService sparkService,
            JobManagerService jobService
    ){
        this.sparkService = sparkService;
        this.elasticService = elasticService;
        this.sql = elasticService.getSql();
        this.jobService = jobService;
    }

    public Stream<Map<String,Object>> sampleTest(){
        Dataset<Row> rows = sparkService.sampleTest(sql);
        Stream<Map<String,Object>> stream = AgensUtilHelper.dataset2stream(rows);
        return stream;
    }

    public Object count(String datasource, List<String> excludeV, List<String> excludeE){
        GraphFrame g = elasticService.graph(datasource, excludeV, excludeE);
        Tuple2<Object,Object> counts = sparkService.count(g);

        Map<String, Long> result = new HashMap<>();
        result.put("V", (Long)counts._1());
        result.put("E", (Long)counts._2());
        return result;
    }

    @Async(value="agensExecutor")
    public void dropKeys(AgensSparkJob job){
        job.setTotalSteps(4);
        job.setState(SparkJob.STATE.RUNNING);
        job.notifyProgress("settup paramters => "+convertMap2String(job.getParams()));

        // STEP1)
        Map<String,Object> params = job.getParams();
        String operator = (String)params.get("operator");
        String datasource = (String)params.get("datasource");
        ElasticSparkService.INDEX index = !params.containsKey("datasource") ? null : ElasticSparkService.INDEX.get((String)params.get("type"));
        List<String> keys = !params.containsKey("datasource") ? null : (List<String>)params.get("keys");
        if( operator==null || datasource==null || index==null || keys==null ){
            job.setState(SparkJob.STATE.FAIL);
            job.notifyProgress("parameter missing: operator, datasource, index, keys");
            return;
        }

        // STEP2)
        job.notifyProgress("make dataframe from "+index.resource);
        Dataset<Row> df = elasticService.read((String)params.get("datasource")
                , index, Collections.EMPTY_LIST);
        if( df.count() == 0 ){
            job.setState(SparkJob.STATE.FAIL);
            job.notifyProgress("source dataframe is empty");
            return;
        }

        try {
            // STEP3)
            job.notifyProgress(operator+" : running on spark");
            Dataset<Row> rows = sparkService.dropKeys(df, (List<String>)params.get("keys"));
            // STEP4)
            job.notifyProgress("write results to elasticsearch");
            if(rows.count() > 0) elasticService.write(rows, index);

        } catch(Exception e){
            System.out.println("  - "+operator+" ERROR ==> "+e.getMessage());
            job.setState(SparkJob.STATE.FAIL);
            job.notifyProgress(e.getMessage());
            return;
        }
        // STEP5)
        job.setState(SparkJob.STATE.SUCCESS);
        job.notifyProgress(operator+" : all process are done");
    }

    ///////////////////////////////////////////////////////

/*
    public Stream<Map<String,Object>> inDegree(String datasource, List<String> excludeV, List<String> excludeE){
        GraphFrame g = elasticService.graph(datasource, excludeV, excludeE);
        if( g == null ) return Stream.empty();

        Stream<Map<String,Object>> stream = Stream.empty();
        try {
            Dataset<Row> rows = sparkService.inDegree(g);
            elasticService.write(rows, ElasticSparkService.INDEX.VERTEX);
            stream = AgensUtilHelper.dataset2stream(rows);
        }catch (Exception e){
            System.out.println("  - inDegree ERROR ==> "+e.getMessage());
        }
        return stream;
    }
*/

    @Async(value="agensExecutor")
    public void pageRank(AgensSparkJob job){
        job.setTotalSteps(4);
        job.setState(SparkJob.STATE.RUNNING);
        job.notifyProgress("settup paramters => "+convertMap2String(job.getParams()));

        // STEP1)
        Map<String,Object> params = job.getParams();
        String operator = (String)params.get("operator");
        String datasource = (String)params.get("datasource");
        List<String> excludeV = !params.containsKey("excludeV") ? Collections.EMPTY_LIST :
                (List<String>)params.get("excludeV");
        List<String> excludeE = !params.containsKey("excludeE") ? Collections.EMPTY_LIST :
                (List<String>)params.get("excludeE");
        if( operator == null || datasource==null ){
            job.setState(SparkJob.STATE.FAIL);
            job.notifyProgress("parameter missing: operator, datasource (, excludeV, excludeE)");
            return;
        }

        // STEP2)
        job.notifyProgress("make graphframe from "+datasource);
        GraphFrame g = elasticService.graph(datasource, excludeV, excludeE);
        if( g == null ){
            job.setState(SparkJob.STATE.FAIL);
            job.notifyProgress("source dataframe is empty");
            return;
        }

        try {
            // STEP3)
            job.notifyProgress("running on spark: "+operator);
            Dataset<Row> rows = sparkService.pageRank(g, operator);

            // STEP4)
            long count = rows.count();
            job.notifyProgress("write results to elasticsearch: size="+count);
            if(count > 0) elasticService.write(rows, ElasticSparkService.INDEX.VERTEX);
        }catch (Exception e){
            System.out.println("  - "+operator+" ERROR ==> "+e.getMessage());
            job.setState(SparkJob.STATE.FAIL);
            job.notifyProgress(e.getMessage());
            return;
        }
        // STEP5)
        job.setState(SparkJob.STATE.SUCCESS);
        job.notifyProgress(operator+" : all process are done");
    }

    @Async(value="agensExecutor")
    public void outDegree(AgensSparkJob job){
        job.setTotalSteps(4);
        job.setState(SparkJob.STATE.RUNNING);
        job.notifyProgress("settup paramters => "+convertMap2String(job.getParams()));

        // STEP1)
        Map<String,Object> params = job.getParams();
        String operator = (String)params.get("operator");
        String datasource = (String)params.get("datasource");
        List<String> excludeV = !params.containsKey("excludeV") ? Collections.EMPTY_LIST :
                (List<String>)params.get("excludeV");
        List<String> excludeE = !params.containsKey("excludeE") ? Collections.EMPTY_LIST :
                (List<String>)params.get("excludeE");
        if( operator==null || datasource==null ){
            job.setState(SparkJob.STATE.FAIL);
            job.notifyProgress("parameter missing: operator, datasource (, excludeV, excludeE)");
            return;
        }

        // STEP2)
        job.notifyProgress("make graphframe from "+datasource);
        GraphFrame g = elasticService.graph(datasource, excludeV, excludeE);
        if( g == null ){
            job.setState(SparkJob.STATE.FAIL);
            job.notifyProgress("source dataframe is empty");
            return;
        }

        try {
            // STEP3)
            job.notifyProgress(operator+" : running on spark");
            Dataset<Row> rows = sparkService.outDegree(g, operator);

            // STEP4)
            long count = rows.count();
            job.notifyProgress("write results to elasticsearch: size="+count);
            if(count > 0) elasticService.write(rows, ElasticSparkService.INDEX.VERTEX);
        }catch (Exception e){
            System.out.println("  - "+operator+" ERROR ==> "+e.getMessage());
            job.setState(SparkJob.STATE.FAIL);
            job.notifyProgress(e.getMessage());
            return;
        }
        // STEP5)
        job.setState(SparkJob.STATE.SUCCESS);
        job.notifyProgress(operator+" : all process are done");
    }

    @Async(value="agensExecutor")
    public void inDegree(AgensSparkJob job){
        job.setTotalSteps(4);
        job.setState(SparkJob.STATE.RUNNING);
        job.notifyProgress("settup paramters => "+convertMap2String(job.getParams()));

        // STEP1)
        Map<String,Object> params = job.getParams();
        String operator = (String)params.get("operator");
        String datasource = (String)params.get("datasource");
        List<String> excludeV = !params.containsKey("excludeV") ? Collections.EMPTY_LIST :
                (List<String>)params.get("excludeV");
        List<String> excludeE = !params.containsKey("excludeE") ? Collections.EMPTY_LIST :
                (List<String>)params.get("excludeE");
        if( operator==null || datasource==null ){
            job.setState(SparkJob.STATE.FAIL);
            job.notifyProgress("parameter missing: operator, datasource (, excludeV, excludeE)");
            return;
        }

        // STEP2)
        job.notifyProgress("make graphframe from "+datasource);
        GraphFrame g = elasticService.graph(datasource, excludeV, excludeE);
        if( g == null ){
            job.setState(SparkJob.STATE.FAIL);
            job.notifyProgress("source dataframe is empty");
            return;
        }

        try {
            // STEP3)
            job.notifyProgress(operator+" : running on spark");
            Dataset<Row> rows = sparkService.inDegree(g, operator);

            // STEP4)
            long count = rows.count();
            job.notifyProgress("write results to elasticsearch: size="+count);
            if(count > 0) elasticService.write(rows, ElasticSparkService.INDEX.VERTEX);
        }catch (Exception e){
            System.out.println("  - "+operator+" ERROR ==> "+e.getMessage());
            job.setState(SparkJob.STATE.FAIL);
            job.notifyProgress(e.getMessage());
            return;
        }
        // STEP5)
        job.setState(SparkJob.STATE.SUCCESS);
        job.notifyProgress(operator+" : all process are done");
    }

    @Async(value="agensExecutor")
    public void connComponent(AgensSparkJob job){
        job.setTotalSteps(4);
        job.setState(SparkJob.STATE.RUNNING);
        job.notifyProgress("settup paramters => "+convertMap2String(job.getParams()));

        // STEP1)
        Map<String,Object> params = job.getParams();
        String operator = (String)params.get("operator");
        String datasource = (String)params.get("datasource");
        List<String> excludeV = !params.containsKey("excludeV") ? Collections.EMPTY_LIST :
                (List<String>)params.get("excludeV");
        List<String> excludeE = !params.containsKey("excludeE") ? Collections.EMPTY_LIST :
                (List<String>)params.get("excludeE");
        if( operator==null || datasource==null ){
            job.setState(SparkJob.STATE.FAIL);
            job.notifyProgress("parameter missing: operator, datasource (, excludeV, excludeE)");
            return;
        }

        // STEP2)
        job.notifyProgress("make graphframe from "+datasource);
        GraphFrame g = elasticService.graph(datasource, excludeV, excludeE);
        if( g == null ){
            job.setState(SparkJob.STATE.FAIL);
            job.notifyProgress("source dataframe is empty");
            return;
        }

        try {
            // STEP3)
            job.notifyProgress(operator+" : running on spark");
            Dataset<Row> rows = sparkService.connComponent(g, operator);

            // STEP4)
            long count = rows.count();
            job.notifyProgress("write results to elasticsearch: size="+count);
            if(count > 0) elasticService.write(rows, ElasticSparkService.INDEX.VERTEX);
        }catch (Exception e){
            System.out.println("  - "+operator+" ERROR ==> "+e.getMessage());
            job.setState(SparkJob.STATE.FAIL);
            job.notifyProgress(e.getMessage());
            return;
        }
        // STEP5)
        job.setState(SparkJob.STATE.SUCCESS);
        job.notifyProgress(operator+" : all process are done");
    }

    @Async(value="agensExecutor")
    public void sconnComponent(AgensSparkJob job){
        job.setTotalSteps(4);
        job.setState(SparkJob.STATE.RUNNING);
        job.notifyProgress("settup paramters => "+convertMap2String(job.getParams()));

        // STEP1)
        Map<String,Object> params = job.getParams();
        String operator = (String)params.get("operator");
        String datasource = (String)params.get("datasource");
        List<String> excludeV = !params.containsKey("excludeV") ? Collections.EMPTY_LIST :
                (List<String>)params.get("excludeV");
        List<String> excludeE = !params.containsKey("excludeE") ? Collections.EMPTY_LIST :
                (List<String>)params.get("excludeE");
        if( operator==null || datasource==null ){
            job.setState(SparkJob.STATE.FAIL);
            job.notifyProgress("parameter missing: operator, datasource (, excludeV, excludeE)");
            return;
        }

        // STEP2)
        job.notifyProgress("make graphframe from "+datasource);
        GraphFrame g = elasticService.graph(datasource, excludeV, excludeE);
        if( g == null ){
            job.setState(SparkJob.STATE.FAIL);
            job.notifyProgress("source dataframe is empty");
            return;
        }

        try {
            // STEP3)
            job.notifyProgress(operator+" : running on spark");
            Dataset<Row> rows = sparkService.sconnComponent(g, operator);

            // STEP4)
            long count = rows.count();
            job.notifyProgress("write results to elasticsearch: size="+count);
            if(count > 0) elasticService.write(rows, ElasticSparkService.INDEX.VERTEX);
        }catch (Exception e){
            System.out.println("  - "+operator+" ERROR ==> "+e.getMessage());
            job.setState(SparkJob.STATE.FAIL);
            job.notifyProgress(e.getMessage());
            return;
        }
        // STEP5)
        job.setState(SparkJob.STATE.SUCCESS);
        job.notifyProgress(operator+" : all process are done");
    }

}
