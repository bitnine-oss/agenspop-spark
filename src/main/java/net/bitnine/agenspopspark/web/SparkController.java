package net.bitnine.agenspopspark.web;

import com.fasterxml.jackson.databind.ObjectMapper;

import com.google.common.collect.ImmutableMap;
import net.bitnine.agenspopspark.config.properties.ProductProperties;
import net.bitnine.agenspopspark.job.AgensSparkJob;
import net.bitnine.agenspopspark.job.SparkJob;
import net.bitnine.agenspopspark.service.AgensGraphService;
import net.bitnine.agenspopspark.service.ElasticSparkService;
import net.bitnine.agenspopspark.service.JobManagerService;
import net.bitnine.agenspopspark.util.AgensUtilHelper;
import net.bitnine.agenspopspark.util.DatasetResult;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@RestController
@RequestMapping(value = "${agens.api.base-path}/spark")
public class SparkController {

    private final ObjectMapper mapper;
    private final SimpMessagingTemplate template;
    private final ElasticSparkService esService;
    private final AgensGraphService graphService;
    private final JobManagerService jobManager;
    private final ProductProperties productProperties;

    @Autowired
    SparkController(
            ObjectMapper objectMapper,
            SimpMessagingTemplate template,
            ElasticSparkService esService,
            AgensGraphService graphService,
            JobManagerService jobManager,
            ProductProperties productProperties
    ){
        this.mapper = objectMapper;
        this.template = template;
        this.esService = esService;
        this.graphService = graphService;
        this.jobManager = jobManager;
        this.productProperties = productProperties;
    }

    @GetMapping(value="/hello", produces="application/json; charset=UTF-8")
    @ResponseStatus(HttpStatus.OK)
    public String hello() throws Exception {
        return "{ \"msg\": \"Hello, spark!\"}";
    }

    @GetMapping(value="/test", produces="application/json; charset=UTF-8")
    public ResponseEntity test() throws Exception {
        Stream<Map<String,Object>> stream = graphService.sampleTest();
        return AgensUtilHelper.responseStream(mapper, stream
                , AgensUtilHelper.productHeaders(productProperties));
    }

    //////////////////////////////////////////////

    @GetMapping(value="/{datasource}/{type}/read", produces="application/json; charset=UTF-8")
    public ResponseEntity read(
            @PathVariable String datasource, @PathVariable String type,
            @RequestParam(value="excludeLabel", required=false, defaultValue="") List<String> excludeLabels
    ) throws Exception {
        Dataset<Row> rows = esService.read(datasource, ElasticSparkService.INDEX.get(type), excludeLabels);
        Stream<Map<String,Object>> stream = AgensUtilHelper.dataset2stream(rows);
        return AgensUtilHelper.responseStream(mapper, stream
                , AgensUtilHelper.productHeaders(productProperties));
    }

/*
curl -X GET "localhost:8081/api/spark/modern/v/dropkeys?q=_$$indegree,_$$outdegree,_$$pagerank"
 */
    @GetMapping(value="/{datasource}/{type}/dropkeys", produces="application/json; charset=UTF-8")
    public ResponseEntity dropKeys(
            @PathVariable String datasource, @PathVariable String type,
            @RequestParam(value="q", defaultValue="") List<String> keys
    ) throws Exception {
        Map<String,Object> params = ImmutableMap.of("operator", "dropkeys"
                , "datasource", datasource, "type", type,"keys", keys
        );
        AgensSparkJob job = jobManager.newJob(params);
        graphService.dropKeys(job);
        return new ResponseEntity(job.getMessage()
                , AgensUtilHelper.productHeaders(productProperties), HttpStatus.OK);
    }

/*
curl -X GET "localhost:8081/api/spark/modern/count"
 */
    @GetMapping(value="/{datasource}/count", produces="application/json; charset=UTF-8")
    public ResponseEntity count(
            @PathVariable String datasource,
            @RequestParam(value="excludeV", required=false, defaultValue="") List<String> excludeV,
            @RequestParam(value="excludeE", required=false, defaultValue="") List<String> excludeE
    ) throws Exception {
        Map<String,Object> params = ImmutableMap.of("operator", "count"
                , "datasource", datasource,"excludeV", excludeV,"excludeE", excludeE
        );
        Object result = graphService.count(datasource, excludeV, excludeE);
        return new ResponseEntity(result
                , AgensUtilHelper.productHeaders(productProperties), HttpStatus.OK);
    }

    //////////////////////////////////////////////

/*
curl -X GET "localhost:8081/api/spark/modern/pagerank"
curl -X GET "localhost:8081/api/spark/northwind/pagerank?excludeV=supplier,category&excludeE=reports_to,part_of,supplies"
curl -X GET "localhost:8081/api/spark/airroutes/pagerank"
 */
    @GetMapping(value="/{datasource}/pagerank", produces="application/json; charset=UTF-8")
    public ResponseEntity pageRank(
            @PathVariable String datasource,
            @RequestParam(value="excludeV", required=false, defaultValue="") List<String> excludeV,
            @RequestParam(value="excludeE", required=false, defaultValue="") List<String> excludeE
    ) throws Exception {
        Map<String,Object> params = ImmutableMap.of("operator","pagerank"
                , "datasource", datasource,"excludeV", excludeV,"excludeE", excludeE
        );
        AgensSparkJob job = jobManager.newJob(params);
        graphService.pageRank(job);
        return new ResponseEntity(job.getMessage()
                , AgensUtilHelper.productHeaders(productProperties), HttpStatus.OK);
    }
/*
    @GetMapping(value="/{datasource}/indegree", produces="application/json; charset=UTF-8")
    public ResponseEntity inDegree(
            @PathVariable String datasource,
            @RequestParam(value="excludeV", required=false, defaultValue="") List<String> excludeV,
            @RequestParam(value="excludeE", required=false, defaultValue="") List<String> excludeE
    ) throws Exception {
        Stream<Map<String,Object>> stream = graphService.inDegree(datasource, excludeV, excludeE);
        return AgensUtilHelper.responseStream(mapper, stream
                , AgensUtilHelper.productHeaders(productProperties));
    }
*/

/*
curl -X GET "localhost:8081/api/spark/modern/outdegree"
curl -X GET "localhost:8081/api/spark/northwind/outdegree?excludeV=supplier,category&excludeE=reports_to,part_of,supplies"
curl -X GET "localhost:8081/api/spark/airroutes/outdegree"
 */
    @GetMapping(value="/{datasource}/outdegree", produces="application/json; charset=UTF-8")
    public ResponseEntity outDegree(
            @PathVariable String datasource,
            @RequestParam(value="excludeV", required=false, defaultValue="") List<String> excludeV,
            @RequestParam(value="excludeE", required=false, defaultValue="") List<String> excludeE
    ) throws Exception {
        Map<String,Object> params = ImmutableMap.of("operator", "outdegree"
                , "datasource", datasource,"excludeV", excludeV,"excludeE", excludeE
        );
        AgensSparkJob job = jobManager.newJob(params);
        graphService.outDegree(job);
        return new ResponseEntity(job.getMessage()
                , AgensUtilHelper.productHeaders(productProperties), HttpStatus.OK);
    }

/*
curl -X GET "localhost:8081/api/spark/modern/indegree"
curl -X GET "localhost:8081/api/spark/northwind/indegree?excludeV=supplier,category&excludeE=reports_to,part_of,supplies"
curl -X GET "localhost:8081/api/spark/airroutes/indegree"
 */
    @GetMapping(value="/{datasource}/indegree", produces="application/json; charset=UTF-8")
    public ResponseEntity inDegree(
            @PathVariable String datasource,
            @RequestParam(value="excludeV", required=false, defaultValue="") List<String> excludeV,
            @RequestParam(value="excludeE", required=false, defaultValue="") List<String> excludeE
    ) throws Exception {
        Map<String,Object> params = ImmutableMap.of("operator", "indegree"
                , "datasource", datasource,"excludeV", excludeV,"excludeE", excludeE
        );
        AgensSparkJob job = jobManager.newJob(params);
        graphService.inDegree(job);
        return new ResponseEntity(job.getMessage()
                , AgensUtilHelper.productHeaders(productProperties), HttpStatus.OK);
    }

    /*
    curl -X GET "localhost:8081/api/spark/modern/component"
    curl -X GET "localhost:8081/api/spark/northwind/component?excludeV=supplier,category&excludeE=reports_to,part_of,supplies"
    curl -X GET "localhost:8081/api/spark/airroutes/component"
     */
    @GetMapping(value="/{datasource}/component", produces="application/json; charset=UTF-8")
    public ResponseEntity connComponent(
            @PathVariable String datasource,
            @RequestParam(value="excludeV", required=false, defaultValue="") List<String> excludeV,
            @RequestParam(value="excludeE", required=false, defaultValue="") List<String> excludeE
    ) throws Exception {
        Map<String,Object> params = ImmutableMap.of("operator", "component"
                , "datasource", datasource,"excludeV", excludeV,"excludeE", excludeE
        );
        AgensSparkJob job = jobManager.newJob(params);
        graphService.connComponent(job);
        return new ResponseEntity(job.getMessage()
                , AgensUtilHelper.productHeaders(productProperties), HttpStatus.OK);
    }

    /*
    curl -X GET "localhost:8081/api/spark/modern/scomponent"
    curl -X GET "localhost:8081/api/spark/northwind/scomponent?excludeV=supplier,category&excludeE=reports_to,part_of,supplies"
    curl -X GET "localhost:8081/api/spark/airroutes/scomponent"
     */
    @GetMapping(value="/{datasource}/scomponent", produces="application/json; charset=UTF-8")
    public ResponseEntity sconnComponent(
            @PathVariable String datasource,
            @RequestParam(value="excludeV", required=false, defaultValue="") List<String> excludeV,
            @RequestParam(value="excludeE", required=false, defaultValue="") List<String> excludeE
    ) throws Exception {
        Map<String,Object> params = ImmutableMap.of("operator", "scomponent"
                , "datasource", datasource,"excludeV", excludeV,"excludeE", excludeE
        );
        AgensSparkJob job = jobManager.newJob(params);
        graphService.sconnComponent(job);
        return new ResponseEntity(job.getMessage()
                , AgensUtilHelper.productHeaders(productProperties), HttpStatus.OK);
    }

}
