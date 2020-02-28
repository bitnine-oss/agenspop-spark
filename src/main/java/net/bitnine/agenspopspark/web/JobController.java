package net.bitnine.agenspopspark.web;

import net.bitnine.agenspopspark.dto.JobMessage;
import net.bitnine.agenspopspark.job.AgensSparkJob;
import net.bitnine.agenspopspark.job.Greeting;
import net.bitnine.agenspopspark.dto.HelloMessage;
import net.bitnine.agenspopspark.job.SparkJob;
import net.bitnine.agenspopspark.service.AgensGraphService;
import net.bitnine.agenspopspark.service.JobManagerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.messaging.handler.annotation.DestinationVariable;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.util.HtmlUtils;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Controller
public class JobController {

    private static int jobNumber;
    private final JobManagerService jobManager;
    private final AgensGraphService graphService;

    private final SimpMessagingTemplate template;

    @Autowired
    public JobController(
            JobManagerService jobManager,
            AgensGraphService graphService,
            SimpMessagingTemplate messagingTemplate
    ) {
        this.jobManager = jobManager;
        this.graphService = graphService;
        this.template = messagingTemplate;
    }

    @GetMapping(value="/job/test1", produces="application/json; charset=UTF-8")
    @ResponseStatus(HttpStatus.OK)
    @ResponseBody
    public String hello1() throws Exception {
        String msg = "{ \"msg\": \"Hello, TEST01\"}";

        Optional<AgensSparkJob> job = jobManager.getJob("job-00002");
        if( job.isPresent() ){
            template.convertAndSend("/topic/jobstatus",
                    job.get().getMessage() );
        }
        return msg;
    }

    @GetMapping(value="/job/test2", produces="application/json; charset=UTF-8")
    @ResponseStatus(HttpStatus.OK)
    @ResponseBody
    public String hello2() throws Exception {
        Optional<AgensSparkJob> job = jobManager.getJob("job-00001");
        if( job.isPresent() ) jobManager.runJob(job.get());
        return "{ \"msg\": \"Hello, spark!\"}";
    }

    @MessageMapping("/hello")       // receiver from "/app/hellp"
    @SendTo("/topic/greetings")     // send to
    public Greeting greeting(HelloMessage message) throws Exception {
        Thread.sleep(1000); // simulated delay
        return new Greeting("Hello, " + HtmlUtils.htmlEscape(message.getName()) + "!");
    }

    @MessageMapping("/trigger")         // receiver from
    @SendTo("/topic/jobcreated")        // send to
    public List<JobMessage> startWork(Map<String,Object> params) {
        AgensSparkJob job = jobManager.newJob(params);
        String operator = (String)job.getParams().getOrDefault("operator","");
        switch(operator){
            case "indegree": graphService.inDegree(job);
                break;
            case "outdegree": graphService.outDegree(job);
                break;
            case "pagerank": graphService.pageRank(job);
                break;
            default:
                jobManager.removeJob(job.getJobId());
                job = null;
        }
        return job==null ? Collections.EMPTY_LIST : jobManager.getJobOne(job.getJobId());
    }

    @MessageMapping("/status")  // receive from "/app/status"
    @SendTo("/topic/jobstatus")         // send to
    public List<JobMessage> fetchStatus(String jobId) {
        if( jobId.equals("all") ) return this.jobManager.getJobList();
        else return jobManager.getJobOne(jobId);
    }

}
