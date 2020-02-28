package net.bitnine.agenspopspark.service;

import com.google.common.collect.ImmutableMap;
import net.bitnine.agenspopspark.dto.JobMessage;
import net.bitnine.agenspopspark.job.AgensSparkJob;
import net.bitnine.agenspopspark.job.SparkJob;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

@Service
@Scope("singleton")
public class JobManagerService {

    private AtomicInteger jobCounter = new AtomicInteger(0);
    private final ConcurrentHashMap<String, AgensSparkJob> jobs = new ConcurrentHashMap<>();

    private final SimpMessagingTemplate template;

    @Autowired
    public JobManagerService(
            SimpMessagingTemplate messagingTemplate
    ){
        this.template = messagingTemplate;
    }

    @PostConstruct
    private void ready(){
        AgensSparkJob job1 = newJob(ImmutableMap.of("operator","graphJob01"));
        job1.setTotalSteps(4); job1.setCurrentStep(2); job1.setState(SparkJob.STATE.FAIL);
        AgensSparkJob job2 = newJob(ImmutableMap.of("operator","graphJob02"));
        job2.setTotalSteps(5); job2.setCurrentStep(1); job2.setState(SparkJob.STATE.RUNNING);
        AgensSparkJob job3 = newJob(ImmutableMap.of("operator","graphJob03"));
        job3.setTotalSteps(3); job3.setCurrentStep(3); job3.setState(SparkJob.STATE.SUCCESS);
    }

    // **NOTE: https://jeong-pro.tistory.com/187
    @Async(value="agensExecutor")
    public void runJob(Runnable runnable) {
        System.out.println("Got runnable " + runnable);
        runnable.run();
    }

    public AgensSparkJob newJob(Map<String,Object> params){
        String jobId = String.format("job-%05d",jobCounter.incrementAndGet());
        AgensSparkJob job = new AgensSparkJob(jobId, params, template);
        jobs.put(jobId, job);
        return job;
    }
    public void addJob(AgensSparkJob job){
        String jobId = String.format("job-%05d",jobCounter.incrementAndGet());
        job.setJobId(jobId);
        job.setTemplate(template);
        jobs.put(job.getJobId(), job);
    }

    public List<JobMessage> getJobList(){
        return jobs.values().stream().map(r->r.getMessage()).collect(Collectors.toList());
    }

    public Optional<AgensSparkJob> getJob(String jobId) {
        return Optional.of( jobs.get(jobId) );
    }

    public List<JobMessage> getJobOne(String jobId){
        AgensSparkJob job = jobs.get(jobId);
        return job == null ? Collections.EMPTY_LIST
                : Collections.singletonList(jobs.get(jobId).getMessage());
    }

    public AgensSparkJob removeJob(String jobId){ return jobs.remove(jobId); }
}
