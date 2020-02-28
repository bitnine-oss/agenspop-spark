package net.bitnine.agenspopspark.job;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import net.bitnine.agenspopspark.dto.JobMessage;
import org.springframework.messaging.simp.SimpMessagingTemplate;

import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;


////////////////////////////////////////////////
//
//  **Job
//      - 사용자 요청 작업에 대한 명세 : jobName, parameters
//      - 작업 상태와 시작/종료 시간
//      - 작업은 LinkedList (queue) 에 저장되고 상태 조회시 호출됨
//      - 상태(status) 변경시 socket 으로 send message
//          ==> doing, success, fail
//      - 과정(propgress) 이력은 사전에 내부 LinkedList progress 에 명시
//          ==> ex) 총 5단계라 했을 때, 2 -> 3 단계 변경시 메시지 전달
//
////////////////////////////////////////////////

@NoArgsConstructor
@ToString
public class AgensSparkJob implements SparkJob {

    private SimpMessagingTemplate template;

    private String jobId = "";
    private Map<String,Object> params;

    private STATE state = STATE.READY;
    private int totalSteps;
    private AtomicInteger currentStep = new AtomicInteger(0);
    private long startTime = System.currentTimeMillis();

    private LinkedList<String> history = new LinkedList<>();

    public AgensSparkJob( Map<String,Object> params ){
        this.jobId = jobId;
        this.params = params;
    }

    public void setJobId(String jobId){ this.jobId = jobId; }
    public void setTemplate(SimpMessagingTemplate template){ this.template = template; }

    public AgensSparkJob( String jobId, Map<String,Object> params,
            SimpMessagingTemplate template
    ) {
        this.jobId = jobId;
        this.params = params;
        this.template = template;
    }

    @Override
    public void run() {
        sendProgress();
//        try {
//            Thread.sleep(5000);
//
//            currentStep.set(currentStep.incrementAndGet());
//            sendProgress();
//        } catch (Exception e) {
//            e.printStackTrace();
//            state = STATE.FAIL;
//            sendProgress();
//            return;
//        }
    }

    public String getElapsedTime(){
        long ms = System.currentTimeMillis() - startTime;  // milli-second
        SimpleDateFormat elapsedTime;
        if( ms < (60*1000)) elapsedTime = new SimpleDateFormat("ss.SSS");
        else if( ms < (60*60*1000)) elapsedTime = new SimpleDateFormat("mm:ss");
        else if( ms < (24*60*60*1000)) elapsedTime = new SimpleDateFormat("HH:mm:ss");
        else elapsedTime = new SimpleDateFormat("dd days HH:mm:ss");
        return elapsedTime.format(ms);
    }

    public JobMessage getMessage() {
        JobMessage temp = new JobMessage(jobId, totalSteps);
        temp.setCurrentStep(currentStep.get());
        temp.setElapsedTime(getElapsedTime());
        temp.setState(state.value);
        temp.setProgress(history.size() > 0 ? history.getLast() : "");
        return temp;
    }

    public void sendProgress() {
        template.convertAndSend("/topic/jobstatus", getMessage());
    }

    @Override
    public int getCurrentStep() { return currentStep.get(); }
    @Override
    public int getTotalSteps() { return totalSteps; }
    @Override
    public STATE getState() { return state; }
    @Override
    public String getJobId() { return jobId; }
    @Override
    public Map<String,Object> getParams() { return params; }

    public LinkedList<String> getHistory() { return history; }

    public void setTotalSteps(int totalSteps){ this.totalSteps = totalSteps; }
    public void setCurrentStep(int step){ this.currentStep.set(step); }
    public void setState(STATE state){ this.state = state; }

    public int notifyProgress(String progress){
        history.add(progress);
        template.convertAndSend("/topic/jobstatus", getMessage());
        return currentStep.getAndIncrement();
    }
}