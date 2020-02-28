package net.bitnine.agenspopspark.job;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public interface SparkJob extends Runnable {

    enum STATE {
        READY("ready"),
        RUNNING("running"),
        SUCCESS("success"),
        FAIL("fail");

        public final String value;
        STATE(String value){ this.value = value; }

        @Override
        public String toString() {
            return this.value;
        }
    }

    class Parameters {
        public final String datasource;
        public final List<String> excludeV;   // excluding labels of vertices
        public final List<String> excludeE;   // excluding labels of edges

        public Parameters(){
            this.datasource = "";
            this.excludeV = Collections.EMPTY_LIST;
            this.excludeE = Collections.EMPTY_LIST;
        }
        public Parameters(String datasource, List<String> excludeV, List<String> excludeE){
            this.datasource = datasource;
            this.excludeV = excludeV;
            this.excludeE = excludeE;
        }
    }

    String getJobId();
    Map<String,Object> getParams();

    STATE getState();
    int getTotalSteps();
    int getCurrentStep();

}
