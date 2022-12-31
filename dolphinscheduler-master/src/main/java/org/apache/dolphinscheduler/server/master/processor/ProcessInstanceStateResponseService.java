package org.apache.dolphinscheduler.server.master.processor;

import org.apache.dolphinscheduler.server.master.event.StateEvent;
import org.omg.PortableServer.THREAD_POLICY_ID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

/*
 * @Author Administrator
 * @Date 2022/12/30
 **/
@Component
public class ProcessInstanceStateResponseService {
    private final Logger logger = LoggerFactory.getLogger(ProcessInstanceStateResponseService.class);

    /**
     *  requestKey和request
     */
    private ConcurrentHashMap<String, StateEvent> queryProcessStateCache = new ConcurrentHashMap<>();
    /**
     * state event queue
     */
    private final ConcurrentLinkedQueue<StateEvent> stateEvents = new ConcurrentLinkedQueue<>();

    public void addCache(String key,StateEvent event){
        queryProcessStateCache.put(key,event);
    }

    @PostConstruct
    public void init(){
        Executors.newSingleThreadExecutor().execute(new Runnable() {

            //StateEvent(key=null, processDefinitionCode=null, type=TASK_STATE_CHANGE, executionStatus=RUNNING_EXECUTION, taskInstanceId=56, taskCode=0, processInstanceId=33, context=null, channel=null)

            @Override
            public void run() {
//                while (true){
//                    for (StateEvent event : stateEvents) {
//                        logger.info(event.toString());
//                    }
//                }
            }
        });
    }

    private void removeCache(String key){
        StateEvent event = queryProcessStateCache.get(key);
        if (event != null){
            event.getChannel().close();
            queryProcessStateCache.remove(key,event);
            logger.info("remove processInstance query event: {}",event.toString());
        }
    }


    public void addEvents(StateEvent stateEvent){
        this.stateEvents.add(stateEvent);
    }
}
