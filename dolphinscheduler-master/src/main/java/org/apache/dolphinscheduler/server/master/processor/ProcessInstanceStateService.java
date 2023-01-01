package org.apache.dolphinscheduler.server.master.processor;

import org.apache.dolphinscheduler.plugin.task.api.enums.ExecutionStatus;
import org.apache.dolphinscheduler.remote.command.ProcessInstanceStateCommand;
import org.apache.dolphinscheduler.server.master.event.StateEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.concurrent.*;

/*
 * @Author Administrator
 * @Date 2022/12/30
 **/
@Component
public class ProcessInstanceStateService {
    private final Logger logger = LoggerFactory.getLogger(ProcessInstanceStateService.class);

    /**
     *  requestKey和request
     *
     *
     */
    private ConcurrentHashMap<String, StateEvent> queryProcessStateCache = new ConcurrentHashMap<>();
    /**
     * state event queue
     */
    private final BlockingQueue<StateEvent> eventQueue = new LinkedBlockingQueue<>();

    public void addCache(String key,StateEvent event){
        queryProcessStateCache.put(key,event);
    }

    @PostConstruct
    public void init(){
        Executors.newSingleThreadExecutor().execute(new Runnable() {

//event :StateEvent(key=null, processDefinitionCode=null, type=TASK_STATE_CHANGE, executionStatus=SUCCESS, taskInstanceId=62, taskCode=0, processInstanceId=35, context=null, channel=null)
// StateEvent(key=null, processDefinitionCode=null, type=PROCESS_STATE_CHANGE, executionStatus=SUCCESS, taskInstanceId=0, taskCode=0, processInstanceId=35, context=null, channel=null)
            @Override
            public void run() {
                while (true){
                    try {
                        if (!eventQueue.isEmpty()) {
                            StateEvent take = eventQueue.take();
                            StateEvent stateEvent = queryProcessStateCache.get("123");
                            ProcessInstanceStateCommand stateCommand = new ProcessInstanceStateCommand();
                            stateCommand.setProcessInstanceId(take.getProcessInstanceId());
                            stateCommand.setProcessDefinitionCode(take.getProcessDefinitionCode());
                            ExecutionStatus executionStatus = take.getExecutionStatus();
                            stateCommand.setExecutionStatus(executionStatus);
                            stateCommand.setId("123");
                            stateEvent.getChannel().writeAndFlush(stateCommand.convert2Command());
                            logger.info("event :{}",take.toString());
                        }
                    }catch (Exception exception){
                        exception.printStackTrace();
                    }
                }
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
        this.eventQueue.add(stateEvent);
    }
}
