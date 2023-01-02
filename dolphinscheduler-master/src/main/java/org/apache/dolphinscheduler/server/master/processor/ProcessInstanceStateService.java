package org.apache.dolphinscheduler.server.master.processor;

import io.netty.channel.Channel;
import org.apache.dolphinscheduler.dao.entity.ProcessInstance;
import org.apache.dolphinscheduler.plugin.task.api.enums.ExecutionStatus;
import org.apache.dolphinscheduler.remote.command.ProcessInstanceStateCommand;
import org.apache.dolphinscheduler.remote.utils.Pair;
import org.apache.dolphinscheduler.server.master.event.ProcessInstanceStateEvent;
import org.apache.dolphinscheduler.server.master.event.StateEvent;
import org.apache.dolphinscheduler.service.process.ProcessServiceImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.net.SocketAddress;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;

/*
 * @Author Administrator
 * @Date 2022/12/30
 **/
@Component
public class ProcessInstanceStateService {
    private final Logger logger = LoggerFactory.getLogger(ProcessInstanceStateService.class);




    @Autowired
    private ProcessServiceImpl processService;


    /**
     *  command <==> unStartProcessInstance
     */
    private final ConcurrentHashMap<Integer, ProcessInstanceStateEvent> unStartCommandMap = new ConcurrentHashMap<>();

    /**
     *  订阅列表
     */
    private final ConcurrentHashMap<SocketAddress, ProcessInstanceStateEvent> subscribeAllCache = new ConcurrentHashMap<>();

    /**
     * 订阅单个任务缓存
     */
    private final ConcurrentHashMap<ProcessInstancePair, ProcessInstanceStateEvent> subOneCache = new ConcurrentHashMap<>();

    /**
     *  检测任务是否创建
     */
    private final BlockingQueue<ProcessInstancePair> unStartProcessInstance = new LinkedBlockingQueue<>(100);

    /**
     * state event queue
     * 1、如果事件已经发送过来，但是对应的消费者尚未准备成功，事件不能被消费
     * 2、如果每一个instance实例的事件全部混在一起，就能乱了
     */
    private final ConcurrentHashMap<String, BlockingQueue<StateEvent>> eventQueue = new ConcurrentHashMap<>();



    /**
     * state event queue
     * 1、如果事件已经发送过来，但是对应的消费者尚未准备成功，事件不能被消费
     * 2、如果每一个instance实例的事件全部混在一起，就能乱了
     */
    private final BlockingQueue<StateEvent> eventQueues = new LinkedBlockingQueue<>();

    private Thread thread = null;


    private final ExecutorService executorService = Executors.newSingleThreadExecutor(r -> {
        Thread thread = new Thread(r);
        thread.setName("query processInstance thread");
        return thread;
    });



    @PostConstruct
    public void init() {
//event :StateEvent(key=null, processDefinitionCode=null, type=TASK_STATE_CHANGE, executionStatus=SUCCESS, taskInstanceId=62, taskCode=0, processInstanceId=35, context=null, channel=null)
// StateEvent(key=null, processDefinitionCode=null, type=PROCESS_STATE_CHANGE, executionStatus=SUCCESS, taskInstanceId=0, taskCode=0, processInstanceId=35, context=null, channel=null)
        Runnable runnable = new Runnable() {

            @Override
            public void run() {
                while (true) {
                    try {
                        StateEvent event = eventQueues.take();
                        int processInstanceId = event.getProcessInstanceId();
                        ExecutionStatus executionStatus = event.getExecutionStatus();
                        // 生成响应的command
                        ProcessInstanceStateCommand stateCommand = new ProcessInstanceStateCommand();
                        stateCommand.setProcessInstanceId(event.getProcessInstanceId());
                        stateCommand.setTaskInstanceId(event.getTaskInstanceId());
                        stateCommand.setProcessDefinitionCode(event.getProcessDefinitionCode());
                        stateCommand.setExecutionStatus(executionStatus);
                        stateCommand.setEventType(event.getType());
                        // 先找到单独订阅得，返送给订阅者
                        subOneCache.forEach((pair, suber) -> {
                            if (pair.getLeft() == processInstanceId) {
                                stateCommand.setId(String.valueOf(pair.requestId));
                                stateCommand.setCommandType(ProcessInstanceStateCommand.CommandType.ADD);
                                stateCommand.setCommandId(pair.getRight());
                                stateCommand.setConsumerType(suber.getConsumerType());
                                logger.info("send command:{}", stateCommand);
                                suber.getChannel().writeAndFlush(stateCommand.convert2Command());
                            }
                        });

                        Iterator<Map.Entry<SocketAddress, ProcessInstanceStateEvent>> iterator = subscribeAllCache.entrySet().iterator();
                        while (iterator.hasNext()) {
                            Map.Entry<SocketAddress, ProcessInstanceStateEvent> entry = iterator.next();
                            ProcessInstanceStateEvent elem = entry.getValue();
                            Channel channel = elem.getChannel();
                            if (!channel.isActive()) {
                                logger.info("consumerId:{} not active,remove it",entry.getKey());
                                iterator.remove();
                            } else {
                                stateCommand.setCommandType(ProcessInstanceStateCommand.CommandType.ADD);
                                stateCommand.setConsumerType(elem.getConsumerType());
                                logger.info("send command:{}", stateCommand);
                                channel.writeAndFlush(stateCommand.convert2Command());
                            }
                        }
                    } catch (Exception exception) {
                        exception.printStackTrace();
                    }

                }
            }
        };
        thread = new Thread(runnable,"processInstanceState process thread");
        thread.start();


        executorService.execute(()->{
            try {
                while (true){
                    ProcessInstancePair pair = unStartProcessInstance.take();
                    Integer commandId = pair.getRight();
                    ProcessInstance processInstance = processService.findProcessInstanceByCommandId(commandId);
                    if (null != processInstance){
                        pair.setLeft(processInstance.getId());
                        ProcessInstanceStateEvent processInstanceStateEvent = unStartCommandMap.get(commandId);
                        subOneCache.put(pair,processInstanceStateEvent);
                        unStartCommandMap.remove(commandId,processInstanceStateEvent);
                    }else {
                        // 加入队列尾部
                        unStartProcessInstance.put(pair);
                    }

                }
            }catch (Exception exception){
                exception.printStackTrace();
            }
        });
    }


    /**
     *  任务可能还没有开始调度
     * @param event
     */
    private void subOne(ProcessInstanceStateEvent event) {
        int commandId = event.getCommandId();
        ProcessInstance instance = processService.findProcessInstanceByCommandId(commandId);
        int id = null != instance ? instance.getId() : -1;
        // 如果是-1，说明processInstance还没有开始创建
        ProcessInstancePair instancePair = new ProcessInstancePair(id, commandId,event.getRequestId());
        if (id == -1){
            if (unStartProcessInstance.offer(instancePair)) {
                unStartCommandMap.put(commandId,event);
            }
        }else {
            if (event.getProcessInstanceId() == 0){
                event.setProcessInstanceId(instance.getId());
                event.setProcessDefinitionCode(instance.getProcessDefinitionCode());
            }
            subOneCache.put(instancePair,event);
        }
        thread.interrupt();
    }

    public void removeCache(ProcessInstanceStateEvent event) {

        ProcessInstanceStateEvent removeObj = null;
        int processInstanceId = event.getProcessInstanceId();
        Integer commandId = event.getCommandId();
        Long requestId = event.getRequestId();

        ProcessInstanceStateCommand.ConsumerType consumerType = event.getConsumerType();
        switch (consumerType){
            case SUBSCRIBE_ONE: {
                ProcessInstancePair pair = new ProcessInstancePair(processInstanceId, commandId, requestId);
                removeObj = subOneCache.get(pair);
                if (null != removeObj){
                    removeObj.getChannel().close();
                    subOneCache.remove(pair,removeObj);
                }
                break;
            }
            case SUBSCRIBE_ALL: {
                Channel channel = event.getChannel();
                SocketAddress key = channel.remoteAddress();
                removeObj = subscribeAllCache.get(key);
                if (null != removeObj){
                    removeObj.getChannel().close();
                    subscribeAllCache.remove(key,removeObj);
                }
                break;
            }
            default:
        }
        if (null != removeObj){
            logger.info("remove consumer from cache:【{}】 ",removeObj.toString());
        }
    }


    public void addEvents(StateEvent stateEvent) throws InterruptedException {
        String processId = String.valueOf(stateEvent.getProcessInstanceId());
        logger.info("processId:{},add event:{}",processId,stateEvent);
        this.eventQueues.add(stateEvent);
    }


    public void subscribe(ProcessInstanceStateEvent stateEvent) {
        ProcessInstanceStateCommand.ConsumerType consumerType = stateEvent.getConsumerType();

        switch (consumerType){
            case SUBSCRIBE_ONE: {
                subOne(stateEvent);
                break;
            }
            case SUBSCRIBE_ALL: {
                Channel channel = stateEvent.getChannel();
                this.subscribeAllCache.put(channel.remoteAddress(),stateEvent);
                break;
            }
            default:
        }
    }

    /**
     *  key:processInstanceId
     *  value:commandId
     */
    class ProcessInstancePair extends Pair<Integer, Integer> {

        private Long requestId;

        /**
         *  是否
         */
        private boolean isLoopQuery = false;

        public ProcessInstancePair(Integer left, Integer right, Long requestId) {
            super(left, right);
            this.requestId = requestId;
        }

        public ProcessInstancePair(Integer left, Integer right) {
            super(left, right);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ProcessInstancePair that = (ProcessInstancePair) o;
            return requestId.equals(that.getRequestId());
        }

        @Override
        public int hashCode() {
            return Objects.hash(requestId);
        }

        public boolean getIsLoopQuery() {
            return isLoopQuery;
        }

        public void setIsLoopQuery(boolean isLoopQuery) {
            this.isLoopQuery = isLoopQuery;
        }

        public Long getRequestId() {
            return requestId;
        }

        public void setRequestId(Long requestId) {
            this.requestId = requestId;
        }
    }
}
