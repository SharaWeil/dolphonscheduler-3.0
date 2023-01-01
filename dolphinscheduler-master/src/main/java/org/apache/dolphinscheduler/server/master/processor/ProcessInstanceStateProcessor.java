package org.apache.dolphinscheduler.server.master.processor;

import com.google.common.base.Preconditions;
import io.netty.channel.Channel;
import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.apache.dolphinscheduler.remote.command.Command;
import org.apache.dolphinscheduler.remote.command.ProcessInstanceStateCommand;
import org.apache.dolphinscheduler.remote.processor.NettyRequestProcessor;
import org.apache.dolphinscheduler.server.master.event.ProcessInstanceStateEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/*
 * @Author Administrator
 * @Date 2022/12/30
 **/
@Component
public class ProcessInstanceStateProcessor implements NettyRequestProcessor {




    private final Logger logger = LoggerFactory.getLogger(ProcessInstanceStateProcessor.class);

    @Autowired
    private ProcessInstanceStateService processInstanceStateService;

    @Override
    public void process(Channel channel, Command command) {
        Preconditions.checkArgument(org.apache.dolphinscheduler.remote.command.CommandType.PROCESS_INSTANCE_STATE == command.getType(), String.format("invalid command type : %s", command.getType()));
        ProcessInstanceStateCommand processInstanceStateCommand = JSONUtils.parseObject(command.getBody(), ProcessInstanceStateCommand.class);
        logger.info("processInstanceStateCommand: {}", processInstanceStateCommand);
        Long requestId = Long.valueOf(processInstanceStateCommand.getId());
        ProcessInstanceStateCommand.CommandType commandType = processInstanceStateCommand.getCommandType();
        ProcessInstanceStateEvent stateEvent = new ProcessInstanceStateEvent();
        stateEvent.setProcessInstanceId(processInstanceStateCommand.getProcessInstanceId());
        stateEvent.setTaskInstanceId(processInstanceStateCommand.getTaskInstanceId());
        stateEvent.setChannel(channel);
        stateEvent.setCommandType(commandType);
        stateEvent.setCommandId(processInstanceStateCommand.getCommandId());
        stateEvent.setRequestId(requestId);
        stateEvent.setConsumerType(processInstanceStateCommand.getConsumerType());
        switch (commandType){
            case ADD:{
                processInstanceStateService.subscribe(stateEvent);
                break;
            }
            case DELETE:{
                processInstanceStateService.removeCache(stateEvent);
                break;
            }
            default:throw new RuntimeException("not found subscribe type,chose one or all,detail class org.apache.dolphinscheduler.remote.command.ProcessInstanceStateCommand.SubType");
        }
    }
}
