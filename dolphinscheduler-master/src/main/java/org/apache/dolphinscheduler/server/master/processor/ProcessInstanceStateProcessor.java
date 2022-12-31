package org.apache.dolphinscheduler.server.master.processor;

import com.google.common.base.Preconditions;
import io.netty.channel.Channel;
import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.apache.dolphinscheduler.registry.api.Event;
import org.apache.dolphinscheduler.remote.command.Command;
import org.apache.dolphinscheduler.remote.command.CommandType;
import org.apache.dolphinscheduler.remote.command.ProcessInstanceStateCommand;
import org.apache.dolphinscheduler.remote.processor.NettyRequestProcessor;
import org.apache.dolphinscheduler.server.master.event.StateEvent;
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
    private ProcessInstanceStateResponseService processInstanceStateResponseService;

    @Override
    public void process(Channel channel, Command command) {
        Preconditions.checkArgument(CommandType.PROCESS_INSTANCE_STATE == command.getType(), String.format("invalid command type : %s", command.getType()));
        ProcessInstanceStateCommand processInstanceStateCommand = JSONUtils.parseObject(command.getBody(), ProcessInstanceStateCommand.class);
        logger.info("processInstanceStateCommand: {}", processInstanceStateCommand);
        StateEvent stateEvent = new StateEvent();
        stateEvent.setProcessInstanceId(processInstanceStateCommand.getProcessInstanceId());
        stateEvent.setTaskInstanceId(processInstanceStateCommand.getTaskInstanceId());
        stateEvent.setChannel(channel);
        //21
        String workFlowId = processInstanceStateCommand.getId();

        processInstanceStateResponseService.addCache(workFlowId, stateEvent);
    }
}
