package org.apache.dolphinscheduler.server.master.event;


import org.apache.dolphinscheduler.remote.command.ProcessInstanceStateCommand.*;

/*
 * @Author Administrator
 * @Date 2023/1/1
 **/
public class ProcessInstanceStateEvent extends StateEvent {

    private CommandType commandType;

    private ConsumerType consumerType;

    private Integer commandId;

    private Long requestId;

    public CommandType getCommandType() {
        return commandType;
    }

    public void setCommandType(CommandType commandType) {
        this.commandType = commandType;
    }

    public Integer getCommandId() {
        return commandId;
    }

    public void setCommandId(Integer commandId) {
        this.commandId = commandId;
    }


    public Long getRequestId() {
        return requestId;
    }

    public void setRequestId(Long requestId) {
        this.requestId = requestId;
    }

    public ConsumerType getConsumerType() {
        return consumerType;
    }

    public void setConsumerType(ConsumerType consumerType) {
        this.consumerType = consumerType;
    }
}
