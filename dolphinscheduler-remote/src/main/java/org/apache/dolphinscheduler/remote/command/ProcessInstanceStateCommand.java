package org.apache.dolphinscheduler.remote.command;

import org.apache.dolphinscheduler.common.enums.StateEventType;
import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.apache.dolphinscheduler.plugin.task.api.enums.ExecutionStatus;

/**
 * @author ysear
 * @date 2022/12/31
 */
public class ProcessInstanceStateCommand {

    /**
     *  全局唯一ID
     */
    private String id;
    private String projectCode;
    private Long processDefinitionCode;

    private ExecutionStatus executionStatus;

    private StateEventType eventType;

    private int taskInstanceId;
    private int processInstanceId;

    private int commandId;

    /**
     *  订阅所有的信息，还是单独的一个请求
     *
     */
    private CommandType commandType;


    private ConsumerType consumerType;

    public Command convert2Command() {
        Command command = new Command();
        command.setType(org.apache.dolphinscheduler.remote.command.CommandType.PROCESS_INSTANCE_STATE);
        byte[] body = JSONUtils.toJsonByteArray(this);
        command.setBody(body);
        return command;
    }
    public Command convert2Command(org.apache.dolphinscheduler.remote.command.CommandType commandType) {
        Command command = new Command();
        command.setType(commandType);
        byte[] body = JSONUtils.toJsonByteArray(this);
        command.setBody(body);
        return command;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getProjectCode() {
        return projectCode;
    }

    public void setProjectCode(String projectCode) {
        this.projectCode = projectCode;
    }

    public Long getProcessDefinitionCode() {
        return processDefinitionCode;
    }

    public void setProcessDefinitionCode(Long processDefinitionCode) {
        this.processDefinitionCode = processDefinitionCode;
    }

    public int getTaskInstanceId() {
        return taskInstanceId;
    }

    public void setTaskInstanceId(int taskInstanceId) {
        this.taskInstanceId = taskInstanceId;
    }

    public int getProcessInstanceId() {
        return processInstanceId;
    }

    public void setProcessInstanceId(int processInstanceId) {
        this.processInstanceId = processInstanceId;
    }

    public ExecutionStatus getExecutionStatus() {
        return executionStatus;
    }

    public void setExecutionStatus(ExecutionStatus executionStatus) {
        this.executionStatus = executionStatus;
    }

    public CommandType getCommandType() {
        return commandType;
    }

    public void setCommandType(CommandType commandType) {
        this.commandType = commandType;
    }


    public int getCommandId() {
        return commandId;
    }

    public void setCommandId(int commandId) {
        this.commandId = commandId;
    }


    public StateEventType getEventType() {
        return eventType;
    }

    public void setEventType(StateEventType eventType) {
        this.eventType = eventType;
    }


    public ConsumerType getConsumerType() {
        return consumerType;
    }

    public void setConsumerType(ConsumerType consumerType) {
        this.consumerType = consumerType;
    }

    @Override
    public String toString() {
        return "ProcessInstanceStateCommand{" +
                "id='" + id + '\'' +
                ", projectCode='" + projectCode + '\'' +
                ", processDefinitionCode=" + processDefinitionCode +
                ", executionStatus=" + executionStatus +
                ", eventType=" + eventType +
                ", taskInstanceId=" + taskInstanceId +
                ", processInstanceId=" + processInstanceId +
                ", commandId=" + commandId +
                ", commandType=" + commandType +
                ", consumerType=" + consumerType +
                '}';
    }

    public enum CommandType {
        DELETE,
        ADD
    }

    public enum ConsumerType{
        SUBSCRIBE_ONE,
        SUBSCRIBE_ALL;
    }
}
