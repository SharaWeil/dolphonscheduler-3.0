package org.apache.dolphinscheduler.remote.command;

import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.apache.dolphinscheduler.plugin.task.api.enums.ExecutionStatus;

/**
 * @author ysear
 * @date 2022/12/31
 */
public class ProcessInstanceStateCommand {

    private String id;
    private String projectCode;
    private Long processDefinitionCode;

    private ExecutionStatus executionStatus;

    private int taskInstanceId;
    private int processInstanceId;

    public Command convert2Command() {
        Command command = new Command();
        command.setType(CommandType.PROCESS_INSTANCE_STATE);
        byte[] body = JSONUtils.toJsonByteArray(this);
        command.setBody(body);
        return command;
    }
    public Command convert2Command(CommandType commandType) {
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

    @Override
    public String toString() {
        return "ProcessInstanceStateCommand{" +
                "id='" + id + '\'' +
                ", projectCode='" + projectCode + '\'' +
                ", processDefinitionCode=" + processDefinitionCode +
                ", executionStatus=" + executionStatus +
                ", taskInstanceId=" + taskInstanceId +
                ", processInstanceId=" + processInstanceId +
                '}';
    }
}
