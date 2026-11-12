package nju.jgd.planact;

import java.io.Serial;
import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;

/** One tool invocation in a Plan-Act execution plan. */
public final class PlanActStep implements Serializable {

    @Serial private static final long serialVersionUID = 1L;

    private String id;
    private String tool;
    private Map<String, Object> arguments = new LinkedHashMap<>();
    private String purpose = "";

    public PlanActStep() {}

    public PlanActStep(String id, String tool, Map<String, Object> arguments, String purpose) {
        this.id = id;
        this.tool = tool;
        this.arguments = arguments == null ? new LinkedHashMap<>() : new LinkedHashMap<>(arguments);
        this.purpose = purpose == null ? "" : purpose;
    }

    public void validate() {
        if (id == null || id.isBlank()) {
            throw new PlanActException("Plan step id must not be blank");
        }
        if (tool == null || tool.isBlank()) {
            throw new PlanActException("Plan step tool must not be blank");
        }
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getTool() {
        return tool;
    }

    public void setTool(String tool) {
        this.tool = tool;
    }

    public Map<String, Object> getArguments() {
        if (arguments == null) {
            arguments = new LinkedHashMap<>();
        }
        return arguments;
    }

    public void setArguments(Map<String, Object> arguments) {
        this.arguments = arguments == null ? new LinkedHashMap<>() : new LinkedHashMap<>(arguments);
    }

    public String getPurpose() {
        return purpose == null ? "" : purpose;
    }

    public void setPurpose(String purpose) {
        this.purpose = purpose == null ? "" : purpose;
    }
}