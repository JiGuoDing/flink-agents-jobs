package nju.jgd.planact;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.flink.agents.api.annotation.ToolParam;
import org.apache.flink.agents.api.tools.FunctionTool;
import org.apache.flink.agents.api.tools.Tool;
import org.apache.flink.agents.api.tools.ToolMetadata;
import org.apache.flink.agents.api.tools.ToolParameters;
import org.apache.flink.agents.api.tools.ToolResponse;
import org.apache.flink.agents.api.tools.ToolType;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Parameter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Executable static-method tool used by {@link PlanActAgent}.
 *
 * <p>Flink Agents 0.4.0 separates the API-level {@code FunctionTool} descriptor from the runtime
 * callable tool. Plan-Act needs a callable tool because the executor invokes plan steps directly,
 * so this adapter keeps the method reference and exposes a small JSON schema for the planner.
 */
public final class PlanActTool extends Tool {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final FunctionTool functionTool;
    private final String declaringClassName;
    private final String methodName;
    private final List<String> parameterNames;
    private final List<String> parameterTypes;

    private PlanActTool(
            ToolMetadata metadata,
            FunctionTool functionTool,
            String declaringClassName,
            String methodName,
            List<String> parameterNames,
            List<String> parameterTypes) {
        super(metadata);
        this.functionTool = functionTool;
        this.declaringClassName = declaringClassName;
        this.methodName = methodName;
        this.parameterNames = List.copyOf(parameterNames);
        this.parameterTypes = List.copyOf(parameterTypes);
    }

    public static PlanActTool fromStaticMethod(Method method) {
        if (method == null) {
            throw new PlanActException("Tool method must not be null");
        }
        if (!Modifier.isStatic(method.getModifiers())) {
            throw new PlanActException("PlanActTool methods must be static: " + method);
        }
        String name = method.getName();
        if (name == null || name.isBlank()) {
            throw new PlanActException("Tool method must have a name");
        }

        List<String> names = new ArrayList<>();
        List<String> types = new ArrayList<>();
        Map<String, Object> properties = new LinkedHashMap<>();
        List<String> required = new ArrayList<>();
        Parameter[] parameters = method.getParameters();
        for (Parameter parameter : parameters) {
            ToolParam annotation = parameter.getAnnotation(ToolParam.class);
            String parameterName =
                    annotation != null && !annotation.name().isBlank()
                            ? annotation.name()
                            : parameter.getName();
            if (parameterName == null || parameterName.isBlank()) {
                throw new PlanActException(
                        "Tool parameter name is unavailable; compile with -parameters: " + method);
            }
            String description = annotation == null ? "" : annotation.description();
            Map<String, Object> property = new LinkedHashMap<>();
            property.put("type", jsonType(parameter.getType()));
            if (!description.isBlank()) {
                property.put("description", description);
            }
            properties.put(parameterName, property);
            if (annotation == null || annotation.required()) {
                required.add(parameterName);
            }
            names.add(parameterName);
            types.add(parameter.getType().getName());
        }

        Map<String, Object> schema = new LinkedHashMap<>();
        schema.put("type", "object");
        schema.put("properties", properties);
        if (!required.isEmpty()) {
            schema.put("required", required);
        }
        String inputSchema;
        try {
            inputSchema = MAPPER.writeValueAsString(schema);
        } catch (JsonProcessingException exception) {
            throw new PlanActException("Unable to create tool schema for " + method, exception);
        }

        org.apache.flink.agents.api.annotation.Tool toolAnnotation =
                method.getAnnotation(org.apache.flink.agents.api.annotation.Tool.class);
        String description =
                toolAnnotation == null || toolAnnotation.description().isBlank()
                        ? "Tool " + name
                        : toolAnnotation.description();
        ToolMetadata metadata = new ToolMetadata(name, description, inputSchema);
        return new PlanActTool(
                metadata,
                FunctionTool.fromMethod(method),
                method.getDeclaringClass().getName(),
                name,
                names,
                types);
    }

    /** API-level descriptor registered with Flink Agents for runtime compilation. */
    public FunctionTool functionTool() {
        return functionTool;
    }

    @Override
    public ToolType getToolType() {
        return ToolType.FUNCTION;
    }

    @Override
    public ToolResponse call(ToolParameters parameters) {
        long startedAt = System.currentTimeMillis();
        try {
            Class<?> declaringClass = Class.forName(declaringClassName);
            Class<?>[] types = new Class<?>[parameterTypes.size()];
            for (int index = 0; index < parameterTypes.size(); index++) {
                types[index] = resolveType(parameterTypes.get(index));
            }
            Method method = declaringClass.getMethod(methodName, types);
            Object[] arguments = new Object[parameterNames.size()];
            for (int index = 0; index < parameterNames.size(); index++) {
                String parameterName = parameterNames.get(index);
                if (!parameters.hasParameter(parameterName)) {
                    return ToolResponse.error(
                            "Missing required tool parameter '" + parameterName + "'",
                            Math.max(0L, System.currentTimeMillis() - startedAt),
                            getName());
                }
                arguments[index] = parameters.getParameter(parameterName, boxed(types[index]));
            }
            Object result = method.invoke(null, arguments);
            String text = result == null ? "" : String.valueOf(result);
            return ToolResponse.text(
                    text, Math.max(0L, System.currentTimeMillis() - startedAt), getName());
        } catch (ReflectiveOperationException | RuntimeException exception) {
            return ToolResponse.error(
                    exception,
                    Math.max(0L, System.currentTimeMillis() - startedAt));
        }
    }

    private static Class<?> boxed(Class<?> type) {
        if (!type.isPrimitive()) {
            return type;
        }
        if (type == boolean.class) {
            return Boolean.class;
        }
        if (type == byte.class) {
            return Byte.class;
        }
        if (type == char.class) {
            return Character.class;
        }
        if (type == short.class) {
            return Short.class;
        }
        if (type == int.class) {
            return Integer.class;
        }
        if (type == long.class) {
            return Long.class;
        }
        if (type == float.class) {
            return Float.class;
        }
        if (type == double.class) {
            return Double.class;
        }
        return type;
    }

    private static Class<?> resolveType(String typeName) throws ClassNotFoundException {
        return switch (typeName) {
            case "boolean" -> boolean.class;
            case "byte" -> byte.class;
            case "char" -> char.class;
            case "short" -> short.class;
            case "int" -> int.class;
            case "long" -> long.class;
            case "float" -> float.class;
            case "double" -> double.class;
            default -> Class.forName(typeName);
        };
    }

    private static String jsonType(Class<?> type) {
        if (type == String.class || CharSequence.class.isAssignableFrom(type) || type == char.class
                || type == Character.class) {
            return "string";
        }
        if (type == int.class || type == Integer.class || type == long.class || type == Long.class
                || type == short.class || type == Short.class || type == byte.class
                || type == Byte.class) {
            return "integer";
        }
        if (type == double.class || type == Double.class || type == float.class
                || type == Float.class) {
            return "number";
        }
        if (type == boolean.class || type == Boolean.class) {
            return "boolean";
        }
        if (type.isArray() || Iterable.class.isAssignableFrom(type)) {
            return "array";
        }
        return "object";
    }
}