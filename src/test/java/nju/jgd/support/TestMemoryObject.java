package nju.jgd.support;

import org.apache.flink.agents.api.context.MemoryObject;
import org.apache.flink.agents.api.context.MemoryRef;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Minimal flat-path memory used to test the logical cache and history helpers. */
final class TestMemoryObject implements MemoryObject {

    private final Map<String, Object> values = new LinkedHashMap<>();

    @Override
    public MemoryObject get(String path) {
        if (!values.containsKey(path)) {
            throw new IllegalArgumentException("Missing path: " + path);
        }
        Object value = values.get(path);
        return new MemoryObject() {
            @Override
            public MemoryObject get(String nestedPath) {
                throw new UnsupportedOperationException();
            }

            @Override
            public MemoryObject get(MemoryRef ref) {
                return null;
            }

            @Override
            public MemoryRef set(String nestedPath, Object nestedValue) {
                throw new UnsupportedOperationException();
            }

            @Override
            public MemoryObject newObject(String nestedPath, boolean overwrite) {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean isExist(String nestedPath) {
                return false;
            }

            @Override
            public List<String> getFieldNames() {
                return List.of();
            }

            @Override
            public Map<String, Object> getFields() {
                return Map.of();
            }

            @Override
            public Object getValue() {
                return value;
            }

            @Override
            public boolean isNestedObject() {
                return false;
            }
        };
    }

    @Override
    public MemoryObject get(MemoryRef ref) {
        return null;
    }

    @Override
    public MemoryRef set(String path, Object value) {
        values.put(path, value);
        return null;
    }

    @Override
    public MemoryObject newObject(String path, boolean overwrite) {
        return this;
    }

    @Override
    public boolean isExist(String path) {
        return values.containsKey(path);
    }

    @Override
    public List<String> getFieldNames() {
        return new ArrayList<>(values.keySet());
    }

    @Override
    public Map<String, Object> getFields() {
        return Map.copyOf(values);
    }

    @Override
    public Object getValue() {
        return values;
    }

    @Override
    public boolean isNestedObject() {
        return true;
    }
}
