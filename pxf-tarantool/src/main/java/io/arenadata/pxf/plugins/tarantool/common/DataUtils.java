package io.arenadata.pxf.plugins.tarantool.common;

import org.greenplum.pxf.api.io.DataType;

public class DataUtils {
    private DataUtils() {
    }

    public static Object mapAndValidate(int type, Object value) {
        DataType dataType = DataType.get(type);
        switch (dataType) {
            case BOOLEAN:
            case BIGINT:
            case INTEGER:
            case REAL:
            case FLOAT8:
            case TEXT:
            case VARCHAR:
                return value;
            default:
                throw new IllegalArgumentException("DataType not supported: " + dataType.name());
        }
    }
}
