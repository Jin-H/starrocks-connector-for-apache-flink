package org.apache.flink.connector.clickhouse.internal.converter;

import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;

import com.clickhouse.data.value.UnsignedByte;
import com.clickhouse.data.value.UnsignedInteger;
import com.clickhouse.data.value.UnsignedLong;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.connector.clickhouse.util.ClickHouseUtil.toEpochDayOneTimestamp;

/** convert between internal and external data types. */
public class ClickHouseConverterUtils {

    public static final int BOOL_TRUE = 1;

    public static Object toExternal(Object value, LogicalType type) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case INTERVAL_YEAR_MONTH:
            case BIGINT:
            case INTERVAL_DAY_TIME:
            case FLOAT:
            case DOUBLE:
            case BINARY:
            case VARBINARY:
                return value;
            case CHAR:
            case VARCHAR:
                return value.toString();
            case DATE:
                return LocalDate.ofEpochDay((Integer) value);
            case TIME_WITHOUT_TIME_ZONE:
                LocalTime localTime = LocalTime.ofNanoOfDay(((Integer) value) * 1_000_000L);
                return toEpochDayOneTimestamp(localTime);
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return ((TimestampData) value).toTimestamp();
            case DECIMAL:
                return ((DecimalData) value).toBigDecimal();
            case ARRAY:
                LogicalType elementType =
                        ((ArrayType) type)
                                .getChildren().stream()
                                        .findFirst()
                                        .orElseThrow(
                                                () ->
                                                        new RuntimeException(
                                                                "Unknown array element type"));
                ArrayData.ElementGetter elementGetter = ArrayData.createElementGetter(elementType);
                ArrayData arrayData = ((ArrayData) value);
                Object[] objectArray = new Object[arrayData.size()];
                for (int i = 0; i < arrayData.size(); i++) {
                    objectArray[i] =
                            toExternal(elementGetter.getElementOrNull(arrayData, i), elementType);
                }
                return objectArray;
            case MAP:
                LogicalType keyType = ((MapType) type).getKeyType();
                LogicalType valueType = ((MapType) type).getValueType();
                ArrayData.ElementGetter keyGetter = ArrayData.createElementGetter(keyType);
                ArrayData.ElementGetter valueGetter = ArrayData.createElementGetter(valueType);
                MapData mapData = (MapData) value;
                ArrayData keyArrayData = mapData.keyArray();
                ArrayData valueArrayData = mapData.valueArray();
                Map<Object, Object> objectMap = new HashMap<>(keyArrayData.size());
                for (int i = 0; i < keyArrayData.size(); i++) {
                    objectMap.put(
                            toExternal(keyGetter.getElementOrNull(keyArrayData, i), keyType),
                            toExternal(valueGetter.getElementOrNull(valueArrayData, i), valueType));
                }
                return objectMap;
            case MULTISET:
            case ROW:
            case RAW:
            default:
                throw new UnsupportedOperationException("Unsupported type:" + type);
        }
    }

    public static Object toInternal(Object value, LogicalType type) {
        switch (type.getTypeRoot()) {
            case NULL:
                return null;
            case BOOLEAN:
                return BOOL_TRUE == ((Number) value).intValue();
            case FLOAT:
            case DOUBLE:
            case INTERVAL_YEAR_MONTH:
            case INTERVAL_DAY_TIME:
            case INTEGER:
            case BIGINT:
            case BINARY:
            case VARBINARY:
                if (value instanceof UnsignedInteger) {
                    return ((UnsignedInteger) value).longValue();
                } else if (value instanceof UnsignedLong) {
                    return ((UnsignedLong) value).longValue();
                } else {
                    return value;
                }
            case TINYINT:
                return ((UnsignedByte) value).byteValue();
            case SMALLINT:
                return ((UnsignedByte) value).shortValue();
            case DECIMAL:
                final int precision = ((DecimalType) type).getPrecision();
                final int scale = ((DecimalType) type).getScale();
                return DecimalData.fromUnscaledLong(
                        ((UnsignedLong) value).longValue(), precision, scale);
            case DATE:
                return (int) (((LocalDate) value).toEpochDay());
            case TIME_WITHOUT_TIME_ZONE:
                return (int) (((LocalTime) value).toNanoOfDay() / 1_000_000L);
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return TimestampData.fromLocalDateTime((LocalDateTime) value);
            case CHAR:
            case VARCHAR:
                return StringData.fromString((String) value);
            case ARRAY:
                LogicalType elementType =
                        type.getChildren().stream()
                                .findFirst()
                                .orElseThrow(
                                        () -> new RuntimeException("Unknown array element type"));
                Object[] array = (Object[]) value;
                int externalArrayLength = array.length;
                Object[] internalArray = new Object[externalArrayLength];
                for (int i = 0; i < externalArrayLength; i++) {
                    internalArray[i] = toInternal(array[i], elementType);
                }
                return new GenericArrayData(internalArray);
            case MAP:
                LogicalType keyType = ((MapType) type).getKeyType();
                LogicalType valueType = ((MapType) type).getValueType();
                Map<?, ?> externalMap = (Map<?, ?>) value;
                Map<Object, Object> internalMap = new HashMap<>(externalMap.size());
                for (Map.Entry<?, ?> entry : externalMap.entrySet()) {
                    internalMap.put(
                            toInternal(entry.getKey(), keyType),
                            toInternal(entry.getValue(), valueType));
                }
                return new GenericMapData(internalMap);
            case ROW:
            case MULTISET:
            case RAW:
            default:
                throw new UnsupportedOperationException("Unsupported type:" + type);
        }
    }
}
