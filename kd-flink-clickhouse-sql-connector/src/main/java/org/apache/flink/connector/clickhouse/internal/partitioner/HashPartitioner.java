package org.apache.flink.connector.clickhouse.internal.partitioner;

import java.util.Objects;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.RowData.FieldGetter;

/** Use primary-key's hash code to partition data. */
public class HashPartitioner extends ClickHousePartitioner {

    private static final long serialVersionUID = 1L;

    public HashPartitioner(FieldGetter getter) {
        super(getter);
    }

    @Override
    public int select(RowData record, int numShards) {
        return Math.abs(Objects.hashCode(fieldGetter.getFieldOrNull(record)) % numShards);
    }
}
