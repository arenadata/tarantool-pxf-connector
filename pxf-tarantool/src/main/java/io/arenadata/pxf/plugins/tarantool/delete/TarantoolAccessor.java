package io.arenadata.pxf.plugins.tarantool.delete;

import io.arenadata.pxf.plugins.tarantool.common.TarantoolAccessorBase;
import io.tarantool.driver.api.conditions.Conditions;
import io.tarantool.driver.metadata.TarantoolIndexMetadata;
import io.tarantool.driver.metadata.TarantoolIndexPartMetadata;
import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.model.Accessor;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class TarantoolAccessor extends TarantoolAccessorBase implements Accessor {
    private static final int TARANTOOL_PRIMARY_INDEX = 0; // default in tarantool

    @Override
    public boolean openForWrite() throws Exception {
        super.openForWrite();

        Optional<TarantoolIndexMetadata> primaryIndexOptional = connection.getClient().metadata().getIndexById(spaceName, TARANTOOL_PRIMARY_INDEX);
        if (!primaryIndexOptional.isPresent()) {
            throw new IllegalArgumentException(String.format("Space: %s has no primary index", spaceName));
        }

        TarantoolIndexMetadata primaryIndex = primaryIndexOptional.get();
        List<TarantoolIndexPartMetadata> indexParts = primaryIndex.getIndexParts();

        if (indexParts.size() != context.getColumns()) {
            String externalTableColumns = IntStream.range(0, context.getColumns())
                    .mapToObj(i -> context.getColumn(i).columnName())
                    .collect(Collectors.joining(",", "[", "]"));
            String tarantoolIndex = indexParts.stream()
                    .map(TarantoolIndexPartMetadata::getPath)
                    .collect(Collectors.joining(",", "[", "]"));

            throw new IllegalArgumentException(String.format("Columns don't match tarantool primary key columns: %s, got: %s",
                    tarantoolIndex, externalTableColumns));
        }

        for (int i = 0; i < context.getColumns(); i++) {
            ColumnDescriptor externalTableColumn = context.getColumn(i);
            TarantoolIndexPartMetadata indexColumn = indexParts.get(i);
            if (!externalTableColumn.columnName().equals(indexColumn.getPath())) {
                throw new IllegalArgumentException(String.format("Column %d (%s) not equal to tarantool primary index column with order, expected: %s",
                        i, externalTableColumn.columnName(), indexColumn.getPath()));
            }
        }

        return true;
    }

    @Override
    public boolean writeNextObject(OneRow oneRow) throws Exception {
        try {
            List<?> fields = (List<?>) oneRow.getData();
            Conditions condition = Conditions.indexEquals(TARANTOOL_PRIMARY_INDEX, fields);
            return futures.add(spaceOperations.delete(condition));
        } catch (Exception e) {
            LOG.error("Exception during request", e);
            return false;
        }
    }
}
