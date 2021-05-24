package io.arenadata.pxf.plugins.tarantool.upsert;

import io.arenadata.pxf.plugins.tarantool.common.DataUtils;
import org.greenplum.pxf.api.OneField;
import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.model.BasePlugin;
import org.greenplum.pxf.api.model.Resolver;

import java.util.List;
import java.util.stream.Collectors;

public class TarantoolResolver extends BasePlugin implements Resolver {
    @Override
    public List<OneField> getFields(OneRow oneRow) throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    public OneRow setFields(List<OneField> list) throws Exception {
        List<?> fields = list.stream()
                .map(oneField -> DataUtils.mapAndValidate(oneField.type, oneField.val))
                .collect(Collectors.toList());
        return new OneRow(fields);
    }
}
