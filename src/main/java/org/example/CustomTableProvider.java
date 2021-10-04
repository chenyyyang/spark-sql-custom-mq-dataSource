package org.example;

import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.Map;

public class CustomTableProvider implements TableProvider {
    @Override
    public StructType inferSchema(CaseInsensitiveStringMap caseInsensitiveStringMap) {
        StructType schema = new StructType();
        schema.add("value", DataTypes.BinaryType);
        return schema;
    }

    @Override
    public Table getTable(StructType structType, Transform[] transforms, Map<String, String> map) {
        return new CustomTable(map);
    }
}
