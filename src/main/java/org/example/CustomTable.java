package org.example;

import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.SupportsWrite;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class CustomTable implements Table , SupportsRead , SupportsWrite {

    private  Map<String, String> options;

    public CustomTable(Map<String, String> map) {
        this.options = map;
    }

    @Override
    public String name() {
        return "custom mq datasource";
    }

    @Override
    public StructType schema() {
        StructType schema = new StructType();
        schema.add("value", DataTypes.BinaryType);
        return schema;
    }

    @Override
    public Set<TableCapability> capabilities() {
        Set<TableCapability> capabilities = new HashSet<>();
        capabilities.add(TableCapability.MICRO_BATCH_READ);
        return capabilities;
    }

    @Override
    public ScanBuilder newScanBuilder(CaseInsensitiveStringMap ignore) {
        return () -> new CustomScan(options);
    }

    @Override
    public WriteBuilder newWriteBuilder(LogicalWriteInfo info) {
        throw new UnsupportedOperationException();
    }
}
