package com.linbit.linstor.core.apicallhandler.controller.db;

import com.linbit.linstor.dbdrivers.k8s.crd.LinstorSpec;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class DbExportPojoData extends DbExportPojoMeta
{
    @JsonProperty("tables")
    public final List<Table> tables;

    @JsonCreator
    public DbExportPojoData(
        @JsonProperty("linstor_version") String linstorVersionRef,
        @JsonProperty("export_timestamp_ms") long exportTimestampRef,
        @JsonProperty("exported_from") String exportedByRef,
        @JsonProperty("data_version") String genCrdVersionRef,
        @JsonProperty("sql_version") String sqlVerionRef,
        @JsonProperty("etcd_version") int etcdVersionRef,
        @JsonProperty("k8s_version") int k8sVersionRef,
        @JsonProperty("tables") List<Table> tablesRef
    )
    {
        super(
            linstorVersionRef,
            exportTimestampRef,
            exportedByRef,
            genCrdVersionRef,
            sqlVerionRef,
            etcdVersionRef,
            k8sVersionRef
        );
        tables = tablesRef;
    }

    public static class Table
    {
        @JsonProperty("name")
        public final String name;
        @JsonProperty("column_description")
        public final List<Column> columnDescription;
        @JsonProperty("data")
        public final List<LinstorSpec> data;

        @JsonCreator
        public Table(
            @JsonProperty("name") String nameRef,
            @JsonProperty("column_description") List<Column> columnDescriptionRef,
            @JsonProperty("data") List<LinstorSpec> dataRef
        )
        {
            super();
            name = nameRef;
            columnDescription = columnDescriptionRef;
            data = dataRef;
        }
    }

    public static class Column
    {
        @JsonProperty("name")
        public final String name;
        @JsonProperty("sql_type")
        public final int sqlType;
        @JsonProperty("is_primary_key")
        public final boolean isPrimaryKey;
        @JsonProperty("is_nullable")
        public final boolean isNullable;

        @JsonCreator
        public Column(
            @JsonProperty("name") String nameRef,
            @JsonProperty("sql_type") int sqlTypeRef,
            @JsonProperty("is_primary_key") boolean isPrimaryKeyRef,
            @JsonProperty("is_nullable") boolean isNullableRef
        )
        {
            name = nameRef;
            sqlType = sqlTypeRef;
            isPrimaryKey = isPrimaryKeyRef;
            isNullable = isNullableRef;
        }
    }
}
