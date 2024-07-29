package com.linbit.linstor.core.apicallhandler.controller.db;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.dbdrivers.k8s.crd.LinstorCrd;
import com.linbit.linstor.dbdrivers.k8s.crd.LinstorSpec;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
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
        public static final String JSON_CLM_NAME = "name";
        public static final String JSON_CLM_CLM_DESCR = "column_description";
        public static final String JSON_CLM_DATA = "data";

        @JsonProperty(JSON_CLM_NAME)
        public final String name;
        @JsonProperty(JSON_CLM_CLM_DESCR)
        public final List<Column> columnDescription;
        @JsonProperty(JSON_CLM_DATA)
        public final List<LinstorSpec<?, ?>> data;

        @JsonIgnore
        public final @Nullable Class<? extends LinstorCrd<?>> crdClass;

        // no JsonCreator annotation, since we need a custom deserializer to also inject the correct crdClass
        // which cannot be properly deserialized by Jackson
        public Table(
            String nameRef,
            List<Column> columnDescriptionRef,
            List<LinstorSpec<?, ?>> dataRef,
            @Nullable Class<? extends LinstorCrd<?>> crdClassRef
        )
        {
            name = nameRef;
            columnDescription = columnDescriptionRef;
            data = dataRef;
            crdClass = crdClassRef;
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
