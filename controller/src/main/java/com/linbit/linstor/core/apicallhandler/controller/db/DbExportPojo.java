package com.linbit.linstor.core.apicallhandler.controller.db;

import com.linbit.linstor.dbdrivers.k8s.crd.LinstorSpec;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class DbExportPojo
{
    @JsonProperty("linstor_version")
    public final String linstorVersion;
    @JsonProperty("export_timestamp")
    public final long exportTimestamp;
    @JsonProperty("exported_from")
    public final String exportedBy;
    @JsonProperty("sql_version")
    public final String sqlVersion;
    @JsonProperty("etcd_version")
    public final int etcdVersion;
    @JsonProperty("k8s_version")
    public final int k8sVersion;

    @JsonProperty("tables")
    public final Map<String, Table> tables;

    @JsonCreator
    public DbExportPojo(
        @JsonProperty("linstor_version") String linstorVersionRef,
        @JsonProperty("export_timestamp") long exportTimestampRef,
        @JsonProperty("exported_from") String exportedByRef,
        @JsonProperty("sql_version") String sqlVerionRef,
        @JsonProperty("etcd_version") int etcdVersionRef,
        @JsonProperty("k8s_version") int k8sVersionRef,
        @JsonProperty("tables") Map<String, Table> tablesRef
    )
    {
        linstorVersion = linstorVersionRef;
        exportTimestamp = exportTimestampRef;
        exportedBy = exportedByRef;
        sqlVersion = sqlVerionRef;
        etcdVersion = etcdVersionRef;
        k8sVersion = k8sVersionRef;
        tables = tablesRef;
    }

    public static class Table
    {
        @JsonProperty("data")
        public final List<LinstorSpec> data;

        @JsonCreator
        public Table(
            @JsonProperty("data") List<LinstorSpec> dataRef
        )
        {
            super();
            data = dataRef;
        }
    }
}
