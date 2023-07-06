package com.linbit.linstor.core.apicallhandler.controller.db;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class DbExportPojoMeta
{
    @JsonProperty("linstor_version")
    public final String linstorVersion;
    @JsonProperty("export_timestamp_ms")
    public final long exportTimestamp;
    @JsonProperty("exported_from")
    public final String exportedBy;
    @JsonProperty("data_version")
    public final String genCrdVersion;
    @JsonProperty("sql_version")
    public final String sqlVersion;
    @JsonProperty("etcd_version")
    public final int etcdVersion;
    @JsonProperty("k8s_version")
    public final int k8sVersion;

    @JsonCreator
    public DbExportPojoMeta(
        @JsonProperty("linstor_version") String linstorVersionRef,
        @JsonProperty("export_timestamp_ms") long exportTimestampRef,
        @JsonProperty("exported_from") String exportedByRef,
        @JsonProperty("data_version") String genCrdVersionRef,
        @JsonProperty("sql_version") String sqlVerionRef,
        @JsonProperty("etcd_version") int etcdVersionRef,
        @JsonProperty("k8s_version") int k8sVersionRef
    )
    {
        linstorVersion = linstorVersionRef;
        exportTimestamp = exportTimestampRef;
        exportedBy = exportedByRef;
        genCrdVersion = genCrdVersionRef;
        sqlVersion = sqlVerionRef;
        etcdVersion = etcdVersionRef;
        k8sVersion = k8sVersionRef;
    }
}

