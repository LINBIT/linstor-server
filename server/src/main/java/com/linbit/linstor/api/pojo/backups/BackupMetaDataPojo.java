package com.linbit.linstor.api.pojo.backups;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class BackupMetaDataPojo
{
    private final LayerMetaPojo layerData;
    private final RscDfnMetaPojo rscDfn;
    private final RscMetaPojo rsc;
    private final List<List<BackupInfoPojo>> backups;

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public BackupMetaDataPojo(
        @JsonProperty("layerData") LayerMetaPojo layerDataRef,
        @JsonProperty("rscDfn") RscDfnMetaPojo rscDfnRef,
        @JsonProperty("rsc") RscMetaPojo rscRef,
        @JsonProperty("backups") List<List<BackupInfoPojo>> backupsRef
    )
    {
        layerData = layerDataRef;
        rscDfn = rscDfnRef;
        rsc = rscRef;
        backups = backupsRef;
    }

    public LayerMetaPojo getLayerData()
    {
        return layerData;
    }

    public RscDfnMetaPojo getRscDfn()
    {
        return rscDfn;
    }

    public RscMetaPojo getRsc()
    {
        return rsc;
    }

    public List<List<BackupInfoPojo>> getBackups()
    {
        return backups;
    }
}
