package com.linbit.linstor.api.pojo.backups;

import com.linbit.linstor.api.interfaces.RscLayerDataApi;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class BackupMetaDataPojo
{
    private final RscLayerDataApi layerData;
    private final RscDfnMetaPojo rscDfn;
    private final RscMetaPojo rsc;
    private final LuksLayerMetaPojo luksInfo;
    private final Map<Integer, List<BackupInfoPojo>> backups;

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public BackupMetaDataPojo(
        @JsonProperty("layerData") RscLayerDataApi layerDataRef,
        @JsonProperty("rscDfn") RscDfnMetaPojo rscDfnRef,
        @JsonProperty("rsc") RscMetaPojo rscRef,
        @JsonProperty("luksInfo") LuksLayerMetaPojo luksInfoRef,
        @JsonProperty("backups") Map<Integer, List<BackupInfoPojo>> backupsRef
    )
    {
        layerData = layerDataRef;
        rscDfn = rscDfnRef;
        rsc = rscRef;
        luksInfo = luksInfoRef;
        backups = backupsRef;
    }

    public RscLayerDataApi getLayerData()
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

    public LuksLayerMetaPojo getLuksInfo()
    {
        return luksInfo;
    }

    public Map<Integer, List<BackupInfoPojo>> getBackups()
    {
        return backups;
    }
}
