package com.linbit.linstor.api.pojo.backups;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.interfaces.RscLayerDataApi;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class BackupMetaDataPojo
{
    private final String rscName;
    private final String nodeName;
    private final long startTimestamp;
    private final long finishTimestamp;
    private final @Nullable String basedOn;

    private final RscLayerDataApi layerData;
    private final RscDfnMetaPojo rscDfn;
    private final RscMetaPojo rsc;
    private final @Nullable LuksLayerMetaPojo luksInfo;
    private final Map<Integer, List<BackupMetaInfoPojo>> backups; // vlmNr -> List<backupInfo>

    private final String clusterId;
    private final String snapDfnUuid;

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public BackupMetaDataPojo(
        @JsonProperty("rscName") String rscNameRef,
        @JsonProperty("nodeName") String nodeNameRef,
        @JsonProperty("startTimestamp") long startTimestampRef,
        @JsonProperty("finishTimestamp") long finishTimestampRef,
        @JsonProperty("basedOn") @Nullable String basedOnRef,
        @JsonProperty("layerData") RscLayerDataApi layerDataRef,
        @JsonProperty("rscDfn") RscDfnMetaPojo rscDfnRef,
        @JsonProperty("rsc") RscMetaPojo rscRef,
        @JsonProperty("luksInfo") @Nullable LuksLayerMetaPojo luksInfoRef,
        @JsonProperty("backups") Map<Integer, List<BackupMetaInfoPojo>> backupsRef,
        @JsonProperty("clusterId") String clusterIdRef,
        @JsonProperty("snapDfnUuid") String snapDfnUuidRef
    )
    {
        rscName = rscNameRef;
        nodeName = nodeNameRef;
        startTimestamp = startTimestampRef;
        finishTimestamp = finishTimestampRef;
        basedOn = basedOnRef;
        layerData = layerDataRef;
        rscDfn = rscDfnRef;
        rsc = rscRef;
        luksInfo = luksInfoRef;
        backups = backupsRef;
        clusterId = clusterIdRef;
        snapDfnUuid = snapDfnUuidRef;
    }

    public String getNodeName()
    {
        return nodeName;
    }

    public String getRscName()
    {
        return rscName;
    }

    public long getStartTimestamp()
    {
        return startTimestamp;
    }

    public long getFinishTimestamp()
    {
        return finishTimestamp;
    }

    public @Nullable String getBasedOn()
    {
        return basedOn;
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

    public @Nullable LuksLayerMetaPojo getLuksInfo()
    {
        return luksInfo;
    }

    public Map<Integer, List<BackupMetaInfoPojo>> getBackups()
    {
        return backups;
    }

    public String getClusterId()
    {
        return clusterId;
    }

    public String getSnapDfnUuid()
    {
        return snapDfnUuid;
    }
}
