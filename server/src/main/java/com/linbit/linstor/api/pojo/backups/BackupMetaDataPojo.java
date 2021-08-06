package com.linbit.linstor.api.pojo.backups;

import com.linbit.linstor.api.interfaces.RscLayerDataApi;

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
    private final String basedOn;

    private final RscLayerDataApi layerData;
    private final RscDfnMetaPojo rscDfn;
    private final RscMetaPojo rsc;
    private final LuksLayerMetaPojo luksInfo;
    private final Map<Integer, BackupInfoPojo> backups; // vlmNr -> backupInfo

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public BackupMetaDataPojo(
        @JsonProperty("rscName") String rscNameRef,
        @JsonProperty("nodeName") String nodeNameRef,
        @JsonProperty("startTimestamp") long startTimestampRef,
        @JsonProperty("finishTimestamp") long finishTimestampRef,
        @JsonProperty("basedOn") String basedOnRef,
        @JsonProperty("layerData") RscLayerDataApi layerDataRef,
        @JsonProperty("rscDfn") RscDfnMetaPojo rscDfnRef,
        @JsonProperty("rsc") RscMetaPojo rscRef,
        @JsonProperty("luksInfo") LuksLayerMetaPojo luksInfoRef,
        @JsonProperty("backups") Map<Integer, BackupInfoPojo> backupsRef
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

    public String getBasedOn()
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

    public LuksLayerMetaPojo getLuksInfo()
    {
        return luksInfo;
    }

    public Map<Integer, BackupInfoPojo> getBackups()
    {
        return backups;
    }
}
