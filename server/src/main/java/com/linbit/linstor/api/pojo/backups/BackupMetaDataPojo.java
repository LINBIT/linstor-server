package com.linbit.linstor.api.pojo.backups;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class BackupMetaDataPojo
{
    private final LayerMetaPojo layerData;
    private final RscDfnMetaPojo rscDfn;
    private final RscMetaPojo rsc;
    private final List<List<String>> backups;

    public BackupMetaDataPojo(
        LayerMetaPojo layerDataRef,
        RscDfnMetaPojo rscDfnRef,
        RscMetaPojo rscRef,
        List<List<String>> backupsRef
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

    public List<List<String>> getBackups()
    {
        return backups;
    }
}
