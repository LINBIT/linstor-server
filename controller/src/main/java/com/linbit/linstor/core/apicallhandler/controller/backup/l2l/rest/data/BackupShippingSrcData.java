package com.linbit.linstor.core.apicallhandler.controller.backup.l2l.rest.data;

import com.linbit.linstor.api.pojo.backups.BackupMetaDataPojo;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.remotes.LinstorRemote;
import com.linbit.linstor.core.objects.remotes.StltRemote;

import javax.annotation.Nullable;

import java.time.LocalDateTime;
import java.util.Map;

public class BackupShippingSrcData
{
    private final String srcClusterId;
    private final String srcRscName;
    private final String srcBackupName;
    private final LocalDateTime now;
    private final String dstRscName;
    private final LinstorRemote linstorRemote;
    private final Map<String, String> storPoolRename;

    private Snapshot srcSnapshot;
    private String srcNodeName;
    private BackupMetaDataPojo metaDataPojo;
    private String dstNodeName;
    private String dstNetIfName;
    private String dstStorPool;
    private @Nullable String dstRscGrp;
    private String scheduleName;

    private StltRemote stltRemote;
    private String dstBaseSnapName;
    private String dstActualNodeName;
    private boolean resetData;
    private boolean useZstd;
    private boolean downloadOnly;
    private boolean forceRestore;
    private boolean allowIncremental;
    private boolean forceRscGrp;

    public BackupShippingSrcData(
        String srcClusterIdRef,
        String srcNodeNameRef,
        String srcRscNameRef,
        String srcBackupNameRef,
        LocalDateTime nowRef,
        LinstorRemote linstorRemoteRef,
        String dstRscNameRef,
        String dstNodeNameRef,
        String dstNetIfNameRef,
        String dstStorPoolRef,
        Map<String, String> storPoolRenameRef,
        @Nullable String dstRscGrpRef,
        boolean downloadOnlyRef,
        boolean forceRestoreRef,
        String scheduleNameRef,
        boolean allowIncrementalRef,
        boolean forceRscGrpRef
    )
    {
        srcClusterId = srcClusterIdRef;
        srcNodeName = srcNodeNameRef;
        srcRscName = srcRscNameRef;
        srcBackupName = srcBackupNameRef;
        now = nowRef;
        linstorRemote = linstorRemoteRef;
        dstRscName = dstRscNameRef;
        dstNodeName = dstNodeNameRef;
        dstNetIfName = dstNetIfNameRef;
        dstStorPool = dstStorPoolRef;
        storPoolRename = storPoolRenameRef;
        dstRscGrp = dstRscGrpRef;
        downloadOnly = downloadOnlyRef;
        forceRestore = forceRestoreRef;
        scheduleName = scheduleNameRef;
        allowIncremental = allowIncrementalRef;
        forceRscGrp = forceRscGrpRef;
    }

    public String getSrcClusterId()
    {
        return srcClusterId;
    }

    public String getSrcRscName()
    {
        return srcRscName;
    }

    public String getSrcBackupName()
    {
        return srcBackupName;
    }

    public LocalDateTime getNow()
    {
        return now;
    }

    public String getDstRscName()
    {
        return dstRscName;
    }

    public LinstorRemote getLinstorRemote()
    {
        return linstorRemote;
    }

    public Map<String, String> getStorPoolRename()
    {
        return storPoolRename;
    }

    public Snapshot getSrcSnapshot()
    {
        return srcSnapshot;
    }

    public String getSrcNodeName()
    {
        return srcNodeName;
    }

    public BackupMetaDataPojo getMetaDataPojo()
    {
        return metaDataPojo;
    }

    public String getDstNodeName()
    {
        return dstNodeName;
    }

    public String getDstNetIfName()
    {
        return dstNetIfName;
    }

    public String getDstStorPool()
    {
        return dstStorPool;
    }

    public @Nullable String getDstRscGrp()
    {
        return dstRscGrp;
    }

    public String getScheduleName()
    {
        return scheduleName;
    }

    public StltRemote getStltRemote()
    {
        return stltRemote;
    }

    public String getDstBaseSnapName()
    {
        return dstBaseSnapName;
    }

    public String getDstActualNodeName()
    {
        return dstActualNodeName;
    }

    public boolean isResetData()
    {
        return resetData;
    }

    public boolean isUseZstd()
    {
        return useZstd;
    }

    public boolean isDownloadOnly()
    {
        return downloadOnly;
    }

    public boolean isForceRestore()
    {
        return forceRestore;
    }

    public boolean isAllowIncremental()
    {
        return allowIncremental;
    }

    public boolean isForceRscGrp()
    {
        return forceRscGrp;
    }

    public void setSrcSnapshot(Snapshot srcSnapshotRef)
    {
        srcSnapshot = srcSnapshotRef;
    }

    public void setSrcNodeName(String srcNodeNameRef)
    {
        srcNodeName = srcNodeNameRef;
    }

    public void setMetaDataPojo(BackupMetaDataPojo metaDataPojoRef)
    {
        metaDataPojo = metaDataPojoRef;
    }

    public void setDstNodeName(String dstNodeNameRef)
    {
        dstNodeName = dstNodeNameRef;
    }

    public void setDstNetIfName(String dstNetIfNameRef)
    {
        dstNetIfName = dstNetIfNameRef;
    }

    public void setDstStorPool(String dstStorPoolRef)
    {
        dstStorPool = dstStorPoolRef;
    }

    public void setScheduleName(String scheduleNameRef)
    {
        scheduleName = scheduleNameRef;
    }

    public void setStltRemote(StltRemote stltRemoteRef)
    {
        stltRemote = stltRemoteRef;
    }

    public void setDstBaseSnapName(String dstBaseSnapNameRef)
    {
        dstBaseSnapName = dstBaseSnapNameRef;
    }

    public void setDstActualNodeName(String dstActualNodeNameRef)
    {
        dstActualNodeName = dstActualNodeNameRef;
    }

    public void setResetData(boolean resetDataRef)
    {
        resetData = resetDataRef;
    }

    public void setUseZstd(boolean useZstdRef)
    {
        useZstd = useZstdRef;
    }

    public void setDownloadOnly(boolean downloadOnlyRef)
    {
        downloadOnly = downloadOnlyRef;
    }

    public void setForceRestore(boolean forceRestoreRef)
    {
        forceRestore = forceRestoreRef;
    }

    public void setAllowIncremental(boolean allowIncrementalRef)
    {
        allowIncremental = allowIncrementalRef;
    }
}
