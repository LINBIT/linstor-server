package com.linbit.linstor.core.apicallhandler.controller.backup.l2l.rest.data;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.pojo.backups.BackupMetaDataPojo;
import com.linbit.linstor.core.identifier.SnapshotName;
import com.linbit.linstor.core.objects.remotes.StltRemote;

import java.util.Map;

public class BackupShippingDstData
{
    private @Nullable StltRemote stltRemote;
    private @Nullable SnapshotName snapName;
    private final String srcL2LRemoteName; // linstorRemoteName, not StltRemoteName
    private final String srcStltRemoteName;
    private final String srcL2LRemoteUrl;
    private final int[] srcVersion;
    private final String dstRscName;
    private final BackupMetaDataPojo metaData;
    private final String srcBackupName;
    private final String srcClusterId;
    private final String srcRscName;
    private @Nullable String incrBaseSnapDfnUuid;
    private @Nullable String dstNodeName;
    private @Nullable String dstNetIfName;
    private @Nullable String dstStorPool;
    private Map<String, String> storPoolRenameMap;
    private final @Nullable String dstRscGrp;
    private final Map<String, Integer> snapShipPorts;
    private boolean useZstd;
    private boolean downloadOnly;
    private boolean forceRestore;
    private final boolean resetData;
    private final boolean forceRscGrp;
    private final @Nullable String dstBaseSnapName;
    private final @Nullable String dstActualNodeName;

    public BackupShippingDstData(
        int[] srcVersionRef,
        String srcL2LRemoteNameRef, // linstorRemoteName, not StltRemoteName
        String srcStltRemoteNameRef,
        String srcL2LRemoteUrlRef,
        String dstRscNameRef,
        BackupMetaDataPojo metaDataRef,
        String srcBackupNameRef,
        String srcClusterIdRef,
        String srcRscNameRef,
        @Nullable String dstNodeNameRef, // the node the user wants the receive to happen on
        @Nullable String dstNetIfNameRef,
        @Nullable String dstStorPoolRef,
        Map<String, String> storPoolRenameMapRef,
        @Nullable String dstRscGrpRef,
        Map<String, Integer> snapShipPortsRef,
        boolean useZstdRef,
        boolean downloadOnlyRef,
        boolean forceRestoreRef,
        boolean resetDataRef,
        @Nullable String dstBaseSnapNameRef,
        @Nullable String dstActualNodeNameRef, // the node that needs to do the receive
        boolean forceRscGrpRef
    )
    {
        srcVersion = srcVersionRef;
        srcL2LRemoteName = srcL2LRemoteNameRef;
        srcStltRemoteName = srcStltRemoteNameRef;
        srcL2LRemoteUrl = srcL2LRemoteUrlRef;
        dstRscName = dstRscNameRef;
        metaData = metaDataRef;
        srcBackupName = srcBackupNameRef;
        srcClusterId = srcClusterIdRef;
        srcRscName = srcRscNameRef;
        dstNodeName = dstNodeNameRef;
        dstNetIfName = dstNetIfNameRef;
        dstStorPool = dstStorPoolRef;
        storPoolRenameMap = storPoolRenameMapRef;
        dstRscGrp = dstRscGrpRef;
        snapShipPorts = snapShipPortsRef;
        useZstd = useZstdRef;
        downloadOnly = downloadOnlyRef;
        forceRestore = forceRestoreRef;
        resetData = resetDataRef;
        dstBaseSnapName = dstBaseSnapNameRef;
        dstActualNodeName = dstActualNodeNameRef;
        forceRscGrp = forceRscGrpRef;
    }

    public @Nullable StltRemote getStltRemote()
    {
        return stltRemote;
    }

    public @Nullable SnapshotName getSnapName()
    {
        return snapName;
    }

    public String getSrcL2LRemoteName()
    {
        return srcL2LRemoteName;
    }

    public String getSrcStltRemoteName()
    {
        return srcStltRemoteName;
    }

    public String getSrcL2LRemoteUrl()
    {
        return srcL2LRemoteUrl;
    }

    public int[] getSrcVersion()
    {
        return srcVersion;
    }

    public String getDstRscName()
    {
        return dstRscName;
    }

    public BackupMetaDataPojo getMetaData()
    {
        return metaData;
    }

    public String getSrcBackupName()
    {
        return srcBackupName;
    }

    public String getSrcClusterId()
    {
        return srcClusterId;
    }

    public String getSrcRscName()
    {
        return srcRscName;
    }

    public @Nullable String getIncrBaseSnapDfnUuid()
    {
        return incrBaseSnapDfnUuid;
    }

    public @Nullable String getDstNodeName()
    {
        return dstNodeName;
    }

    public @Nullable String getDstNetIfName()
    {
        return dstNetIfName;
    }

    public @Nullable String getDstStorPool()
    {
        return dstStorPool;
    }

    public Map<String, String> getStorPoolRenameMap()
    {
        return storPoolRenameMap;
    }

    public String getDstRscGrp()
    {
        return dstRscGrp;
    }

    public Map<String, Integer> getSnapShipPorts()
    {
        return snapShipPorts;
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

    public boolean isResetData()
    {
        return resetData;
    }

    public @Nullable String getDstBaseSnapName()
    {
        return dstBaseSnapName;
    }

    public @Nullable String getDstActualNodeName()
    {
        return dstActualNodeName;
    }

    public boolean isForceRscGrp()
    {
        return forceRscGrp;
    }

    public void setStltRemote(StltRemote stltRemoteRef)
    {
        stltRemote = stltRemoteRef;
    }

    public void setSnapName(SnapshotName snapNameRef)
    {
        snapName = snapNameRef;
    }

    public void setIncrBaseSnapDfnUuid(String incrBaseSnapDfnUuidRef)
    {
        incrBaseSnapDfnUuid = incrBaseSnapDfnUuidRef;
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

    public void setStorPoolRenameMap(Map<String, String> storPoolRenameMapRef)
    {
        storPoolRenameMap = storPoolRenameMapRef;
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
}
