package com.linbit.linstor.core.apicallhandler.controller.backup.l2l.rest.data;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.pojo.backups.BackupMetaDataPojo;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Initial request sent from the source Linstor cluster to the target Linstor cluster.
 * The target Linstor cluster needs to verify if the shipping can be accepted. That means:
 * * Does the target node exist
 * * Does the target node have the requested storage pool
 * * Does the target sp have
 * * * enough space
 * * * compatible provider type (lvm/zfs/...)
 *
 * This request is expected to be answered with {@link BackupShippingResponse}
 */
public class BackupShippingRequest
{
    public final int[] srcVersion;
    public final String dstRscName;
    public final Map<String, String> storPoolRenameMap;
    public final BackupMetaDataPojo metaData;
    public final String srcBackupName;
    public final String srcClusterId;
    public final String srcL2LRemoteName;
    public final String srcStltRemoteName;
    public final String srcRscName;

    public final @Nullable String dstNodeName;
    public final @Nullable String dstNodeNetIfName;
    public final @Nullable String dstStorPool;
    public final @Nullable String dstRscGrp;
    public final boolean useZstd;
    public final boolean downloadOnly;
    public final boolean forceRestore;
    public final boolean resetData;
    public final @Nullable String dstBaseSnapName;
    public final @Nullable String dstActualNodeName;
    public final boolean forceRscGrp;

    @JsonCreator
    public BackupShippingRequest(
        @JsonProperty("srcVersion") int[] srcVersionRef,
        @JsonProperty("metaData") BackupMetaDataPojo metaDataRef,
        @JsonProperty("srcBackupName") String srcBackupNameRef,
        @JsonProperty("srcClusterId") String srcClusterIdRef,
        @JsonProperty("srcL2LRemoteName") String srcL2LRemoteNameRef, // linstorRemoteName, not StltRemoteName
        @JsonProperty("srcStltRemoteName") String srcStltRemoteNameRef,
        @JsonProperty("dstRscName") String dstRscNameRef,
        @JsonProperty("dstNodeName") @Nullable String dstNodeNameRef,
        @JsonProperty("dstNodeNetIfName") @Nullable String dstNodeNetIfNameRef,
        @JsonProperty("dstStorPool") @Nullable String dstStorPoolRef,
        @JsonProperty("storPoolRenameMap") @Nullable Map<String, String> storPoolRenameMapRef,
        @JsonProperty("dstRscGrp") @Nullable String dstRscGrpRef,
        @JsonProperty("useZstd") boolean useZstdRef,
        @JsonProperty("downloadOnly") boolean downloadOnlyRef,
        @JsonProperty("forceRestore") boolean forceRestoreRef,
        @JsonProperty("resetData") boolean resetDataRef,
        @JsonProperty("dstBaseSnapName") @Nullable String dstBaseSnapNameRef,
        @JsonProperty("dstActualNodeName") @Nullable String dstActualNodeNameRef,
        @JsonProperty("srcRscName") String srcRscNameRef,
        @JsonProperty("forceRscGrp") boolean forceRscGrpRef
    )
    {
        srcL2LRemoteName = Objects.requireNonNull(srcL2LRemoteNameRef, "source linstor remote name must not be null!");
        srcStltRemoteName = Objects.requireNonNull(srcStltRemoteNameRef, "source stlt remote name must not be null!");
        srcVersion = Objects.requireNonNull(srcVersionRef, "Version must not be null!");
        dstRscName = Objects.requireNonNull(dstRscNameRef, "Target resource name must not be null!");
        srcBackupName = Objects.requireNonNull(srcBackupNameRef, "BackupName must not be null!");
        srcClusterId = Objects.requireNonNull(srcClusterIdRef, "Source Cluster ID must not be null!");
        srcRscName = srcRscNameRef;
        storPoolRenameMap = storPoolRenameMapRef == null ?
            Collections.emptyMap() :
            Collections.unmodifiableMap(storPoolRenameMapRef);
        metaData = Objects.requireNonNull(metaDataRef, "Metadata must not be null!");

        dstNodeName = dstNodeNameRef;
        dstNodeNetIfName = dstNodeNetIfNameRef;
        dstStorPool = dstStorPoolRef;
        dstRscGrp = dstRscGrpRef;
        useZstd = useZstdRef;
        downloadOnly = downloadOnlyRef;
        forceRestore = forceRestoreRef;
        resetData = resetDataRef;
        dstBaseSnapName = dstBaseSnapNameRef;
        dstActualNodeName = dstActualNodeNameRef;
        forceRscGrp = forceRscGrpRef;
    }
}
