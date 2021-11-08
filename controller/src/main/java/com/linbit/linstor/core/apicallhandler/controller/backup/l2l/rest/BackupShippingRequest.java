package com.linbit.linstor.core.apicallhandler.controller.backup.l2l.rest;

import com.linbit.linstor.api.pojo.backups.BackupMetaDataPojo;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.sentry.util.Objects;

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
    public final Set<String> srcSnapDfnUuids;

    public final @Nullable String dstNodeName;
    public final @Nullable String dstNodeNetIfName;
    public final @Nullable String dstStorPool;
    public final boolean useZstd;
    public final boolean downloadOnly;

    @JsonCreator
    public BackupShippingRequest(
        @JsonProperty("srcVersion") int[] srcVersionRef,
        @JsonProperty("metaData") BackupMetaDataPojo metaDataRef,
        @JsonProperty("srcBackupName") String srcBackupNameRef,
        @JsonProperty("srcClusterId") String srcClusterIdRef,
        @JsonProperty("srcL2LRemoteName") String srcL2LRemoteNameRef,
        @JsonProperty("srcSnapUuids") HashSet<String> srcSnapDfnUuidsRef,
        @JsonProperty("dstRscName") String dstRscNameRef,
        @JsonProperty("dstNodeName") @Nullable String dstNodeNameRef,
        @JsonProperty("dstNodeNetIfName") @Nullable String dstNodeNetIfNameRef,
        @JsonProperty("dstStorPool") @Nullable String dstStorPoolRef,
        @JsonProperty("storPoolRenameMap") @Nullable Map<String, String> storPoolRenameMapRef,
        @JsonProperty("useZstd") boolean useZstdRef,
        @JsonProperty("downloadOnly") boolean downloadOnlyRef
    )
    {
        srcL2LRemoteName = Objects.requireNonNull(srcL2LRemoteNameRef, "source linstor remote name must not be null!");
        srcVersion = Objects.requireNonNull(srcVersionRef, "Version must not be null!");
        dstRscName = Objects.requireNonNull(dstRscNameRef, "Target resource name must not be null!");
        srcBackupName = Objects.requireNonNull(srcBackupNameRef, "BackupName must not be null!");
        srcClusterId = Objects.requireNonNull(srcClusterIdRef, "Source Cluster ID must not be null!");
        storPoolRenameMap = storPoolRenameMapRef == null ?
            Collections.emptyMap() :
            Collections.unmodifiableMap(storPoolRenameMapRef);
        metaData = Objects.requireNonNull(metaDataRef, "Metadata must not be null!");
        srcSnapDfnUuids = srcSnapDfnUuidsRef == null ?
            Collections.emptySet() :
            Collections.unmodifiableSet(srcSnapDfnUuidsRef);

        dstNodeName = dstNodeNameRef;
        dstNodeNetIfName = dstNodeNetIfNameRef;
        dstStorPool = dstStorPoolRef;
        useZstd = useZstdRef;
        downloadOnly = downloadOnlyRef;
    }
}
