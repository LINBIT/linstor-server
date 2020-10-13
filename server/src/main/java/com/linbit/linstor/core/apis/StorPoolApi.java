package com.linbit.linstor.core.apis;

import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.storage.StorageConstants;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

public interface StorPoolApi
{
    UUID getStorPoolUuid();
    String getStorPoolName();
    UUID getStorPoolDfnUuid();
    String getNodeName();
    UUID getNodeUuid();
    DeviceProviderKind getDeviceProviderKind();
    String getFreeSpaceManagerName();
    Optional<Long> getFreeCapacity();
    Optional<Long> getTotalCapacity();

    Map<String, String> getStorPoolProps();
    Map<String, String> getStorPoolStaticTraits();
    Map<String, String> getStorPoolDfnProps();
    ApiCallRc getReports();
    Boolean supportsSnapshots();
    Boolean isPmem();
    Boolean isVDO();
    Boolean isExternalLocking();

    @Nonnull
    default String getBackingPoolName() {
        switch (getDeviceProviderKind()) {
            case LVM_THIN:
            case ZFS:
            case ZFS_THIN:
            case FILE_THIN:
            case FILE:
            case LVM: return getStorPoolProps().get(StorageConstants.NAMESPACE_STOR_DRIVER +
                    "/" + ApiConsts.KEY_STOR_POOL_NAME);
            case SPDK:
            case DISKLESS:
            case OPENFLEX_TARGET:
            case FAIL_BECAUSE_NOT_A_VLM_PROVIDER_BUT_A_VLM_LAYER:
            default: return "";
        }
    }
}
