package com.linbit.linstor.core.apis;

import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.pojo.StorPoolPojo;
import com.linbit.linstor.storage.StorageConstants;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    include = JsonTypeInfo.As.PROPERTY,
    property = "kind"
)
@JsonSubTypes(
    {
        @Type(value = StorPoolPojo.class, name = "storPool")
    }
)
@JsonInclude(JsonInclude.Include.NON_NULL)
public interface StorPoolApi
{
    @JsonIgnore
    UUID getStorPoolUuid();
    String getStorPoolName();
    @JsonIgnore
    UUID getStorPoolDfnUuid();
    @JsonIgnore
    String getNodeName();
    @JsonIgnore
    UUID getNodeUuid();

    DeviceProviderKind getDeviceProviderKind();
    @JsonIgnore
    String getFreeSpaceManagerName();
    @JsonIgnore
    Optional<Long> getFreeCapacity();
    @JsonIgnore
    Optional<Long> getTotalCapacity();
    @JsonIgnore
    double getOversubscriptionRatio();
    @JsonIgnore
    Double getMaxFreeCapacityOversubscriptionRatio();
    @JsonIgnore
    Double getMaxTotalCapacityOversubscriptionRatio();
    @JsonIgnore
    Map<String, String> getStorPoolProps();
    @JsonIgnore
    Map<String, String> getStorPoolStaticTraits();
    @JsonIgnore
    Map<String, String> getStorPoolDfnProps();
    @JsonIgnore
    ApiCallRc getReports();
    @JsonIgnore
    Boolean isPmem();
    @JsonIgnore
    Boolean isVDO();

    boolean isExternalLocking();

    @JsonIgnore
    default String getBackingPoolName()
    {
        String result;
        switch (getDeviceProviderKind())
        {
            case LVM_THIN: // fall-through
            case ZFS: // fall-through
            case ZFS_THIN: // fall-through
            case FILE_THIN: // fall-through
            case FILE: // fall-through
            case EXOS: // fall-through
            case EBS_INIT: // fall-through
            case EBS_TARGET: // fall-through
            case LVM:
                // fall-through
            case STORAGE_SPACES:
                // fall-through
            case STORAGE_SPACES_THIN:
                result = getStorPoolProps().get(
                    StorageConstants.NAMESPACE_STOR_DRIVER + "/" + ApiConsts.KEY_STOR_POOL_NAME
                );
                break;
            case SPDK: // fall-through
            case REMOTE_SPDK: // fall-through
            case DISKLESS: // fall-through
            case FAIL_BECAUSE_NOT_A_VLM_PROVIDER_BUT_A_VLM_LAYER: // fall-through
            default:
                result = "";
                break;
        }
        return result;
    }
}
