package com.linbit.linstor.core.apis;

import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.pojo.StorPoolPojo;
import com.linbit.linstor.storage.StorageConstants;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;

import javax.annotation.Nonnull;

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
    Map<String, String> getStorPoolProps();
    @JsonIgnore
    Map<String, String> getStorPoolStaticTraits();
    @JsonIgnore
    Map<String, String> getStorPoolDfnProps();
    @JsonIgnore
    ApiCallRc getReports();
    @JsonIgnore
    Boolean supportsSnapshots();
    @JsonIgnore
    Boolean isPmem();
    @JsonIgnore
    Boolean isVDO();
    Boolean isExternalLocking();

    @Nonnull
    @JsonIgnore
    default String getBackingPoolName() {
        switch (getDeviceProviderKind()) {
            case LVM_THIN:
            case ZFS:
            case ZFS_THIN:
            case FILE_THIN:
            case FILE:
            case EXOS:
            case LVM: return getStorPoolProps().get(StorageConstants.NAMESPACE_STOR_DRIVER +
                    "/" + ApiConsts.KEY_STOR_POOL_NAME);
            case SPDK:
            case REMOTE_SPDK:
            case DISKLESS:
            case OPENFLEX_TARGET:
            case FAIL_BECAUSE_NOT_A_VLM_PROVIDER_BUT_A_VLM_LAYER:
            default: return "";
        }
    }
}
