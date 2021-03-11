package com.linbit.linstor.api.interfaces;

import com.linbit.linstor.api.pojo.CacheRscPojo.CacheVlmPojo;
import com.linbit.linstor.api.pojo.DrbdRscPojo.DrbdVlmPojo;
import com.linbit.linstor.api.pojo.LuksRscPojo.LuksVlmPojo;
import com.linbit.linstor.api.pojo.NvmeRscPojo.NvmeVlmPojo;
import com.linbit.linstor.api.pojo.StorageRscPojo.DisklessVlmPojo;
import com.linbit.linstor.api.pojo.StorageRscPojo.FileThinVlmPojo;
import com.linbit.linstor.api.pojo.StorageRscPojo.FileVlmPojo;
import com.linbit.linstor.api.pojo.StorageRscPojo.LvmThinVlmPojo;
import com.linbit.linstor.api.pojo.StorageRscPojo.LvmVlmPojo;
import com.linbit.linstor.api.pojo.StorageRscPojo.SpdkVlmPojo;
import com.linbit.linstor.api.pojo.StorageRscPojo.ZfsThinVlmPojo;
import com.linbit.linstor.api.pojo.StorageRscPojo.ZfsVlmPojo;
import com.linbit.linstor.api.pojo.WritecacheRscPojo.WritecacheVlmPojo;
import com.linbit.linstor.core.apis.StorPoolApi;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * Marker interface
 */
@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    include = JsonTypeInfo.As.PROPERTY,
    property = "kind"
)
@JsonSubTypes(
    {
        @Type(value = DisklessVlmPojo.class, name = "diskless"),
        @Type(value = LvmVlmPojo.class, name = "lvm"),
        @Type(value = SpdkVlmPojo.class, name = "spdk"),
        @Type(value = LvmThinVlmPojo.class, name = "lvmThin"),
        @Type(value = ZfsVlmPojo.class, name = "zfs"),
        @Type(value = ZfsThinVlmPojo.class, name = "zfsThin"),
        @Type(value = FileVlmPojo.class, name = "file"),
        @Type(value = FileThinVlmPojo.class, name = "fileThin"),
        @Type(value = CacheVlmPojo.class, name = "cache"),
        @Type(value = DrbdVlmPojo.class, name = "drbd"),
        @Type(value = LuksVlmPojo.class, name = "luks"),
        @Type(value = NvmeVlmPojo.class, name = "nvme"),
        @Type(value = WritecacheVlmPojo.class, name = "writecache")
    }
)
@JsonInclude(JsonInclude.Include.NON_NULL)
public interface VlmLayerDataApi
{
    int getVlmNr();

    @JsonIgnore
    DeviceLayerKind getLayerKind();

    @JsonIgnore
    DeviceProviderKind getProviderKind();

    @JsonIgnore
    String getDevicePath();

    @JsonIgnore
    long getAllocatedSize();

    @JsonIgnore
    long getUsableSize();

    @JsonIgnore
    String getDiskState();

    default StorPoolApi getStorPoolApi()
    {
        return null; // layers should not have storage pools (only storage layer / diskless vlm)
    }
}
