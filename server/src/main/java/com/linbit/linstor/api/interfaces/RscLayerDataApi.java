package com.linbit.linstor.api.interfaces;

import com.linbit.linstor.api.pojo.BCacheRscPojo;
import com.linbit.linstor.api.pojo.CacheRscPojo;
import com.linbit.linstor.api.pojo.DrbdRscPojo;
import com.linbit.linstor.api.pojo.LuksRscPojo;
import com.linbit.linstor.api.pojo.NvmeRscPojo;
import com.linbit.linstor.api.pojo.StorageRscPojo;
import com.linbit.linstor.api.pojo.WritecacheRscPojo;
import com.linbit.linstor.layer.LayerIgnoreReason;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

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
        @Type(value = CacheRscPojo.class, name = "cache"),
        @Type(value = DrbdRscPojo.class, name = "drbd"),
        @Type(value = LuksRscPojo.class, name = "luks"),
        @Type(value = NvmeRscPojo.class, name = "nvme"),
        @Type(value = StorageRscPojo.class, name = "storage"),
        @Type(value = WritecacheRscPojo.class, name = "writecache"),
        @Type(value = BCacheRscPojo.class, name = "bcache")
    // openflex not serialized (for now)
    }
)
@JsonInclude(JsonInclude.Include.NON_NULL)
public interface RscLayerDataApi
{
    int BACK_DFLT_ID = -1;

    @JsonIgnore
    int getId();

    List<RscLayerDataApi> getChildren();

    String getRscNameSuffix();

    @JsonIgnore
    DeviceLayerKind getLayerKind();

    @JsonIgnore
    boolean getSuspend();

    @JsonIgnore
    Set<LayerIgnoreReason> getIgnoreReasons();

    <T extends VlmLayerDataApi> List<T> getVolumeList();

    @JsonIgnore
    default <T extends VlmLayerDataApi> Map<Integer, T> getVolumeMap()
    {
        return this.<T>getVolumeList().stream().collect(
            Collectors.toMap(
                vlmDataPojo -> vlmDataPojo.getVlmNr(),
                Function.identity()
            )
        );
    }
}
