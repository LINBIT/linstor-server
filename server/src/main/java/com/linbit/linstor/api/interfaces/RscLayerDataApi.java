package com.linbit.linstor.api.interfaces;

import com.linbit.linstor.storage.kinds.DeviceLayerKind;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public interface RscLayerDataApi
{
    int getId();

    List<RscLayerDataApi> getChildren();

    String getRscNameSuffix();

    DeviceLayerKind getLayerKind();

    boolean getSuspend();

    <T extends VlmLayerDataApi> List<T> getVolumeList();

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
