package com.linbit.linstor.api.interfaces;

import com.linbit.linstor.storage.kinds.DeviceLayerKind;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public interface RscLayerDataPojo
{
    int getId();

    List<RscLayerDataPojo> getChildren();

    String getRscNameSuffix();

    DeviceLayerKind getLayerKind();

    <T extends VlmLayerDataPojo> List<T> getVolumeList();

    default <T extends VlmLayerDataPojo> Map<Integer, T> getVolumeMap()
    {
        return this.<T>getVolumeList().stream().collect(
            Collectors.toMap(
                vlmDataPojo -> vlmDataPojo.getVlmNr(),
                Function.identity()
            )
        );
    }
}
