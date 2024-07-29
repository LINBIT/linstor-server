package com.linbit.linstor.core.devmgr.exceptions;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiConsts.DeviceLayerKind;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;

public class DeviceException extends StorageException
{
    private static final long serialVersionUID = -1604152350394905076L;

    private final transient AbsRscLayerObject<Resource> rscData;
    private final transient @Nullable VlmProviderObject<Resource> vlmData;

    public DeviceException(
        String actionRef,
        DeviceLayerKind layerRef,
        AbsRscLayerObject<Resource> rscDataRef,
        Exception nestedExceptionRef
    )
    {
        super(
            String.format(
                "Failed to %s on layer %s for resource %s",
                actionRef,
                layerRef.name(),
                rscDataRef.getSuffixedResourceName()
            ),
            nestedExceptionRef
        );
        rscData = rscDataRef;
        vlmData = null;
    }

    public DeviceException(
        String actionRef,
        DeviceLayerKind layerRef,
        VlmProviderObject<Resource> vlmDataRef,
        Exception nestedExceptionRef
    )
    {
        super(
            String.format(
                "Failed to %s on layer %s for resource %s, volume number: %d, volume identifier: %s, device: %s",
                actionRef,
                layerRef.name(),
                vlmDataRef.getRscLayerObject().getSuffixedResourceName(),
                vlmDataRef.getVlmNr().value,
                vlmDataRef.getIdentifier(),
                vlmDataRef.getDevicePath()
            ),
            nestedExceptionRef
        );
        rscData = vlmDataRef.getRscLayerObject();
        vlmData = vlmDataRef;
    }

    public AbsRscLayerObject<Resource> getRscData()
    {
        return rscData;
    }

    public @Nullable VlmProviderObject<Resource> getVlmData()
    {
        return vlmData;
    }
}
