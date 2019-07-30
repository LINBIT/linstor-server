package com.linbit.linstor.storage.interfaces.categories.resource;

import com.linbit.linstor.api.interfaces.VlmDfnLayerDataApi;
import com.linbit.linstor.core.objects.VolumeDefinition;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.storage.interfaces.categories.LayerObject;

/**
 * @author Gabor Hernadi &lt;gabor.hernadi@linbit.com&gt;
 */
public interface VlmDfnLayerObject extends LayerObject
{
    String getRscNameSuffix();

    VolumeDefinition getVolumeDefinition();

    void delete() throws DatabaseException;

    VlmDfnLayerDataApi getApiData(AccessContext accCtxRef);
}
