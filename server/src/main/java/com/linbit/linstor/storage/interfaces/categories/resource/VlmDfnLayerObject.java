package com.linbit.linstor.storage.interfaces.categories.resource;

import com.linbit.linstor.VolumeDefinition;
import com.linbit.linstor.api.interfaces.VlmDfnLayerDataApi;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.storage.interfaces.categories.LayerObject;

import java.sql.SQLException;

/**
 * @author Gabor Hernadi &lt;gabor.hernadi@linbit.com&gt;
 */
public interface VlmDfnLayerObject extends LayerObject
{
    String getRscNameSuffix();

    VolumeDefinition getVolumeDefinition();

    void delete() throws SQLException;

    VlmDfnLayerDataApi getApiData(AccessContext accCtxRef);
}
