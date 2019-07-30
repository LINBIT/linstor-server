package com.linbit.linstor.storage.interfaces.categories.resource;

import com.linbit.linstor.api.interfaces.RscDfnLayerDataApi;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.storage.interfaces.categories.LayerObject;

/**
 * @author Gabor Hernadi &lt;gabor.hernadi@linbit.com&gt;
 */
public interface RscDfnLayerObject extends LayerObject
{
    String getRscNameSuffix();

    ResourceDefinition getResourceDefinition();

    void delete() throws DatabaseException;

    RscDfnLayerDataApi getApiData(AccessContext accCtxRef);
}
