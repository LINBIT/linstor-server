package com.linbit.linstor.storage.interfaces.categories;

import com.linbit.linstor.ResourceDefinition;
import com.linbit.linstor.api.interfaces.RscDfnLayerDataApi;
import com.linbit.linstor.security.AccessContext;

import java.sql.SQLException;

/**
 * @author Gabor Hernadi &lt;gabor.hernadi@linbit.com&gt;
 */
public interface RscDfnLayerObject extends LayerObject
{
    String getRscNameSuffix();

    ResourceDefinition getResourceDefinition();

    void delete() throws SQLException;

    RscDfnLayerDataApi getApiData(AccessContext accCtxRef);
}
