package com.linbit.linstor.storage.interfaces.categories;

import com.linbit.linstor.ResourceDefinition;

/**
 * @author Gabor Hernadi &lt;gabor.hernadi@linbit.com&gt;
 */
public interface RscDfnLayerObject extends LayerObject
{
    ResourceDefinition getResourceDefinition();
}
