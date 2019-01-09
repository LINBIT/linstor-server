package com.linbit.linstor.storage.interfaces.categories;

import com.linbit.linstor.VolumeDefinition;

/**
 * @author Gabor Hernadi &lt;gabor.hernadi@linbit.com&gt;
 */
public interface VlmDfnLayerObject extends LayerObject
{
    VolumeDefinition getVolumeDefinition();
}
