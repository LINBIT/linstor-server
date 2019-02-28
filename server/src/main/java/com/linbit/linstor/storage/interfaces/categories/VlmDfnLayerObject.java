package com.linbit.linstor.storage.interfaces.categories;

import com.linbit.linstor.VolumeDefinition;

import java.sql.SQLException;

/**
 * @author Gabor Hernadi &lt;gabor.hernadi@linbit.com&gt;
 */
public interface VlmDfnLayerObject extends LayerObject
{
    String getRscNameSuffix();

    VolumeDefinition getVolumeDefinition();

    void delete() throws SQLException;
}
