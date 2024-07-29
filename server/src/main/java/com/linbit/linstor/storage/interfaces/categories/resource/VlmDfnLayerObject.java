package com.linbit.linstor.storage.interfaces.categories.resource;

import com.linbit.GenericName;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.interfaces.VlmDfnLayerDataApi;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.SnapshotName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.storage.interfaces.categories.LayerObject;
import com.linbit.utils.StringUtils;

/**
 * @author Gabor Hernadi &lt;gabor.hernadi@linbit.com&gt;
 */
public interface VlmDfnLayerObject extends LayerObject, Comparable<VlmDfnLayerObject>
{
    String getRscNameSuffix();

    ResourceName getResourceName();

    @Nullable
    SnapshotName getSnapshotName();

    VolumeNumber getVolumeNumber();

    void delete() throws DatabaseException;

    VlmDfnLayerDataApi getApiData(AccessContext accCtxRef);

    @Override
    default int compareTo(VlmDfnLayerObject oRef)
    {
        int cmp = getResourceName().compareTo(oRef.getResourceName());
        if (cmp == 0)
        {
            cmp = StringUtils.compareToNullable(getRscNameSuffix(), oRef.getRscNameSuffix());
            if (cmp == 0)
            {
                cmp = GenericName.compareToNullable(getSnapshotName(), oRef.getSnapshotName());
                if (cmp == 0)
                {
                    cmp = getVolumeNumber().compareTo(oRef.getVolumeNumber());
                }
            }
        }
        return cmp;
    }
}
