package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.CollectionDatabaseDriver;
import com.linbit.SingleColumnDatabaseDriver;
import com.linbit.linstor.core.objects.ResourceGroupData;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;

/**
 * Database driver for {@link ResourceGroupData}.
 *
 * @author Gabor Hernadi &lt;gabor.hernadi@linbit.com&gt;
 */
public interface ResourceGroupDataDatabaseDriver extends GenericDatabaseDriver<ResourceGroupData>
{
    SingleColumnDatabaseDriver<ResourceGroupData, String> getDescriptionDriver();

    CollectionDatabaseDriver<ResourceGroupData, DeviceLayerKind> getLayerStackDriver();

    /*
     *  drivers needed by autoPlace
     */
    SingleColumnDatabaseDriver<ResourceGroupData, Integer> getReplicaCountDriver();

    SingleColumnDatabaseDriver<ResourceGroupData, String>  getStorPoolNameDriver();

    CollectionDatabaseDriver<ResourceGroupData, String> getDoNotPlaceWithRscListDriver();

    SingleColumnDatabaseDriver<ResourceGroupData, String> getDoNotPlaceWithRscRegexDriver();

    CollectionDatabaseDriver<ResourceGroupData, String> getReplicasOnSameListDriver();

    CollectionDatabaseDriver<ResourceGroupData, String> getReplicasOnDifferentDriver();

    CollectionDatabaseDriver<ResourceGroupData, DeviceProviderKind> getAllowedProviderListDriver();

    SingleColumnDatabaseDriver<ResourceGroupData, Boolean> getDisklessOnRemainingDriver();
}
