package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.linstor.core.objects.ResourceGroup;
import com.linbit.linstor.dbdrivers.interfaces.updater.CollectionDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.updater.MapDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;

/**
 * Database driver for {@link ResourceGroup}.
 *
 * @author Gabor Hernadi &lt;gabor.hernadi@linbit.com&gt;
 */
public interface ResourceGroupDatabaseDriver extends GenericDatabaseDriver<ResourceGroup>
{
    SingleColumnDatabaseDriver<ResourceGroup, String> getDescriptionDriver();

    CollectionDatabaseDriver<ResourceGroup, DeviceLayerKind> getLayerStackDriver();

    /*
     *  drivers needed by autoPlace
     */
    SingleColumnDatabaseDriver<ResourceGroup, Integer> getReplicaCountDriver();

    CollectionDatabaseDriver<ResourceGroup, String> getNodeNameDriver();

    CollectionDatabaseDriver<ResourceGroup, String> getStorPoolNameDriver();

    CollectionDatabaseDriver<ResourceGroup, String> getStorPoolDisklessNameDriver();

    CollectionDatabaseDriver<ResourceGroup, String> getDoNotPlaceWithRscListDriver();

    SingleColumnDatabaseDriver<ResourceGroup, String> getDoNotPlaceWithRscRegexDriver();

    CollectionDatabaseDriver<ResourceGroup, String> getReplicasOnSameListDriver();

    CollectionDatabaseDriver<ResourceGroup, String> getReplicasOnDifferentDriver();

    MapDatabaseDriver<ResourceGroup, String, Integer> getXReplicasOnDifferentMapDriver();

    CollectionDatabaseDriver<ResourceGroup, DeviceProviderKind> getAllowedProviderListDriver();

    SingleColumnDatabaseDriver<ResourceGroup, Boolean> getDisklessOnRemainingDriver();

    SingleColumnDatabaseDriver<ResourceGroup, Short> getPeerSlotsDriver();

}
