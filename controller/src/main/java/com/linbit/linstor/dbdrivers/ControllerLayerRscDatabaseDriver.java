package com.linbit.linstor.dbdrivers;

import com.linbit.InvalidIpAddressException;
import com.linbit.InvalidNameException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.drbd.md.MdException;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.core.objects.AbsLayerRscDataDbDriver.ParentObjects;
import com.linbit.linstor.core.objects.AbsResource;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.StorPool.InitMaps;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.utils.Pair;

import java.util.Map;
import java.util.Set;

public interface ControllerLayerRscDatabaseDriver
{
    class LoadingObjects
    {
        final Map<Pair<NodeName, StorPoolName>, Pair<StorPool, StorPool.InitMaps>> storPoolMapRef;

        public LoadingObjects(Map<Pair<NodeName, StorPoolName>, Pair<StorPool, InitMaps>> storPoolMapRefRef)
        {
            storPoolMapRef = storPoolMapRefRef;
        }
    }

    DeviceLayerKind getDeviceLayerKind();

    void cacheAll(ParentObjects parentObjects) throws DatabaseException;

    void clearLoadingCaches();

    <RSC extends AbsResource<RSC>> Pair<? extends AbsRscLayerObject<RSC>, Set<AbsRscLayerObject<RSC>>> load(
        RSC rscRef,
        int rscLayerId
    )
        throws DatabaseException, InvalidNameException, ValueOutOfRangeException, InvalidIpAddressException,
        MdException, AccessDeniedException;

    void loadAllLayerVlmData() throws DatabaseException;
}
