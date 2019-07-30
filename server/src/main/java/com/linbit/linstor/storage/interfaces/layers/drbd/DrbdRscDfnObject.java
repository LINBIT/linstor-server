package com.linbit.linstor.storage.interfaces.layers.drbd;

import com.linbit.linstor.TcpPortNumber;
import com.linbit.linstor.core.objects.ResourceDefinition.TransportType;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.storage.interfaces.categories.resource.RscDfnLayerObject;

public interface DrbdRscDfnObject extends RscDfnLayerObject
{
    TcpPortNumber getTcpPort();

    TransportType getTransportType();

    String getSecret();

    short getPeerSlots();

    int getAlStripes();

    long getAlStripeSize();

    boolean isDown();

    void setDown(boolean downRef) throws DatabaseException;
}
