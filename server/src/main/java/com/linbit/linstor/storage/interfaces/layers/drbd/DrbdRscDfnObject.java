package com.linbit.linstor.storage.interfaces.layers.drbd;

import com.linbit.linstor.ResourceDefinition.TransportType;
import com.linbit.linstor.storage.interfaces.categories.RscDfnLayerObject;

import java.sql.SQLException;

import com.linbit.linstor.TcpPortNumber;

public interface DrbdRscDfnObject extends RscDfnLayerObject
{
    TcpPortNumber getTcpPort();

    TransportType getTransportType();

    String getSecret();

    short getPeerSlots();

    int getAlStripes();

    long getAlStripeSize();

    boolean isDown();

    void setDown(boolean downRef) throws SQLException;
}
