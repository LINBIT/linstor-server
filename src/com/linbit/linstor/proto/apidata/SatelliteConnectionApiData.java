package com.linbit.linstor.proto.apidata;

import com.linbit.linstor.SatelliteConnection.SatelliteConnectionApi;
import com.linbit.linstor.proto.SatelliteConnectionOuterClass.SatelliteConnection;

public class SatelliteConnectionApiData implements SatelliteConnectionApi
{
    private SatelliteConnection stltConn;

    public SatelliteConnectionApiData(SatelliteConnection stltConnRef)
    {
        stltConn = stltConnRef;
    }

    @Override
    public String getNetInterfaceName()
    {
        return stltConn.getNetInterfaceName();
    }

    @Override
    public int getPort()
    {
        return stltConn.getPort();
    }

    @Override
    public String getEncryptionType()
    {
        return stltConn.getEncryptionType();
    }

}
