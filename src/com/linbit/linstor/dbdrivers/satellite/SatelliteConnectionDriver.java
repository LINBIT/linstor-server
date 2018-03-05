package com.linbit.linstor.dbdrivers.satellite;

import com.linbit.SingleColumnDatabaseDriver;
import com.linbit.linstor.Node;
import com.linbit.linstor.SatelliteConnection;
import com.linbit.linstor.SatelliteConnectionData;
import com.linbit.linstor.TcpPortNumber;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.dbdrivers.interfaces.SatelliteConnectionDataDatabaseDriver;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import javax.inject.Inject;
import java.sql.SQLException;

public class SatelliteConnectionDriver implements SatelliteConnectionDataDatabaseDriver
{
    private final SingleColumnDatabaseDriver<?, ?> singleColDriver = new SatelliteSingleColDriver<>();
    private final AccessContext dbCtx;

    @Inject
    public SatelliteConnectionDriver(@SystemContext AccessContext dbCtxRef)
    {
        dbCtx = dbCtxRef;
    }

    @Override
    public SatelliteConnectionData load(
        Node node,
        boolean logWarnIfNotExists
    )
        throws SQLException
    {
        SatelliteConnectionData stltConn = null;
        try
        {
            stltConn = (SatelliteConnectionData) node.getSatelliteConnection(dbCtx);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            SatelliteDbDriverExceptionHandler.handleAccessDeniedException(accDeniedExc);
        }
        return stltConn;
    }

    @Override
    public void create(SatelliteConnection satelliteConnectionData) throws SQLException
    {
        // no-op
    }

    @Override
    public void delete(SatelliteConnection satelliteConnectionData) throws SQLException
    {
        // no-op
    }

    @SuppressWarnings("unchecked")
    @Override
    public SingleColumnDatabaseDriver<SatelliteConnectionData, TcpPortNumber> getSatelliteConnectionPortDriver()
    {
        return (SingleColumnDatabaseDriver<SatelliteConnectionData, TcpPortNumber>) singleColDriver;
    }

    @SuppressWarnings("unchecked")
    @Override
    public SingleColumnDatabaseDriver<SatelliteConnectionData, SatelliteConnection.EncryptionType>
        getSatelliteConnectionTypeDriver()
    {
        return (SingleColumnDatabaseDriver<SatelliteConnectionData, SatelliteConnection.EncryptionType>)
            singleColDriver;
    }

}
