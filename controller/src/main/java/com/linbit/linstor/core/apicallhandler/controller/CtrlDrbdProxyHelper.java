package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ExhaustedPoolException;
import com.linbit.ImplementationError;
import com.linbit.ValueInUseException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.apicallhandler.response.ApiDatabaseException;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.objects.ResourceConnection;
import com.linbit.linstor.core.types.TcpPortNumber;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.InvalidValueException;


import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.util.UUID;

@Singleton
public class CtrlDrbdProxyHelper
{
    private final CtrlRscConnectionHelper ctrlRscConnectionHelper;

    @Inject
    public CtrlDrbdProxyHelper(
        CtrlRscConnectionHelper ctrlRscConnectionHelperRef
    )
    {
        ctrlRscConnectionHelper = ctrlRscConnectionHelperRef;
    }

    public ResourceConnection enableProxy(
        @Nullable UUID rscConnUuid,
        String nodeName1,
        String nodeName2,
        String rscNameStr,
        @Nullable Integer drbdProxyPortSrc,
        @Nullable Integer drbdProxyPortTarget
    )
    {
        ResourceConnection rscConn = ctrlRscConnectionHelper.loadOrCreateRscConn(
            rscConnUuid,
            nodeName1,
            nodeName2,
            rscNameStr
        );

        setOrAutoAllocateDrbdProxyPort(rscConn, drbdProxyPortSrc, true);
        setOrAutoAllocateDrbdProxyPort(rscConn, drbdProxyPortTarget, false);

        enableLocalProxyFlag(rscConn);

        // set protocol A as default
        setPropHardcoded(rscConn, "protocol", "A", ApiConsts.NAMESPC_DRBD_NET_OPTIONS);
        return rscConn;
    }

    private void setOrAutoAllocateDrbdProxyPort(
        ResourceConnection rscConnRef,
        @Nullable Integer port,
        boolean srcPort
    )
    {
        try
        {
            if (port == null)
            {
                if (srcPort)
                {
                    rscConnRef.autoAllocateDrbdProxyPortSource();
                }
                else
                {
                    rscConnRef.autoAllocateDrbdProxyPortTarget();
                }
            }
            else
            {
                if (srcPort)
                {
                    rscConnRef.setDrbdProxyPortSource(new TcpPortNumber(port));
                }
                else
                {
                    rscConnRef.setDrbdProxyPortTarget(new TcpPortNumber(port));
                }
            }
        }
        catch (ExhaustedPoolException exc)
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_POOL_EXHAUSTED_TCP_PORT,
                    "Could not find free TCP port"
                ),
                exc
            );
        }
        catch (DatabaseException exc)
        {
            throw new ApiDatabaseException(exc);
        }
        catch (ValueOutOfRangeException | ValueInUseException exc)
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_INVLD_RSC_PORT,
                    String.format(
                        "The specified TCP port '%d' is invalid.",
                        port
                    )
                ),
                exc
            );
        }
    }

    private void setPropHardcoded(
        ResourceConnection rscConn,
        String key,
        String value,
        String namespace
    )
    {
        try
        {
            rscConn.getProps().setProp(key, value, namespace);
        }
        catch (InvalidKeyException | InvalidValueException exc)
        {
            throw new ImplementationError("Invalid hardcoded resource-connection properties", exc);
        }
        catch (DatabaseException exc)
        {
            throw new ApiDatabaseException(exc);
        }
    }

    private void enableLocalProxyFlag(ResourceConnection rscConn)
    {
        try
        {
            rscConn.getStateFlags().enableFlags(ResourceConnection.Flags.LOCAL_DRBD_PROXY);
        }
        catch (DatabaseException exc)
        {
            throw new ApiDatabaseException(exc);
        }
    }
}
