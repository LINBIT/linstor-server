package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ExhaustedPoolException;
import com.linbit.ImplementationError;
import com.linbit.ValueInUseException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.core.apicallhandler.response.ApiDatabaseException;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.objects.ResourceConnection;
import com.linbit.linstor.core.types.TcpPortNumber;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.InvalidValueException;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;

import static com.linbit.linstor.core.apicallhandler.controller.CtrlRscConnectionApiCallHandler.getResourceConnectionDescriptionInline;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.UUID;

@Singleton
public class CtrlDrbdProxyHelper
{
    private final AccessContext apiCtx;
    private final CtrlRscConnectionHelper ctrlRscConnectionHelper;
    private final Provider<AccessContext> peerAccCtx;

    @Inject
    public CtrlDrbdProxyHelper(
        @ApiContext AccessContext apiCtxRef,
        CtrlRscConnectionHelper ctrlRscConnectionHelperRef,
        @PeerContext Provider<AccessContext> peerAccCtxRef
    )
    {
        apiCtx = apiCtxRef;
        ctrlRscConnectionHelper = ctrlRscConnectionHelperRef;
        peerAccCtx = peerAccCtxRef;
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
                    rscConnRef.autoAllocateDrbdProxyPortSource(peerAccCtx.get());
                }
                else
                {
                    rscConnRef.autoAllocateDrbdProxyPortTarget(peerAccCtx.get());
                }
            }
            else
            {
                if (srcPort)
                {
                    rscConnRef.setDrbdProxyPortSource(peerAccCtx.get(), new TcpPortNumber(port));
                }
                else
                {
                    rscConnRef.setDrbdProxyPortTarget(peerAccCtx.get(), new TcpPortNumber(port));
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
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                (port == null ? "auto-allocate" : "set") + " TCP port for " +
                    getResourceConnectionDescriptionInline(apiCtx, rscConnRef),
                ApiConsts.FAIL_ACC_DENIED_RSC_DFN
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
            rscConn.getProps(peerAccCtx.get()).setProp(key, value, namespace);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "accessing properties of " + getResourceConnectionDescriptionInline(apiCtx, rscConn),
                ApiConsts.FAIL_ACC_DENIED_RSC_CONN
            );
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
            rscConn.getStateFlags().enableFlags(peerAccCtx.get(), ResourceConnection.Flags.LOCAL_DRBD_PROXY);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "enable local proxy flag of " + getResourceConnectionDescriptionInline(apiCtx, rscConn),
                ApiConsts.FAIL_ACC_DENIED_RSC_CONN
            );
        }
        catch (DatabaseException exc)
        {
            throw new ApiDatabaseException(exc);
        }
    }
}
