package com.linbit.linstor.core;

import com.linbit.ImplementationError;
import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.NodeData;
import com.linbit.linstor.Resource;
import com.linbit.linstor.Volume;
import com.linbit.linstor.VolumeConnectionData;
import com.linbit.linstor.VolumeConnectionDataFactory;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.api.prop.WhitelistProps;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.transaction.TransactionMgr;

import javax.inject.Inject;
import javax.inject.Provider;
import java.sql.SQLException;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;

class CtrlVlmConnectionApiCallHandler extends AbsApiCallHandler
{
    private String currentNodeName1;
    private String currentNodeName2;
    private String currentRscName;
    private Integer currentVlmNr;
    private final VolumeConnectionDataFactory volumeConnectionDataFactory;

    @Inject
    CtrlVlmConnectionApiCallHandler(
        ErrorReporter errorReporterRef,
        CtrlStltSerializer interComSerializer,
        @ApiContext AccessContext apiCtxRef,
        CtrlObjectFactories objectFactories,
        VolumeConnectionDataFactory volumeConnectionDataFactoryRef,
        Provider<TransactionMgr> transMgrProviderRef,
        @PeerContext AccessContext peerAccCtxRef,
        Provider<Peer> peerRef,
        WhitelistProps whitelistPropsRef
    )
    {
        super(
            errorReporterRef,
            apiCtxRef,
            LinStorObject.VOLUME_CONN,
            interComSerializer,
            objectFactories,
            transMgrProviderRef,
            peerAccCtxRef,
            peerRef,
            whitelistPropsRef
        );
        volumeConnectionDataFactory = volumeConnectionDataFactoryRef;
    }

    public ApiCallRc createVolumeConnection(
        String nodeName1Str,
        String nodeName2Str,
        String rscNameStr,
        int vlmNrInt,
        Map<String, String> vlmConnPropsMap
    )
    {
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();

        try (
            AbsApiCallHandler basicallyThis = setContext(
                ApiCallType.CREATE,
                apiCallRc,
                nodeName1Str,
                nodeName2Str,
                rscNameStr,
                vlmNrInt
            );
        )
        {
            VolumeConnectionData vlmConn = createVlmConn(nodeName1Str, nodeName2Str, rscNameStr, vlmNrInt);

            fillProperties(vlmConnPropsMap, getProps(vlmConn), ApiConsts.FAIL_ACC_DENIED_VLM_CONN);

            commit();

            reportSuccess(vlmConn.getUuid());
            updateSatellites(vlmConn);
        }
        catch (ApiCallHandlerFailedException ignore)
        {
            // a report and a corresponding api-response already created. nothing to do here
        }
        catch (Exception | ImplementationError exc)
        {
            reportStatic(
                exc,
                ApiCallType.CREATE,
                getObjectDescriptionInline(nodeName1Str, nodeName2Str, rscNameStr, vlmNrInt),
                getObjRefs(nodeName1Str, nodeName2Str, rscNameStr, vlmNrInt),
                getVariables(nodeName1Str, nodeName2Str, rscNameStr, vlmNrInt),
                apiCallRc
            );
        }

        return apiCallRc;
    }

    public ApiCallRc modifyVolumeConnection(
        UUID rscConnUuid,
        String nodeName1Str,
        String nodeName2Str,
        String rscNameStr,
        int vlmNrInt,
        Map<String, String> overrideProps,
        Set<String> deletePropKeys
    )
    {
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();

        try (
            AbsApiCallHandler basicallyThis = setContext(
                ApiCallType.MODIFY,
                apiCallRc,
                nodeName1Str,
                nodeName2Str,
                rscNameStr,
                vlmNrInt
            );
        )
        {
            VolumeConnectionData vlmConn = loadVlmConn(nodeName1Str, nodeName2Str, rscNameStr, vlmNrInt);

            if (rscConnUuid != null && !rscConnUuid.equals(vlmConn.getUuid()))
            {
                addAnswer(
                    "UUID-check failed",
                    ApiConsts.FAIL_UUID_VLM_CONN
                );
                throw new ApiCallHandlerFailedException();
            }

            Props props = getProps(vlmConn);
            Map<String, String> propsMap = props.map();

            fillProperties(overrideProps, getProps(vlmConn), ApiConsts.FAIL_ACC_DENIED_VLM_CONN);

            for (String delKey : deletePropKeys)
            {
                propsMap.remove(delKey);
            }

            commit();

            reportSuccess(vlmConn.getUuid());
            updateSatellites(vlmConn);
        }
        catch (ApiCallHandlerFailedException ignore)
        {
            // a report and a corresponding api-response already created. nothing to do here
        }
        catch (Exception | ImplementationError exc)
        {
            reportStatic(
                exc,
                ApiCallType.MODIFY,
                getObjectDescriptionInline(nodeName1Str, nodeName2Str, rscNameStr, vlmNrInt),
                getObjRefs(nodeName1Str, nodeName2Str, rscNameStr, vlmNrInt),
                getVariables(nodeName1Str, nodeName2Str, rscNameStr, vlmNrInt),
                apiCallRc
            );
        }

        return apiCallRc;
    }

    public ApiCallRc deleteVolumeConnection(
        String nodeName1Str,
        String nodeName2Str,
        String rscNameStr,
        int vlmNrInt
    )
    {
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();

        try (
            AbsApiCallHandler basicallyThis = setContext(
                ApiCallType.DELETE,
                apiCallRc,
                nodeName1Str,
                nodeName2Str,
                rscNameStr,
                vlmNrInt
            );
        )
        {
            VolumeConnectionData vlmConn = loadVlmConn(nodeName1Str, nodeName2Str, rscNameStr, vlmNrInt);
            if (vlmConn == null)
            {
                addAnswer(
                    "Could not delete " + getObjectDescriptionInline() + " as it does not exist.",
                    ApiConsts.WARN_NOT_FOUND
                );
            }
            else
            {
                UUID vlmConnUuid = vlmConn.getUuid();
                delete(vlmConn);
                commit();

                reportSuccess(vlmConnUuid);
            }
        }
        catch (ApiCallHandlerFailedException ignore)
        {
            // a report and a corresponding api-response already created. nothing to do here
        }
        catch (Exception | ImplementationError exc)
        {
            reportStatic(
                exc,
                ApiCallType.DELETE,
                getObjectDescriptionInline(nodeName1Str, nodeName2Str, rscNameStr, vlmNrInt),
                getObjRefs(nodeName1Str, nodeName2Str, rscNameStr, vlmNrInt),
                getVariables(nodeName1Str, nodeName2Str, rscNameStr, vlmNrInt),
                apiCallRc
            );
        }

        return apiCallRc;
    }

    private AbsApiCallHandler setContext(
        ApiCallType type,
        ApiCallRcImpl apiCallRc,
        String nodeName1,
        String nodeName2,
        String rscNameStr,
        Integer vlmNr
    )
    {
        super.setContext(
            type,
            apiCallRc,
            true, // autoClose
            getObjRefs(nodeName1, nodeName2, rscNameStr, vlmNr),
            getVariables(nodeName1, nodeName2, rscNameStr, vlmNr)
        );

        currentNodeName1 = nodeName1;
        currentNodeName2 = nodeName2;
        currentRscName = rscNameStr;
        currentVlmNr = vlmNr;

        return this;
    }

    private Map<String, String> getObjRefs(
        String nodeName1Str,
        String nodeName2Str,
        String rscNameStr,
        Integer vlmNr
    )
    {
        Map<String, String> objRefs = new TreeMap<>();
        objRefs.put(ApiConsts.KEY_1ST_NODE, nodeName1Str);
        objRefs.put(ApiConsts.KEY_2ND_NODE, nodeName2Str);
        objRefs.put(ApiConsts.KEY_RSC_DFN, rscNameStr);
        if (vlmNr != null)
        {
            objRefs.put(ApiConsts.KEY_VLM_NR, Integer.toString(vlmNr));
        }
        return objRefs;
    }

    private Map<String, String> getVariables(
        String nodeName1Str,
        String nodeName2Str,
        String rscNameStr,
        Integer vlmNr
    )
    {
        Map<String, String> vars = new TreeMap<>();
        vars.put(ApiConsts.KEY_1ST_NODE_NAME, nodeName1Str);
        vars.put(ApiConsts.KEY_2ND_NODE_NAME, nodeName2Str);
        vars.put(ApiConsts.KEY_RSC_NAME, rscNameStr);
        if (vlmNr != null)
        {
            vars.put(ApiConsts.KEY_VLM_NR, Integer.toString(vlmNr));
        }
        return vars;
    }

    @Override
    protected String getObjectDescription()
    {
        return "Volume connection between nodes " + currentNodeName1 + " and " +
            currentNodeName2 + " on resource " + currentRscName + " on volume number " +
            currentVlmNr;
    }

    @Override
    protected String getObjectDescriptionInline()
    {
        return getObjectDescriptionInline(
            currentNodeName1,
            currentNodeName2,
            currentRscName,
            currentVlmNr
        );
    }

    private String getObjectDescriptionInline(
        String nodeName1Str,
        String nodeName2Str,
        String rscNameStr,
        int vlmNr
    )
    {
        return "volume connection between nodes '" + nodeName1Str + "' and '" +
            nodeName2Str + "' on resource '" + rscNameStr + "' on volume number '" +
            vlmNr + "'";
    }

    private VolumeConnectionData createVlmConn(
        String nodeName1Str,
        String nodeName2Str,
        String rscNameStr,
        int vlmNrInt
    )
    {
        NodeData node1 = loadNode(nodeName1Str, true);
        NodeData node2 = loadNode(nodeName2Str, true);

        Resource rsc1 = getRsc(node1, rscNameStr);
        Resource rsc2 = getRsc(node2, rscNameStr);

        Volume sourceVolume = getVlm(rsc1, vlmNrInt);
        Volume targetVolume = getVlm(rsc2, vlmNrInt);

        VolumeConnectionData vlmConn;
        try
        {
            vlmConn = volumeConnectionDataFactory.getInstance(
                peerAccCtx,
                sourceVolume,
                targetVolume,
                true,
                true
            );
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw asAccDeniedExc(
                accDeniedExc,
                "create " + getObjectDescriptionInline(),
                ApiConsts.FAIL_ACC_DENIED_VLM_CONN
            );
        }
        catch (LinStorDataAlreadyExistsException alreadyExistsExc)
        {
            throw asExc(
                alreadyExistsExc,
                getObjectDescription() + " already exists",
                ApiConsts.FAIL_EXISTS_VLM_CONN
            );
        }
        catch (SQLException sqlExc)
        {
            throw asSqlExc(
                sqlExc,
                "creating " + getObjectDescriptionInline()
            );
        }
        return vlmConn;
    }

    private VolumeConnectionData loadVlmConn(
        String nodeName1,
        String nodeName2,
        String rscNameStr,
        int vlmNr
    )
        throws ApiCallHandlerFailedException
    {
        NodeData node1 = loadNode(nodeName1, true);
        NodeData node2 = loadNode(nodeName2, true);

        Resource rsc1 = getRsc(node1, rscNameStr);
        Resource rsc2 = getRsc(node2, rscNameStr);

        Volume vlm1 = getVlm(rsc1, vlmNr);
        Volume vlm2 = getVlm(rsc2, vlmNr);

        VolumeConnectionData vlmConn;
        try
        {
            vlmConn = volumeConnectionDataFactory.getInstance(
                peerAccCtx,
                vlm1,
                vlm2,
                false,
                false
            );
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw asAccDeniedExc(
                accDeniedExc,
                "access " + getObjectDescriptionInline(),
                ApiConsts.FAIL_ACC_DENIED_VLM_CONN
            );
        }
        catch (LinStorDataAlreadyExistsException dataAlreadyExistsExc)
        {
            throw asImplError(dataAlreadyExistsExc);
        }
        catch (SQLException sqlExc)
        {
            throw asSqlExc(
                sqlExc,
                "loading " + getObjectDescriptionInline()
            );
        }
        return vlmConn;
    }

    private Resource getRsc(NodeData node, String rscNameStr) throws ApiCallHandlerFailedException
    {
        Resource rsc;
        try
        {
            rsc = node.getResource(peerAccCtx, asRscName(rscNameStr));
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw asAccDeniedExc(
                accDeniedExc,
                "access resource '" + rscNameStr + "' from node '" + node.getName().displayValue + "'",
                ApiConsts.FAIL_ACC_DENIED_NODE
            );
        }
        return rsc;
    }

    private Volume getVlm(Resource rsc, int vlmNr)
    {
        return rsc.getVolume(asVlmNr(vlmNr));
    }

    private Props getProps(VolumeConnectionData vlmConn)
    {
        Props props;
        try
        {
            props = vlmConn.getProps(peerAccCtx);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw asAccDeniedExc(
                accDeniedExc,
                "accessing properties of " + getObjectDescriptionInline(),
                ApiConsts.FAIL_ACC_DENIED_VLM_CONN
            );
        }
        return props;
    }

    private void delete(VolumeConnectionData vlmConn)
    {
        try
        {
            vlmConn.delete(peerAccCtx);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw asAccDeniedExc(
                accDeniedExc,
                "delete " + getObjectDescriptionInline(),
                ApiConsts.FAIL_ACC_DENIED_VLM_CONN
            );
        }
        catch (SQLException sqlExc)
        {
            throw asSqlExc(
                sqlExc,
                "deleting " + getObjectDescriptionInline()
            );
        }
    }

    private void updateSatellites(VolumeConnectionData vlmConn)
    {
        try
        {
            updateSatellites(vlmConn.getSourceVolume(apiCtx).getResourceDefinition());
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw asImplError(accDeniedExc);
        }
    }
}
