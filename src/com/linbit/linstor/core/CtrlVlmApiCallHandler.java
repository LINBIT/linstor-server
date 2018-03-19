package com.linbit.linstor.core;

import com.linbit.ImplementationError;
import com.linbit.linstor.ResourceData;
import com.linbit.linstor.Volume;
import com.linbit.linstor.Volume.VlmFlags;
import com.linbit.linstor.VolumeDefinition;
import com.linbit.linstor.VolumeDefinition.VlmDfnFlags;
import com.linbit.linstor.VolumeNumber;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.transaction.TransactionMgr;

import javax.inject.Inject;
import javax.inject.Provider;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

public class CtrlVlmApiCallHandler extends AbsApiCallHandler
{
    private String currentNodeName;
    private String currentRscName;
    private Integer currentVlmNr;

    @Inject
    protected CtrlVlmApiCallHandler(
        ErrorReporter errorReporterRef,
        @ApiContext AccessContext apiCtxRef,
        CtrlObjectFactories objectFactories,
        Provider<TransactionMgr> transMgrProviderRef,
        @PeerContext AccessContext peerAccCtxRef,
        Peer peerRef
    )
    {
        super(
            errorReporterRef,
            apiCtxRef,
            ApiConsts.MASK_VLM,
            null, // interComSerializer
            objectFactories,
            transMgrProviderRef,
            peerAccCtxRef,
            peerRef
        );
    }

    ApiCallRc volumeDeleted(
        String nodeNameStr,
        String rscNameStr,
        int volumeNr
    )
    {
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
        try (
            AbsApiCallHandler basicallyThis = setContext(
                ApiCallType.DELETE,
                apiCallRc,
                nodeNameStr,
                rscNameStr,
                volumeNr
            )
        )
        {
            ResourceData rscData = loadRsc(nodeNameStr, rscNameStr, true);
            VolumeNumber volumeNumber = asVlmNr(volumeNr);

            Volume vlm = rscData.getVolume(volumeNumber);
            UUID vlmUuid = vlm.getUuid(); // prevent access to deleted object

            markClean(vlm);

            boolean allVlmsClean = true;
            VolumeDefinition vlmDfn = vlm.getVolumeDefinition();
            UUID vlmDfnUuid = vlmDfn.getUuid();
            Iterator<Volume> vlmIterator = getVolumeIterator(vlmDfn);
            while (vlmIterator.hasNext())
            {
                Volume volume = vlmIterator.next();
                if (!isMarkedAsClean(volume))
                {
                    allVlmsClean = false;
                    break;
                }
            }
            boolean vlmDfnDeleted = false;
            if (allVlmsClean && isMarkedForDeletion(vlmDfn))
            {
                delete(vlmDfn); // also deletes all of its volumes
                vlmDfnDeleted = true;
            }

            commit();

            reportSuccess(vlmUuid);
            errorReporter.logDebug(
                String.format("Volume with number '%d' deleted on node '%s'.", volumeNr, nodeNameStr)
            );
            if (vlmDfnDeleted)
            {
                reportSuccess(
                    "Volume definition with number '" + volumeNumber.value + "' of resource definition '" +
                        rscData.getDefinition().getName().displayValue + "' deleted.",
                        "Volume definition's UUID was: " + vlmDfnUuid.toString(),
                        ApiConsts.MASK_DEL | ApiConsts.MASK_VLM_DFN | ApiConsts.DELETED
                );
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
                getObjectDescriptionInline(nodeNameStr, rscNameStr, volumeNr),
                getObjRefs(nodeNameStr, rscNameStr, volumeNr),
                getVariables(nodeNameStr, rscNameStr, volumeNr),
                apiCallRc
            );
        }

        return apiCallRc;
    }

    private AbsApiCallHandler setContext(
        ApiCallType type,
        ApiCallRcImpl apiCallRc,
        String nodeNameStr,
        String rscNameStr,
        Integer vlmNr
    )
    {
        super.setContext(
            type,
            apiCallRc,
            true, // autoClose
            getObjRefs(nodeNameStr, rscNameStr, vlmNr),
            getVariables(nodeNameStr, rscNameStr, vlmNr)
        );
        currentNodeName = nodeNameStr;
        currentRscName = rscNameStr;
        currentVlmNr = vlmNr;

        return this;
    }

    @Override
    protected String getObjectDescription()
    {
        return "Node: " + currentNodeName + ", Resource: " + currentRscName +
            " Volume number: " + currentVlmNr;
    }

    @Override
    protected String getObjectDescriptionInline()
    {
        return getObjectDescriptionInline(currentNodeName, currentRscName, currentVlmNr);
    }

    private String getObjectDescriptionInline(String nodeNameStr, String rscNameStr, Integer vlmNr)
    {
        return "volume with volume number '" + vlmNr + "' on resource '" + rscNameStr + "' on node '" +
            nodeNameStr + "'";
    }

    private Map<String, String> getObjRefs(String nodeNameStr, String rscNameStr, Integer vlmNr)
    {
        Map<String, String> map = new TreeMap<>();
        map.put(ApiConsts.KEY_NODE, nodeNameStr);
        map.put(ApiConsts.KEY_RSC_DFN, rscNameStr);
        if (vlmNr != null)
        {
            map.put(ApiConsts.KEY_VLM_NR, rscNameStr);
        }
        return map;
    }

    private Map<String, String> getVariables(String nodeNameStr, String rscNameStr, Integer vlmNr)
    {
        Map<String, String> map = new TreeMap<>();
        map.put(ApiConsts.KEY_NODE_NAME, nodeNameStr);
        map.put(ApiConsts.KEY_RSC_NAME, rscNameStr);
        if (vlmNr != null)
        {
            map.put(ApiConsts.KEY_VLM_NR, rscNameStr);
        }
        return map;
    }

    private void markClean(Volume vol)
    {
        try
        {
            vol.getFlags().enableFlags(apiCtx, VlmFlags.CLEAN);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw asImplError(accDeniedExc);
        }
        catch (SQLException sqlExc)
        {
            throw asSqlExc(
                sqlExc,
                "marking " + getObjectDescriptionInline() + " as clean."
            );
        }
    }

    private Iterator<Volume> getVolumeIterator(VolumeDefinition vlmDfn)
    {
        Iterator<Volume> iterator;
        try
        {
            iterator = vlmDfn.iterateVolumes(apiCtx);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw asImplError(accDeniedExc);
        }
        return iterator;
    }

    private boolean isMarkedAsClean(Volume vlm)
    {
        boolean isMarkedAsClean;
        try
        {
            isMarkedAsClean = vlm.getFlags().isSet(apiCtx, VlmFlags.CLEAN);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw asImplError(accDeniedExc);
        }
        return isMarkedAsClean;
    }

    private boolean isMarkedForDeletion(VolumeDefinition vlmDfn)
    {
        boolean isMarkedAsDeleted;
        try
        {
            isMarkedAsDeleted = vlmDfn.getFlags().isSet(apiCtx, VlmDfnFlags.DELETE);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw asImplError(accDeniedExc);
        }
        return isMarkedAsDeleted;
    }

    private void delete(VolumeDefinition vlmDfn)
    {
        try
        {
            vlmDfn.delete(apiCtx);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw asImplError(accDeniedExc);
        }
        catch (SQLException sqlExc)
        {
            throw asSqlExc(
                sqlExc,
                "deleting " + CtrlVlmDfnApiCallHandler.getObjectDescriptionInline(
                    vlmDfn.getResourceDefinition().getName().displayValue,
                    vlmDfn.getVolumeNumber().value
                )
            );
        }
    }

}
