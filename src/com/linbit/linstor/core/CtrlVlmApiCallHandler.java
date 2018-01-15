package com.linbit.linstor.core;

import java.sql.SQLException;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import com.linbit.ImplementationError;
import com.linbit.TransactionMgr;
import com.linbit.linstor.ResourceData;
import com.linbit.linstor.Volume;
import com.linbit.linstor.Volume.VlmFlags;
import com.linbit.linstor.VolumeDefinition;
import com.linbit.linstor.VolumeDefinition.VlmDfnFlags;
import com.linbit.linstor.VolumeNumber;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;

public class CtrlVlmApiCallHandler extends AbsApiCallHandler
{
    private final ThreadLocal<String> currentNodeName = new ThreadLocal<>();
    private final ThreadLocal<String> currentRscName = new ThreadLocal<>();
    private final ThreadLocal<Integer> currentVlmNr = new ThreadLocal<>();


    protected CtrlVlmApiCallHandler(
        ApiCtrlAccessors apiCtrlAccessorsRef,
        AccessContext apiCtxRef
    )
    {
        super(
            apiCtrlAccessorsRef,
            apiCtxRef,
            ApiConsts.MASK_VLM
        );
        super.setNullOnAutoClose(
            currentNodeName,
            currentRscName,
            currentVlmNr
        );
    }

    ApiCallRc volumeDeleted(
        AccessContext accCtx,
        Peer client,
        String nodeNameStr,
        String rscNameStr,
        int volumeNr
    )
    {
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
        try (
            AbsApiCallHandler basicallyThis = setContext(
                accCtx,
                client,
                ApiCallType.DELETE,
                apiCallRc,
                null,
                nodeNameStr,
                rscNameStr,
                volumeNr
            )
        )
        {
            ResourceData rscData = loadRsc(nodeNameStr, rscNameStr);
            VolumeNumber volumeNumber = asVlmNr(volumeNr);

            Volume vlm = rscData.getVolume(volumeNumber);
            markClean(vlm);

            commit();

            boolean allVlmsClean = true;
            VolumeDefinition vlmDfn = vlm.getVolumeDefinition();
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
            if (allVlmsClean && isMarkedForDeletion(vlmDfn))
            {
                delete(vlmDfn); // also deletes all of its volumes
            }

            reportSuccess(vlm.getUuid());
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
                apiCallRc,
                accCtx,
                client
            );
        }

        return apiCallRc;
    }

    private AbsApiCallHandler setContext(
        AccessContext accCtx,
        Peer client,
        ApiCallType type,
        ApiCallRcImpl apiCallRc,
        TransactionMgr transMgr,
        String nodeNameStr,
        String rscNameStr,
        Integer vlmNr
    )
    {
        super.setContext(
            accCtx,
            client,
            type,
            apiCallRc,
            transMgr,
            getObjRefs(nodeNameStr, rscNameStr, vlmNr),
            getVariables(nodeNameStr, rscNameStr, vlmNr)
        );
        currentNodeName.set(nodeNameStr);
        currentRscName.set(rscNameStr);
        currentVlmNr.set(vlmNr);

        return this;
    }

    @Override
    protected String getObjectDescription()
    {
        return "Node: " + currentNodeName.get() + ", Resource: " + currentRscName.get() +
            " Volume number: " + currentVlmNr.get();
    }

    @Override
    protected String getObjectDescriptionInline()
    {
        return getObjectDescriptionInline(currentRscName.get(), currentNodeName.get(), currentVlmNr.get());
    }

    private String getObjectDescriptionInline(String nodeNameStr, String rscNameStr, Integer vlmNr)
    {
        return "resource '" + rscNameStr + "' with volume number '" + vlmNr + "' on node '" + nodeNameStr + "'";
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
        try
        {
            return vlmDfn.iterateVolumes(apiCtx);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw asImplError(accDeniedExc);
        }
    }

    private boolean isMarkedAsClean(Volume vlm)
    {
        try
        {
            return vlm.getFlags().isSet(apiCtx, VlmFlags.CLEAN);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw asImplError(accDeniedExc);
        }
    }

    private boolean isMarkedForDeletion(VolumeDefinition vlmDfn)
    {
        try
        {
            return vlmDfn.getFlags().isSet(apiCtx, VlmDfnFlags.DELETE);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw asImplError(accDeniedExc);
        }
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
