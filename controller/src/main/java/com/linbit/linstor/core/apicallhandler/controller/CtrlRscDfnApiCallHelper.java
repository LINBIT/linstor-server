package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ImplementationError;
import com.linbit.linstor.PriorityProps;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.prop.Property;
import com.linbit.linstor.api.prop.RangeProperty;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.core.apicallhandler.response.ApiDatabaseException;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.ResourceGroup;
import com.linbit.linstor.core.objects.VolumeDefinition;
import com.linbit.linstor.core.repository.SystemConfRepository;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.InvalidValueException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.data.RscLayerSuffixes;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.utils.layer.LayerRscUtils;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

@Singleton
public class CtrlRscDfnApiCallHelper
{
    public static final String KEY_PROP_DRBD_OPT_DISK_RS_DISC_GRAN = "rs-discard-granularity";
    public static final String FULL_KEY_DISC_GRAN = ApiConsts.NAMESPC_DRBD_DISK_OPTIONS + "/" +
        KEY_PROP_DRBD_OPT_DISK_RS_DISC_GRAN;

    private static final int DRBD_DISC_GRAN_MIN = 4 * 1024; // min 4k
    private static final int DRBD_DISC_GRAN_MAX = 1 * 1024 * 1024; // max 1M

    private final Provider<AccessContext> peerAccCtx;
    private final SystemConfRepository sysConfRepo;

    @Inject
    public CtrlRscDfnApiCallHelper(
        @PeerContext Provider<AccessContext> peerAccCtxRef,
        SystemConfRepository sysConfRepoRef
    )
    {
        peerAccCtx = peerAccCtxRef;
        sysConfRepo = sysConfRepoRef;
    }

    boolean updateDrbdProps(ResourceDefinition rscDfnRef)
    {
        final AccessContext peerCtx = peerAccCtx.get();
        boolean ret = false;
        try
        {
            final Iterator<VolumeDefinition> vlmDfnIt = rscDfnRef.iterateVolumeDfn(peerCtx);
            while (vlmDfnIt.hasNext())
            {
                final VolumeDefinition vlmDfn = vlmDfnIt.next();

                if (isAutoManagingEnabled(
                    vlmDfn,
                    ApiConsts.KEY_DRBD_AUTO_RS_DISCARD_GRANULARITY,
                    ApiConsts.NAMESPC_DRBD_OPTIONS,
                    peerCtx
                ))
                {
                    ret |= updateRsDiscardGranProp(rscDfnRef, peerCtx, vlmDfn);
                }
                else
                {
                    // unset rs-discard-granularity unless explicitly set
                    // we consider the prop to be explicitly set if the auto-* is disabled on vlmDfn level
                    Props vlmDfnProps = vlmDfn.getProps(peerCtx);
                    String autoProp = vlmDfnProps
                        .getProp(
                            ApiConsts.KEY_DRBD_AUTO_RS_DISCARD_GRANULARITY,
                            ApiConsts.NAMESPC_DRBD_OPTIONS
                        );
                    if (!ApiConsts.VAL_FALSE.equalsIgnoreCase(autoProp))
                    {
                        String removedProp = vlmDfnProps.removeProp(FULL_KEY_DISC_GRAN);
                        ret |= removedProp != null;
                    }
                }
            }

        }
        catch (AccessDeniedException exc)
        {
            throw new ApiAccessDeniedException(exc, "Calculating DRBD properties", ApiConsts.FAIL_ACC_DENIED_VLM_DFN);
        }
        catch (DatabaseException exc)
        {
            throw new ApiDatabaseException(exc);
        }
        catch (InvalidKeyException | InvalidValueException exc)
        {
            throw new ImplementationError(exc);
        }
        return ret;
    }

    private boolean updateRsDiscardGranProp(
        ResourceDefinition rscDfnRef,
        final AccessContext peerCtx,
        final VolumeDefinition vlmDfnRef
    )
        throws AccessDeniedException, InvalidValueException, DatabaseException
    {
        boolean propChanged = false;
        final VolumeNumber vlmNr = vlmDfnRef.getVolumeNumber();
        final Props vlmDfnProps = vlmDfnRef.getProps(peerCtx);

        final TreeSet<Long> discGrans = new TreeSet<>();
        List<Resource> rscs = new ArrayList<>();
        final Iterator<Resource> rscIt = rscDfnRef.iterateResource(peerCtx);
        while (rscIt.hasNext())
        {
            final Resource rsc = rscIt.next();
            rscs.add(rsc);
            final Set<AbsRscLayerObject<Resource>> drbdRscDataSet = LayerRscUtils.getRscDataByLayer(
                rsc.getLayerData(peerCtx),
                DeviceLayerKind.DRBD
            );
            // technically we only support zero or one DrbdRscData, but for this calculation we do not care
            // if there are more... maybe as a future feature?
            for (AbsRscLayerObject<Resource> drbdRscData : drbdRscDataSet)
            {
                // we have a DrbdRscData here, but regarding the "rs-discard-granularity" we need to
                // consider what the layer below DRBD reports
                AbsRscLayerObject<Resource> childRscData = drbdRscData.getChildBySuffix(
                    RscLayerSuffixes.SUFFIX_DATA
                );
                VlmProviderObject<Resource> childVlmData = childRscData.getVlmProviderObject(vlmNr);
                discGrans.add(childVlmData.getDiscGran());
            }
        }

        Long chooseDrbdDiscGran = chooseDrbdDiscGran(peerCtx, discGrans, rscs);

        if (chooseDrbdDiscGran != null)
        {
            String newProp = Long.toString(chooseDrbdDiscGran);
            String oldProp = vlmDfnProps.setProp(
                FULL_KEY_DISC_GRAN,
                newProp
            );
            propChanged = !newProp.equals(oldProp);
        }
        else
        {
            String oldProp = vlmDfnProps.removeProp(FULL_KEY_DISC_GRAN);
            propChanged = oldProp != null;
        }
        return propChanged;
    }

    private boolean isAutoManagingEnabled(
        VolumeDefinition vlmDfnRef,
        String keyRef,
        String namespcRef,
        AccessContext peerCtxRef
    )
        throws AccessDeniedException
    {
        ResourceDefinition rscDfn = vlmDfnRef.getResourceDefinition();
        ResourceGroup rscGrp = rscDfn.getResourceGroup();
        PriorityProps prioProps = new PriorityProps(
            vlmDfnRef.getProps(peerCtxRef),
            rscDfn.getProps(peerCtxRef),
            rscGrp.getVolumeGroupProps(peerCtxRef, vlmDfnRef.getVolumeNumber()),
            rscGrp.getProps(peerCtxRef),
            sysConfRepo.getCtrlConfForView(peerCtxRef)
        );
        final String setBy = prioProps.getProp(keyRef, namespcRef);
        return setBy == null || setBy.equalsIgnoreCase(ApiConsts.VAL_TRUE);
    }

    private @Nullable Long chooseDrbdDiscGran(
        AccessContext accCtxRef,
        TreeSet<Long> discGransRef,
        List<Resource> rscsRef
    )
        throws AccessDeniedException
    {
        long discGranMax = getMaxDiscGran(accCtxRef, rscsRef);
        discGransRef.remove(VlmProviderObject.UNINITIALIZED_SIZE);

        Long selected;
        if (discGransRef.isEmpty() || discGransRef.contains(0L))
        {
            // if either all peers reported -1 (uninitialized) or any of the peers report 0 as DISC-GRAN, do not write
            // any value for "rs-discard-granularity" in the .res files
            selected = null;
        }
        else
        {
            selected = discGransRef.last();
            if (selected != null)
            {
                selected = Math.max(DRBD_DISC_GRAN_MIN, selected);
                selected = Math.min(discGranMax, selected);
            }
        }
        return selected;
    }

    private long getMaxDiscGran(AccessContext accCtxRef, List<Resource> rscsRef) throws AccessDeniedException
    {
        TreeSet<Long> maxDiscGrans = new TreeSet<>();
        for (Resource rsc : rscsRef)
        {
            Property dynProp = rsc.getNode().getPeer(accCtxRef).getDynamicProperty(FULL_KEY_DISC_GRAN);
            if (dynProp instanceof RangeProperty)
            {
                maxDiscGrans.add(((RangeProperty) dynProp).getMax());
            }
        }
        long ret;
        if (maxDiscGrans.isEmpty())
        {
            ret = DRBD_DISC_GRAN_MAX;
        }
        else
        {
            ret = maxDiscGrans.first();
        }
        return ret;
    }
}
