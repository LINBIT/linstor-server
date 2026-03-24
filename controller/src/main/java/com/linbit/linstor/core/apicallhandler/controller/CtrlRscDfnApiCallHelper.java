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
    public static final String FULL_KEY_RS_DISC_GRAN = ApiConsts.NAMESPC_DRBD_DISK_OPTIONS + "/" +
        KEY_PROP_DRBD_OPT_DISK_RS_DISC_GRAN;

    public static final String KEY_PROP_DRBD_OPT_DISK_DISC_GRAN = "discard-granularity";
    public static final String FULL_KEY_DISCARD_GRAN = ApiConsts.NAMESPC_DRBD_DISK_OPTIONS + "/" +
        KEY_PROP_DRBD_OPT_DISK_DISC_GRAN;

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

    /**
     * Automatically updates the DRBD rs-discard-granularity and discard-granularity properties
     * for all volume definitions of the given resource definition.
     *
     * <p>For each volume definition, this method checks whether automatic management is enabled
     * for the respective property. If enabled, the property is recalculated based on the underlying
     * storage. If disabled, the property is removed unless it was explicitly set by the user
     * (indicated by the auto-flag being set to {@code false} on the volume definition level).
     *
     * @param rscDfnRef the resource definition whose volume definitions should be updated
     *
     * @return {@code true} if any DRBD properties were actually changed, {@code false} otherwise
     */
    public boolean updateDrbdProps(ResourceDefinition rscDfnRef)
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
                    @Nullable String autoProp = vlmDfnProps
                        .getProp(
                            ApiConsts.KEY_DRBD_AUTO_RS_DISCARD_GRANULARITY,
                            ApiConsts.NAMESPC_DRBD_OPTIONS
                        );
                    // TODO the prop was set on vlmdfn to mark that it is set by Linstor
                    // We still need to rework this, but need Linstor to know who set each property
                    // that is why it is removed here
                    if (!ApiConsts.VAL_FALSE.equalsIgnoreCase(autoProp))
                    {
                        @Nullable String removedProp = vlmDfnProps.removeProp(FULL_KEY_RS_DISC_GRAN);
                        ret |= removedProp != null;
                    }
                }

                if (isAutoManagingEnabled(
                    vlmDfn,
                    ApiConsts.KEY_DRBD_AUTO_DISCARD_GRANULARITY,
                    ApiConsts.NAMESPC_LINSTOR_DRBD,
                    peerCtx
                ))
                {
                    ret |= updateDiscardGranProp(vlmDfn, peerCtx);
                }
                else
                {
                    Props vlmDfnProps = vlmDfn.getProps(peerCtx);
                    @Nullable String autoProp = vlmDfnProps
                        .getProp(
                            ApiConsts.KEY_DRBD_AUTO_DISCARD_GRANULARITY,
                            ApiConsts.NAMESPC_LINSTOR_DRBD
                        );
                    // autoProp == null -> default true
                    // TODO the prop was set on vlmdfn to mark that it is set by Linstor
                    // We still need to rework this, but need Linstor to know who set each property
                    // that is why it is removed here
                    if (!ApiConsts.VAL_FALSE.equalsIgnoreCase(autoProp))
                    {
                        @Nullable String removedProp = vlmDfnProps.removeProp(FULL_KEY_DISCARD_GRAN);
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
                FULL_KEY_RS_DISC_GRAN,
                newProp
            );
            propChanged = !newProp.equals(oldProp);
        }
        else
        {
            String oldProp = vlmDfnProps.removeProp(FULL_KEY_RS_DISC_GRAN);
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
        final @Nullable String setBy = prioProps.getProp(keyRef, namespcRef);
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
            Property dynProp = rsc.getNode().getPeer(accCtxRef).getDynamicProperty(FULL_KEY_RS_DISC_GRAN);
            if (dynProp instanceof RangeProperty rangeProp)
            {
                maxDiscGrans.add(rangeProp.getMax());
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

    private boolean updateDiscardGranProp(
        final VolumeDefinition vlmDfnRef,
        final AccessContext peerCtx
    )
        throws AccessDeniedException, InvalidValueException, DatabaseException
    {
        boolean propChanged = false;
        final VolumeNumber vlmNr = vlmDfnRef.getVolumeNumber();
        final Props vlmDfnProps = vlmDfnRef.getProps(peerCtx);

        final TreeSet<Long> discGrans = new TreeSet<>();
        final Iterator<Resource> rscIt = vlmDfnRef.getResourceDefinition().iterateResource(peerCtx);
        while (rscIt.hasNext())
        {
            final Resource rsc = rscIt.next();
            final Set<AbsRscLayerObject<Resource>> drbdRscDataSet = LayerRscUtils.getRscDataByLayer(
                rsc.getLayerData(peerCtx),
                DeviceLayerKind.DRBD
            );
            for (AbsRscLayerObject<Resource> drbdRscData : drbdRscDataSet)
            {
                // We need the discard granularity of the block device immediately below DRBD,
                // which may be LUKS / CACHE / BCACHE / NVME / WRITECACHE / STORAGE. The satellite
                // populates getDiscGran() per layer via lsblk against each layer's device path.
                AbsRscLayerObject<Resource> childRscData = drbdRscData.getChildBySuffix(
                    RscLayerSuffixes.SUFFIX_DATA
                );
                VlmProviderObject<Resource> childVlmData = childRscData.getVlmProviderObject(vlmNr);
                discGrans.add(childVlmData.getDiscGran());
            }
        }

        @Nullable Long chosenDiscGran = chooseDiscardGran(discGrans);

        if (chosenDiscGran != null)
        {
            String newProp = Long.toString(chosenDiscGran);
            @Nullable String oldProp = vlmDfnProps.setProp(
                FULL_KEY_DISCARD_GRAN,
                newProp
            );
            propChanged = !newProp.equals(oldProp);
        }
        else
        {
            @Nullable String oldProp = vlmDfnProps.removeProp(FULL_KEY_DISCARD_GRAN);
            propChanged = oldProp != null;
        }
        return propChanged;
    }

    /**
     * Aggregation for discard-granularity:
     * - If no values collected: null (don't write)
     * - If any value is 0: return 0 (explicitly disable discards)
     * - Otherwise: return max of all values
     */
    private @Nullable Long chooseDiscardGran(TreeSet<Long> discGransRef)
    {
        discGransRef.remove(VlmProviderObject.UNINITIALIZED_SIZE);
        @Nullable Long selected;
        if (discGransRef.isEmpty())
        {
            selected = null;
        }
        else if (discGransRef.contains(0L))
        {
            selected = 0L;
        }
        else
        {
            selected = discGransRef.last();
        }
        return selected;
    }
}
