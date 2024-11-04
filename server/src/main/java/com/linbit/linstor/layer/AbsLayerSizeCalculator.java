package com.linbit.linstor.layer;

import com.linbit.linstor.PriorityProps;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.core.StltConfigAccessor;
import com.linbit.linstor.core.objects.AbsResource;
import com.linbit.linstor.core.objects.AbsVolume;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.ResourceGroup;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.core.objects.VolumeDefinition;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.ReadOnlyProps;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

public abstract class AbsLayerSizeCalculator<VLM_TYPE extends VlmProviderObject<?>>
{
    /**
     * The only purpose of this class is to make it easier to later add parameters for the actual
     * {@link AbsLayerSizeCalculator}.
     * Using this class, we only need to add the new parameter to the constructor {@link AbsLayerSizeCalculatorInit},
     * and the constructor of {@link AbsLayerSizeCalculator} can grab that from its {@code initRef}.
     *
     * The alternative would require every subclass of {@link AbsLayerSizeCalculator} to add such a new parameter to
     * pass it to its {@code super(...);} call
     */
    @Singleton
    public static class AbsLayerSizeCalculatorInit
    {
        private final AccessContext sysCtx;
        private final ErrorReporter errorReporter;
        private final StltConfigAccessor stltCfgAccessor;
        private final Provider<LayerSizeHelper> layerSizeHelper;

        @Inject
        public AbsLayerSizeCalculatorInit(
            @SystemContext AccessContext sysCtxRef,
            ErrorReporter errorReporterRef,
            StltConfigAccessor stltCfgAccessorRef,
            Provider<LayerSizeHelper> layerSizeHelperProviderRef
        )
        {
            sysCtx = sysCtxRef;
            errorReporter = errorReporterRef;
            stltCfgAccessor = stltCfgAccessorRef;
            layerSizeHelper = layerSizeHelperProviderRef;
        }
    }

    protected final AccessContext sysCtx;
    protected final DeviceLayerKind kind;
    protected final ReadOnlyProps stltProps;
    protected final ErrorReporter errorReporter;
    protected final Provider<LayerSizeHelper> layerSizeHelper;

    protected AbsLayerSizeCalculator(AbsLayerSizeCalculatorInit initRef, DeviceLayerKind kindRef)
    {
        sysCtx = initRef.sysCtx;
        errorReporter = initRef.errorReporter;
        stltProps = initRef.stltCfgAccessor.getReadonlyProps();
        layerSizeHelper = initRef.layerSizeHelper;
        kind = kindRef;
    }

    public DeviceLayerKind getKind()
    {
        return kind;
    }

    /**
     * Returns the next {@link AbsLayerSizeCalculator} depending on the {@link DeviceLayerKind} of the given
     * {@code vlmData}.
     *
     * @param vlmData
     */
    public AbsLayerSizeCalculator<VlmProviderObject<?>> getLayerSizeCalculator(VlmProviderObject<?> vlmData)
    {
        return layerSizeHelper.get().getLayerSizeCalculator(vlmData.getRscLayerObject().getLayerKind());
    }

    /**
     * The layer implementing this method can assume that its usable size is already set.
     * The implementation is expected to update its own allocated size.
     * The implementation is expected to call an update*Size method for the next layer
     *
     * @param vlmDataRef
     *
     * @throws DatabaseException
     * @throws AccessDeniedException
     */
    public final void updateAllocatedSizeFromUsableSize(VlmProviderObject<?> vlmDataRef)
        throws AccessDeniedException, DatabaseException
    {
        AbsLayerSizeCalculator<VlmProviderObject<?>> sizeCalc = getLayerSizeCalculator(vlmDataRef);
        String suffixedRscName = vlmDataRef.getRscLayerObject().getSuffixedResourceName();
        errorReporter.logTrace(
            "Layer '%s' updating gross size of volume '%s/%d' (usable: %d)",
            sizeCalc.kind.name(),
            suffixedRscName,
            vlmDataRef.getVlmNr().value,
            vlmDataRef.getUsableSize()
        );

        sizeCalc.updateAllocatedSizeFromUsableSizeImpl(vlmDataRef);

        errorReporter.logTrace(
            "Layer '%s' finished calculating sizes of volume '%s/%d'. Allocated: %d, usable: %d",
            sizeCalc.kind.name(),
            suffixedRscName,
            vlmDataRef.getVlmNr().value,
            vlmDataRef.getAllocatedSize(),
            vlmDataRef.getUsableSize()
        );
    }

    /**
     * The layer implementing this method can assume that its allocated size is already set.
     * The implementation is expected to update its own usable size.
     * The implementation might lower its allocated size if the layer below provides less usable size.
     * The implementation is expected to call an update*Size method for the next layer
     *
     * @param vlmDataRef
     *
     * @throws DatabaseException
     * @throws AccessDeniedException
     */
    public final void updateUsableSizeFromAllocatedSize(VlmProviderObject<?> vlmDataRef)
        throws AccessDeniedException, DatabaseException
    {
        AbsLayerSizeCalculator<VlmProviderObject<?>> sizeCalc = getLayerSizeCalculator(vlmDataRef);
        String kindName = sizeCalc.kind.name();
        String suffixedRscName = vlmDataRef.getRscLayerObject().getSuffixedResourceName();
        int vlmNr = vlmDataRef.getVlmNr().value;
        errorReporter.logTrace(
            "Layer '%s' updating net size of volume '%s/%d' (allocated: %d)",
            kindName,
            suffixedRscName,
            vlmNr,
            vlmDataRef.getAllocatedSize()
        );

        sizeCalc.updateUsableSizeFromAllocatedSizeImpl(vlmDataRef);

        errorReporter.logTrace(
            "Layer '%s' finished calculating sizes of volume '%s/%d'. Allocated: %d, usable: %d",
            kindName,
            suffixedRscName,
            vlmNr,
            vlmDataRef.getAllocatedSize(),
            vlmDataRef.getUsableSize()
        );
    }

    /**
     * Returns a {@link PriorityProps} with the order of:
     * <ul>
     * <li>VolumeDefinition</li>
     * <li>VolumeGroup</li>
     * <li>Resource</li>
     * <li>ResourceDefinition</li>
     * <li>ResourceGroup</li>
     * <li>Node</li>
     * <li>Satellite</li>
     * </ul>
     *
     * @param vlmRef
     *
     * @return
     *
     * @throws AccessDeniedException
     */
    protected PriorityProps getPrioProps(AbsVolume<?> vlmRef) throws AccessDeniedException
    {
        VolumeDefinition vlmDfn = vlmRef.getVolumeDefinition();
        ResourceDefinition rscDfn = vlmRef.getResourceDefinition();
        ResourceGroup rscGrp = rscDfn.getResourceGroup();
        ReadOnlyProps absRscProps;
        AbsResource<?> absRsc = vlmRef.getAbsResource();
        if (vlmRef instanceof Volume)
        {
            absRscProps = ((Resource) absRsc).getProps(sysCtx);
        }
        else
        {
            absRscProps = ((Snapshot) absRsc).getRscProps(sysCtx);
        }
        Node node = absRsc.getNode();
        return new PriorityProps(
            vlmDfn.getProps(sysCtx),
            rscGrp.getVolumeGroupProps(sysCtx, vlmDfn.getVolumeNumber()),
            absRscProps,
            rscDfn.getProps(sysCtx),
            rscGrp.getProps(sysCtx),
            node.getProps(sysCtx),
            stltProps
        );
    }

    /**
     * The layer implementing this method can assume that its usable size is already set.
     * The implementation is expected to update its own allocated size.
     * The implementation is expected to call an update*Size method for the next layer
     *
     * @param vlmData
     *
     * @throws AccessDeniedException
     * @throws DatabaseException
     */
    protected abstract void updateAllocatedSizeFromUsableSizeImpl(VLM_TYPE vlmData)
        throws AccessDeniedException, DatabaseException;

    /**
     * The layer implementing this method can assume that its allocated size is already set.
     * The implementation is expected to update its own usable size.
     * The implementation might lower its allocated size if the layer below provides less usable size.
     * The implementation is expected to call an update*Size method for the next layer
     *
     * @param vlmData
     *
     * @throws AccessDeniedException
     * @throws DatabaseException
     */
    protected abstract void updateUsableSizeFromAllocatedSizeImpl(VLM_TYPE vlmData)
        throws AccessDeniedException, DatabaseException;
}
