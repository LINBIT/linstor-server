package com.linbit.linstor.layer;

import com.linbit.ImplementationError;
import com.linbit.exceptions.InvalidSizeException;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.AbsResource;
import com.linbit.linstor.core.objects.AbsVolume;
import com.linbit.linstor.core.objects.VolumeDefinition;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.utils.layer.LayerRscUtils;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.util.Map;
import java.util.Set;

@Singleton
public class LayerSizeHelper
{
    private final Map<DeviceLayerKind, AbsLayerSizeCalculator<?>> kindToCalculatorMap;

    @Inject
    public LayerSizeHelper(
        Map<DeviceLayerKind, AbsLayerSizeCalculator<?>> kindToCalculatorMapRef
    )
    {
        kindToCalculatorMap = kindToCalculatorMapRef;
    }

    /**
     * Updates the allocation and/or usable sizes of all vlmData, starting with the parameter and everything below it.
     *
     */
    public <RSC extends AbsResource<RSC>, VLM_TYPE extends VlmProviderObject<RSC>> void calculateSize(
        VLM_TYPE vlmDataRef
    )
        throws InvalidSizeException
    {
        AbsLayerSizeCalculator<VLM_TYPE> sizeCalc = getLayerSizeCalculator(vlmDataRef.getLayerKind());
        if (sizeCalc == null)
        {
            throw new ImplementationError("Unknown layerKind: " + vlmDataRef.getLayerKind());
        }

        VolumeDefinition vlmDfn = vlmDataRef.getVolume().getVolumeDefinition();
        long vlmDfnSize = vlmDfn.getVolumeSize();
        boolean calculateNetSizes = vlmDfn.getFlags().isSet(VolumeDefinition.Flags.GROSS_SIZE);

        try
        {
            if (calculateNetSizes)
            {
                vlmDataRef.setAllocatedSize(vlmDfnSize);
                sizeCalc.updateUsableSizeFromAllocatedSize(vlmDataRef);
                vlmDataRef.setOriginalSize(vlmDfnSize);
            }
            else
            {
                vlmDataRef.setUsableSize(vlmDfnSize);
                sizeCalc.updateAllocatedSizeFromUsableSize(vlmDataRef);
                vlmDataRef.setOriginalSize(vlmDfnSize);
            }
        }
        catch (DatabaseException exc)
        {
            // the used setters should not have a database driver
            throw new ImplementationError(exc);
        }
    }

    /**
     * Updates the allocation and/or usable sizes of all vlmData, starting with the parameter and everything below it.
     *
     * @return The allocation size of the STORAGE layer with the specified {@code rscSuffixRef}
     *
     */
    public <RSC extends AbsResource<RSC>> long calculateSize(
        VlmProviderObject<RSC> vlmDataRef,
        String rscSuffixRef
    )
        throws InvalidSizeException
    {
        long ret;

        calculateSize(vlmDataRef);

        Set<AbsRscLayerObject<RSC>> storRscDataSet = LayerRscUtils.getRscDataByLayer(
            vlmDataRef.getRscLayerObject(),
            DeviceLayerKind.STORAGE,
            rscSuffixRef::equalsIgnoreCase
        );
        if (storRscDataSet.size() > 1)
        {
            StringBuilder sb = new StringBuilder(
                "More than one storage resources with the same suffix exists:\n"
            );
            for (AbsRscLayerObject<RSC> storRscData : storRscDataSet)
            {
                sb.append("  ")
                    .append(storRscData.getSuffixedResourceName())
                    .append(" (ID: ")
                    .append(storRscData.getRscLayerId())
                    .append(")\n");
            }
            sb.setLength(sb.length() - 1); // cut last "/n"
            throw new ImplementationError(sb.toString());
        }
        if (storRscDataSet.isEmpty())
        {
            // might happen in NVMe setups where one tree ends with the NVMe target (i.e. no STORAGE layer)
            ret = vlmDataRef.getVolume().getVolumeSize();
        }
        else
        {
            ret = storRscDataSet.iterator()
                .next()
                .getVlmProviderObject(vlmDataRef.getVlmNr())
                .getAllocatedSize();
        }
        return ret;
    }

    public <RSC extends AbsResource<RSC>> long calculateSize(
        AbsVolume<RSC> vlmRef,
        String rscSuffixRef
    )
        throws InvalidSizeException
    {
        long ret;

        AbsRscLayerObject<RSC> layerData = vlmRef.getAbsResource().getLayerData();
        VolumeDefinition vlmDfn = vlmRef.getVolumeDefinition();
        if (layerData == null)
        {
            ret = vlmDfn.getVolumeSize();
        }
        else
        {
            ret = calcSize(layerData, vlmDfn, rscSuffixRef);
        }
        return ret;
    }

    /**
     * Calls {@link #calculateSize(VlmProviderObject)} and returns the usableSize of the given volume /
     * rscLayerSuffix
     *
     */
    private <RSC extends AbsResource<RSC>> long calcSize(
        AbsRscLayerObject<RSC> layerDataRef,
        VolumeDefinition vlmDfnRef,
        String rscLayerSuffixRef
    )
        throws InvalidSizeException
    {
        long ret;

        VolumeNumber vlmNr = vlmDfnRef.getVolumeNumber();
        calculateSize(layerDataRef.getVlmProviderObject(vlmNr));

        Set<AbsRscLayerObject<RSC>> storRscDataSet = LayerRscUtils.getRscDataByLayer(
            layerDataRef,
            DeviceLayerKind.STORAGE,
            rscLayerSuffixRef::equalsIgnoreCase
        );
        if (storRscDataSet.size() > 1)
        {
            StringBuilder sb = new StringBuilder(
                "More than one storage resources with the same suffix exists:\n"
            );
            for (AbsRscLayerObject<RSC> storRscData : storRscDataSet)
            {
                sb.append("  ")
                    .append(storRscData.getSuffixedResourceName())
                    .append(" (ID: ")
                    .append(storRscData.getRscLayerId())
                    .append(")\n");
            }
            sb.setLength(sb.length() - 1); // cut last "/n"
            throw new ImplementationError(sb.toString());
        }
        if (storRscDataSet.isEmpty())
        {
            // might happen in NVMe setups where one tree ends with the NVMe target (i.e. no STORAGE layer)
            ret = vlmDfnRef.getVolumeSize();
        }
        else
        {
            ret = storRscDataSet.iterator()
                .next()
                .getVlmProviderObject(vlmNr)
                .getUsableSize();
        }
        return ret;
    }

    @SuppressWarnings("unchecked")
    <VLM_TYPE extends VlmProviderObject<?>> AbsLayerSizeCalculator<VLM_TYPE> getLayerSizeCalculator(
        DeviceLayerKind layerKindRef
    )
    {
        return (AbsLayerSizeCalculator<VLM_TYPE>) kindToCalculatorMap.get(layerKindRef);
    }
}
