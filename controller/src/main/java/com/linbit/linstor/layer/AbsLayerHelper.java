package com.linbit.linstor.layer;

import com.linbit.ExhaustedPoolException;
import com.linbit.ImplementationError;
import com.linbit.ValueInUseException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.Resource;
import com.linbit.linstor.ResourceDefinition;
import com.linbit.linstor.StorPool;
import com.linbit.linstor.Volume;
import com.linbit.linstor.VolumeDefinition;
import com.linbit.linstor.VolumeNumber;
import com.linbit.linstor.layer.CtrlLayerDataHelper.LayerResult;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.numberpool.DynamicNumberPool;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.interfaces.categories.RscDfnLayerObject;
import com.linbit.linstor.storage.interfaces.categories.RscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.VlmDfnLayerObject;
import com.linbit.linstor.storage.interfaces.categories.VlmProviderObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.utils.LayerDataFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

abstract class AbsLayerHelper<RSC extends RscLayerObject, VLM extends VlmProviderObject,
    RSC_DFN extends RscDfnLayerObject, VLM_DFN extends VlmDfnLayerObject>
{
    protected final ErrorReporter errorReporter;
    protected final AccessContext apiCtx;
    protected final LayerDataFactory layerDataFactory;
    protected final DynamicNumberPool layerRscIdPool;

    private final Class<RSC> rscClass;
    private final DeviceLayerKind kind;

    AbsLayerHelper(
        ErrorReporter errorReporterRef,
        AccessContext apiCtxRef,
        LayerDataFactory layerDataFactoryRef,
        DynamicNumberPool layerRscIdPoolRef,
        Class<RSC> rscClassRef,
        DeviceLayerKind kindRef
    )
    {
        errorReporter = errorReporterRef;
        apiCtx = apiCtxRef;
        layerDataFactory = layerDataFactoryRef;
        layerRscIdPool = layerRscIdPoolRef;
        rscClass = rscClassRef;
        kind = kindRef;
    }

    /**
     * If the current layer needs a special {@link RscDfnLayerObject}, this method will create it if the given
     * {@link ResourceDefinition} does not already have a layer data with the given layer type and
     * resource name suffix.
     *
     * If the resource definition already has such an object, it depends on the layer whether the
     * content can be merged / updated or not.
     */
    public RSC_DFN ensureResourceDefinitionExists(
        ResourceDefinition rscDfn,
        String rscNameSuffix,
        LayerPayload payload
    )
        throws AccessDeniedException, SQLException, ValueOutOfRangeException, ExhaustedPoolException,
            ValueInUseException
    {
        RSC_DFN rscDfnData = rscDfn.getLayerData(apiCtx, kind, rscNameSuffix);
        if (rscDfnData == null)
        {
            rscDfnData = createRscDfnData(rscDfn, rscNameSuffix, payload);
            if (rscDfnData != null)
            {
                rscDfn.setLayerData(apiCtx, rscDfnData);
            }
        }
        else
        {
            mergeRscDfnData(rscDfnData, payload);
        }

        return rscDfnData;
    }

    /**
     * If the current layer needs a special {@link VlmDfnLayerObject}, this method will create it if the given
     * {@link VolumeDefinition} does not already have a layer data with the given layer type and
     * resource name suffix.
     *
     * If the volume definition already has such an object, it depends on the layer whether the
     * content can be merged / updated or not.
     */
    public VLM_DFN ensureVolumeDefinitionExists(
        VolumeDefinition vlmDfn,
        String rscNameSuffix,
        LayerPayload payload
    )
        throws AccessDeniedException, SQLException, ValueOutOfRangeException, ExhaustedPoolException,
            ValueInUseException
    {
        VLM_DFN vlmDfnData = vlmDfn.getLayerData(apiCtx, kind, rscNameSuffix);
        if (vlmDfnData == null)
        {
            vlmDfnData = createVlmDfnData(vlmDfn, rscNameSuffix, payload);
            vlmDfn.setLayerData(apiCtx, vlmDfnData);
        }
        else
        {
            mergeVlmDfnData(vlmDfnData, payload);
        }
        return vlmDfnData;
    }

    /**
     * Creates a {@link RscLayerObject} if needed and wraps it in a LayerResult which additional contains
     * a list of resource name suffixes for the lower layers.
     *
     * The {@link RscLayerObject} is created if:
     * <ul>
     *  <li>the given {@link Resource} has no layer data as root data</li>
     *  <li>the parent object is not null and has a child that would match the object this method would create</li>
     * </ul>
     *
     * In case the desired object already exists, the object will be updated if possible according to the data taken
     * from the payload.
     *
     * If the object does not exist a new object is created and added to the parent object's list of children,
     * unless the parent object is null of course.
     *
     * @param layerStackRef
     * @throws LinStorException
     */
    @SuppressWarnings("unchecked")
    public LayerResult ensureRscDataCreated(
        Resource rscRef,
        LayerPayload payloadRef,
        String rscNameSuffixRef,
        RscLayerObject parentObjectRef
    )
        throws SQLException, ExhaustedPoolException, ValueOutOfRangeException,
            ValueInUseException, LinStorException
    {
        RSC rscData = null;
        if (parentObjectRef == null)
        {
            RscLayerObject rootData = rscRef.getLayerData(apiCtx);
            if (rootData != null && !rootData.getClass().equals(rscClass))
            {
                throw new ImplementationError(
                    "Expected null or instance of " + rscClass.getSimpleName() + ", but got instance of " +
                        rootData.getClass().getSimpleName()
                );
            }
            // Suppressing warning as we have already performed a class check.
            rscData = (RSC) rootData;
        }
        else
        {
            for (RscLayerObject child : parentObjectRef.getChildren())
            {
                if (rscNameSuffixRef.equals(child.getResourceNameSuffix()))
                {
                    // Suppressing warning as the layer list cannot be altered once deployed.
                    rscData = (RSC) child;
                    break;
                }
            }
        }

        if (rscData == null)
        {
            rscData = createRscData(rscRef, payloadRef, rscNameSuffixRef, parentObjectRef);
        }
        else
        {
            mergeRscData(rscData, payloadRef);
        }

        // create, merge or delete vlmData
        Map<VolumeNumber, VLM> vlmLayerObjects = (Map<VolumeNumber, VLM>) rscData.getVlmLayerObjects();
        List<VolumeNumber> existingVlmsDataToBeDeleted = new ArrayList<>(vlmLayerObjects.keySet());

        Iterator<Volume> iterateVolumes = rscRef.iterateVolumes();
        while (iterateVolumes.hasNext())
        {
            Volume vlm = iterateVolumes.next();

            VolumeDefinition vlmDfn = vlm.getVolumeDefinition();

            VolumeNumber vlmNr = vlmDfn.getVolumeNumber();
            if (!vlmLayerObjects.containsKey(vlmNr))
            {
                vlmLayerObjects.put(
                    vlmNr,
                    createVlmLayerData(rscData, vlm, payloadRef)
                );
            }
            existingVlmsDataToBeDeleted.remove(vlmNr);
        }

        for (VolumeNumber vlmNr : existingVlmsDataToBeDeleted)
        {
            vlmLayerObjects.remove(vlmNr);
        }

        return new LayerResult(
            rscData,
            getRscNameSuffixes(rscData)
        );
    }

    /**
     * Only returns the current rscData's resource name suffix by default
     *
     * @param rscDataRef
     * @return
     */
    protected List<String> getRscNameSuffixes(RSC rscDataRef)
    {
        return Arrays.asList(rscDataRef.getResourceNameSuffix());
    }

    /**
     * By default, this returns null, meaning that the layer above should be asked
     * @param vlmRef
     * @param childRef
     * @return
     */
    protected StorPool getStorPool(Volume vlmRef, RscLayerObject childRef)
    {
        return null;
    }

    protected abstract RSC createRscData(
        Resource rscRef,
        LayerPayload payloadRef,
        String rscNameSuffixRef,
        RscLayerObject parentObjectRef
    )
        throws AccessDeniedException, SQLException, ValueOutOfRangeException, ExhaustedPoolException,
            ValueInUseException;

    protected abstract void mergeRscData(
        RSC rscDataRef,
        LayerPayload payload
    )
        throws AccessDeniedException, SQLException;


    protected abstract RSC_DFN createRscDfnData(
        ResourceDefinition rscDfn,
        String rscNameSuffix,
        LayerPayload payload
    )
        throws AccessDeniedException, SQLException, ValueOutOfRangeException, ExhaustedPoolException,
            ValueInUseException;

    protected abstract void mergeRscDfnData(RSC_DFN rscDfn, LayerPayload payload)
        throws SQLException, ExhaustedPoolException, ValueOutOfRangeException, ValueInUseException;

    protected abstract VLM_DFN createVlmDfnData(
        VolumeDefinition vlmDfnRef,
        String rscNameSuffixRef,
        LayerPayload payloadRef
    )
        throws AccessDeniedException, SQLException, ValueOutOfRangeException, ExhaustedPoolException,
            ValueInUseException;

    protected abstract void mergeVlmDfnData(
        VLM_DFN vlmDfnDataRef,
        LayerPayload payloadRef
    );

    protected abstract VLM createVlmLayerData(
        RSC rscDataRef,
        Volume vlm,
        LayerPayload payloadRef
    )
        throws AccessDeniedException, SQLException, ValueOutOfRangeException, ExhaustedPoolException,
            ValueInUseException, LinStorException;
}
