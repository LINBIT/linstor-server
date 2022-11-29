package com.linbit.linstor.layer.openflex;

import com.linbit.ImplementationError;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.SpaceInfo;
import com.linbit.linstor.core.devmgr.DeviceHandler;
import com.linbit.linstor.core.devmgr.exceptions.ResourceException;
import com.linbit.linstor.core.devmgr.exceptions.VolumeException;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.Resource.Flags;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.core.pojos.LocalPropsChangePojo;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.event.common.ResourceState;
import com.linbit.linstor.layer.DeviceLayer;
import com.linbit.linstor.layer.nvme.NvmeUtils;
import com.linbit.linstor.layer.storage.openflex.rest.OpenflexRestClient;
import com.linbit.linstor.layer.storage.openflex.rest.responses.OpenflexPool;
import com.linbit.linstor.layer.storage.openflex.rest.responses.OpenflexVolume;
import com.linbit.linstor.layer.storage.openflex.rest.responses.OpenflexVolumesCollection;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.stateflags.StateFlags;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.data.adapter.nvme.OpenflexRscData;
import com.linbit.linstor.storage.data.adapter.nvme.OpenflexVlmData;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.utils.Align;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Singleton
public class OpenflexLayer implements DeviceLayer
{
    private static final int KiB = 1024;
    // linstor calculates everything in KiB.. 1<<20 is therefore 1GiB outside of linstor
    private static final Align ALIGN_TO_NEXT_GB = new Align(1L << 20);

    // private static final String FORMAT_NAME = "%s_%05d";
    private static final String FORMAT_NAME = "%s";

    private final AccessContext sysCtx;
    private final ErrorReporter errorReporter;
    private final OpenflexRestClient restClient;
    private final NvmeUtils nvmeUtils;
    private final Provider<DeviceHandler> resourceProcessorProvider;
    /*
     * temporary, will be cleared in "clearCache()"
     */
    private List<StorPool> changedStorPools;

    @Inject
    public OpenflexLayer(
        @SystemContext AccessContext sysCtxRef,
        ErrorReporter errorReporterRef,
        OpenflexRestClient restClientRef,
        NvmeUtils nvmeUtilsRef,
        Provider<DeviceHandler> resourceProcessorRef
    )
    {
        sysCtx = sysCtxRef;
        errorReporter = errorReporterRef;
        restClient = restClientRef;
        nvmeUtils = nvmeUtilsRef;
        resourceProcessorProvider = resourceProcessorRef;

        changedStorPools = new ArrayList<>();
    }

    @Override
    public String getName()
    {
        return this.getClass().getSimpleName();
    }

    @Override
    public void prepare(Set<AbsRscLayerObject<Resource>> rscObjListRef, Set<AbsRscLayerObject<Snapshot>> snapObjListRef)
        throws StorageException, AccessDeniedException, DatabaseException
    {
        // no-op
    }

    @Override
    public void updateAllocatedSizeFromUsableSize(VlmProviderObject<Resource> vlmDataRef)
        throws AccessDeniedException, DatabaseException
    {
        // usable size was just updated (set) by the layer above us. copy that, so we can
        // update it again with the actual usable size when we are finished
        OpenflexVlmData<Resource> ofVlmData = (OpenflexVlmData<Resource>) vlmDataRef;

        // basically no-op. gross == net for Openflex
        ofVlmData.setExepectedSize(ALIGN_TO_NEXT_GB.ceiling(ofVlmData.getUsableSize()));
    }

    @Override
    public void updateUsableSizeFromAllocatedSize(VlmProviderObject<Resource> vlmDataRef)
        throws AccessDeniedException, DatabaseException
    {
        OpenflexVlmData<Resource> ofVlmData = (OpenflexVlmData<Resource>) vlmDataRef;

        // basically no-op. gross == net for Openflex

        ofVlmData.setAllocatedSize(ALIGN_TO_NEXT_GB.ceiling(ofVlmData.getUsableSize()));
    }

    @Override
    public void process(
        AbsRscLayerObject<Resource> rscLayerDataRef,
        List<Snapshot> ignored,
        ApiCallRcImpl apiCallRcRef
    ) throws StorageException, ResourceException, VolumeException, AccessDeniedException, DatabaseException
    {
        OpenflexRscData<Resource> ofRscData = (OpenflexRscData<Resource>) rscLayerDataRef;

        if (ofRscData.isInitiator(sysCtx))
        {
            processInitiator(ofRscData, apiCallRcRef);
        }
        else
        {
            processTarget(ofRscData, apiCallRcRef);
        }
    }

    private void processTarget(OpenflexRscData<Resource> ofRscData, ApiCallRcImpl apiCallRcRef)
        throws AccessDeniedException, StorageException, DatabaseException
    {
        OpenflexVolumesCollection allVlms = restClient.getAllVolumes();
        Map<String, OpenflexVolume> ofVlmsByName = new HashMap<>();

        boolean inUse = false;
        if (allVlms.Members != null)
        {
            for (OpenflexVolume ofVlm : allVlms.Members)
            {
                ofVlmsByName.put(ofVlm.Name, ofVlm);
                if (ofVlm.RemoteConnections != null &&
                    ofVlm.RemoteConnections.Members != null &&
                    ofVlm.RemoteConnections.Members.length > 0)
                {
                    inUse = true;
                }
            }
        }

        ofRscData.setInUse(inUse);

        for (OpenflexVlmData<Resource> vlm : ofRscData.getVlmLayerObjects().values())
        {
            String vlmName = getName(vlm);
            OpenflexVolume ofVlm = ofVlmsByName.get(vlmName);

            vlm.setExists(ofVlm != null);
            boolean vlmShouldExist = !((Volume) vlm.getVolume()).getFlags().isSet(
                sysCtx,
                Volume.Flags.DELETE
            );
            vlmShouldExist &= !vlm.getRscLayerObject().getAbsResource().getStateFlags().isSet(
                sysCtx,
                Resource.Flags.DISK_REMOVING
            );

            if (vlm.exists())
            {
                long expectedSize = vlm.getExepectedSize();
                long actualSize = ofVlm.Capacity / KiB;

                vlm.getRscLayerObject().getRscDfnLayerObject().setNqn(ofVlm.NQN);
                vlm.setIdentifier(ofVlm.ID);

                errorReporter.logTrace("Openflex volume found: %s %s", vlmName, ofVlm.Self);
                if (!restClient.isEtagKnown(ofVlm.Self))
                {
                    errorReporter.logTrace("Recaching Openflex volume Etag for: %s %s", vlmName, ofVlm.Self);
                    restClient.refreshEtag(ofVlm.Self, vlm.getStorPool());
                }
                if (vlmShouldExist)
                {
                    if (actualSize < expectedSize)
                    {
                        errorReporter.logTrace("Openflex volume %s will be resized", vlmName);
                        resize(vlm);
                    }
                    // else if (actualSize > maxToleranceSize)
                    // {
                    // }
                }
                else
                {
                    errorReporter.logTrace("Openflex volume %s will be deleted", vlmName);
                    delete(vlm);
                }
            }
            else
            {
                if (vlmShouldExist)
                {
                    errorReporter.logTrace("Openflex volume %s will be created", vlmName);
                    create(vlm);
                }
                else
                {
                    errorReporter.logTrace("Lv %s should be deleted but does not exist; no-op", vlmName);
                }
            }
        }
    }

    private void processInitiator(OpenflexRscData<Resource> ofRscDataRef, ApiCallRcImpl apiCallRcRef)
        throws AccessDeniedException, StorageException, DatabaseException
    {
//        Resource targetRsc = nvmeUtils.getTargetResource(ofRscDataRef.getAbsResource(), sysCtx);

        nvmeUtils.setDevicePaths(ofRscDataRef, ofRscDataRef.exists());

        StateFlags<Flags> rscFlags = ofRscDataRef.getAbsResource().getStateFlags();
        if (
            ofRscDataRef.exists() &&
                (rscFlags.isSet(sysCtx, Resource.Flags.DELETE) || rscFlags.isSet(sysCtx, Resource.Flags.INACTIVE))
        )
        {
            nvmeUtils.disconnect(ofRscDataRef);
            ofRscDataRef.setInUse(false);
        }
        else
        if (!ofRscDataRef.exists() &&
            !rscFlags.isSet(sysCtx, Resource.Flags.DELETE) &&
            !rscFlags.isSet(sysCtx, Resource.Flags.INACTIVE)
        )
        {
            nvmeUtils.connect(ofRscDataRef, sysCtx);
            if (!nvmeUtils.setDevicePaths(ofRscDataRef, true))
            {
                throw new StorageException("Failed to set NVMe (Openflex) device path!");
            }
            ofRscDataRef.setInUse(true);
        }
        else
        {
            errorReporter.logDebug(
                "Openflex Intiator resource '%s' already in expected state, nothing to be done.",
                ofRscDataRef.getSuffixedResourceName()
            );
        }
    }

    private String getName(OpenflexVlmData<Resource> vlmRef)
    {
        OpenflexRscData<Resource> rlo = vlmRef.getRscLayerObject();
        return String.format(
            FORMAT_NAME,
            rlo.getRscDfnLayerObject().getShortName()
            // vlmRef.getVlmNr().value
        );
    }

    private void create(OpenflexVlmData<Resource> vlm)
        throws AccessDeniedException, StorageException, DatabaseException
    {
        long expectedSize = vlm.getExepectedSize();
        // openflex only allows multiples of GiB
        long sizeRoundedUp = ALIGN_TO_NEXT_GB.ceiling(expectedSize);

        vlm.setAllocatedSize(sizeRoundedUp);
        vlm.setUsableSize(sizeRoundedUp);

        OpenflexVolume ofVlm = restClient.postVolume(vlm.getStorPool(), getName(vlm), vlm.getAllocatedSize() * KiB);
        vlm.getRscLayerObject().getRscDfnLayerObject().setNqn(ofVlm.NQN);
        vlm.setIdentifier(ofVlm.ID);

        changedStorPools.add(vlm.getStorPool());
    }

    private void resize(OpenflexVlmData<Resource> vlm)
    {
        long expectedSize = vlm.getExepectedSize();
        // openflex only allows multiples of GiB
        long sizeRoundedUp = ALIGN_TO_NEXT_GB.ceiling(expectedSize);

        vlm.setAllocatedSize(sizeRoundedUp);
        vlm.setUsableSize(sizeRoundedUp);

        changedStorPools.add(vlm.getStorPool());

        throw new ImplementationError("Resize is currently not supported by openflex layer");
    }

    private void delete(OpenflexVlmData<Resource> vlm) throws AccessDeniedException, StorageException
    {
        restClient.deleteVolume(vlm.getStorPool(), vlm.getIdentifier());
        changedStorPools.add(vlm.getStorPool());
    }

    @Override
    public SpaceInfo getStoragePoolSpaceInfo(StorPool storPoolRef) throws AccessDeniedException, StorageException
    {
        OpenflexPool openflexPool = restClient.getPool(storPoolRef);
        return new SpaceInfo(
            openflexPool.TotalCapacity / KiB,
            openflexPool.RemainingCapacity / KiB
        );
    }

    @Override
    public void clearCache() throws StorageException
    {
        changedStorPools.clear();
    }

    @Override
    public LocalPropsChangePojo setLocalNodeProps(Props localNodePropsRef)
    {
        restClient.setLocalNodeProps(localNodePropsRef);
        return null;
    }

    @Override
    public boolean resourceFinished(AbsRscLayerObject<Resource> layerDataRef) throws AccessDeniedException
    {
        OpenflexRscData<Resource> ofRscData = (OpenflexRscData<Resource>) layerDataRef;
        if (ofRscData.getAbsResource().getStateFlags().isSet(sysCtx, Resource.Flags.DELETE))
        {
            resourceProcessorProvider.get().sendResourceDeletedEvent(ofRscData);
        }
        else
        {
            resourceProcessorProvider.get().sendResourceCreatedEvent(
                ofRscData,
                new ResourceState(
                    true,
                    // no (drbd) connections to peers
                    Collections.emptyMap(),
                    null, // will be mapped to unknown
                    ofRscData.isInUse(),
                    null,
                    null
                )
            );
        }
        return true;
    }

}
