package com.linbit.linstor.core.devmgr.helper;

import com.linbit.utils.AccessUtils;
import com.linbit.utils.Pair;
import com.linbit.utils.RemoveAfterDevMgrRework;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.linbit.ImplementationError;
import com.linbit.linstor.Resource;
import com.linbit.linstor.Volume;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.interfaces.categories.RscLayerObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

@RemoveAfterDevMgrRework
@Singleton
public class LayeredResourcesHelper
{
    private final AccessContext sysCtx;
    private final ErrorReporter errorReporter;

    private final List<LayerDataConverter> converterList;

    @Inject
    public LayeredResourcesHelper(
        @SystemContext AccessContext sysCtxRef,
        ErrorReporter errorReporterRef,
        DrbdDataConverter drbdConverter,
        CryptDataConverter cryptConverter,
        StoragetDataConverter storageConverter
    )
    {

        sysCtx = sysCtxRef;
        errorReporter = errorReporterRef;

        converterList = Arrays.asList(
            drbdConverter,
            cryptConverter,
            storageConverter
        );
    }

    @RemoveAfterDevMgrRework
    public Collection<Resource> extractLayers(Collection<Resource> origResources)
    {
        List<Resource> resourcesToConvert =
            origResources.stream().map(Resource::getDefinition)
                .flatMap(rscDfn -> AccessUtils.execPrivileged(() -> rscDfn.streamResource(sysCtx)))
                .collect(Collectors.toList());
        try
        {
            /*
             * although we iterate ALL resources (including peer resources), we only add our local resources to the
             * layeredResources list as the device manager should only process those.
             */
            for (Resource rsc : resourcesToConvert)
            {
                /* currently a resource can have 3 layers at maximum
                *
                *  DRBD (not on swordfish)
                *  Crypt (not on swordfish)
                *  {,Thin}{Lvm,Zfs}, Swordfish{Target,Initiator}
                *
                *  The upper two layers stay the same (and might get extended in the future)
                *
                *  The lowest layer will be grouped into a "StorgeLayer" which
                *  basically only switches the input for the layers from resource-based
                *  to volume-based. As this Layer groups the current {,Thin}{Lvm,Zfs} and
                *  Swordfish{Target,Initiator} drivers, it will also decide which volume should be created
                *  using which driver.
                *  This is needed as otherwise we could not compose a resource with volumes using different
                *  backing-drivers.
                */

                /*
                 * The stacked resource will keep the origRsc as the lowest resource and will create
                 * parent-resources as needed.
                 */

                errorReporter.logTrace("Creating layer data for resource %s", rsc.toString());
                RscLayerObject parentRscData = null;
                RscLayerObject rootRscData = null;
                List<RscLayerObject> parentChildrenRscData = new ArrayList<>();

                List<DeviceLayerKind> layerStack = new ArrayList<>();

                for (LayerDataConverter converter : converterList)
                {
                    if (converter.isLayerNeeded(rsc))
                    {
                        Pair<RscLayerObject, List<RscLayerObject>> ret = converter.convert(rsc, parentRscData);

                        if (rootRscData == null)
                        {
                            rootRscData = ret.objA;
                        }
                        if (parentChildrenRscData != null)
                        {
                            parentChildrenRscData.add(ret.objA);
                        }

                        parentRscData = ret.objA;
                        parentChildrenRscData = ret.objB;

                        layerStack.add(converter.getKind());
                    }
                }

                for (Volume vlm : rsc.streamVolumes().collect(Collectors.toList()))
                {
                    vlm.setLayerStack(layerStack);
                }

                rsc.setLayerData(sysCtx, rootRscData);
            }
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ImplementationError("Privileged context has not enough privileges", accDeniedExc);
        }
        catch (InvalidKeyException | SQLException exc)
        {
            throw new ImplementationError(exc);
        }
        return origResources;
    }

    /**
     * This method is called to synchronize the changes performed onto a typed resource to be tracked on the
     * default resource
     * @param origResources
     */
    public void cleanupResources(Collection<Resource> origResources)
    {
        // no-op
    }
}
