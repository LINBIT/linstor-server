package com.linbit.linstor.event.handler;

import com.linbit.ImplementationError;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.apicallhandler.controller.CtrlApiDataLoader;
import com.linbit.linstor.core.apicallhandler.controller.internal.CtrlSatelliteUpdateCaller;
import com.linbit.linstor.core.apicallhandler.controller.utils.ResourceDataUtils;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.event.EventIdentifier;
import com.linbit.linstor.layer.drbd.drbdstate.DiskState;
import com.linbit.linstor.layer.resource.CtrlRscLayerDataFactory;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.InvalidValueException;
import com.linbit.linstor.satellitestate.SatelliteVolumeState;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;

import javax.inject.Inject;
import javax.inject.Singleton;

import reactor.core.publisher.Flux;

@Singleton
public class StateSequenceDetector
{
    private final CtrlApiDataLoader ctrlApiDataLoader;
    private final ErrorReporter errorReporter;
    private final AccessContext accCtx;
    private final CtrlRscLayerDataFactory ctrlRscLayerDataFactory;
    private final CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCaller;

    @Inject
    public StateSequenceDetector(
        CtrlApiDataLoader ctrlApiDataLoaderRef,
        ErrorReporter errorReporterRef,
        @SystemContext AccessContext accCtxRef,
        CtrlRscLayerDataFactory ctrlRscLayerDataFactoryRef,
        CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCallerRef
    )
    {
        ctrlApiDataLoader = ctrlApiDataLoaderRef;
        errorReporter = errorReporterRef;
        accCtx = accCtxRef;
        ctrlRscLayerDataFactory = ctrlRscLayerDataFactoryRef;
        ctrlSatelliteUpdateCaller = ctrlSatelliteUpdateCallerRef;
    }

    public void processAndSetDiskState(EventIdentifier eventIdentifier, SatelliteVolumeState vlmState, String nextState)
    {
        try
        {
            String currentState = vlmState.getDiskState();
            if (DiskState.FAILED.getLabel().equals(currentState) && DiskState.DISKLESS.getLabel().equals(nextState))
            {
                NodeName nodeName = eventIdentifier.getNodeName();
                ResourceName rscName = eventIdentifier.getResourceName();
                Resource rsc = ctrlApiDataLoader.loadRsc(nodeName, rscName, false);
                if (rsc != null)
                {
                    rsc.getProps(accCtx)
                        .setProp(
                            ApiConsts.KEY_DRBD_SKIP_DISK,
                            ApiConsts.VAL_TRUE,
                            ApiConsts.NAMESPC_DRBD_OPTIONS
                        );
                    boolean changed = ResourceDataUtils.recalculateVolatileRscData(ctrlRscLayerDataFactory, rsc);
                    if (changed)
                    {
                        ctrlSatelliteUpdateCaller.updateSatellites(rsc, Flux.empty())
                            .subscribe();
                    }
                }
                else
                {
                    errorReporter.logError("Event received for non-existent rsc %s on node %s", rscName, nodeName);
                }
            }
            vlmState.setDiskState(nextState);
        }
        catch (AccessDeniedException | InvalidKeyException | InvalidValueException exc)
        {
            throw new ImplementationError(exc);
        }
        catch (DatabaseException exc)
        {
            errorReporter.reportError(exc);
        }
    }
}
