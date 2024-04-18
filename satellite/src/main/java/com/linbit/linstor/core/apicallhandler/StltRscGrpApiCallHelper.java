package com.linbit.linstor.core.apicallhandler;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.api.interfaces.AutoSelectFilterApi;
import com.linbit.linstor.core.ControllerPeerConnector;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.DeviceManager;
import com.linbit.linstor.core.apis.ResourceGroupApi;
import com.linbit.linstor.core.apis.VolumeGroupApi;
import com.linbit.linstor.core.identifier.ResourceGroupName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.AutoSelectorConfig;
import com.linbit.linstor.core.objects.ResourceGroup;
import com.linbit.linstor.core.objects.ResourceGroupSatelliteFactory;
import com.linbit.linstor.core.objects.VolumeGroup;
import com.linbit.linstor.core.objects.VolumeGroupSatelliteFactory;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.Map;
import java.util.TreeMap;

@Singleton
class StltRscGrpApiCallHelper
{
    private final ErrorReporter errorReporter;
    private final AccessContext apiCtx;
    private final DeviceManager deviceManager;
    private final ControllerPeerConnector controllerPeerConnector;
    private final CoreModule.ResourceGroupMap rscGrpMap;
    private final ResourceGroupSatelliteFactory resourceGroupFactory;
    private final VolumeGroupSatelliteFactory volumeGroupFactory;
    private final Provider<TransactionMgr> transMgrProvider;

    @Inject
     StltRscGrpApiCallHelper(
        ErrorReporter errorReporterRef,
        @SystemContext AccessContext apiCtxRef,
        DeviceManager deviceManagerRef,
        ControllerPeerConnector controllerPeerConnectorRef,
        CoreModule.ResourceGroupMap rscGrpMapRef,
        ResourceGroupSatelliteFactory resourceGroupFactoryRef,
        VolumeGroupSatelliteFactory volumeGroupFactoryRef,
        Provider<TransactionMgr> transMgrProviderRef
    )
    {
        errorReporter = errorReporterRef;
        apiCtx = apiCtxRef;
        deviceManager = deviceManagerRef;
        controllerPeerConnector = controllerPeerConnectorRef;
        rscGrpMap = rscGrpMapRef;
        resourceGroupFactory = resourceGroupFactoryRef;
        volumeGroupFactory = volumeGroupFactoryRef;
        transMgrProvider = transMgrProviderRef;
    }

    public ResourceGroup mergeResourceGroup(ResourceGroupApi rscGrpApiRef)
        throws InvalidNameException, AccessDeniedException, DatabaseException
    {
        ResourceGroupName rscGrpName = new ResourceGroupName(rscGrpApiRef.getName());
        AutoSelectFilterApi autoPlaceConfigPojo = rscGrpApiRef.getAutoSelectFilter();

        ResourceGroup rscGrp = rscGrpMap.get(rscGrpName);
        if (rscGrp == null)
        {
            rscGrp = resourceGroupFactory.getInstanceSatellite(
                rscGrpApiRef.getUuid(),
                rscGrpName,
                rscGrpApiRef.getDescription(),
                autoPlaceConfigPojo.getLayerStackList(),
                autoPlaceConfigPojo.getReplicaCount(),
                autoPlaceConfigPojo.getNodeNameList(),
                autoPlaceConfigPojo.getStorPoolNameList(),
                autoPlaceConfigPojo.getStorPoolDisklessNameList(),
                autoPlaceConfigPojo.getDoNotPlaceWithRscList(),
                autoPlaceConfigPojo.getDoNotPlaceWithRscRegex(),
                autoPlaceConfigPojo.getReplicasOnSameList(),
                autoPlaceConfigPojo.getReplicasOnDifferentList(),
                autoPlaceConfigPojo.getXReplicasOnDifferentMap(),
                autoPlaceConfigPojo.getProviderList(),
                autoPlaceConfigPojo.getDisklessOnRemaining(),
                rscGrpApiRef.getPeerSlots()
            );
            rscGrp.getProps(apiCtx).map().putAll(rscGrpApiRef.getProps());
            rscGrpMap.put(rscGrpName, rscGrp);
        }
        else
        {
            Map<String, String> targetProps = rscGrp.getProps(apiCtx).map();
            targetProps.clear();
            targetProps.putAll(rscGrpApiRef.getProps());

            rscGrp.setDescription(apiCtx, rscGrpApiRef.getDescription());

            AutoSelectorConfig autoPlaceConfig = rscGrp.getAutoPlaceConfig();

            autoPlaceConfig.applyChanges(autoPlaceConfigPojo);
        }

        Map<VolumeNumber, VolumeGroup> vlmGrpsToDelete = new TreeMap<>();
        // add all current volume group and delete them again if they are still in the vlmGrpApiList
        for (VolumeGroup vlmGrp : rscGrp.getVolumeGroups(apiCtx))
        {
            vlmGrpsToDelete.put(vlmGrp.getVolumeNumber(), vlmGrp);
        }

        try
        {
            for (VolumeGroupApi vlmGrpApi : rscGrpApiRef.getVlmGrpList())
            {
                VolumeNumber vlmNr = new VolumeNumber(vlmGrpApi.getVolumeNr());
                VolumeGroup vlmGrp = vlmGrpsToDelete.remove(vlmNr);
                Props vlmGrpProps;
                if (vlmGrp == null)
                {
                    vlmGrp = volumeGroupFactory.getInstanceSatellite(
                        vlmGrpApi.getUUID(),
                        rscGrp,
                        vlmNr,
                        vlmGrpApi.getFlags()
                    );
                    vlmGrpProps = vlmGrp.getProps(apiCtx);
                }
                else
                {
                    vlmGrp.getFlags().resetFlagsTo(
                        apiCtx,
                        VolumeGroup.Flags.restoreFlags(vlmGrpApi.getFlags())
                    );
                    vlmGrpProps = vlmGrp.getProps(apiCtx);
                    vlmGrpProps.clear();
                }
                vlmGrpProps.map().putAll(vlmGrpApi.getProps());
            }

            for (VolumeNumber vlmNr : vlmGrpsToDelete.keySet())
            {
                rscGrp.deleteVolumeGroup(apiCtx, vlmNr);
            }
        }
        catch (ValueOutOfRangeException exc)
        {
            throw new ImplementationError(exc);
        }

        return rscGrp;
    }
}
