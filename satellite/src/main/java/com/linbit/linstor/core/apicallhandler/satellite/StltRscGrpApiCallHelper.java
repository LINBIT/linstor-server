package com.linbit.linstor.core.apicallhandler.satellite;

import com.linbit.InvalidNameException;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.api.interfaces.AutoSelectFilterApi;
import com.linbit.linstor.core.ControllerPeerConnector;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.DeviceManager;
import com.linbit.linstor.core.identifier.ResourceGroupName;
import com.linbit.linstor.core.objects.AutoSelectorConfigData;
import com.linbit.linstor.core.objects.ResourceGroup;
import com.linbit.linstor.core.objects.ResourceGroup.RscGrpApi;
import com.linbit.linstor.core.objects.ResourceGroupData;
import com.linbit.linstor.core.objects.ResourceGroupDataSatelliteFactory;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.transaction.TransactionMgr;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.Map;

@Singleton
class StltRscGrpApiCallHelper
{
    private final ErrorReporter errorReporter;
    private final AccessContext apiCtx;
    private final DeviceManager deviceManager;
    private final ControllerPeerConnector controllerPeerConnector;
    private final CoreModule.ResourceGroupMap rscGrpMap;
    private final ResourceGroupDataSatelliteFactory resourceGroupDataFactory;
    private final Provider<TransactionMgr> transMgrProvider;

    @Inject
    public StltRscGrpApiCallHelper(
        ErrorReporter errorReporterRef,
        @SystemContext AccessContext apiCtxRef,
        DeviceManager deviceManagerRef,
        ControllerPeerConnector controllerPeerConnectorRef,
        CoreModule.ResourceGroupMap rscGrpMapRef,
        ResourceGroupDataSatelliteFactory resourceGroupDataFactoryRef,
        Provider<TransactionMgr> transMgrProviderRef
    )
    {
        errorReporter = errorReporterRef;
        apiCtx = apiCtxRef;
        deviceManager = deviceManagerRef;
        controllerPeerConnector = controllerPeerConnectorRef;
        rscGrpMap = rscGrpMapRef;
        resourceGroupDataFactory = resourceGroupDataFactoryRef;
        transMgrProvider = transMgrProviderRef;
    }

    public ResourceGroup mergeResourceGroup(RscGrpApi rscGrpApiRef)
        throws InvalidNameException, AccessDeniedException, DatabaseException
    {
        ResourceGroupName rscGrpName = new ResourceGroupName(rscGrpApiRef.getName());
        AutoSelectFilterApi autoPlaceConfigPojo = rscGrpApiRef.getAutoSelectFilter();

        ResourceGroup resourceGroup = rscGrpMap.get(rscGrpName);
        if (resourceGroup == null)
        {
            resourceGroup = resourceGroupDataFactory.getInstanceSatellite(
                rscGrpName,
                rscGrpApiRef.getDescription(),
                autoPlaceConfigPojo.getLayerStackList(),
                autoPlaceConfigPojo.getReplicaCount(),
                autoPlaceConfigPojo.getStorPoolNameStr(),
                autoPlaceConfigPojo.getDoNotPlaceWithRscList(),
                autoPlaceConfigPojo.getDoNotPlaceWithRscRegex(),
                autoPlaceConfigPojo.getReplicasOnSameList(),
                autoPlaceConfigPojo.getReplicasOnDifferentList(),
                autoPlaceConfigPojo.getProviderList(),
                autoPlaceConfigPojo.getDisklessOnRemaining()
            );
            resourceGroup.getRscDfnGrpProps(apiCtx).map().putAll(rscGrpApiRef.getRcsDfnProps());
            rscGrpMap.put(rscGrpName, resourceGroup);
        }
        else
        {
            ResourceGroupData rscGrpData = (ResourceGroupData) resourceGroup;
            Map<String, String> targetProps = rscGrpData.getRscDfnGrpProps(apiCtx).map();
            targetProps.clear();
            targetProps.putAll(rscGrpApiRef.getRcsDfnProps());

            rscGrpData.setDescription(apiCtx, rscGrpApiRef.getDescription());

            AutoSelectorConfigData autoPlaceConfig = (AutoSelectorConfigData) rscGrpData.getAutoPlaceConfig();

            autoPlaceConfig.applyChanges(autoPlaceConfigPojo);
        }

        return resourceGroup;
    }
}
