package com.linbit.linstor.core.apicallhandler;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.api.interfaces.AutoSelectFilterApi;
import com.linbit.linstor.core.CoreModule;
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
import com.linbit.linstor.propscon.Props;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.util.Map;
import java.util.TreeMap;

@Singleton
class StltRscGrpApiCallHelper
{
    private final CoreModule.ResourceGroupMap rscGrpMap;
    private final ResourceGroupSatelliteFactory resourceGroupFactory;
    private final VolumeGroupSatelliteFactory volumeGroupFactory;

    @Inject
     StltRscGrpApiCallHelper(
        CoreModule.ResourceGroupMap rscGrpMapRef,
        ResourceGroupSatelliteFactory resourceGroupFactoryRef,
        VolumeGroupSatelliteFactory volumeGroupFactoryRef
    )
    {
        rscGrpMap = rscGrpMapRef;
        resourceGroupFactory = resourceGroupFactoryRef;
        volumeGroupFactory = volumeGroupFactoryRef;
    }

    public ResourceGroup mergeResourceGroup(ResourceGroupApi rscGrpApiRef)
        throws InvalidNameException, DatabaseException
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
            rscGrp.getProps().map().putAll(rscGrpApiRef.getProps());
            rscGrpMap.put(rscGrpName, rscGrp);
        }
        else
        {
            Map<String, String> targetProps = rscGrp.getProps().map();
            targetProps.clear();
            targetProps.putAll(rscGrpApiRef.getProps());

            rscGrp.setDescription(rscGrpApiRef.getDescription());

            AutoSelectorConfig autoPlaceConfig = rscGrp.getAutoPlaceConfig();

            autoPlaceConfig.applyChanges(autoPlaceConfigPojo);
        }

        Map<VolumeNumber, VolumeGroup> vlmGrpsToDelete = new TreeMap<>();
        // add all current volume group and delete them again if they are still in the vlmGrpApiList
        for (VolumeGroup vlmGrp : rscGrp.getVolumeGroups())
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
                    vlmGrpProps = vlmGrp.getProps();
                }
                else
                {
                    vlmGrp.getFlags().resetFlagsTo(
                        VolumeGroup.Flags.restoreFlags(vlmGrpApi.getFlags())
                    );
                    vlmGrpProps = vlmGrp.getProps();
                    vlmGrpProps.clear();
                }
                vlmGrpProps.map().putAll(vlmGrpApi.getProps());
            }

            for (VolumeNumber vlmNr : vlmGrpsToDelete.keySet())
            {
                rscGrp.deleteVolumeGroup(vlmNr);
            }
        }
        catch (ValueOutOfRangeException exc)
        {
            throw new ImplementationError(exc);
        }

        return rscGrp;
    }
}
