package com.linbit.linstor.core.objects;

import com.linbit.ImplementationError;
import com.linbit.InvalidIpAddressException;
import com.linbit.InvalidNameException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.drbd.md.MdException;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.core.identifier.ResourceGroupName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.dbdrivers.AbsDatabaseDriver;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.DbEngine;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables;
import com.linbit.linstor.dbdrivers.interfaces.VolumeGroupDataDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.ObjectProtectionDatabaseDriver;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.utils.Pair;

import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.VolumeGroups.RESOURCE_GROUP_NAME;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.VolumeGroups.UUID;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.VolumeGroups.VLM_NR;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.Map;

@Singleton
public class VolumeGroupDbDriver
    extends AbsDatabaseDriver<VolumeGroupData, Void, Map<ResourceGroupName, ? extends ResourceGroup>>
    implements VolumeGroupDataDatabaseDriver
{
    private final AccessContext dbCtx;
    private final Provider<? extends TransactionMgr> transMgrProvider;
    private final PropsContainerFactory propsContainerFactory;
    private final TransactionObjectFactory transObjFactory;

    @Inject
    public VolumeGroupDbDriver(
        @SystemContext AccessContext dbCtxRef,
        ErrorReporter errorReporterRef,
        DbEngine dbEngineRef,
        Provider<? extends TransactionMgr> transMgrProviderRef,
        ObjectProtectionDatabaseDriver objProtDriverRef,
        PropsContainerFactory propsContainerFactoryRef,
        TransactionObjectFactory transObjFactoryRef
    )
    {
        super(errorReporterRef, GeneratedDatabaseTables.VOLUME_GROUPS, dbEngineRef, objProtDriverRef);
        dbCtx = dbCtxRef;
        transMgrProvider = transMgrProviderRef;
        propsContainerFactory = propsContainerFactoryRef;
        transObjFactory = transObjFactoryRef;

        setColumnSetter(UUID, vlmGrp -> vlmGrp.getUuid().toString());
        setColumnSetter(RESOURCE_GROUP_NAME, vlmGrp -> vlmGrp.getResourceGroup().getName().value);
        setColumnSetter(VLM_NR, vlmGrp -> vlmGrp.getVolumeNumber().value);
    }

    @Override
    protected Pair<VolumeGroupData, Void> load(
        RawParameters raw,
        Map<ResourceGroupName, ? extends ResourceGroup> rscGrpMap
    )
        throws DatabaseException, InvalidNameException, ValueOutOfRangeException, InvalidIpAddressException,
        MdException
    {
        final ResourceGroupName rscGrpName = raw.build(RESOURCE_GROUP_NAME, ResourceGroupName::new);
        final VolumeNumber vlmNr;
        switch (getDbType())
        {
            case ETCD:
                vlmNr = new VolumeNumber(Integer.parseInt(raw.get(VLM_NR)));
                break;
            case SQL:
                vlmNr = raw.build(VLM_NR, VolumeNumber::new);
                break;
            default:
                throw new ImplementationError("Unknown database type: " + getDbType());
        }
        return new Pair<>(
            new VolumeGroupData(
                raw.build(UUID, java.util.UUID::fromString),
                rscGrpMap.get(rscGrpName),
                vlmNr,
                this,
                propsContainerFactory,
                transObjFactory,
                transMgrProvider
            ),
            null
        );
    }

    @Override
    protected String getId(VolumeGroupData vlmGrp)
    {
        return "(RscGrpName=" + vlmGrp.getResourceGroup().getName().displayValue +
            ", VlmNr=" + vlmGrp.getVolumeNumber().value + ")";
    }

}
