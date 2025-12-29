package com.linbit.linstor.core.objects;

import com.linbit.ImplementationError;
import com.linbit.InvalidIpAddressException;
import com.linbit.InvalidNameException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.drbd.md.MdException;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.core.identifier.ResourceGroupName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.dbdrivers.AbsProtectedDatabaseDriver;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.DbEngine;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables;
import com.linbit.linstor.dbdrivers.RawParameters;
import com.linbit.linstor.dbdrivers.interfaces.VolumeGroupCtrlDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.ObjectProtectionFactory;
import com.linbit.linstor.stateflags.StateFlagsPersistence;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgr;
import com.linbit.utils.Pair;

import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.VolumeGroups.FLAGS;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.VolumeGroups.RESOURCE_GROUP_NAME;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.VolumeGroups.UUID;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.VolumeGroups.VLM_NR;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.Map;

@Singleton
public final class VolumeGroupDbDriver
    extends AbsProtectedDatabaseDriver<VolumeGroup, Void, Map<ResourceGroupName, ? extends ResourceGroup>>
    implements VolumeGroupCtrlDatabaseDriver
{
    private final AccessContext dbCtx;
    private final StateFlagsPersistence<VolumeGroup> flagsDriver;
    private final Provider<TransactionMgr> transMgrProvider;
    private final PropsContainerFactory propsContainerFactory;
    private final TransactionObjectFactory transObjFactory;

    @Inject
    public VolumeGroupDbDriver(
        @SystemContext AccessContext dbCtxRef,
        ErrorReporter errorReporterRef,
        DbEngine dbEngineRef,
        Provider<TransactionMgr> transMgrProviderRef,
        ObjectProtectionFactory objProtFactoryRef,
        PropsContainerFactory propsContainerFactoryRef,
        TransactionObjectFactory transObjFactoryRef
    )
    {
        super(dbCtxRef, errorReporterRef, GeneratedDatabaseTables.VOLUME_GROUPS, dbEngineRef, objProtFactoryRef);
        dbCtx = dbCtxRef;
        transMgrProvider = transMgrProviderRef;
        propsContainerFactory = propsContainerFactoryRef;
        transObjFactory = transObjFactoryRef;

        setColumnSetter(UUID, vlmGrp -> vlmGrp.getUuid().toString());
        setColumnSetter(RESOURCE_GROUP_NAME, vlmGrp -> vlmGrp.getResourceGroup().getName().value);
        setColumnSetter(VLM_NR, vlmGrp -> vlmGrp.getVolumeNumber().value);
        setColumnSetter(FLAGS, vlmGrp -> vlmGrp.getFlags().getFlagsBits(dbCtxRef));

        flagsDriver = generateFlagDriver(FLAGS, VolumeGroup.Flags.class);
    }

    @Override
    public StateFlagsPersistence<VolumeGroup> getStateFlagsPersistence()
    {
        return flagsDriver;
    }

    @Override
    protected Pair<VolumeGroup, Void> load(
        RawParameters raw,
        Map<ResourceGroupName, ? extends ResourceGroup> rscGrpMap
    )
        throws DatabaseException, InvalidNameException, ValueOutOfRangeException, InvalidIpAddressException,
        MdException
    {
        final ResourceGroupName rscGrpName = raw.build(RESOURCE_GROUP_NAME, ResourceGroupName::new);
        final VolumeNumber vlmNr;
        final long flags;
        switch (getDbType())
        {
            case SQL: // fall-through
            case K8S_CRD:
                vlmNr = raw.build(VLM_NR, VolumeNumber::new);
                flags = raw.get(FLAGS);
                break;
            default:
                throw new ImplementationError("Unknown database type: " + getDbType());
        }
        return new Pair<>(
            new VolumeGroup(
                raw.build(UUID, java.util.UUID::fromString),
                rscGrpMap.get(rscGrpName),
                vlmNr,
                flags,
                this,
                propsContainerFactory,
                transObjFactory,
                transMgrProvider
            ),
            null
        );
    }

    @Override
    protected String getId(VolumeGroup vlmGrp)
    {
        return "(RscGrpName=" + vlmGrp.getResourceGroup().getName().displayValue +
            ", VlmNr=" + vlmGrp.getVolumeNumber().value + ")";
    }

}
