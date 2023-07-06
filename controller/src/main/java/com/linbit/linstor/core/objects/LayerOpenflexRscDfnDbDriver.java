package com.linbit.linstor.core.objects;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.SnapshotName;
import com.linbit.linstor.core.objects.AbsLayerRscDataDbDriver.ParentObjects;
import com.linbit.linstor.core.objects.AbsLayerRscDataDbDriver.SuffixedResourceName;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.DbEngine;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.LayerOpenflexResourceDefinitions;
import com.linbit.linstor.dbdrivers.RawParameters;
import com.linbit.linstor.dbdrivers.interfaces.LayerOpenflexRscDfnDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.ObjectProtectionFactory;
import com.linbit.linstor.storage.data.adapter.nvme.OpenflexRscData;
import com.linbit.linstor.storage.data.adapter.nvme.OpenflexRscDfnData;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgrSQL;
import com.linbit.linstor.utils.NameShortener;
import com.linbit.utils.Pair;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

@Singleton
public class LayerOpenflexRscDfnDbDriver
    extends AbsLayerRscDfnDataDbDriver<OpenflexRscDfnData<?>, OpenflexRscData<?>>
    implements LayerOpenflexRscDfnDatabaseDriver

{

    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgrSQL> transMgrProvider;

    private final SingleColumnDatabaseDriver<OpenflexRscDfnData<?>, String> nqnDriver;
    private final NameShortener nameShortener;

    @Inject
    public LayerOpenflexRscDfnDbDriver(
        @SystemContext AccessContext dbCtxRef,
        ErrorReporter errorReporterRef,
        DbEngine dbEngineRef,
        ObjectProtectionFactory objProtFactoryRef,
        @Named(NameShortener.OPENFLEX) NameShortener nameShortenerRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<TransactionMgrSQL> transMgrProviderRef
    )
    {
        super(
            dbCtxRef,
            errorReporterRef,
            GeneratedDatabaseTables.LAYER_OPENFLEX_RESOURCE_DEFINITIONS,
            dbEngineRef,
            objProtFactoryRef
        );
        nameShortener = nameShortenerRef;
        transObjFactory = transObjFactoryRef;
        transMgrProvider = transMgrProviderRef;

        setColumnSetter(
            LayerOpenflexResourceDefinitions.RESOURCE_NAME,
            openflexRscDfnData -> openflexRscDfnData.getResourceName().value
        );
        setColumnSetter(LayerOpenflexResourceDefinitions.RESOURCE_NAME_SUFFIX, OpenflexRscDfnData::getRscNameSuffix);
        setColumnSetter(
            LayerOpenflexResourceDefinitions.SNAPSHOT_NAME,
            openflexRscDfnData ->
            {
                String ret;
                SnapshotName snapName = openflexRscDfnData.getSnapshotName();
                if (snapName != null)
                {
                    ret = snapName.value;
                }
                else
                {
                    ret = ResourceDefinitionDbDriver.DFLT_SNAP_NAME_FOR_RSC;
                }
                return ret;
            }
        );
        setColumnSetter(LayerOpenflexResourceDefinitions.NQN, OpenflexRscDfnData::getNqn);

        nqnDriver = generateSingleColumnDriver(
            LayerOpenflexResourceDefinitions.NQN,
            OpenflexRscDfnData::getNqn,
            Function.identity()
        );
    }

    @Override
    public SingleColumnDatabaseDriver<OpenflexRscDfnData<?>, String> getNqnDriver()
    {
        return nqnDriver;
    }

    @Override
    protected Pair<OpenflexRscDfnData<?>, List<OpenflexRscData<?>>> load(RawParameters raw, ParentObjects parentRef)
        throws DatabaseException, AccessDeniedException, InvalidNameException
    {
        ResourceName rscName = raw.buildParsed(LayerOpenflexResourceDefinitions.RESOURCE_NAME, ResourceName::new);
        String rscNameSuffix = raw.getParsed(LayerOpenflexResourceDefinitions.RESOURCE_NAME_SUFFIX);

        // snapshots are currently not supported
        // String snapNameStr = raw.getParsed(LayerOpenflexResourceDefinitions.SNAPSHOT_NAME);

        String nqn = raw.getParsed(LayerOpenflexResourceDefinitions.NQN);

        ResourceDefinition rscDfn = parentRef.rscDfnMap.get(rscName);

        SuffixedResourceName suffixedRscName;
        suffixedRscName = new SuffixedResourceName(
            rscName,
            null, // snapshots are currently not supported
            rscNameSuffix
        );

        return genericCreate(
            rscDfn,
            suffixedRscName,
            nqn
        );
    }

    @SuppressWarnings("unchecked")
    private <RSC extends AbsResource<RSC>> Pair<OpenflexRscDfnData<?>, List<OpenflexRscData<?>>> genericCreate(
        ResourceDefinition rscDfnRef,
        SuffixedResourceName suffixedRscNameRef,
        String nqnRef
    )
        throws DatabaseException, AccessDeniedException
    {
        List<OpenflexRscData<RSC>> rscDataList = new ArrayList<>();

        OpenflexRscDfnData<RSC> openflexRscDfnData;
        try
        {
            openflexRscDfnData = new OpenflexRscDfnData<>(
                suffixedRscNameRef.rscName,
                suffixedRscNameRef.rscNameSuffix,
                // snapshots are currently ignored, Linstor does not support snapshots for Openflex (not sure if
                // Openflex
                // itself does right now)
                nameShortener.shorten(rscDfnRef, suffixedRscNameRef.rscNameSuffix),
                rscDataList,
                nqnRef,
                this,
                transObjFactory,
                transMgrProvider
            );
        }
        catch (LinStorException lsExc)
        {
            throw new ImplementationError(
                "Cannot reload Openflex resource definition from database: " + suffixedRscNameRef.rscName + " " +
                    suffixedRscNameRef.rscNameSuffix,
                lsExc
            );
        }
        return new Pair<>(
            openflexRscDfnData,
            (List<OpenflexRscData<?>>) ((Object) rscDataList)
        );
    }
}
