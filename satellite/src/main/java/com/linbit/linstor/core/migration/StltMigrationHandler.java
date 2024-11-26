package com.linbit.linstor.core.migration;

import com.linbit.ChildProcessTimeoutException;
import com.linbit.ImplementationError;
import com.linbit.extproc.ExtCmdFactory;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.api.protobuf.FullSync;
import com.linbit.linstor.api.protobuf.FullSync.FullSyncStatus;
import com.linbit.linstor.core.ControllerPeerConnector;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Singleton
public class StltMigrationHandler
{
    private final ControllerPeerConnector ctrlPeerConnector;
    private final AccessContext accCtx;
    private final ErrorReporter errorReporter;
    private final ExtCmdFactory extCmdFactory;
    private final Map<SatelliteMigrations, BaseStltMigration> stltMigrationsMap;

    @Inject
    public StltMigrationHandler(
        @ApiContext AccessContext accCtxRef,
        ErrorReporter errorReporterRef,
        ControllerPeerConnector controllerPeerConnectorRef,
        ExtCmdFactory extCmdFactoryRef,
        Map<SatelliteMigrations, BaseStltMigration> stltMigrationsMapRef
    )
    {
        accCtx = accCtxRef;
        errorReporter = errorReporterRef;
        ctrlPeerConnector = controllerPeerConnectorRef;
        extCmdFactory = extCmdFactoryRef;
        stltMigrationsMap = stltMigrationsMapRef;
    }

    /**
     * Checks if there are satellite migrations to apply (via properties in the
     * {@value SatelliteMigrations#NAMESPC_MIGRATION} namespace) and applies them if needed.
     */
    public StltMigrationResult migrate()
    {
        StltMigrationResult ret;
        try
        {
            Node localNode = ctrlPeerConnector.getLocalNode();
            Props localNodeProps = localNode.getProps(accCtx);
            List<SatelliteMigrations> stltMigsToApply = SatelliteMigrations.getMigrationsToApply(localNodeProps);
            errorReporter.logTrace(
                "Checking for satellite migrations... found %d migrations to apply",
                stltMigsToApply.size()
            );
            if (!stltMigsToApply.isEmpty())
            {
                ret = applyMigrations(localNode, stltMigsToApply);
            }
            else
            {
                ret = StltMigrationResult.createEmpty(FullSync.FullSyncStatus.SUCCESS);
            }
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ImplementationError(accDeniedExc);
        }
        return ret;
    }

    private StltMigrationResult applyMigrations(
        Node localNode,
        List<SatelliteMigrations> stltMigsToApply
    )
    {
        FullSyncStatus status = FullSyncStatus.SUCCESS;
        Map<String, String> stltPropsToAdd = new HashMap<>();
        Set<String> stltPropKeysToDelete = new HashSet<>();
        Set<String> stltPropNamespacesToDelete = new HashSet<>();

        for (SatelliteMigrations stltMig : stltMigsToApply)
        {
            errorReporter.logDebug("Applying satellite-migration: %s", stltMig.getPropNamespaceFull());
            try
            {
                BaseStltMigration stltMigration = stltMigrationsMap.get(stltMig);
                StltMigrationResult result = stltMigration.migrate(extCmdFactory, localNode);
                status = result.status;
                result.copyInto(stltPropsToAdd, stltPropKeysToDelete, stltPropNamespacesToDelete);
                errorReporter.logTrace("Migration result: %s", status.name());
            }
            catch (ChildProcessTimeoutException | IOException exc)
            {
                errorReporter.reportError(exc);
                status = FullSyncStatus.FAIL_UNKNOWN;
                // we will still send the changes to the controller so we could at least see what migration
                // properties are still set on the satellite and which succeeded. Just in case this might help us
                // find the error
            }
            if (status != FullSync.FullSyncStatus.SUCCESS)
            {
                break;
            }
        }
        return new StltMigrationResult(status, stltPropsToAdd, stltPropKeysToDelete, stltPropNamespacesToDelete);
    }

    public static class StltMigrationResult
    {
        public final FullSync.FullSyncStatus status;
        public final Map<String, String> stltPropsToAdd;
        public final Set<String> stltPropKeysToDelete;
        public final Set<String> stltPropNamespacesToDelete;

        public StltMigrationResult(
            FullSyncStatus statusRef,
            Map<String, String> stltPropsToAddRef,
            Set<String> stltPropKeysToDeleteRef,
            Set<String> stltPropNamespacesToDeleteRef
        )
        {
            status = statusRef;
            stltPropsToAdd = stltPropsToAddRef;
            stltPropKeysToDelete = stltPropKeysToDeleteRef;
            stltPropNamespacesToDelete = stltPropNamespacesToDeleteRef;
        }

        public static StltMigrationResult createEmpty(FullSyncStatus successRef)
        {
            return new StltMigrationResult(
                successRef,
                Collections.emptyMap(),
                Collections.emptySet(),
                Collections.emptySet()
            );
        }

        public void copyInto(
            Map<String, String> stltPropsToAddRef,
            Set<String> stltPropKeysToDeleteRef,
            Set<String> stltPropNamespacesToDeleteRef
        )
        {
            stltPropsToAddRef.putAll(stltPropsToAdd);
            stltPropKeysToDeleteRef.addAll(stltPropKeysToDelete);
            stltPropNamespacesToDeleteRef.addAll(stltPropNamespacesToDelete);
        }
    }
}
