package com.linbit.linstor.core.migration;

import com.linbit.ChildProcessTimeoutException;
import com.linbit.ImplementationError;
import com.linbit.extproc.ExtCmd.OutputData;
import com.linbit.extproc.ExtCmdFactory;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.protobuf.FullSync.FullSyncStatus;
import com.linbit.linstor.core.migration.StltMigrationHandler.StltMigrationResult;
import com.linbit.linstor.core.objects.Node;
import com.linbit.utils.StringUtils;

import java.io.IOException;
import java.util.Collections;

public abstract class BaseStltMigration
{
    protected abstract StltMigrationResult migrate(ExtCmdFactory extCmdFactoryRef, Node localNodeRef)
        throws ChildProcessTimeoutException, IOException;

    /**
     * Creates and returns a {@link StltMigrationResult} with {@link FullSyncStatus#SUCCESS}, no properties added, but
     * deletes the entire namespace of this migration. The namespace is determined based on the annotation of the
     * implementing class.
     */
    protected StltMigrationResult createDefaultSuccessResult()
    {
        @Nullable StltMigration annotation = this.getClass().getAnnotation(StltMigration.class);
        if (annotation == null)
        {
            // this should have been caught already in the Guice module...
            throw new ImplementationError(this.getClass().getCanonicalName() + " was not properly annotated");
        }
        return new StltMigrationResult(
            FullSyncStatus.SUCCESS,
            Collections.emptyMap(),
            Collections.emptySet(),
            Collections.singleton(annotation.migration().getPropNamespaceFull())
        );
    }

    protected static String extCmd(ExtCmdFactory extCmdFactoryRef, String... cmd)
        throws ChildProcessTimeoutException, IOException
    {
        OutputData out = extCmdCheckRetCodeZero(extCmdFactoryRef, cmd);
        return new String(out.stdoutData);
    }

    protected static OutputData extCmdAsOutputData(ExtCmdFactory extCmdFactoryRef, String... cmd)
        throws ChildProcessTimeoutException, IOException
    {
        return extCmdFactoryRef.create().exec(cmd);
    }

    protected static OutputData extCmdCheckRetCodeZero(ExtCmdFactory extCmdFactoryRef, String... cmd)
        throws IOException, ChildProcessTimeoutException
    {
        OutputData outputData = extCmdAsOutputData(extCmdFactoryRef, cmd);
        if (outputData.exitCode != 0)
        {
            throw new IOException(
                String.format(
                    "External command '%s' returned with exit code %d",
                    StringUtils.join(" ", cmd),
                    outputData.exitCode
                )
            );
        }
        return outputData;
    }
}
