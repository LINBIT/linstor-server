package com.linbit.linstor.core;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.PriorityProps;
import com.linbit.linstor.annotation.DeviceManagerContext;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.CoreModule.ExternalFileMap;
import com.linbit.linstor.core.CoreModule.ResourceDefinitionMap;
import com.linbit.linstor.core.cfg.StltConfig;
import com.linbit.linstor.core.identifier.ExternalFileName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.objects.ExternalFile;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.StorageException;
import com.linbit.utils.StringUtils;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

@Singleton
public class StltExternalFileHandler
{
    private final ErrorReporter errorReporter;
    private final AccessContext wrkCtx;

    private final Map<ExternalFileName, Set<ResourceName>> extFileRequestedByRscDfnsMap;
    private final Map<ResourceName, Set<ExternalFileName>> rscDfnToExtFilesMap;
    private final ExternalFileMap extFileMap;
    private final ResourceDefinitionMap rscDfnMap;
    private final StltConfig stltCfg;

    @Inject
    public StltExternalFileHandler(
        ErrorReporter errorReporterRef,
        @DeviceManagerContext AccessContext wrkCtxRef,
        CoreModule.ExternalFileMap extFileMapRef,
        CoreModule.ResourceDefinitionMap rscDfnMapRef,
        StltConfig stltCfgRef
    )
    {
        errorReporter = errorReporterRef;
        wrkCtx = wrkCtxRef;
        extFileMap = extFileMapRef;
        rscDfnMap = rscDfnMapRef;
        stltCfg = stltCfgRef;
        extFileRequestedByRscDfnsMap = new HashMap<>();
        rscDfnToExtFilesMap = new HashMap<>();
    }

    /**
     * Cleanup caching maps
     */
    public void clear()
    {
        extFileRequestedByRscDfnsMap.clear();
        rscDfnToExtFilesMap.clear();
    }

    public void ensureNotInUse(ExternalFile extFileRef)
    {
        Set<ResourceName> set = extFileRequestedByRscDfnsMap.get(extFileRef.getName());
        if (set != null && !set.isEmpty())
        {
            throw new ImplementationError(
                "External file " + extFileRef.getName().extFileName + " should be deleted but is still in use by:\n" +
                    StringUtils.join(set, ", ")
            );
        }
    }

    public void rebuildExtFilesToRscDfnMaps(Node localNodeRef) throws StorageException
    {
        try
        {
            for (ResourceDefinition rscDfn : rscDfnMap.values())
            {
                Resource localRsc = rscDfn.getResource(wrkCtx, localNodeRef.getName());
                if (localRsc != null)
                {
                    /*
                     * we might have an RD with an SPD with a local snapshot but NO resource
                     */
                    getRequestedExternalFiles(localRsc, false);
                }
            }
        }
        catch (AccessDeniedException | InvalidNameException | DatabaseException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    public void handle(Resource rscRef) throws StorageException
    {
        Set<ExternalFileName> requestedExternalFiles;
        try
        {
            requestedExternalFiles = getRequestedExternalFiles(rscRef, true);
            rewriteIfNeeded(requestedExternalFiles);
        }
        catch (AccessDeniedException | InvalidNameException | DatabaseException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    private Set<ExternalFileName> getRequestedExternalFiles(Resource rscRef, boolean deleteExtFileIfNotNeeded)
        throws AccessDeniedException, InvalidNameException, StorageException, DatabaseException
    {
        Set<ExternalFileName> ret = new HashSet<>();

        ResourceDefinition rscDfn = rscRef.getResourceDefinition();
        ResourceName rscName = rscDfn.getName();
        Set<ExternalFileName> alreadyRequestedExtFileSet = rscDfnToExtFilesMap.get(rscName);
        Set<ExternalFileName> unrequestExtFileSet;
        if (alreadyRequestedExtFileSet != null)
        {
            unrequestExtFileSet = new HashSet<>(alreadyRequestedExtFileSet);
        }
        else
        {
            unrequestExtFileSet = new HashSet<>();
        }

        if (!rscRef.getStateFlags().isSet(wrkCtx, Resource.Flags.DELETE))
        {
            /*
             * only check currently requested extFiles if the resource should not be deleted.
             *
             * otherwise the resource with the resource-definition itself will be deleted
             * soon.
             * As the layers should have already remove the resource, all thats left during this
             * call is the resource in DELETE state. That means we can do the cleanup of extFiles already now
             */

            PriorityProps prioProps = new PriorityProps(
                rscDfn.getProps(wrkCtx) // for now prop is only allowed in RD.
            );

            Map<String, String> files = prioProps.renderRelativeMap(InternalApiConsts.NAMESPC_FILES);
            for (Entry<String, String> file : files.entrySet())
            {
                if (file.getValue().equals(ApiConsts.VAL_TRUE))
                {
                    ExternalFileName extFileName = new ExternalFileName("/" + file.getKey());
                    ret.add(extFileName);

                    lazyAdd(rscDfnToExtFilesMap, rscName, extFileName);
                    lazyAdd(extFileRequestedByRscDfnsMap, extFileName, rscName);
                }
            }
        }

        // cleanup rscDfnToExtFilesMap and extFileRequestedByRscDfnsMap
        unrequestExtFileSet.removeAll(ret);
        for (ExternalFileName extFileName : unrequestExtFileSet)
        {
            Set<ResourceName> requestedByRscDfnSet = extFileRequestedByRscDfnsMap.get(extFileName);
            if (requestedByRscDfnSet != null)
            {
                requestedByRscDfnSet.remove(rscName);

                if (deleteExtFileIfNotNeeded)
                {
                    if (requestedByRscDfnSet.isEmpty())
                    {
                        delete(extFileName);
                    }
                    else
                    {
                        errorReporter.logTrace(
                            "Not deleting %s as it is still used by some resource definitions",
                            extFileName.extFileName
                        );
                    }
                }
            }
        }

        rscDfnToExtFilesMap.put(rscName, ret);

        return ret;
    }

    private <K, V> void lazyAdd(Map<K, Set<V>> map, K key, V value)
    {
        Set<V> set = map.get(key);
        if (set == null)
        {
            set = new HashSet<>();
            map.put(key, set);
        }
        set.add(value);
    }

    private void rewriteIfNeeded(Set<ExternalFileName> requestedExternalFilesRef)
        throws InvalidNameException, StorageException, DatabaseException, AccessDeniedException
    {
        for (ExternalFileName extFileName : requestedExternalFilesRef)
        {
            ExternalFile externalFile = extFileMap.get(extFileName);
            if (externalFile == null)
            {
                throw new ImplementationError("Unknown external file requested");
            }
            if (
                !externalFile.alreadyWritten() &&
                externalFile.getContent(wrkCtx).length > 0 &&
                isWhitelisted(externalFile)
            )
            {
                rewrite(externalFile);
            }
        }
    }

    private boolean isWhitelisted(ExternalFile externalFileRef) throws StorageException
    {
        Path extFilePath = Paths.get(externalFileRef.getName().extFileName).normalize();
        boolean whitelisted = stltCfg.getWhitelistedExternalFilePaths().contains(extFilePath.getParent());
        if (!whitelisted)
        {
            throw new StorageException(
                "The path " + extFilePath + " does not have a whitelisted parent. Allowed parent directories: " +
                    stltCfg.getWhitelistedExternalFilePaths()
            );
        }
        return whitelisted;
    }

    private void rewrite(ExternalFile externalFile) throws StorageException, DatabaseException
    {
        Path path = Paths.get(externalFile.getName().extFileName);
        Path tmpFile;
        try
        {
            tmpFile = Files.createTempFile(path.getParent(), null, ".tmp");
        }
        catch (IOException exc)
        {
            throw new StorageException(
                "Failed to create temporary file in directory: " + path.getParent(),
                exc
            );
        }
        try
        {
            errorReporter.logDebug("Writing into temporary external file: %s", tmpFile.toString());
            Files.write(tmpFile, externalFile.getContent(wrkCtx), StandardOpenOption.WRITE);
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
        catch (IOException exc)
        {
            throw new StorageException("Failed to write content in temporary file: " + tmpFile, exc);
        }

        try
        {
            errorReporter.logDebug(
                "Moving temporary file (%s) to its destination: %s",
                tmpFile.toString(),
                path.toString()
            );
            Files.move(tmpFile, path, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        }
        catch (IOException exc)
        {
            throw new StorageException(
                "Failed to atomically move temporary file (" + tmpFile + ") to its destination (" + path + ")",
                exc
            );
        }
        errorReporter.logInfo("External file %s written successfully", path.toString());
        externalFile.setAlreadyWritten(true);
    }

    private void delete(ExternalFileName extFileNameRef) throws StorageException, DatabaseException
    {
        try
        {
            errorReporter.logTrace("Deleting external file: %s", extFileNameRef.extFileName);
            Files.deleteIfExists(Paths.get(extFileNameRef.extFileName));

            ExternalFile extFile = extFileMap.get(extFileNameRef);
            if (!extFile.isDeleted())
            {
                extFile.setAlreadyWritten(false);
            }
            errorReporter.logInfo("External file %s deleted.", extFileNameRef);
        }
        catch (IOException exc)
        {
            throw new StorageException(
                "Failed to remove external file " + extFileNameRef.extFileName,
                exc
            );
        }
    }
}
