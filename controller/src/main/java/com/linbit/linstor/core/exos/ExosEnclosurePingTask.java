package com.linbit.linstor.core.exos;

import com.linbit.ImplementationError;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.pojo.ExosEnclosureHealthPojo;
import com.linbit.linstor.core.repository.SystemConfRepository;
import com.linbit.linstor.layer.storage.exos.rest.ExosRestClient;
import com.linbit.linstor.layer.storage.exos.rest.responses.ExosRestSystemCollection;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.ReadOnlyProps;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.tasks.TaskScheduleService.Task;
import com.linbit.linstor.utils.PropsUtils;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Deprecated(forRemoval = true)
@Singleton
public class ExosEnclosurePingTask implements Task
{
    public static final int RETRY_MS = 10 * 60_000; // recache every 10 min

    private final ErrorReporter errorReporter;
    private final SystemConfRepository systemConfRepository;
    private final AccessContext sysCtx;

    private Map<String, EnclosureInfo> enclosureInfo = new HashMap<>();

    @Inject
    public ExosEnclosurePingTask(
        ErrorReporter errorReporterRef,
        SystemConfRepository systemConfRepositoryRef,
        @SystemContext AccessContext sysCtxRef
    )
    {
        errorReporter = errorReporterRef;
        systemConfRepository = systemConfRepositoryRef;
        sysCtx = sysCtxRef;
    }

    public List<ExosEnclosureHealthPojo> getPojos(boolean recache)
    {
        recacheIfEmpty(recache);

        synchronized (enclosureInfo)
        {
            return enclosureInfo.values().stream()
                .map(EnclosureInfo::asPojo)
                .collect(Collectors.toList());
        }
    }

    public void enclosureDeleted(String enclosureName)
    {
        synchronized (enclosureInfo)
        {
            enclosureInfo.remove(enclosureName);
        }
    }

    private void recacheIfEmpty(boolean forceRecache)
    {
        boolean empty;
        synchronized (enclosureInfo)
        {
            empty = enclosureInfo.isEmpty();
        }

        if (forceRecache || empty)
        {
            recache();
        }
    }

    public ExosRestClient getClient(String enclosureName)
    {
        ExosRestClient ret = null;

        recacheIfEmpty(false);
        synchronized (enclosureInfo)
        {
            EnclosureInfo info = enclosureInfo.get(enclosureName);
            if (info != null)
            {
                ret = info.client;
            }
        }
        return ret;
    }

    @Override
    public long run(long scheduledAt)
    {
        recache();
        return getNextFutureReschedule(scheduledAt, RETRY_MS);
    }

    private void recache()
    {
        try
        {
            ReadOnlyProps props = systemConfRepository.getStltConfForView(sysCtx);
            ReadOnlyProps exosProps = props.getNamespace(ApiConsts.NAMESPC_EXOS).orElse(null);
            if (exosProps != null)
            {
                Set<String> enclosuresToDelete;
                synchronized (enclosureInfo)
                {
                    enclosuresToDelete = new HashSet<>(enclosureInfo.keySet());
                }
                Map<String, EnclosureInfo> entriesToAdd = new HashMap<>();
                Iterator<String> namespaceIt = exosProps.iterateNamespaces();
                while (namespaceIt.hasNext())
                {
                    String enclosureName = namespaceIt.next();
                    String ctrlAIp = getIp(
                        props,
                        ApiConsts.NAMESPC_EXOS + "/" + enclosureName + "/" + ExosRestClient.CONTROLLERS[0] + "/"
                    );
                    String ctrlBIp = getIp(
                        props,
                        ApiConsts.NAMESPC_EXOS + "/" + enclosureName + "/" + ExosRestClient.CONTROLLERS[1] + "/"
                    );

                    EnclosureInfo info = enclosureInfo.get(enclosureName);
                    if (info != null)
                    {
                        enclosuresToDelete.remove(enclosureName);
                        info.ctrlAIp = ctrlAIp;
                        info.ctrlBIp = ctrlBIp;
                    }
                    else
                    {
                        info = new EnclosureInfo(enclosureName, ctrlAIp, ctrlBIp);
                        entriesToAdd.put(enclosureName, info);
                    }
                    ping(info);
                }

                synchronized (enclosureInfo)
                {
                    enclosureInfo.putAll(entriesToAdd);
                    enclosureInfo.keySet().removeAll(enclosuresToDelete);
                }
            }
        }
        catch (AccessDeniedException exc)
        {
            errorReporter.reportError(new ImplementationError(exc));
        }
    }

    private void ping(EnclosureInfo info) throws AccessDeniedException
    {
        try
        {
            info.system = info.client.showSystem();
        }
        catch (StorageException exc)
        {
            info.system = null;
            info.exception = exc;
            errorReporter.reportError(exc);
        }
    }

    private String getIp(ReadOnlyProps ctrlPropsRef, String baseKey)
    {
        return PropsUtils
            .getPropOrEnv(
                ctrlPropsRef,
                baseKey + ApiConsts.KEY_STOR_POOL_EXOS_API_IP,
                baseKey + ApiConsts.KEY_STOR_POOL_EXOS_API_IP_ENV
            );
    }

    private class EnclosureInfo
    {
        final String enclosureName;
        String ctrlAIp;
        String ctrlBIp;

        ExosRestSystemCollection system;
        StorageException exception;

        final ExosRestClient client;

        EnclosureInfo(String enclosureNameRef, String ctrlAIpRef, String ctrlBIpRef)
            throws AccessDeniedException
        {
            enclosureName = enclosureNameRef;
            ctrlAIp = ctrlAIpRef;
            ctrlBIp = ctrlBIpRef;

            client = new ExosRestClient(
                sysCtx,
                errorReporter,
                systemConfRepository.getStltConfForView(sysCtx),
                enclosureNameRef
            );
        }

        public ExosEnclosureHealthPojo asPojo()
        {
            String health;
            String healthReason;
            if (system != null && system.system != null && system.system.length > 0)
            {
                health = system.system[0].health;
                healthReason = system.system[0].healthReason;
            }
            else if (exception != null)
            {
                health = "StorageException";
                healthReason = exception.getMessage();
            }
            else
            {
                health = null;
                healthReason = null;
            }
            return new ExosEnclosureHealthPojo(
                enclosureName,
                ctrlAIp,
                ctrlBIp,
                health,
                healthReason
            );
        }
    }
}
