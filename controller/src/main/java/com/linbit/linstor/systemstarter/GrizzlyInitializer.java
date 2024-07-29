package com.linbit.linstor.systemstarter;

import com.linbit.ServiceName;
import com.linbit.SystemService;
import com.linbit.SystemServiceStartException;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.rest.v1.config.GrizzlyHttpService;
import com.linbit.linstor.core.cfg.CtrlConfig;
import com.linbit.linstor.logging.ErrorReporter;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

import com.google.inject.Injector;
import org.slf4j.event.Level;

public class GrizzlyInitializer implements StartupInitializer
{
    private final Injector injector;
    private final ErrorReporter errorReporter;
    private final CtrlConfig ctrlCfg;
    private final Map<ServiceName, SystemService> systemServicesMap;
    private @Nullable GrizzlyHttpService grizzlyHttpService;

    public GrizzlyInitializer(
        Injector injectorRef,
        ErrorReporter errorReporterRef,
        CtrlConfig ctrlCfgRef,
        Map<ServiceName, SystemService> systemServicesMapRef
    )
    {
        injector = injectorRef;
        errorReporter = errorReporterRef;
        ctrlCfg = ctrlCfgRef;
        systemServicesMap = systemServicesMapRef;
    }

    @Override
    public void initialize()
        throws SystemServiceStartException
    {
        boolean restEnabled = ctrlCfg.isRestEnabled();

        try
        {
            Path keyStorePath = null;
            String keyStorePassword;
            Path trustStorePath = null;
            String trustStorePassword;
            final String keyStorePathProp = ctrlCfg.getRestSecureKeystore();
            final String trustStorePathProp = ctrlCfg.getRestSecureTruststore();
            if (keyStorePathProp != null && ctrlCfg.isRestSecureEnabled())
            {
                keyStorePath = Paths.get(keyStorePathProp);
                if (!keyStorePath.isAbsolute())
                {
                    keyStorePath = ctrlCfg.getConfigPath().resolve(keyStorePath);
                }
            }
            if (trustStorePathProp != null)
            {
                trustStorePath = Paths.get(trustStorePathProp);
                if (!trustStorePath.isAbsolute())
                {
                    trustStorePath = ctrlCfg.getConfigPath().resolve(trustStorePath);
                }
            }

            keyStorePassword = ctrlCfg.getRestSecureKeystorePassword();
            trustStorePassword = ctrlCfg.getRestSecureTruststorePassword();

            if (restEnabled)
            {
                grizzlyHttpService = new GrizzlyHttpService(
                    injector,
                    errorReporter,
                    ctrlCfg.getRestBindAddressWithPort(),
                    ctrlCfg.getRestSecureBindAddressWithPort(),
                    keyStorePath,
                    keyStorePassword,
                    trustStorePath,
                    trustStorePassword,
                    ctrlCfg.getLogRestAccessLogPath(),
                    ctrlCfg.getLogRestAccessMode(),
                    ctrlCfg.getWebUiDirectory()
                );
                systemServicesMap.put(grizzlyHttpService.getInstanceName(), grizzlyHttpService);
                grizzlyHttpService.start();
            }
        }
        catch (Exception exc)
        {
            String reportId = errorReporter.reportError(Level.ERROR, exc);
            errorReporter.logError(
                "Initialization of the REST service failed, see error report %s for details",
                reportId
            );
        }
    }

    @Override
    public void shutdown()
    {
        if (grizzlyHttpService != null)
        {
            grizzlyHttpService.shutdown();
        }
    }

    @Override
    public void awaitShutdown(long timeout)
    {
        if (grizzlyHttpService != null)
        {
            grizzlyHttpService.awaitShutdown(timeout);
        }
    }

    @Override
    public SystemService getSystemService()
    {
        return grizzlyHttpService;
    }
}
