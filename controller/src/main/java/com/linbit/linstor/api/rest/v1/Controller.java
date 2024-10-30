package com.linbit.linstor.api.rest.v1;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.prop.LinStorObject;
import com.linbit.linstor.api.rest.v1.serializer.JsonGenTypes;
import com.linbit.linstor.api.rest.v1.utils.ApiCallRcRestUtils;
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.core.apicallhandler.controller.CtrlApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlPropsInfoApiCallHandler;
import com.linbit.linstor.core.apis.ControllerConfigApi;
import com.linbit.linstor.core.cfg.CtrlConfig;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.utils.Pair;
import com.linbit.utils.TimeUtils;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.HashSet;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.glassfish.grizzly.http.server.Request;
import org.slf4j.MDC;
import reactor.core.publisher.Flux;

@Path("v1/controller")
@Produces(MediaType.APPLICATION_JSON)
public class Controller
{
    private final ErrorReporter errorReporter;
    private final ObjectMapper objectMapper;
    private final RequestHelper requestHelper;
    private final CtrlApiCallHandler ctrlApiCallHandler;
    private final CtrlConfig ctrlCfg;
    private final CtrlPropsInfoApiCallHandler ctrlPropsInfoApiCallHandler;

    public static final String DB_BACKUP_BASE_DIR = "/var/lib/linstor/";

    @Inject
    public Controller(
        ErrorReporter errorReporterRef,
        RequestHelper requestHelperRef,
        CtrlApiCallHandler ctrlApiCallHandlerRef,
        CtrlConfig ctrlCfgRef,
        CtrlPropsInfoApiCallHandler ctrlPropsInfoApiCallHandlerRef
    )
    {
        errorReporter = errorReporterRef;
        requestHelper = requestHelperRef;
        ctrlApiCallHandler = ctrlApiCallHandlerRef;
        ctrlCfg = ctrlCfgRef;
        ctrlPropsInfoApiCallHandler = ctrlPropsInfoApiCallHandlerRef;

        objectMapper = new ObjectMapper();
    }

    @GET
    @Path("properties")
    public Response listProperties(
        @Context Request request
    )
    {
        return requestHelper.doInScope(
            ApiConsts.API_LST_CTRL_PROPS, request,
            () ->
                Response.status(Response.Status.OK)
                    .entity(objectMapper.writeValueAsString(ctrlApiCallHandler.listCtrlCfg()))
                    .build(),
            false
        );
    }

    private Pair<String, String> splitFullKey(final String fullKey)
    {
        String key = fullKey;
        String namespace = "";
        int lastSlash = fullKey.lastIndexOf('/');
        if (lastSlash > 0)
        {
            namespace = fullKey.substring(0, lastSlash);
            key = fullKey.substring(lastSlash + 1);
        }

        return new Pair<>(key, namespace);
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Path("properties")
    public void setProperties(
        @Context Request request,
        @Suspended final AsyncResponse asyncResponse,
        String jsonData
    )
    {
        try (var ignore = MDC.putCloseable(ErrorReporter.LOGID, ErrorReporter.getNewLogId()))
        {
            JsonGenTypes.ControllerPropsModify properties = objectMapper.readValue(
                jsonData,
                JsonGenTypes.ControllerPropsModify.class
            );

            Flux<ApiCallRc> flux = ctrlApiCallHandler.modifyCtrl(
                properties.override_props,
                new HashSet<>(properties.delete_props),
                new HashSet<>(properties.delete_namespaces)
            );

            requestHelper.doFlux(
                ApiConsts.API_SET_CTRL_PROP,
                request,
                asyncResponse,
                ApiCallRcRestUtils.mapToMonoResponse(flux, Response.Status.CREATED)
            );
        }
        catch (IOException ioExc)
        {
            ApiCallRcRestUtils.handleJsonParseException(ioExc, asyncResponse);
        }
    }

    @DELETE
    @Path("properties/{key : .*}")
    public void deleteProperty(
        @Context Request request,
        @Suspended final AsyncResponse asyncResponse,
        @PathParam("key") String key
    )
    {
        Pair<String, String> keyPair = splitFullKey(key);
        Flux<ApiCallRc> flux = ctrlApiCallHandler.deleteCtrlCfgProp(keyPair.objA, keyPair.objB
        );
        requestHelper.doFlux(
            ApiConsts.API_SET_CTRL_PROP,
            request,
            asyncResponse,
            ApiCallRcRestUtils.mapToMonoResponse(flux, Response.Status.OK)
        );
    }

    @GET
    @Path("properties/info")
    public Response listCtrlPropsInfo(
        @Context Request request
    )
    {
        return requestHelper.doInScope(
            ApiConsts.API_LST_PROPS_INFO, request,
            () -> Response.status(Response.Status.OK)
                .entity(
                    objectMapper
                        .writeValueAsString(ctrlPropsInfoApiCallHandler.listFilteredProps(LinStorObject.CONTROLLER))
                )
                .build(),
            false
        );
    }

    @GET
    @Path("properties/info/all")
    public Response listFullPropsInfo(
        @Context Request request
    )
    {
        return requestHelper.doInScope(
            ApiConsts.API_LST_CTRL_PROPS, request,
            () -> Response.status(Response.Status.OK)
                .entity(objectMapper.writeValueAsString(ctrlPropsInfoApiCallHandler.listAllProps()))
                .build(),
            false
        );
    }

    @GET
    @Path("version")
    public Response version(
        @Context Request request
    )
    {
        JsonGenTypes.ControllerVersion controllerVersion = new JsonGenTypes.ControllerVersion();
        controllerVersion.version = LinStor.VERSION_INFO_PROVIDER.getVersion();
        controllerVersion.git_hash = LinStor.VERSION_INFO_PROVIDER.getGitCommitId();
        controllerVersion.build_time = LinStor.VERSION_INFO_PROVIDER.getBuildTime();
        controllerVersion.rest_api_version = JsonGenTypes.REST_API_VERSION;

        Response resp;
        try
        {
            resp =  Response
                .status(Response.Status.OK)
                .entity(objectMapper.writeValueAsString(controllerVersion))
                .build();
        }
        catch (JsonProcessingException exc)
        {
            errorReporter.reportError(exc);
            resp = Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
        }

        return resp;
    }

    @GET
    @Path("config")
    public Response info(
        @Context Request request
    )
    {
        JsonGenTypes.ControllerConfig controllerConfig = new JsonGenTypes.ControllerConfig();
        /*
         * Blacklisted Properties:
         *
         * ctrlCfg.getDbClientKeyPassword();
         * ctrlCfg.getDbClientKeyPkcs8Pem();
         * ctrlCfg.getDbPassword();
         * ctrlCfg.getDbUser();
         * ctrlCfg.getMasterPassphrase();
         * ctrlCfg.getRestSecureKeystore();
         * ctrlCfg.getRestSecureKeystorePassword();
         * ctrlCfg.getRestSecureTruststore();
         * ctrlCfg.getRestSecureTruststorePassword();
         */

        controllerConfig.config = new JsonGenTypes.ControllerConfigConfig();
        controllerConfig.config.dir = ctrlCfg.getConfigDir();

        controllerConfig.db = new JsonGenTypes.ControllerConfigDb();
        controllerConfig.db.ca_certificate = ctrlCfg.getDbCaCertificate();
        controllerConfig.db.client_certificate = ctrlCfg.getDbClientCertificate();
        controllerConfig.db.connection_url = ctrlCfg.getDbConnectionUrl();
        controllerConfig.db.in_memory = ctrlCfg.getDbInMemory();
        controllerConfig.db.version_check_disabled = ctrlCfg.isDbVersionCheckDisabled();

        controllerConfig.db.etcd = new JsonGenTypes.ControllerConfigDbEtcd();
        controllerConfig.db.etcd.operations_per_transaction = ctrlCfg.getEtcdOperationsPerTransaction();
        controllerConfig.db.etcd.prefix = ctrlCfg.getEtcdPrefix();

        controllerConfig.db.k8s = new JsonGenTypes.ControllerConfigDbK8s();
        controllerConfig.db.k8s.request_retries = ctrlCfg.getK8sRequestRetries();
        controllerConfig.db.k8s.max_rollback_entries = ctrlCfg.getK8sMaxRollbackEntries();

        controllerConfig.debug = new JsonGenTypes.ControllerConfigDebug();
        controllerConfig.debug.console_enabled = ctrlCfg.isDebugConsoleEnabled();

        controllerConfig.ldap = new JsonGenTypes.ControllerConfigLdap();
        controllerConfig.ldap.dn = ctrlCfg.getLdapDn();
        controllerConfig.ldap.enabled = ctrlCfg.isLdapEnabled();
        controllerConfig.ldap.public_access_allowed = ctrlCfg.isLdapPublicAccessAllowed();
        controllerConfig.ldap.search_base = ctrlCfg.getLdapSearchBase();
        controllerConfig.ldap.search_filter = ctrlCfg.getLdapSearchFilter();
        controllerConfig.ldap.uri = ctrlCfg.getLdapUri();

        controllerConfig.log = new JsonGenTypes.ControllerConfigLog();
        controllerConfig.log.directory = ctrlCfg.getLogDirectory();
        controllerConfig.log.level = ctrlCfg.getLogLevel();
        controllerConfig.log.level_linstor = ctrlCfg.getLogLevelLinstor();
        controllerConfig.log.print_stack_trace = ctrlCfg.isLogPrintStackTrace();
        controllerConfig.log.rest_access_log_path = ctrlCfg.getLogRestAccessLogPath();
        controllerConfig.log.rest_access_mode = ctrlCfg.getLogRestAccessMode().name();

        controllerConfig.http = new JsonGenTypes.ControllerConfigHttp();
        controllerConfig.http.enabled = ctrlCfg.isRestEnabled();
        controllerConfig.http.listen_address = ctrlCfg.getRestBindAddress();
        controllerConfig.http.port = ctrlCfg.getRestBindPort();

        controllerConfig.https = new JsonGenTypes.ControllerConfigHttps();
        controllerConfig.https.enabled = ctrlCfg.isRestSecureEnabled();
        controllerConfig.https.listen_address = ctrlCfg.getRestSecureBindAddress();
        controllerConfig.https.port = ctrlCfg.getRestSecureBindPort();

        Response resp;
        try
        {
            resp = Response
                .status(Response.Status.OK)
                .entity(objectMapper.writeValueAsString(controllerConfig))
                .build();
        }
        catch (JsonProcessingException exc)
        {
            errorReporter.reportError(exc);
            resp = Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
        }
        return resp;
    }

    private static class ControllerConfigPojo implements ControllerConfigApi
    {
        private final JsonGenTypes.ControllerConfig config;

        ControllerConfigPojo(JsonGenTypes.ControllerConfig configRef)
        {
            config = configRef;
        }

        @Override
        public String getLogLevel()
        {
            return config.log.level;
        }

        @Override
        public String getLogLevelLinstor()
        {
            return config.log.level_linstor;
        }

        @Override
        public String getLogLevelGlobal()
        {
            return config.log.level_global;
        }

        @Override
        public String getLogLevelLinstorGlobal()
        {
            return config.log.level_linstor_global;
        }
    }

    @PUT
    @Path("config")
    public void setConfig(
        @Context
        Request request,
        @Suspended final AsyncResponse asyncResponse,
        String jsonData
    )
    {
        Flux<ApiCallRc> flux = Flux.empty();
        try
        {
            JsonGenTypes.ControllerConfig config = objectMapper
                .readValue(jsonData, JsonGenTypes.ControllerConfig.class);
            ControllerConfigPojo conf = new ControllerConfigPojo(config);
            flux = ctrlApiCallHandler.setConfig(conf);
        }
        catch (IOException ioExc)
        {
            ApiCallRcRestUtils.handleJsonParseException(ioExc, asyncResponse);
        }
        catch (AccessDeniedException exc)
        {
            ApiCallRc rc = ApiCallRcImpl.singleApiCallRc(
                ApiConsts.MODIFIED | ApiConsts.MASK_CTRL_CONF,
                exc.toString()
            );
            requestHelper.doFlux(
                InternalApiConsts.API_MOD_CONFIG,
                request,
                asyncResponse,
                ApiCallRcRestUtils.mapToMonoResponse(Flux.just(rc), Response.Status.UNAUTHORIZED)
            );
        }
        requestHelper.doFlux(
            InternalApiConsts.API_MOD_CONFIG,
            request,
            asyncResponse,
            ApiCallRcRestUtils.mapToMonoResponse(flux, Response.Status.OK)
        );
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Path("backup/db")
    public Response backupDB(
        @Context Request request,
        String jsonData
    )
    {
        return requestHelper.doInScope("BackupDb", request, () ->
        {
            JsonGenTypes.DatabaseBackupRequest req = objectMapper
                .readValue(jsonData, JsonGenTypes.DatabaseBackupRequest.class);

            String backupPath = req.backup_name;

            if (backupPath == null)
            {
                backupPath = DB_BACKUP_BASE_DIR + "linstordb-backup-" + TimeUtils.DTF_NO_SPACE.format(
                    LocalDateTime.now()
                ) + ".zip";
            }
            else
            {
                backupPath = DB_BACKUP_BASE_DIR + backupPath;
            }

            if (!backupPath.endsWith(".zip"))
            {
                backupPath += ".zip";
            }

            ApiCallRc apiCallRc = ctrlApiCallHandler.backupDb(backupPath);
            return ApiCallRcRestUtils.toResponse(apiCallRc, Response.Status.CREATED);
        }, true);
    }
}
