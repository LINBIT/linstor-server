package com.linbit.linstor.api.rest.v1;

import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.rest.v1.serializer.JsonGenTypes;
import com.linbit.linstor.api.rest.v1.utils.ApiCallRcRestUtils;
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.core.apicallhandler.controller.CtrlApiCallHandler;
import com.linbit.linstor.core.cfg.CtrlConfig;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.utils.Pair;

import javax.inject.Inject;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.util.HashSet;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.glassfish.grizzly.http.server.Request;

@Path("controller")
@Produces(MediaType.APPLICATION_JSON)
public class Controller
{
    private final ErrorReporter errorReporter;
    private final ObjectMapper objectMapper;
    private final RequestHelper requestHelper;
    private final CtrlApiCallHandler ctrlApiCallHandler;
    private final CtrlConfig ctrlCfg;

    @Inject
    public Controller(
        ErrorReporter errorReporterRef,
        RequestHelper requestHelperRef,
        CtrlApiCallHandler ctrlApiCallHandlerRef,
        CtrlConfig ctrlCfgRef
    )
    {
        errorReporter = errorReporterRef;
        requestHelper = requestHelperRef;
        ctrlApiCallHandler = ctrlApiCallHandlerRef;
        ctrlCfg = ctrlCfgRef;

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
    @Path("properties")
    public Response setProperties(
        @Context Request request,
        String jsonData
    )
    {
        return requestHelper.doInScope(ApiConsts.API_SET_CTRL_PROP, request, () ->
        {
            JsonGenTypes.ControllerPropsModify properties = objectMapper.readValue(
                jsonData,
                JsonGenTypes.ControllerPropsModify.class
            );

            ApiCallRc apiCallRc = ctrlApiCallHandler.modifyCtrl(
                properties.override_props,
                new HashSet<>(properties.delete_props),
                new HashSet<>(properties.delete_namespaces)
            );

            return ApiCallRcRestUtils.toResponse(apiCallRc, Response.Status.CREATED);

        }, true);
    }

    @DELETE
    @Path("properties/{key : .*}")
    public Response deleteProperty(
        @Context Request request,
        @PathParam("key") String key
    )
    {
        return requestHelper.doInScope(ApiConsts.API_DEL_CTRL_PROP, request, () ->
        {
            Pair<String, String> keyPair = splitFullKey(key);

            ApiCallRc apiCallRc = ctrlApiCallHandler.deleteCtrlCfgProp(
                keyPair.objA,
                keyPair.objB
            );

            return ApiCallRcRestUtils.toResponse(apiCallRc, Response.Status.OK);
        }, true);
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
}
