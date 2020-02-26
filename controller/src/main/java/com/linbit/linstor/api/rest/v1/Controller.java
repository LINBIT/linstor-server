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
    @Path("info")
    public Response info(
        @Context Request request
    )
    {
        JsonGenTypes.ControllerInfo controllerInfo = new JsonGenTypes.ControllerInfo();
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
        controllerInfo.config_dir = ctrlCfg.getConfigDir();
        controllerInfo.config_path = ctrlCfg.getConfigPath().toString();
        controllerInfo.db_ca_certificate = ctrlCfg.getDbCaCertificate();
        controllerInfo.db_client_certificate = ctrlCfg.getDbClientCertificate();
        controllerInfo.db_connection_url = ctrlCfg.getDbConnectionUrl();
        controllerInfo.db_in_memory = ctrlCfg.getDbInMemory();
        controllerInfo.db_version_check_disabled = ctrlCfg.isDbVersionCheckDisabled();
        controllerInfo.debug_console_enabled = ctrlCfg.isDebugConsoleEnabled();
        controllerInfo.etcd_operations_per_transaction = ctrlCfg.getEtcdOperationsPerTransaction();
        controllerInfo.ldap_dn = ctrlCfg.getLdapDn();
        controllerInfo.ldap_enabled = ctrlCfg.isLdapEnabled();
        controllerInfo.ldap_public_access_allowed = ctrlCfg.isLdapPublicAccessAllowed();
        controllerInfo.ldap_search_base = ctrlCfg.getLdapSearchBase();
        controllerInfo.ldap_search_filter = ctrlCfg.getLdapSearchFilter();
        controllerInfo.ldap_uri = ctrlCfg.getLdapUri();
        controllerInfo.log_directory = ctrlCfg.getLogDirectory();
        controllerInfo.log_level = ctrlCfg.getLogLevel();
        controllerInfo.log_level_linstor = ctrlCfg.getLogLevelLinstor();
        controllerInfo.log_print_stack_trace = ctrlCfg.isLogPrintStackTrace();
        controllerInfo.log_rest_access_log_path = ctrlCfg.getLogRestAccessLogPath();
        controllerInfo.log_rest_access_mode = ctrlCfg.getLogRestAccessMode().toString();
        controllerInfo.rest_bind_address_with_port = ctrlCfg.getRestBindAddressWithPort();
        controllerInfo.rest_enabled = ctrlCfg.isRestEnabled();
        controllerInfo.rest_secure_bind_address_with_port = ctrlCfg.getRestSecureBindAddressWithPort();
        controllerInfo.rest_secure_enabled = ctrlCfg.isRestSecureEnabled();

        Response resp;
        try
        {
            resp = Response
                .status(Response.Status.OK)
                .entity(objectMapper.writeValueAsString(controllerInfo))
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
