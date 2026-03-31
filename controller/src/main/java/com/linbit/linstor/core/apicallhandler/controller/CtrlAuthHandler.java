package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ServiceName;
import com.linbit.SystemService;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.LinStorRuntimeException;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.api.rest.v1.config.GrizzlyHttpService;
import com.linbit.linstor.api.rest.v1.serializer.JsonGenTypes;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.core.apicallhandler.response.ApiDatabaseException;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.objects.AuthToken;
import com.linbit.linstor.core.objects.AuthTokenControllerFactory;
import com.linbit.linstor.core.objects.NetInterface;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.repository.AuthTokenRepository;
import com.linbit.linstor.core.repository.NodeRepository;
import com.linbit.linstor.core.repository.SystemConfRepository;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.InvalidValueException;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.locks.LockGuardFactory;
import com.linbit.utils.PairNonNull;
import com.linbit.utils.StringUtils;

import static com.linbit.locks.LockGuardFactory.LockObj.AUTH_TOKEN_MAP;
import static com.linbit.locks.LockGuardFactory.LockObj.CTRL_CONFIG;
import static com.linbit.locks.LockGuardFactory.LockObj.NODES_MAP;
import static com.linbit.locks.LockGuardFactory.LockType.READ;
import static com.linbit.locks.LockGuardFactory.LockType.WRITE;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import reactor.core.publisher.Flux;

import static com.google.common.net.InetAddresses.isInetAddress;

@Singleton
public class CtrlAuthHandler
{
    private static final int TOKEN_LENGTH = 32;

    private final ErrorReporter errorReporter;
    private final ScopeRunner scopeRunner;
    private final LockGuardFactory lockGuardFactory;
    private final CtrlTransactionHelper ctrlTransactionHelper;
    private final AuthTokenRepository authTokenRepository;
    private final AuthTokenControllerFactory authTokenFactory;
    private final SystemConfRepository systemConfRepository;
    private final NodeRepository nodeRepository;
    private final CtrlStltSerializer ctrlStltSerializer;
    private final Map<ServiceName, SystemService> systemServicesMap;
    private final AccessContext sysCtx;

    @Inject
    public CtrlAuthHandler(
        ErrorReporter errorReporterRef,
        ScopeRunner scopeRunnerRef,
        LockGuardFactory lockGuardFactoryRef,
        CtrlTransactionHelper ctrlTransactionHelperRef,
        AuthTokenRepository authTokenRepositoryRef,
        AuthTokenControllerFactory authTokenFactoryRef,
        SystemConfRepository systemConfRepositoryRef,
        NodeRepository nodeRepositoryRef,
        CtrlStltSerializer ctrlStltSerializerRef,
        Map<ServiceName, SystemService> systemServicesMapRef,
        @SystemContext AccessContext sysCtxRef
    )
    {
        errorReporter = errorReporterRef;
        scopeRunner = scopeRunnerRef;
        lockGuardFactory = lockGuardFactoryRef;
        ctrlTransactionHelper = ctrlTransactionHelperRef;
        authTokenRepository = authTokenRepositoryRef;
        authTokenFactory = authTokenFactoryRef;
        systemConfRepository = systemConfRepositoryRef;
        nodeRepository = nodeRepositoryRef;
        ctrlStltSerializer = ctrlStltSerializerRef;
        systemServicesMap = systemServicesMapRef;
        sysCtx = sysCtxRef;
    }

    public Flux<ApiCallRc> createToken(String description, @Nullable String expiresAtStr, @Nullable String ipFilter)
    {
        return scopeRunner
            .fluxInTransactionalScope(
                "Create auth token",
                lockGuardFactory.buildDeferred(WRITE, AUTH_TOKEN_MAP),
                () -> Flux.just(createTokenInTransaction(description, expiresAtStr, ipFilter))
            );
    }

    public Flux<ApiCallRc> initializeTokenAuth(boolean onlySatellites, String description, boolean noHttps)
    {
        return scopeRunner
            .fluxInTransactionalScope(
                "Initialize token auth",
                lockGuardFactory.buildDeferred(WRITE, AUTH_TOKEN_MAP, CTRL_CONFIG, NODES_MAP),
                () -> initializeTokenAuthInTransaction(onlySatellites, description, noHttps)
            );
    }

    private boolean isAutoHttpsEnabled()
    {
        try
        {
            @Nullable String autoHttps = systemConfRepository
                .getCtrlConfForView(sysCtx)
                .getProp(ApiConsts.KEY_AUTO_HTTPS, ApiConsts.NAMESPC_REST);
            return Boolean.parseBoolean(autoHttps);
        }
        catch (AccessDeniedException ignored)
        {
            return false;
        }
    }

    private Flux<ApiCallRc> initializeTokenAuthInTransaction(
        boolean onlySatellites, String description, boolean noHttps)
    {
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
        boolean autoHttpsAlreadyEnabled = isAutoHttpsEnabled();

        try
        {
            String tokenAuthEnabled = systemConfRepository
                .getCtrlConfForView(sysCtx)
                .getPropWithDefault(ApiConsts.KEY_TOKEN_AUTH_ENABLED, ApiConsts.NAMESPC_AUTH, "false");

            if (!StringUtils.propTrueOrYes(tokenAuthEnabled))
            {
                // Token auth not enabled yet - create initial token and enable it
                PairNonNull<String, AuthToken> createPair = createTokenAndHash(description, null, null, true);

                // Enable token authentication
                systemConfRepository.setCtrlProp(
                    sysCtx,
                    ApiConsts.KEY_TOKEN_AUTH_ENABLED,
                    "true",
                    ApiConsts.NAMESPC_AUTH
                );

                if (!noHttps)
                {
                    systemConfRepository.setCtrlProp(
                        sysCtx,
                        ApiConsts.KEY_AUTO_HTTPS,
                        "true",
                        ApiConsts.NAMESPC_REST
                    );
                }

                createAndSendSatelliteTokens();

                ctrlTransactionHelper.commit();

                apiCallRc.addEntry(
                    ApiCallRcImpl.entryBuilder(
                        ApiConsts.CREATED,
                        String.format("Token authentication initialized successfully. Init token %s", createPair.objA)
                    )
                    .putObjRef("token", createPair.objA)
                    .build()
                );

                errorReporter.logInfo("Token authentication initialized.");
            }
            else
            {
                // Token auth already enabled
                if (onlySatellites)
                {
                    // Revoke all existing non-user tokens
                    for (AuthToken existingToken : authTokenRepository.getMapForView().values())
                    {
                        if (!existingToken.isUserToken() && existingToken.getDeletedAt() == null)
                        {
                            existingToken.setDeletedAt(Instant.now());
                        }
                    }

                    // Create fresh satellite tokens
                    createAndSendSatelliteTokens();

                    ctrlTransactionHelper.commit();

                    apiCallRc.addEntry(
                        ApiCallRcImpl.entryBuilder(
                            ApiConsts.MODIFIED,
                            "Satellite tokens re-initialized successfully"
                        )
                        .build()
                    );

                    errorReporter.logInfo("Satellite auth tokens re-initialized");
                }
                else
                {
                    apiCallRc.addEntry(
                        ApiCallRcImpl.entryBuilder(
                            ApiConsts.WARN_NO_EFFECT,
                            "Token authentication is already enabled"
                        )
                        .setSkipErrorReport(true)
                        .build()
                    );
                }
            }
        }
        catch (AccessDeniedException exc)
        {
            throw new ApiAccessDeniedException(
                exc, "initialize token authentication", ApiConsts.FAIL_ACC_DENIED_COMMAND);
        }
        catch (LinStorDataAlreadyExistsException exc)
        {
            throw new ApiRcException(
                ApiCallRcImpl.entryBuilder(
                    ApiConsts.FAIL_EXISTS_NODE,
                    "Auth token already exists"
                )
                .setCause("A token with the generated ID already exists")
                .setCorrection("Please retry the operation")
                .setSkipErrorReport(true)
                .build(),
                exc
            );
        }
        catch (DatabaseException exc)
        {
            throw new ApiDatabaseException(exc);
        }
        catch (InvalidKeyException | InvalidValueException exc)
        {
            throw new ApiRcException(
                ApiCallRcImpl.entryBuilder(
                    ApiConsts.FAIL_INVLD_CONF,
                    "Failed to set token authentication property"
                )
                .setCause(exc.getMessage())
                .setSkipErrorReport(true)
                .build(),
                exc
            );
        }

        Flux<ApiCallRc> flux = Flux.just(apiCallRc);
        if (!noHttps && !autoHttpsAlreadyEnabled)
        {
            flux = flux.doFinally(ignored -> restartGrizzlyHttpService());
        }
        return flux;
    }

    private void restartGrizzlyHttpService()
    {
        SystemService svc = systemServicesMap.get(GrizzlyHttpService.INSTANCE_NAME);
        if (svc instanceof GrizzlyHttpService grizzlyHttpService)
        {
            grizzlyHttpService.restart();
        }
    }

    private void createAndSendSatelliteTokens()
        throws AccessDeniedException, DatabaseException, LinStorDataAlreadyExistsException
    {
        for (Node node : nodeRepository.getMapForView(sysCtx).values())
        {
            Node.Type nodeType = node.getNodeType(sysCtx);
            if (nodeType != Node.Type.CONTROLLER &&
                nodeType != Node.Type.AUXILIARY &&
                !node.getFlags().isSet(sysCtx, Node.Flags.DELETE))
            {
                String nodeName = node.getName().displayValue;
                @Nullable String ipFilter = null;
                @Nullable NetInterface activeStltConn = node.getActiveStltConn(sysCtx);
                if (activeStltConn != null)
                {
                    ipFilter = activeStltConn.getAddress(sysCtx).getAddress();
                }

                PairNonNull<String, AuthToken> createPair =
                    createTokenAndHash("satellite:" + nodeName, null, ipFilter, false);

                sendAuthTokenToSatellite(node, createPair.objA);
            }
        }
    }

    /**
     * Find or create an auth token for the given satellite node and send it.
     * Called during satellite (re)connection when token auth is enabled.
     */
    public void ensureAndSendSatelliteToken(Node node)
    {
        try
        {
            String nodeName = node.getName().displayValue;
            String description = "satellite:" + nodeName;

            // We cannot recover the raw token from the hash, so if an existing token is found
            // we must revoke it and create a new one to send the raw token to the satellite.
            @Nullable AuthToken existingToken = authTokenRepository.findActiveSystemTokenByDescription(description);

            // Revoke existing token if found
            if (existingToken != null)
            {
                existingToken.setDeletedAt(Instant.now());
                existingToken.setActive(false);
            }

            // Create a new token for this satellite
            @Nullable String ipFilter = null;
            @Nullable NetInterface activeStltConn = node.getActiveStltConn(sysCtx);
            if (activeStltConn != null)
            {
                ipFilter = activeStltConn.getAddress(sysCtx).getAddress();
            }

            PairNonNull<String, AuthToken> createPair =
                createTokenAndHash(description, null, ipFilter, false);
            ctrlTransactionHelper.commit();
            sendAuthTokenToSatellite(node, createPair.objA);
        }
        catch (AccessDeniedException | DatabaseException | LinStorDataAlreadyExistsException exc)
        {
            errorReporter.reportError(exc);
        }
    }

    public boolean isTokenAuthEnabled()
    {
        try
        {
            String tokenAuthEnabled = systemConfRepository
                .getCtrlConfForView(sysCtx)
                .getPropWithDefault(ApiConsts.KEY_TOKEN_AUTH_ENABLED, ApiConsts.NAMESPC_AUTH, "false");
            return StringUtils.propTrueOrYes(tokenAuthEnabled);
        }
        catch (AccessDeniedException exc)
        {
            throw new ApiAccessDeniedException(
                exc, "check token auth status", ApiConsts.FAIL_ACC_DENIED_COMMAND);
        }
    }

    void sendAuthTokenToSatellite(Node node, String rawToken)
    {
        try
        {
            @Nullable Peer peer = node.getPeer(sysCtx);
            if (peer != null && peer.isOnline())
            {
                byte[] msg = ctrlStltSerializer
                    .onewayBuilder(InternalApiConsts.API_APPLY_AUTH_TOKEN)
                    .authTokenMessage(rawToken)
                    .build();
                peer.sendMessage(msg, InternalApiConsts.API_APPLY_AUTH_TOKEN);
                errorReporter.logInfo(
                    "Sent auth token to satellite '%s'",
                    node.getName().displayValue
                );
            }
        }
        catch (AccessDeniedException exc)
        {
            errorReporter.reportError(exc);
        }
    }

    private ApiCallRc createTokenInTransaction(String description,
                                               @Nullable String expiresAtStr,
                                               @Nullable String ipFilter)
    {
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();

        try
        {
            @Nullable Timestamp expiresAt = null;
            if (!StringUtils.isEmpty(expiresAtStr))
            {
                try
                {
                    // Extract just the date part (first 10 chars: YYYY-MM-DD)
                    String datePart = expiresAtStr.length() >= 10 ? expiresAtStr.substring(0, 10) : expiresAtStr;
                    LocalDate localDate = LocalDate.parse(datePart, DateTimeFormatter.ISO_LOCAL_DATE);
                    expiresAt = Timestamp.from(localDate.atStartOfDay(ZoneOffset.UTC).toInstant());
                }
                catch (DateTimeParseException exc)
                {
                    throw new ApiRcException(
                        ApiCallRcImpl.entryBuilder(
                            ApiConsts.FAIL_INVLD_CONF,
                            "Invalid expiration date format: " + expiresAtStr
                        )
                        .setCause("The expiration date must start with a valid date (e.g., 2025-12-31)")
                        .setSkipErrorReport(true)
                        .build()
                    );
                }
            }

            if (!StringUtils.isEmpty(ipFilter) && !isInetAddress(ipFilter))
            {
                throw new ApiRcException(
                    ApiCallRcImpl.entryBuilder(
                        ApiConsts.FAIL_INVLD_CONF,
                        "Invalid IP filter: " + ipFilter
                    )
                    .setCause("The IP filter must be a valid IPv4 or IPv6 address")
                    .setSkipErrorReport(true)
                    .build()
                );
            }

            PairNonNull<String, AuthToken> createPair = createTokenAndHash(description, expiresAt, ipFilter, true);

            ctrlTransactionHelper.commit();

            apiCallRc.addEntry(
                ApiCallRcImpl.entryBuilder(
                    ApiConsts.CREATED,
                    "Auth token created successfully: " + createPair.objA
                )
                .putObjRef("token", createPair.objA)
                .build()
            );

            errorReporter.logInfo("Created auth token with id: %d", createPair.objB.getId());
        }
        catch (LinStorDataAlreadyExistsException exc)
        {
            throw new ApiRcException(
                ApiCallRcImpl.entryBuilder(
                    ApiConsts.FAIL_EXISTS_NODE,
                    "Auth token already exists"
                )
                .setCause("A token with the generated ID already exists")
                .setCorrection("Please retry the operation")
                .setSkipErrorReport(true)
                .build(),
                exc
            );
        }
        catch (DatabaseException exc)
        {
            throw new ApiDatabaseException(exc);
        }

        return apiCallRc;
    }

    public List<JsonGenTypes.AuthToken> listTokens()
    {
        List<JsonGenTypes.AuthToken> tokenList = new ArrayList<>();

        try (var ignored = lockGuardFactory.build(READ, AUTH_TOKEN_MAP))
        {
            for (AuthToken authToken : authTokenRepository.getMapForView().values())
            {
                if (authToken.getDeletedAt() == null)
                {
                    JsonGenTypes.AuthToken tokenJson = new JsonGenTypes.AuthToken();
                    tokenJson.id = authToken.getId();
                    tokenJson.description = authToken.getDescription();
                    tokenJson.is_active = authToken.isActive();
                    tokenJson.created_at = formatDate(authToken.getCreatedAt());
                    tokenJson.deleted_at = authToken.getDeletedAt() != null ?
                        formatDate(authToken.getDeletedAt()) : null;
                    tokenJson.ip_filter = authToken.getIPFilter();
                    tokenJson.expires_at = authToken.getExpiresAt() != null ?
                        formatDate(authToken.getExpiresAt()) : null;
                    tokenJson.is_user_token = authToken.isUserToken();
                    tokenList.add(tokenJson);
                }
            }
        }

        return tokenList;
    }

    public Flux<ApiCallRc> modifyToken(
        int tokenId,
        @Nullable String description,
        @Nullable String ipFilter,
        @Nullable Boolean isActive
    )
    {
        return scopeRunner
            .fluxInTransactionalScope(
                "Modify auth token",
                lockGuardFactory.buildDeferred(WRITE, AUTH_TOKEN_MAP),
                () -> Flux.just(modifyTokenInTransaction(tokenId, description, ipFilter, isActive))
            );
    }

    private ApiCallRc modifyTokenInTransaction(
        int tokenId,
        @Nullable String description,
        @Nullable String ipFilter,
        @Nullable Boolean isActive
    )
    {
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();

        @Nullable AuthToken authToken = authTokenRepository.get(tokenId);
        if (authToken == null || authToken.getDeletedAt() != null)
        {
            throw new ApiRcException(
                ApiCallRcImpl.entryBuilder(
                    ApiConsts.FAIL_NOT_FOUND_NODE,
                    "Auth token not found"
                )
                .setCause("The specified auth token does not exist")
                .setSkipErrorReport(true)
                .build()
            );
        }

        if (!StringUtils.isEmpty(ipFilter) && !isInetAddress(ipFilter))
        {
            throw new ApiRcException(
                ApiCallRcImpl.entryBuilder(
                    ApiConsts.FAIL_INVLD_CONF,
                    "Invalid IP filter: " + ipFilter
                )
                .setCause("The IP filter must be a valid IPv4 or IPv6 address")
                .setSkipErrorReport(true)
                .build()
            );
        }

        try
        {
            if (description != null)
            {
                authToken.setDescription(description);
            }
            if (ipFilter != null)
            {
                authToken.setIpFilter(ipFilter);
            }
            if (isActive != null)
            {
                authToken.setActive(isActive);
            }
        }
        catch (DatabaseException exc)
        {
            throw new ApiDatabaseException(exc);
        }

        ctrlTransactionHelper.commit();

        apiCallRc.addEntry(
            ApiCallRcImpl.entryBuilder(
                ApiConsts.MODIFIED,
                "Auth token modified successfully"
            )
            .build()
        );

        errorReporter.logInfo("Modified auth token: %d", tokenId);

        return apiCallRc;
    }

    public Flux<ApiCallRc> revokeToken(int tokenId)
    {
        return scopeRunner
            .fluxInTransactionalScope(
                "Revoke auth token",
                lockGuardFactory.buildDeferred(WRITE, AUTH_TOKEN_MAP),
                () -> Flux.just(revokeTokenInTransaction(tokenId))
            );
    }

    private ApiCallRc revokeTokenInTransaction(int tokenId)
    {
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();

        @Nullable AuthToken authToken = authTokenRepository.get(tokenId);
        if (authToken == null || authToken.getDeletedAt() != null)
        {
            apiCallRc.addEntry(
                ApiCallRcImpl.entryBuilder(
                    ApiConsts.WARN_NOT_FOUND,
                    "Auth token not found or already revoked"
                )
                .setCause("The specified auth token does not exist or was already revoked")
                .setSkipErrorReport(true)
                .build()
            );
            return apiCallRc;
        }

        try
        {
            authToken.setDeletedAt(Instant.now());
            authToken.setActive(false);
        }
        catch (DatabaseException exc)
        {
            throw new ApiDatabaseException(exc);
        }
        ctrlTransactionHelper.commit();

        apiCallRc.addEntry(
            ApiCallRcImpl.entryBuilder(
                ApiConsts.DELETED,
                "Auth token revoked successfully"
            )
            .build()
        );

        errorReporter.logInfo("Revoked auth token: %d", tokenId);

        return apiCallRc;
    }

    private PairNonNull<String, AuthToken> createTokenAndHash(
        String description,
        @Nullable Timestamp expiresAt,
        @Nullable String ipFilter,
        boolean isUserToken
    )
        throws DatabaseException, LinStorDataAlreadyExistsException
    {
        String rawToken = StringUtils.randomAlphaNumString(TOKEN_LENGTH);
        String tokenHash = sha256Hex(rawToken);

        return new PairNonNull<>(rawToken, authTokenFactory.create(
            null,
            tokenHash,
            description,
            true, // isActive
            expiresAt,
            ipFilter,
            isUserToken
        ));
    }

    public static String sha256Hex(String input)
    {
        try
        {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(input.getBytes(StandardCharsets.UTF_8));
            StringBuilder hexString = new StringBuilder(64);
            for (byte b : hash)
            {
                String hex = Integer.toHexString(0xff & b);
                if (hex.length() == 1)
                {
                    hexString.append('0');
                }
                hexString.append(hex);
            }
            return hexString.toString();
        }
        catch (NoSuchAlgorithmException exc)
        {
            throw new LinStorRuntimeException("SHA-256 algorithm not available", exc);
        }
    }

    private String formatDate(Instant date)
    {
        return OffsetDateTime.ofInstant(date, ZoneOffset.UTC)
            .format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
    }
}
