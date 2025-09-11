package com.linbit.linstor.core.objects;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.core.objects.AuthToken.InitMaps;
import com.linbit.linstor.dbdrivers.AbsDatabaseDriver;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.DbEngine;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables;
import com.linbit.linstor.dbdrivers.RawParameters;
import com.linbit.linstor.dbdrivers.interfaces.AuthTokenCtrlDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgr;
import com.linbit.utils.Pair;

import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.AuthTokens.CREATED_AT;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.AuthTokens.DELETED_AT;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.AuthTokens.DESCRIPTION;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.AuthTokens.EXPIRES_AT;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.AuthTokens.ID;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.AuthTokens.IP_FILTER;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.AuthTokens.IS_ACTIVE;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.AuthTokens.IS_USER_TOKEN;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.AuthTokens.TOKEN_HASH;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.time.Instant;
import java.util.function.Function;

@Singleton
public final class AuthTokenDbDriver
    extends AbsDatabaseDriver<AuthToken, InitMaps, Void>
    implements AuthTokenCtrlDatabaseDriver
{
    private final TransactionObjectFactory transObjFactory;
    private final Provider<? extends TransactionMgr> transMgrProvider;

    private final SingleColumnDatabaseDriver<AuthToken, String> descriptionDriver;
    private final SingleColumnDatabaseDriver<AuthToken, Instant> deletedAtDriver;
    private final SingleColumnDatabaseDriver<AuthToken, String> ipFilterDriver;
    private final SingleColumnDatabaseDriver<AuthToken, Boolean> isActiveDriver;

    @Inject
    public AuthTokenDbDriver(
        ErrorReporter errorReporterRef,
        @SystemContext AccessContext dbCtxRef,
        DbEngine dbEngine,
        Provider<TransactionMgr> transMgrProviderRef,
        TransactionObjectFactory transObjFactoryRef
    )
    {
        super(dbCtxRef, errorReporterRef, GeneratedDatabaseTables.AUTH_TOKENS, dbEngine);
        transMgrProvider = transMgrProviderRef;
        transObjFactory = transObjFactoryRef;

        setColumnSetter(ID, AuthToken::getId);
        setColumnSetter(TOKEN_HASH, AuthToken::getTokenHash);
        setColumnSetter(DESCRIPTION, AuthToken::getDescription);
        setColumnSetter(CREATED_AT, authT -> dbEngine.getDateToDbTypeConverter(authT.getCreatedAt()));
        setColumnSetter(DELETED_AT, authT -> dbEngine.getDateToDbNullableTypeConverter(authT.getDeletedAt()));
        setColumnSetter(EXPIRES_AT, authT -> dbEngine.getDateToDbNullableTypeConverter(authT.getExpiresAt()));
        setColumnSetter(IP_FILTER, AuthToken::getIPFilter);
        setColumnSetter(IS_ACTIVE, AuthToken::isActive);
        setColumnSetter(IS_USER_TOKEN, AuthToken::isUserToken);

        descriptionDriver = generateSingleColumnDriver(
            DESCRIPTION,
            AuthToken::getDescription,
            Function.identity()
        );
        deletedAtDriver = generateSingleColumnDriver(
            DELETED_AT,
            authT -> authT.getDeletedAt() + "",
            dbEngine::getDateToDbTypeConverter
        );
        ipFilterDriver = generateSingleColumnDriver(
            IP_FILTER,
            authT -> authT != null ? authT.getIPFilter() : "null",
            Function.identity()
        );
        isActiveDriver = generateSingleColumnDriver(
            IS_ACTIVE,
            authT -> "" + authT.isActive(),
            Function.identity()
        );
    }

    @Override
    protected String getId(AuthToken dataRef) throws AccessDeniedException
    {
        return "AuthToken(" + dataRef.getId() + ")";
    }

    @Override
    protected Pair<AuthToken, InitMaps> load(RawParameters raw, Void ignored)
        throws DatabaseException
    {
        final int id = raw.getNotNull(ID);
        final String tokenHash = raw.getNotNull(TOKEN_HASH);
        final String description = raw.getNotNull(DESCRIPTION);
        final Long createdAt = raw.getNotNull(CREATED_AT);
        final @Nullable Long deletedAt = raw.get(DELETED_AT);
        final @Nullable Long expiresAt = raw.get(EXPIRES_AT);
        final @Nullable String ipFilter = raw.get(IP_FILTER);
        final boolean isActive = raw.getNotNull(IS_ACTIVE);
        final boolean isUserToken = raw.getNotNull(IS_USER_TOKEN);

        return new Pair<>(
            new AuthToken(
                id,
                tokenHash,
                description,
                isActive,
                Instant.ofEpochMilli(createdAt),
                deletedAt != null ? Instant.ofEpochMilli(deletedAt) : null,
                expiresAt != null ? Instant.ofEpochMilli(expiresAt) : null,
                ipFilter,
                isUserToken,
                transObjFactory,
                transMgrProvider,
                this
            ),
            new InitMapsImpl()
        );
    }

    @Override
    public SingleColumnDatabaseDriver<AuthToken, Instant> getDeletedAtDriver()
    {
        return deletedAtDriver;
    }

    @Override
    public SingleColumnDatabaseDriver<AuthToken, String> getDescriptionDriver()
    {
        return descriptionDriver;
    }

    @Override
    public SingleColumnDatabaseDriver<AuthToken, String> getIpFilterDriver()
    {
        return ipFilterDriver;
    }

    @Override
    public SingleColumnDatabaseDriver<AuthToken, Boolean> getIsActiveDriver()
    {
        return isActiveDriver;
    }

    private static class InitMapsImpl implements InitMaps
    {
        private InitMapsImpl()
        {
        }
    }
}
