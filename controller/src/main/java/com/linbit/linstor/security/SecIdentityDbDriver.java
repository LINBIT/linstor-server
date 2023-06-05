package com.linbit.linstor.security;

import com.linbit.ExhaustedPoolException;
import com.linbit.ImplementationError;
import com.linbit.InvalidIpAddressException;
import com.linbit.InvalidNameException;
import com.linbit.ValueInUseException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.drbd.md.MdException;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.dbdrivers.AbsDatabaseDriver;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.DbEngine;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables;
import com.linbit.linstor.dbdrivers.interfaces.SecIdentityCtrlDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.SecIdentityCtrlDatabaseDriver.SecIdentityInitObj;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.utils.Base64;
import com.linbit.utils.Pair;

import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.SecIdentities.IDENTITY_DSP_NAME;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.SecIdentities.IDENTITY_NAME;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.SecIdentities.ID_ENABLED;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.SecIdentities.ID_LOCKED;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.SecIdentities.PASS_HASH;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.SecIdentities.PASS_SALT;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.util.function.Function;

@Singleton
public class SecIdentityDbDriver extends AbsDatabaseDriver<Identity, SecIdentityInitObj, Void>
    implements SecIdentityCtrlDatabaseDriver
{
    private final AccessContext dbCtx;
    private final SingleColumnDatabaseDriver<Identity, byte[]> passHashDriver;
    private final SingleColumnDatabaseDriver<Identity, byte[]> passSaltDriver;
    private final SingleColumnDatabaseDriver<Identity, Boolean> idEnabledDriver;
    private final SingleColumnDatabaseDriver<Identity, Boolean> idLockedDriver;

    @Inject
    public SecIdentityDbDriver(
        @SystemContext AccessContext dbCtxRef,
        ErrorReporter errorReporterRef,
        DbEngine dbEngineRef,
        ObjectProtectionFactory objProtFactoryRef
    )
    {
        super(dbCtxRef, errorReporterRef, GeneratedDatabaseTables.SEC_IDENTITIES, dbEngineRef, objProtFactoryRef);
        dbCtx = dbCtxRef;

        setColumnSetter(IDENTITY_NAME, id -> id.name.value);
        setColumnSetter(IDENTITY_DSP_NAME, id -> id.name.displayValue);

        // Identities do not store their own password. Passwords are only checked during authentication and are always
        // queried directly from the database
        // That also means that an password-less Identity has to be created first before an (optional, see LDAP
        // authentication) password can be set via the value setters
        setColumnSetter(PASS_HASH, ignored -> null);
        setColumnSetter(PASS_SALT, ignored -> null);
        // new entries are always enabled and unlocked
        setColumnSetter(ID_ENABLED, ignored -> true);
        setColumnSetter(ID_LOCKED, ignored -> false);

        switch (getDbType()) {
            case SQL: // fall-through
            case K8S_CRD:
                passHashDriver = generateSingleColumnDriver(
                    PASS_HASH,
                    ignored -> MSG_DO_NOT_LOG,
                    Function.identity()
                );
                passSaltDriver = generateSingleColumnDriver(
                    PASS_SALT,
                    ignored -> MSG_DO_NOT_LOG,
                    Function.identity()
                );

                idEnabledDriver = generateSingleColumnDriver(
                    ID_ENABLED,
                    this::getId,
                    Function.identity()
                );
                idLockedDriver = generateSingleColumnDriver(
                    ID_LOCKED,
                    this::getId,
                    Function.identity()
                );
                break;
            case ETCD:
                passHashDriver = generateSingleColumnDriver(
                    PASS_HASH,
                    ignored -> MSG_DO_NOT_LOG,
                    Base64::encode
                );
                passSaltDriver = generateSingleColumnDriver(
                    PASS_SALT,
                    ignored -> MSG_DO_NOT_LOG,
                    Base64::encode
                );

                idEnabledDriver = generateSingleColumnDriver(
                    ID_ENABLED,
                    this::getId,
                    enabled -> Boolean.toString(enabled)
                );
                idLockedDriver = generateSingleColumnDriver(
                    ID_LOCKED,
                    this::getId,
                    locked -> Boolean.toString(locked)
                );
                break;
            default:
                throw new ImplementationError("Unexpected Db type: " + getDbType());
        }
    }
    @Override
    public SingleColumnDatabaseDriver<Identity, byte[]> getPassHashDriver()
    {
        return passHashDriver;
    }

    @Override
    public SingleColumnDatabaseDriver<Identity, byte[]> getPassSaltDriver()
    {
        return passSaltDriver;
    }

    @Override
    public SingleColumnDatabaseDriver<Identity, Boolean> getIdEnabledDriver()
    {
        return idEnabledDriver;
    }

    @Override
    public SingleColumnDatabaseDriver<Identity, Boolean> getIdLockedDriver()
    {
        return idLockedDriver;
    }

    @Override
    protected Pair<Identity, SecIdentityInitObj> load(RawParameters rawRef, Void parentRef)
        throws DatabaseException, InvalidNameException, ValueOutOfRangeException, InvalidIpAddressException,
        MdException, ExhaustedPoolException, ValueInUseException, RuntimeException, AccessDeniedException
    {
        IdentityName idName = rawRef.build(IDENTITY_DSP_NAME, IdentityName::new);
        byte[] passHash;
        byte[] passSalt;
        boolean enabled;
        boolean locked;

        switch (getDbType())
        {
            case SQL: // fall-through
            case K8S_CRD:
                passHash = rawRef.get(PASS_HASH);
                passSalt = rawRef.get(PASS_SALT);
                enabled = rawRef.get(ID_ENABLED);
                locked = rawRef.get(ID_LOCKED);
                break;
            case ETCD:
                passHash = rawRef.buildParsed(PASS_HASH, Base64::decode);
                passSalt = rawRef.buildParsed(PASS_SALT, Base64::decode);
                enabled = rawRef.buildParsed(ID_ENABLED, Boolean::parseBoolean);
                locked = rawRef.buildParsed(ID_LOCKED, Boolean::parseBoolean);
                break;
            default:
                throw new ImplementationError("Unexpected Db type: " + getDbType());
        }

        return new Pair<>(
            Identity.create(dbCtx, idName),
            new SecIdentityInitObj(passHash, passSalt, enabled, locked)
        );
    }

    @Override
    protected String getId(Identity dataRef) throws AccessDeniedException
    {
        return "Identity: " + dataRef.name.displayValue;
    }
}
