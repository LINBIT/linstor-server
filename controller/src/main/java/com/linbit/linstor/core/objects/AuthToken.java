package com.linbit.linstor.core.objects;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.transaction.BaseTransactionObject;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.TransactionSimpleObject;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import javax.inject.Provider;

import java.time.Instant;
import java.util.Arrays;

public class AuthToken extends BaseTransactionObject implements Comparable<AuthToken>
{
    public interface InitMaps
    {
        // currently only a place holder for future maps
    }

    private final int id;
    private final String tokenHash;
    private final TransactionSimpleObject<AuthToken, String> description;
    private final TransactionSimpleObject<AuthToken, Boolean> isActive;
    private final Instant createdAt;
    private final TransactionSimpleObject<AuthToken, @Nullable Instant> deletedAt;
    private final @Nullable Instant expiresAt;
    private final TransactionSimpleObject<AuthToken, String> ipFilter;
    private final boolean isUserToken;

    public AuthToken(
        int idRef,
        String tokenHashRef,
        String descriptionRef,
        boolean isActiveRef,
        Instant createdAtRef,
        @Nullable Instant deletedAtRef,
        @Nullable Instant expiresAtRef,
        @Nullable String ipFilterRef,
        boolean isUserTokenRef,
        TransactionObjectFactory transObjFactory,
        Provider<? extends TransactionMgr> transMgrProvider,
        AuthTokenDbDriver dbDriverRef
    )
    {
        super(transMgrProvider);

        id = idRef;
        tokenHash = tokenHashRef;

        createdAt = createdAtRef;
        expiresAt = expiresAtRef;
        isUserToken = isUserTokenRef;

        description = transObjFactory.createTransactionSimpleObject(
            this, descriptionRef, dbDriverRef.getDescriptionDriver());
        isActive = transObjFactory.createTransactionSimpleObject(this, isActiveRef, dbDriverRef.getIsActiveDriver());
        deletedAt = transObjFactory.createTransactionSimpleObject(this, deletedAtRef, dbDriverRef.getDeletedAtDriver());
        ipFilter = transObjFactory.createTransactionSimpleObject(this, ipFilterRef, dbDriverRef.getIpFilterDriver());

        transObjs = Arrays.asList(
            description,
            isActive,
            deletedAt,
            ipFilter
        );
    }

    public int getId()
    {
        return id;
    }

    public String getTokenHash()
    {
        return tokenHash;
    }

    public String getDescription()
    {
        return description.get();
    }

    public void setDescription(String descriptionRef) throws DatabaseException
    {
        description.set(descriptionRef);
    }

    public Instant getCreatedAt()
    {
        return createdAt;
    }

    public @Nullable Instant getDeletedAt()
    {
        return deletedAt.get();
    }

    public void setDeletedAt(@Nullable Instant deletedAtRef) throws DatabaseException
    {
        deletedAt.set(deletedAtRef);
    }

    public @Nullable Instant getExpiresAt()
    {
        return expiresAt;
    }

    public @Nullable String getIPFilter()
    {
        return ipFilter.get();
    }

    public void setIpFilter(@Nullable String ipFilterRef) throws DatabaseException
    {
        ipFilter.set(ipFilterRef);
    }

    public boolean isActive()
    {
        return Boolean.TRUE.equals(isActive.get());
    }

    public void setActive(boolean isActiveRef) throws DatabaseException
    {
        isActive.set(isActiveRef);
    }

    public boolean isUserToken()
    {
        return isUserToken;
    }

    @Override
    public String toString()
    {
        return String.format("AuthToken(%d)", id);
    }

    @Override
    public int compareTo(AuthToken authToken)
    {
        return Integer.compare(id, authToken.id);
    }
}
