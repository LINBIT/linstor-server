package com.linbit.linstor.core.objects;

import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.core.repository.AuthTokenRepository;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import javax.inject.Inject;
import javax.inject.Provider;

import java.sql.Timestamp;
import java.time.Instant;

public class AuthTokenControllerFactory
{
    private final AuthTokenRepository authTokenRepo;
    private final Provider<TransactionMgr> transMgrProvider;
    private final TransactionObjectFactory transObjFactory;
    private final AuthTokenDbDriver dbDriver;

    @Inject
    public AuthTokenControllerFactory(
        AuthTokenRepository authTokenRepoRef,
        Provider<TransactionMgr> transMgrProviderRef,
        TransactionObjectFactory transObjFactoryRef,
        AuthTokenDbDriver authTokenDbDriver
    )
    {
        authTokenRepo = authTokenRepoRef;
        transMgrProvider = transMgrProviderRef;
        transObjFactory = transObjFactoryRef;
        dbDriver = authTokenDbDriver;
    }

    public AuthToken create(
        @Nullable Integer idRef,
        String tokenHashRef,
        String descriptionRef,
        boolean isActiveRef,
        @Nullable Timestamp expiresAtRef,
        @Nullable String ipFilterRef,
        boolean isUserTokenRef
    )
        throws DatabaseException, LinStorDataAlreadyExistsException
    {
        int id = idRef == null ? getNextId() : idRef;
        @Nullable AuthToken ret = authTokenRepo.get(id);
        if (ret != null)
        {
            throw new LinStorDataAlreadyExistsException("AuthToken already exists");
        }

        ret = new AuthToken(
            id,
            tokenHashRef,
            descriptionRef,
            isActiveRef,
            Instant.now(),
            null,
            expiresAtRef != null ? Instant.ofEpochMilli(expiresAtRef.getTime()) : null,
            ipFilterRef,
            isUserTokenRef,
            transObjFactory,
            transMgrProvider,
            dbDriver
        );
        dbDriver.create(ret);
        authTokenRepo.put(id, ret);

        return ret;
    }

    private int getNextId()
    {
        int maxId = 0;
        for (Integer id : authTokenRepo.getMapForView().keySet())
        {
            if (id > maxId)
            {
                maxId = id;
            }
        }
        return maxId + 1;
    }
}
