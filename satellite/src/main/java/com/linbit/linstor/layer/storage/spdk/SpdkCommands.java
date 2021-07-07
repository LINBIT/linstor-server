package com.linbit.linstor.layer.storage.spdk;

import com.linbit.linstor.storage.StorageException;

import java.util.Iterator;

import com.fasterxml.jackson.databind.JsonNode;

public interface SpdkCommands<T>
{
    T createFat(
        String volumeGroup,
        String vlmId,
        long size,
        String... additionalParameters
    )
        throws StorageException;

    T resize(String volumeGroupRef, String vlmIdRef, long sizeRef) throws StorageException;

    T rename(String volumeGroupRef, String vlmCurrentIdRef, String vlmNewIdRef) throws StorageException;

    T delete(String volumeGroupRef, String vlmIdRef) throws StorageException;

    T createTransport(String typeRef) throws StorageException;

    T lvs() throws StorageException;

    T getLvolStores() throws StorageException;

    T lvsByName(String nameRef) throws StorageException;

    T getNvmfSubsystems() throws StorageException;

    Iterator<JsonNode> getJsonElements(T data) throws StorageException;
}
