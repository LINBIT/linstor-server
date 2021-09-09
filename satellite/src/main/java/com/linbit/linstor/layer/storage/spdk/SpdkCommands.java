package com.linbit.linstor.layer.storage.spdk;

import com.linbit.linstor.security.AccessDeniedException;
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
        throws StorageException, AccessDeniedException;

    T resize(String volumeGroupRef, String vlmIdRef, long sizeRef) throws StorageException, AccessDeniedException;

    T rename(String volumeGroupRef, String vlmCurrentIdRef, String vlmNewIdRef)
        throws StorageException, AccessDeniedException;

    T delete(String volumeGroupRef, String vlmIdRef) throws StorageException, AccessDeniedException;

    void ensureTransportExists(String typeRef) throws StorageException, AccessDeniedException;

    T lvs() throws StorageException, AccessDeniedException;

    T getLvolStores() throws StorageException, AccessDeniedException;

    T lvsByName(String nameRef) throws StorageException, AccessDeniedException;

    T getNvmfSubsystems() throws StorageException, AccessDeniedException;

    T nvmSubsystemCreate(String subsystemNameRef) throws StorageException, AccessDeniedException;

    T nvmfSubsystemAddListener(
        String subsystemNameRef,
        String transportTypeRef,
        String addressRef,
        String stringRef,
        String portRef
    )
        throws StorageException, AccessDeniedException;

    T nvmfSubsystemAddNs(String subsystemNameRef, String stringRef) throws StorageException, AccessDeniedException;

    T nvmfDeleteSubsystem(String subsystemNameRef) throws StorageException, AccessDeniedException;

    T nvmfSubsystemRemoveNamespace(String subsystemNameRef, int namespaceNrRef)
        throws StorageException, AccessDeniedException;

    Iterator<JsonNode> getJsonElements(T data) throws StorageException, AccessDeniedException;

    T createSnapshot(String fullQualifiedVlmId, String snapName) throws StorageException, AccessDeniedException;

    T restoreSnapshot(String fullQualifiedSnapId, String newVlmId)
        throws StorageException, AccessDeniedException;

    T decoupleParent(String fullQualifiedIdentifierRef) throws StorageException, AccessDeniedException;

    T clone(String fullQualifiedSourceSnapNameRef, String lvTargetIdRef)
        throws StorageException, AccessDeniedException;
}
