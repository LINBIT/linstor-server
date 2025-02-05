package com.linbit.linstor.logging;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.core.objects.Node;

import java.util.Date;
import java.util.Optional;

public class ErrorReport extends LinstorFile
{
    @Nullable private String version;
    @Nullable private String peer;
    @Nullable private String exception;
    @Nullable private String exceptionMessage;
    @Nullable private String originFile;
    @Nullable private String originMethod;
    @Nullable private Integer originLine;
    private Node.Type module;

    public ErrorReport(
        final String nodeNameRef,
        Node.Type moduleRef,
        final String fileNameRef,
        @Nullable final String versionRef,
        @Nullable final String peerRef,
        @Nullable final String exceptionRef,
        @Nullable final String exceptionMessageRef,
        @Nullable final String originFileRef,
        @Nullable final String originMethodRef,
        @Nullable final Integer originLineRef,
        final Date dateRef,
        @Nullable final String textRef)
    {
        super(nodeNameRef, fileNameRef, dateRef, textRef);
        nodeName = nodeNameRef;
        fileName = fileNameRef;
        version = versionRef;
        peer = peerRef;
        exception = exceptionRef;
        exceptionMessage = exceptionMessageRef;
        originFile = originFileRef;
        originMethod = originMethodRef;
        originLine = originLineRef;
        dateTime = dateRef;
        module = moduleRef;
        text = textRef;
    }

    @Override
    public Date getDateTime()
    {
        return dateTime;
    }

    @Override
    public String getFileName()
    {
        return fileName;
    }

    @Override
    public String getNodeName()
    {
        return nodeName;
    }

    public Optional<String> getVersion() {
        return Optional.ofNullable(version);
    }

    public Optional<String> getPeer() {
        return Optional.ofNullable(peer);
    }

    public Optional<String> getException() {
        return Optional.ofNullable(exception);
    }

    public Optional<String> getExceptionMessage() {
        return Optional.ofNullable(exceptionMessage);
    }

    public Optional<String> getOriginFile() {
        return Optional.ofNullable(originFile);
    }

    public Optional<String> getOriginMethod() {
        return Optional.ofNullable(originMethod);
    }

    public Optional<Integer> getOriginLine() {
        return Optional.ofNullable(originLine);
    }

    public Node.Type getModule() {
        return module;
    }

    public String getModuleString() {
        return module.name();
    }
}
