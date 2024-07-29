package com.linbit.linstor.api.pojo;

import com.linbit.linstor.annotation.Nullable;

@Deprecated(forRemoval = true)
public class ExosEnclosureEventPojo
{
    private final @Nullable String severity;
    private final @Nullable String exosEventId;
    private final @Nullable String controller;
    private final @Nullable String timeStampStr;
    private final @Nullable Long timeStampNumeric;
    private final @Nullable String message;
    private final @Nullable String additionalInformation;
    private final @Nullable String recommendedAction;

    public ExosEnclosureEventPojo(
        @Nullable String severityRef,
        @Nullable String eventIdRef,
        @Nullable String controllerRef,
        @Nullable String timeStampRef,
        @Nullable Long timeStampNumericRef,
        @Nullable String messageRef,
        @Nullable String additionalInformationRef,
        @Nullable String recommendedActionRef
    )
    {
        severity = severityRef;
        exosEventId = eventIdRef;
        controller = controllerRef;
        timeStampStr = timeStampRef;
        timeStampNumeric = timeStampNumericRef;
        message = messageRef;
        additionalInformation = additionalInformationRef;
        recommendedAction = recommendedActionRef;
    }

    public @Nullable String getSeverity()
    {
        return severity;
    }

    public @Nullable String getExosEventId()
    {
        return exosEventId;
    }

    public @Nullable String getController()
    {
        return controller;
    }

    public @Nullable String getTimeStampStr()
    {
        return timeStampStr;
    }

    public @Nullable Long getTimeStampNumeric()
    {
        return timeStampNumeric;
    }

    public @Nullable String getMessage()
    {
        return message;
    }

    public @Nullable String getAdditionalInformation()
    {
        return additionalInformation;
    }

    public @Nullable String getRecommendedAction()
    {
        return recommendedAction;
    }
}
