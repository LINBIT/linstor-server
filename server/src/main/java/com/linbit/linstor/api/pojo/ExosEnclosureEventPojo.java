package com.linbit.linstor.api.pojo;

@Deprecated(forRemoval = true)
public class ExosEnclosureEventPojo
{
    private final String severity;
    private final String exosEventId;
    private final String controller;
    private final String timeStampStr;
    private final Long timeStampNumeric;
    private final String message;
    private final String additionalInformation;
    private final String recommendedAction;

    public ExosEnclosureEventPojo(
        String severityRef,
        String eventIdRef,
        String controllerRef,
        String timeStampRef,
        Long timeStampNumericRef,
        String messageRef,
        String additionalInformationRef,
        String recommendedActionRef
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

    public String getSeverity()
    {
        return severity;
    }

    public String getExosEventId()
    {
        return exosEventId;
    }

    public String getController()
    {
        return controller;
    }

    public String getTimeStampStr()
    {
        return timeStampStr;
    }

    public Long getTimeStampNumeric()
    {
        return timeStampNumeric;
    }

    public String getMessage()
    {
        return message;
    }

    public String getAdditionalInformation()
    {
        return additionalInformation;
    }

    public String getRecommendedAction()
    {
        return recommendedAction;
    }
}
