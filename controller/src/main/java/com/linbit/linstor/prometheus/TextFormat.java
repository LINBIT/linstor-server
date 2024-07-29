package com.linbit.linstor.prometheus;

import com.linbit.linstor.annotation.Nullable;

import java.util.ArrayList;
import java.util.Map;

import io.prometheus.client.Collector;

/**
 * TextFormat implements the simple prometheus text format:
 * https://prometheus.io/docs/instrumenting/exposition_formats/
 */
public class TextFormat
{
    private final StringBuilder sb = new StringBuilder();
    private @Nullable String currentSection;

    private enum Type
    {
        counter,
        gauge,
        histogram,
        summary,
        untyped;
    }

    public void startUntyped(final String sectionName)
    {
        startUntyped(sectionName, "");
    }
    public void startUntyped(final String sectionName, final String help)
    {
        startSection(Type.untyped, sectionName, help);
    }

    public void startGauge(final String sectionName)
    {
        startGauge(sectionName, "");
    }
    public void startGauge(final String sectionName, final String help)
    {
        startSection(Type.gauge, sectionName, help);
    }

    public void startCounter(final String sectionName)
    {
        startCounter(sectionName, "");
    }
    public void startCounter(final String sectionName, final String help)
    {
        startSection(Type.counter, sectionName, help);
    }

    private void startSection(final Type type, final String sectionName, @Nullable final String help)
    {
        currentSection = sectionName.trim();

        if (help != null && !help.isEmpty())
        {
            sb.append("# HELP ");
            sb.append(currentSection);
            sb.append(" ");
            // TODO help escaping
            sb.append(help);
            sb.append('\n');
        }

        sb.append("# TYPE ");
        sb.append(currentSection);
        sb.append(" ");
        sb.append(type);
        sb.append("\n");
    }

    public void writeSample(double value)
    {
        writeSample(null, value);
    }

    public void writeSample(@Nullable final Map<String, String> labels, double value)
    {
        sb.append(currentSection);
        if (labels != null && !labels.isEmpty())
        {
            sb.append('{');
            sb.append(formatLabels(labels));
            sb.append("}");
        }
        sb.append(' ');
        sb.append(Collector.doubleToGoString(value));
        sb.append("\n");
    }

    @Override
    public String toString()
    {
        return sb.toString();
    }

    private String formatLabels(final Map<String, String> labels)
    {
        ArrayList<String> kvVal = new ArrayList<>();
        // TODO value escaping
        for (final Map.Entry<String, String> entry : labels.entrySet())
        {
            kvVal.add(entry.getKey() + "=\"" + entry.getValue() + "\"");
        }
        return String.join(",", kvVal);
    }

}
