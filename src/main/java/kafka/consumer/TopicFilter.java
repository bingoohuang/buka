package kafka.consumer;

import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

public abstract class TopicFilter {
    public final String rawRegex;

    protected TopicFilter(String rawRegex) {
        this.rawRegex = rawRegex;

        regex = rawRegex
                .trim()
                .replace(',', '|')
                .replace(" ", "")
                .replaceAll("^[\"']+", "")
                .replaceAll("[\"']+$", ""); // property files may bring quotes

        try {
            Pattern.compile(regex);
        } catch (PatternSyntaxException e) {
            throw new RuntimeException(regex + " is an invalid regex.");
        }

    }

    public String regex;

    @Override
    public String toString() {
        return regex;
    }


    public abstract boolean isTopicAllowed(String topic);

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TopicFilter that = (TopicFilter) o;

        if (regex != null ? !regex.equals(that.regex) : that.regex != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return regex != null ? regex.hashCode() : 0;
    }
}
