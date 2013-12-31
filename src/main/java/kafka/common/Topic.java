package kafka.common;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Topic {
    static String legalChars = "[a-zA-Z0-9\\._\\-]";
    private static int maxNameLength = 255;
    private static Pattern rgx = Pattern.compile(legalChars + "+");

    public static void validate(String topic) {
        if (topic.length() <= 0)
            throw new InvalidTopicException("topic name is illegal, can't be empty");
        else if (topic.equals(".") || topic.equals(".."))
            throw new InvalidTopicException("topic name cannot be \".\" or \"..\"");
        else if (topic.length() > maxNameLength)
            throw new InvalidTopicException("topic name is illegal, can't be longer than " + maxNameLength + " characters");

        Matcher matcher = rgx.matcher(topic);

        boolean found = matcher.find();
        if (!found)
            throw new InvalidTopicException("topic name " + topic + " is illegal,  contains a character other than ASCII alphanumerics, '.', '_' and '-'");


        if (!matcher.group().equals(topic))
            throw new InvalidTopicException("topic name " + topic + " is illegal, contains a character other than ASCII alphanumerics, '.', '_' and '-'");
    }
}
