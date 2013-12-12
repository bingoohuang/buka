package kafka.common;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class Config {
    public  static  void validateChars(String prop, String value) {
        String legalChars = "[a-zA-Z0-9\\._\\-]";
        Pattern rgx = Pattern.compile(legalChars + "*");

        Matcher matcher = rgx.matcher(value);
        if (!matcher.find()) {
            throw new InvalidConfigException(prop + " " + value + " is illegal, contains a character other than ASCII alphanumerics, '.', '_' and '-'");
        }

        String t = matcher.group();

        if (!t.equals(value))
            throw new InvalidConfigException(prop + " " + value + " is illegal, contains a character other than ASCII alphanumerics, '.', '_' and '-'");
    }
}
