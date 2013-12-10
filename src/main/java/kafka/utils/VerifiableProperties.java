package kafka.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static com.google.common.base.Preconditions.checkState;

public class VerifiableProperties {
    public final Properties props;

    public VerifiableProperties(Properties props) {
        this.props = props;
    }

    private Set<String> referenceSet = new HashSet<String>();

    public VerifiableProperties() {
        this(new Properties());
    }

    public boolean containsKey(String name) {
        return props.containsKey(name);
    }

    public String getProperty(String name) {
        String value = props.getProperty(name);
        referenceSet.add(name);
        return value;
    }

    /**
     * Read a required integer property value or throw an exception if no such property is found
     */
    public int getInt(String name) {
        return Integer.parseInt(getString(name));
    }

    public int getIntInRange(String name, Range<Integer> range) {
        checkState(containsKey(name), "Missing required property '" + name + "'");
        return getIntInRange(name, -1, range);
    }

    /**
     * Read an integer from the properties instance
     *
     * @param name         The property name
     * @param defaultValue The defaultValue value to use if the property is not found
     * @return the integer value
     */
    public int getInt(String name, int defaultValue) {
        return getIntInRange(name, defaultValue, Range.make(Integer.MIN_VALUE, Integer.MAX_VALUE));
    }

    public Short getShort(String name, Short defaultValue) {
        return getShortInRange(name, defaultValue, Range.make(Short.MIN_VALUE, Short.MAX_VALUE));
    }

    /**
     * Read an integer from the properties instance. Throw an exception
     * if the value is not in the given range (inclusive)
     *
     * @param name         The property name
     * @param defaultValue The defaultValue value to use if the property is not found
     * @param range        The range in which the value must fall (inclusive)
     * @return the integer value
     * @throws IllegalArgumentException If the value is not in the given range
     */
    public int getIntInRange(String name, int defaultValue, Range<Integer> range) {
        int v = containsKey(name) ? Integer.parseInt(getProperty(name)) : defaultValue;

        checkState(v >= range._1 && v <= range._2, name + " has value " + v + " which is not in the range " + range + ".");

        return v;
    }

    public short getShortInRange(String name, short defaultValue, Range<Short> range) {
        short v = containsKey(name) ? Short.parseShort(getProperty(name)) : defaultValue;
        checkState(v >= range._1 && v <= range._2, name + " has value " + v + " which is not in the range " + range + ".");
        return v;
    }

    /**
     * Read a required long property value or throw an exception if no such property is found
     */
    public long getLong(String name) {
        return Long.parseLong(getString(name));
    }

    /**
     * Read an long from the properties instance
     *
     * @param name         The property name
     * @param defaultValue The defaultValue value to use if the property is not found
     * @return the long value
     */
    public long getLong(String name, long defaultValue) {
        return getLongInRange(name, defaultValue, Range.make(Long.MIN_VALUE, Long.MAX_VALUE));
    }

    /**
     * Read an long from the properties instance. Throw an exception
     * if the value is not in the given range (inclusive)
     *
     * @param name         The property name
     * @param defaultValue The defaultValue value to use if the property is not found
     * @param range        The range in which the value must fall (inclusive)
     * @return the long value
     * @throws IllegalArgumentException If the value is not in the given range
     */
    public long getLongInRange(String name, long defaultValue, Range<Long> range) {
        long v = containsKey(name) ? Long.parseLong(getProperty(name)) : defaultValue;
        checkState(v >= range._1 && v <= range._2, name + " has value " + v + " which is not in the range " + range + ".");
        return v;
    }

    /**
     * Get a required argument as a double
     *
     * @param name The property name
     * @return the value
     * @throw IllegalArgumentException If the given property is not present
     */
    public double getDouble(String name) {
        return Double.parseDouble(getString(name));
    }

    /**
     * Get an optional argument as a double
     *
     * @param name The property name
     * @defaultValue The defaultValue value for the property if not present
     */
    public double getDouble(String name, double defaultValue) {
        return containsKey(name) ? getDouble(name) : defaultValue;
    }

    /**
     * Read a boolean value from the properties instance
     *
     * @param name         The property name
     * @param defaultValue The defaultValue value to use if the property is not found
     * @return the boolean value
     */
    public boolean getBoolean(String name, boolean defaultValue) {
        if (!containsKey(name))
            return defaultValue;
        else {
            String v = getProperty(name);
            checkState(v == "true" || v == "false", "Unacceptable value for property '" + name + "', boolean values must be either 'true' or 'false");
            return Boolean.parseBoolean(v);
        }
    }

    public boolean getBoolean(String name) {
        return Boolean.parseBoolean(getString(name));
    }

    /**
     * Get a string property, or, if no such property is defined, return the given defaultValue value
     */
    public String getString(String name, String defaultValue) {
        return (containsKey(name)) ? getProperty(name) : defaultValue;
    }

    /**
     * Get a string property or throw and exception if no such property is defined.
     */
    public String getString(String name) {
        checkState(containsKey(name), "Missing required property '" + name + "'");
        return getProperty(name);
    }

    /**
     * Get a Map[String, String] from a property list in the form k1:v2, k2:v2, ...
     */
    public Map<String, String> getMap(String name) {
        return getMap(name, new Function1<String, Boolean>() {
            @Override
            public Boolean apply(String arg) {
                return true;
            }
        });
    }

    public Map<String, String> getMap(String name, Function1<String, Boolean> valid) {
        try {
            Map<String, String> m = Utils.parseCsvMap(getString(name, ""));
            for (Map.Entry<String, String> entry : m.entrySet()) {
                String key = entry.getKey();
                String value = entry.getValue();
                if (!valid.apply(value))
                    throw new IllegalArgumentException(String.format("Invalid entry '%s' = '%s' for property '%s'", key, value, name));
            }
            return m;
        } catch (Exception e) {
            throw new IllegalArgumentException(String.format("Error parsing configuration property '%s': %s", name, e.getMessage()));
        }
    }

    Logger logger = LoggerFactory.getLogger(VerifiableProperties.class);

    public void verify() {
        logger.info("Verifying properties");
        Enumeration<?> enumeration = props.propertyNames();
        while (enumeration.hasMoreElements()) {
            String key = (String) enumeration.nextElement();
            if (!referenceSet.contains(key) && !key.startsWith("external"))
                logger.warn("Property {} is not valid", key);
            else
                logger.info("Property {} is overridden to {}", key, props.getProperty(key));
        }

    }

    @Override
    public String toString() {
        return props.toString();
    }
}
