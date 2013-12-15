package kafka.server;

import com.google.common.collect.Maps;
import kafka.common.KafkaException;
import kafka.common.TopicAndPartition;
import kafka.utils.Callable2;
import kafka.utils.Utils;

import java.io.*;
import java.util.Map;

/**
 * This class saves out a map of topic/partition=>offsets to a file
 */
public class OffsetCheckpoint {
    public File file;

    public OffsetCheckpoint(File file) {
        this.file = file;

        try {
            new File(file + ".tmp").delete(); // try to delete any existing temp files for cleanliness
            file.createNewFile(); // in case the file doesn't exist
        } catch (IOException e) {
            throw new KafkaException(e);
        }
    }

    private Object lock = new Object();

    public void write(Map<TopicAndPartition, Long> offsets) {
        synchronized (lock) {
            // write to temp file and then swap with the existing file
            File temp = new File(file.getAbsolutePath() + ".tmp");

            BufferedWriter writer = null;
            try {
                writer = new BufferedWriter(new FileWriter(temp));
                // write the current version
                writer.write("0");
                writer.newLine();

                // write the number of entries
                writer.write(offsets.size() + "");
                writer.newLine();

                // write the entries
                final BufferedWriter finalWriter = writer;
                Utils.foreach(offsets, new Callable2<TopicAndPartition, Long>() {
                    @Override
                    public void apply(TopicAndPartition topicPart, Long offset) {
                        try {
                            finalWriter.write(String.format("%s %d %d", topicPart.topic, topicPart.partition, offset));
                            finalWriter.newLine();
                        } catch (IOException e) {
                            throw new KafkaException(e);
                        }
                    }
                });

                // flush and overwrite old file
                writer.flush();
            } catch (IOException e) {
                throw new KafkaException(e);
            } finally {
                Utils.closeQuietly(writer);
            }

            // swap new offset checkpoint file with previous one
            if (!temp.renameTo(file)) {
                // renameTo() fails on Windows if the destination file exists.
                file.delete();
                if (!temp.renameTo(file))
                    throw new KafkaException(String.format("File rename from %s to %s failed.", temp.getAbsolutePath(), file.getAbsolutePath()));
            }
        }
    }

    public Map<TopicAndPartition, Long> read() {
        synchronized (lock) {
            BufferedReader reader = null;
            try {
                reader = new BufferedReader(new FileReader(file));
                String line = reader.readLine();
                if (line == null)
                    return Maps.newHashMap();

                int version = Integer.parseInt(line);
                if (version != 0)
                    throw new IOException("Unrecognized version of the highwatermark checkpoint file: " + version);

                line = reader.readLine();
                if (line == null)
                    return Maps.newHashMap();
                int expectedSize = Integer.parseInt(line);
                Map<TopicAndPartition, Long> offsets = Maps.newHashMap();
                line = reader.readLine();
                while (line != null) {
                    String[] pieces = line.split("\\s+");
                    if (pieces.length != 3)
                        throw new IOException(String.format("Malformed line in offset checkpoint file: '%s'.", line));

                    String topic = pieces[0];
                    int partition = Integer.parseInt(pieces[1]);
                    long offset = Long.parseLong(pieces[2]);
                    offsets.put(new TopicAndPartition(topic, partition), offset);
                    line = reader.readLine();
                }
                if (offsets.size() != expectedSize)
                    throw new IOException(String.format("Expected %d entries but found only %d", expectedSize, offsets.size()));
                return offsets;

            } catch (IOException e) {
                throw new KafkaException(e);
            } finally {
                Utils.closeQuietly(reader);
            }
        }
    }
}
