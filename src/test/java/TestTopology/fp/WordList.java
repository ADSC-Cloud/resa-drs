package TestTopology.fp;

import org.apache.storm.serialization.SerializableSerializer;
import com.esotericsoftware.kryo.DefaultSerializer;

import java.io.Serializable;
import java.util.Arrays;

/**
 * Created by ding on 14-6-5.
 */
@DefaultSerializer(SerializableSerializer.class)
public class WordList implements Serializable {

    private int[] words;

    public WordList(int... words) {
        this.words = words;
    }

    public int[] getWords() {
        return words;
    }

    @Override
    public int hashCode() {
        return words == null ? 0 : Arrays.hashCode(words);
    }

    @Override
    public boolean equals(Object obj) {
        return Arrays.equals(words, ((WordList) obj).words);
    }

    @Override
    public String toString() {
        return Arrays.toString(words);
    }

    public static int getPartition(int numPart, WordList wordList) {
        return Math.abs(wordList.hashCode()) % numPart;
    }

}
