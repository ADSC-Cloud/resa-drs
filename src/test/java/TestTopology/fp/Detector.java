package TestTopology.fp;

import org.apache.storm.serialization.SerializableSerializer;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.DefaultSerializers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import resa.util.ConfigUtil;

import java.io.Serializable;
import java.util.*;

/**
 * Created by ding on 14-6-5.
 */
public class Detector extends BaseRichBolt implements Constant {

    private static final Logger LOG = LoggerFactory.getLogger(Detector.class);

    @DefaultSerializer(SerializableSerializer.class)
    public static class Entry implements Serializable {
        int count = 0;
        boolean detectedBySelf;
        int refCount = 0;
        boolean flagMFPattern = false;
        long timestamp;

        public Entry(long timestamp) {
            this.timestamp = timestamp;
        }

        public long setTimestamp(long timestamp) {
            long tmp = this.timestamp;
            this.timestamp = timestamp;
            return tmp;
        }

        public void setDetectedBySelf(boolean detectedBySelf) {
            this.detectedBySelf = detectedBySelf;
        }

        public boolean isDetectedBySelf() {
            return detectedBySelf;
        }

        public void setMFPattern(boolean flag) {
            this.flagMFPattern = flag;
        }

        public boolean isMFPattern() {
            return this.flagMFPattern;
        }

        int getCount() {
            return count;
        }

        int incCountAndGet() {
            return ++count;
        }

        int decCountAndGet() {
            return --count;
        }

        int getRefCount() {
            return refCount;
        }

        int incRefCountAndGet() {
            return ++refCount;
        }

        int decRefCountAndGet() {
            return --refCount;
        }

        boolean hasReference() {
            return this.refCount > 0;
        }

        boolean unused() {
            return count <= 0 && refCount <= 0;
        }

        String reportCnt() {
            return String.format(" cnt: %d, refCnt: %d", this.count, this.refCount);
        }

    }

    private PatternDB patterns;
    private int threshold;
    private OutputCollector collector;
    private List<Integer> targetTasks;

    @DefaultSerializer(DefaultSerializers.KryoSerializableSerializer.class)
    public static class PatternDB extends LinkedHashMap<WordList, Entry> implements KryoSerializable {
        private long maxKeep;

        public PatternDB(long maxKeep) {
            super(65536, 0.75f, true);
            this.maxKeep = maxKeep;
        }

        public PatternDB() {
            this(Long.MAX_VALUE);
        }

        @Override
        protected boolean removeEldestEntry(Map.Entry eldest) {
            return System.currentTimeMillis() - ((Entry) eldest.getValue()).timestamp > maxKeep;
        }

        public void removeExpired(long now) {
            for (Iterator<Map.Entry<WordList, Entry>> iter = entrySet().iterator(); iter.hasNext(); ) {
                Map.Entry<WordList, Entry> e = iter.next();
                if (now - e.getValue().timestamp > maxKeep) {
                    iter.remove();
                } else {
                    return;
                }
            }
        }

        @Override
        public void write(Kryo kryo, Output output) {
            output.writeLong(maxKeep);
            output.writeInt(size());
            output.writeLong(System.currentTimeMillis());
            forEach((k, v) -> {
                kryo.writeClassAndObject(output, k);
                kryo.writeClassAndObject(output, v);
            });
            LOG.info("write out {} patterns", size());
        }

        @Override
        public void read(Kryo kryo, Input input) {
            maxKeep = Long.MAX_VALUE;
            long maxKeepTmp = input.readLong();
            int size = input.readInt();
            long last = input.readLong();
            // rest timestamp
            for (int i = 0; i < size; i++) {
                WordList p = (WordList) kryo.readClassAndObject(input);
                Entry entry = (Entry) kryo.readClassAndObject(input);
                put(p, entry);
            }
            long toAdd = System.currentTimeMillis() - last + 10000;
            forEach((k, v) -> v.setTimestamp(v.timestamp + toAdd));
            maxKeep = maxKeepTmp;
            LOG.info("read in {} patterns", size);
        }
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        String patternData = "pattern";
        this.patterns = (PatternDB) context.getTaskData(patternData);
        if (this.patterns == null) {
            long maxKeepInterval = ConfigUtil.getInt(stormConf, MAX_KEEP_PROP, 60000);
            context.setTaskData(patternData, (this.patterns = new PatternDB(maxKeepInterval)));
        }
        this.collector = collector;
        this.threshold = ConfigUtil.getInt(stormConf, THRESHOLD_PROP, 20);
        targetTasks = context.getComponentTasks(context.getThisComponentId());
        Collections.sort(targetTasks);
        LOG.info("In Detector, threshold: " + threshold);
    }

    /////////////////// State Transition Graph, Implementation III/////////////////////////////
    /// States: ( Cnt > Threshold ? , hasReference? | isMaxFreqPattern? ) Stable state      ///
    ///         [   ,   |  ] Temp State -->  trigger update                                 ///
    /// Event and Output: Cnt (+/-), pattern count inc/dec (DefaultStream)                  ///
    ///                   RefCnt (+/-), reference count update from (Feedback Stream)       ///
    ///                   Output to next bolt: *T*/*F*                                      ///
    ///                                                                                     ///
    ///     +--(Cnt-)---(F, F | F)<--(direct update, *F*)--[F, F | T]                       ///
    ///     |              |   ^                                ^                           ///
    ///     |     (RefCnt-)|   | (RefCnt+)                (Cnt-)|                           ///
    ///     |              V   |                                |                           ///
    ///     |           (F, T | F )                        (T, F | T)<--------------+       ///
    ///     |              |   ^                                |                   |       ///
    ///     |        (Cnt-)|   | (Cnt+)                         |(RefCnt+)          |       ///
    ///     |              V   |                                V                   |       ///
    ///     |           (T, T | F)<--(direct update, *F*)--[T, T | T]               |       ///
    ///     |              |   ^                                                    |       ///
    ///     |     (RefCnt-)|   | (RefCnt+)                                          |       ///
    ///     |              V   |                                                    |       ///
    ///     +---------->[T, F | F ]----------->(direct update, *T*)-----------------+       ///
    ///                                                                                     ///
    ///////////////////////////////////////////////////////////////////////////////////////////

    @Override
    public void execute(Tuple input) {
        //doneTODO:
        //WordList pattern = (WordList) input.getValueByField(PATTERN_FIELD);
        final long now = System.currentTimeMillis();
        ArrayList<WordList> wordListArrayList = (ArrayList<WordList>) input.getValueByField(PATTERN_FIELD);

        wordListArrayList.forEach((pattern) -> {

            Entry entry = patterns.computeIfAbsent(pattern, (k) -> new Entry(now));

            if (!input.getSourceStreamId().equals(FEEDBACK_STREAM)) {
                ///Pattern Count Stream, only affect pattern count
                ///We only change IncRef and DecRef at two events: [++count == threshold] and [--count == threshold-1]
                if (input.getBooleanByField(IS_ADD_FIELD)) {
                    entry.incCountAndGet();
                    LOG.debug(
                            "In DetectorNew(default), cntInc: " + pattern + "," + entry.reportCnt());
                    if (entry.getCount() == threshold) {
                        incRefToSubPatternExcludeSelf(pattern.getWords(), collector, input);
                        LOG.debug(
                                "In DetectorNew(default), cntInc: " + pattern + ",satisfy thresh and incRef");
                    }
                } else {
                    entry.decCountAndGet();
                    LOG.debug(
                            "In DetectorNew(default), cntDec: " + pattern + "," + entry.reportCnt());
                    if (entry.getCount() == threshold - 1) {
                        decRefToSubPatternExcludeSelf(pattern.getWords(), collector, input);
                        LOG.debug(
                                "In DetectorNew(default), cntDec: " + pattern + ",dissatisfy thresh and decRef");
                    }
                }

                ///We separate the action of update refCount and update states
                if (!entry.isMFPattern()) {///entry.isMFPattern == false
                    if (entry.getCount() >= threshold && !entry.hasReference()) {
                        ///State (F, F | F) -> (T, F | T),
                        ///State (T, F | F) -> (T, F | T),
                        ///[output pattern, T]
                        entry.setMFPattern(true);
                        collector.emit(REPORT_STREAM, input, Arrays.asList(pattern, true));
                        LOG.debug(
                                "In DetectorNew(default), set isMFP" + pattern + "," + entry.reportCnt());
                    }
                } else {///entry.isMFPattern == true
                    if (entry.hasReference() || (!entry.hasReference() && entry.getCount() < threshold)) {
                        ///State (T, T | T) -> (F, T | F)
                        ///State (T, T | T) -> (T, T | F)
                        ///State (T, F | T) -> (F, F | F)
                        ///[output pattern, F]
                        entry.setMFPattern(false);
                        collector.emit(REPORT_STREAM, input, Arrays.asList(pattern, false));

                        LOG.debug(
                                "In DetectorNew(default), cancel isMFP" + pattern + "," + entry.reportCnt());
                    }
                }
            } else {
                ///Feedback_STREAM, only affect refCount, also check states.
                ///State (F, F | F) <--> (F, T | F)
                ///State (T, F | F) <--> (T, T | F)
                ///State (T, F | T) <--> (T, T | T)
                if (input.getBooleanByField(IS_ADD_FIELD)) {
                    entry.incRefCountAndGet();
                    LOG.debug(
                            "In DetectorNew(FB), incReferenceCnt: " + pattern + ", " + entry.reportCnt());
                } else {
                    entry.decRefCountAndGet();
                    LOG.debug(
                            "In DetectorNew(FB), DecReferenceCnt: " + pattern + ", " + entry.reportCnt());
                }

                ///TODO: please double check if we need to anchor tuple here?
                ///update states
                if (entry.hasReference() && entry.isMFPattern()) {
                    ///State [*, T | T] --> (*, T | F), update states and output F
                    entry.setMFPattern(false);
                    collector.emit(REPORT_STREAM, input, Arrays.asList(pattern, false));
                    LOG.debug(
                            "In DetectorNew(FB), cancel isMFP: " + pattern + ", " + entry.reportCnt());
                } else if (!entry.hasReference() && !entry.isMFPattern() && entry.getCount() >= threshold) {
                    ///State [T, F | F] --> (T, F | T) update states and output T
                    entry.setMFPattern(true);
                    collector.emit(REPORT_STREAM, input, Arrays.asList(pattern, true));
                    LOG.debug(
                            "In DetectorNew(FB), set isMFP" + pattern + "," + entry.reportCnt());
                }
            }
            if (entry.unused()) {
                patterns.remove(pattern);
            } else {
                entry.setTimestamp(now);
            }
        });
        patterns.removeExpired(now);
        sleep(wordListArrayList.size());
        collector.ack(input);
    }

    private static void sleep(long t) {
        long t1 = System.currentTimeMillis();
        do {
            for (int i = 0; i < 10; i++) {
                Math.atan(Math.sqrt(Math.random() * Integer.MAX_VALUE));
            }
        } while (System.currentTimeMillis() - t1 < t);
    }

    private void incRefToSubPatternExcludeSelf(int[] wordIds, OutputCollector collector, Tuple input) {
        adjRefToSubPatternExcludeSelf(wordIds, collector, input, true);
    }

    private void decRefToSubPatternExcludeSelf(int[] wordIds, OutputCollector collector, Tuple input) {
        adjRefToSubPatternExcludeSelf(wordIds, collector, input, false);
    }

    private void adjRefToSubPatternExcludeSelf(int[] wordIds, OutputCollector collector, Tuple input, boolean adj) {
        int n = wordIds.length;
        int[] buffer = new int[n];
        ArrayList<WordList>[] wordListForTargetTask = new ArrayList[targetTasks.size()];
        ///Note that here we exclude itself as one of the sub-patterns
        ///for (int i = 1; i < (1 << n); i++) {
        for (int i = 1; i < (1 << n) - 1; i++) {
            int k = 0;
            for (int j = 0; j < n; j++) {
                if ((i & (1 << j)) > 0) {
                    buffer[k++] = wordIds[j];
                }
            }
            //doneTODO:
            //collector.emit(FEEDBACK_STREAM, input, Arrays.asList(new WordList(Arrays.copyOf(buffer, k)), adj));
            WordList wl = new WordList(Arrays.copyOf(buffer, k));
            int targetIndex = WordList.getPartition(targetTasks.size(), wl);
            if (wordListForTargetTask[targetIndex] == null) {
                wordListForTargetTask[targetIndex] = new ArrayList<>();
            }
            wordListForTargetTask[targetIndex].add(wl);
        }
        for (int i = 0; i < wordListForTargetTask.length; i++) {
            if (wordListForTargetTask[i] != null && wordListForTargetTask[i].size() > 0) {
                collector.emitDirect(
                        targetTasks.get(i),
                        FEEDBACK_STREAM,
                        input, Arrays.asList(wordListForTargetTask[i], adj));
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(REPORT_STREAM, new Fields(PATTERN_FIELD, IS_ADD_MFP));
        //declarer.declareStream(FEEDBACK_STREAM, new Fields(PATTERN_FIELD, IS_ADD_FIELD));
        //doneTODO: add true for direct grouping
        declarer.declareStream(FEEDBACK_STREAM, true, new Fields(PATTERN_FIELD, IS_ADD_FIELD));
    }
}
