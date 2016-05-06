package uk.ac.susx.baleen;

import javax.servlet.AsyncContext;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Stores a correspondent between raw incoming documents, the HTTP request that
 * they arrived in, and their Baleen-processed version. Incoming documents arrive
 * in batches, many per request, and the processed version of all of them need to
 * be returned as a response to the same request (one round trip only)
 * <p>
 * Doc 1+------+      +---> Processed Doc 1
 *   .         v      +         .
 *   .    Async Context         .
 *   .         ^   ^            .
 *   .         |   |            .
 * Doc N+------+   +---> Processed Doc N
 * <p>
 * Created by mmb28 on 04/05/2016.
 */
public class SussexDataStorage {
    private static SussexDataStorage instance;

    // maps doc id to its HTTP context
    private Map<String, AsyncContext> raw2context;
    // maps HTTP context to number of incoming docs it contained
    private Map<AsyncContext, Integer> context2raw;
    // maps HTTP context to the processed version of all documents that arrived with that request
    private Map<AsyncContext, List<Object>> context2processed;

    private SussexDataStorage() {
        raw2context = new HashMap<>();
        context2raw = new HashMap<>();
        context2processed = new HashMap<>();
    }

    public static SussexDataStorage get() {
        if (instance == null)
            instance = new SussexDataStorage();
        return instance;
    }

    public synchronized void addRawDoc(String id, AsyncContext as) {
        raw2context.put(id, as);
    }

    /** Log a processed version of a doc
     * @param id of the original raw doc
     * @param processedDoc processed version
     * @return
     */
    public synchronized AsyncContext notifyProcessed(String id, Object processedDoc) {
        final AsyncContext as = raw2context.remove(id);

        List<Object> done;
        if (context2processed.containsKey(as))
            done = context2processed.get(as);
        else
            done = new ArrayList<>();

        done.add(processedDoc);
        context2processed.put(as, done);

        return as;
    }

    /**
     * Checks if the whole batch that arrive in a POST has been processed
     * @param as id of the incoming doc
     * @return
     */
    public boolean isBatchDone(AsyncContext as){
        return context2processed.get(as).size() == context2raw.get(as);
    }

    /**
     * Specifies how large a batch that arrived in an HTTP context is
     */
    public synchronized void setBatchSize(AsyncContext as, int size) {
        context2raw.put(as, size);
    }


    /**
     * Return all processed docs that arrived in a POST request. Should only be called
     * if `isBatchDone(as)` because it also cleans up
     * @param as the request context
     * @return
     */
    public synchronized List<Object> getProcessedBatch(AsyncContext as) {
        context2raw.remove(as);
        return context2processed.remove(as);
    }
}
