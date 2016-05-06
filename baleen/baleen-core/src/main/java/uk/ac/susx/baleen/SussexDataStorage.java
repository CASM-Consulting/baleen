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
 * .         v      +           .
 * .      Async Context         .
 * .           ^   ^            .
 * .           |   |            .
 * Doc N+------+   +---> Processed Doc N
 * <p>
 * Created by mmb28 on 04/05/2016.
 */
public class SussexDataStorage {
    private static SussexDataStorage instance;

    // maps doc id to its HTTP context
    private Map<String, AsyncContext> contexts;
    // maps HTTP context to incoming docs it contained
    private Map<AsyncContext, Integer> context2raw;
    // maps HTTP context to the processed version of all documents that arrived with that request
    private Map<AsyncContext, List<Object>> context2processed;

    private SussexDataStorage() {
        contexts = new HashMap<>();
        context2raw = new HashMap<>();
        context2processed = new HashMap<>();
    }

    public static SussexDataStorage get() {
        if (instance == null)
            instance = new SussexDataStorage();
        return instance;
    }

    public synchronized void addRawDoc(String id, AsyncContext as) {
        contexts.put(id, as);
    }

    /** Log a processed version of a doc
     * @param id of the original raw doc
     * @param processedDoc processed version
     * @return
     */
    public synchronized AsyncContext notifyProcessed(String id, Object processedDoc) {
        final AsyncContext as = contexts.remove(id);

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
     * Checks if the whole batch that an incoming doc is contained in has been processed
     * @param id id of the incoming doc
     * @return
     */
    public boolean isBatchDone(String id){
        final AsyncContext as = contexts.get(id);
        return context2processed.get(as).size() == context2raw.get(as);
    }

    /**
     * Specifies how large a batch that arrived in an HTTP context is
     */
    public synchronized void setBatchSize(AsyncContext as, int size) {
        context2raw.put(as, size);
    }


    /**
     * Return all processed docs that arrived at the same time as a doc X
     * @param id id of doc X
     * @return
     */
    public synchronized List<Object> getProcessedBatch(String id) {
        return context2processed.get(contexts.get(id));
    }
}
