package uk.gov.dstl.baleen.collectionreaders;

import javax.servlet.AsyncContext;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * todo move to baleen-core to avoid dependency between baleen-consumers and baleen-producers
 * todo this needs to be a singleton rather than a global static map. whatever
 * Created by mmb28 on 04/05/2016.
 */
public class SussexDataStorage {
    private static Map<String, AsyncContext> contexts = null;
    private static Map<AsyncContext, Integer> docsPerBatch = null;
    private static Map<AsyncContext, List<Object>> outgoing = null;


    public static void init() {
        if (contexts == null)
            contexts = new HashMap<>();
        if (docsPerBatch == null)
            docsPerBatch = new HashMap<>();
        if (outgoing == null)
            outgoing = new HashMap<>();
    }

    public static synchronized void add(String s, AsyncContext as) {
        contexts.put(s, as);
//        if (!docsPerBatch.containsKey(as))
//            docsPerBatch.put(as, 1);
//        else
//            docsPerBatch.put(as, docsPerBatch.get(as) + 1);
    }

    public static synchronized AsyncContext getAndRemove(String s) {
        final AsyncContext as = contexts.remove(s);
//        docsPerBatch.put(as, docsPerBatch.get(as) - 1);
        return as;
    }

    public static synchronized int getBatchSize(AsyncContext as) {
        return docsPerBatch.get(as);
    }

    public static synchronized void setBatchSize(AsyncContext as, int size) {
        docsPerBatch.put(as, size);
    }

    public static synchronized int getNumProcessed(AsyncContext as) {
        return outgoing.get(as).size();
    }

    public static synchronized List<Object> getAllProcessed(AsyncContext as) {
        return outgoing.get(as);
    }

    public static synchronized void addReadyDocument(AsyncContext as, Object processedDoc) {

        List<Object> done;
        if (outgoing.containsKey(as))
            done = outgoing.get(as);
        else
            done = new ArrayList<>();

        done.add(processedDoc);
        outgoing.put(as, done);
    }
}
