package uk.gov.dstl.baleen.collectionreaders;

import javax.servlet.AsyncContext;
import java.util.HashMap;
import java.util.Map;

/**
 * todo move to baleen-core to avoid dependency between baleen-consumers and baleen-producers
 * Created by mmb28 on 04/05/2016.
 */
public class SussexDataStorage {
    private static Map<String, AsyncContext> contexts = null;


    public static void init() {
        if (contexts == null)
            contexts = new HashMap<>();
    }

    public static synchronized void add(String s, AsyncContext as) {
        contexts.put(s, as);
    }

    public static synchronized AsyncContext getAndRemove(String s) {
        return contexts.remove(s);
    }
}
