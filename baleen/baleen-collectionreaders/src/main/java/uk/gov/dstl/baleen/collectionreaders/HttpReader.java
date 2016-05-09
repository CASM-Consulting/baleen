package uk.gov.dstl.baleen.collectionreaders;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.uima.UimaContext;
import org.apache.uima.collection.CollectionException;
import org.apache.uima.jcas.JCas;
import org.apache.uima.jcas.tcas.DocumentAnnotation;
import org.apache.uima.resource.ResourceInitializationException;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.ConcurrentArrayQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.ac.susx.baleen.SussexDataStorage;
import uk.gov.dstl.baleen.uima.BaleenCollectionReader;

import javax.servlet.AsyncContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Queue;

/**
 * A collection reader that listens for HTTP POST requests and puts those down a pipeline
 * Created by mmb28 on 04/05/2016.
 */
public class HttpReader extends BaleenCollectionReader {
    private static final Logger LOGGER = LoggerFactory.getLogger(HttpReader.class);
    Queue<Document> data;
    private ObjectMapper mapper;

    private Server server;
    private ServletContextHandler servletContextHandler;

    @Override
    protected void doInitialize(UimaContext uimaContext) throws ResourceInitializationException {
        data = new ConcurrentArrayQueue<>();
        mapper = new ObjectMapper();

        this.server = new Server(InetSocketAddress.createUnresolved("0.0.0.0", 3124));
        servletContextHandler = new ServletContextHandler();
        servletContextHandler.setContextPath("/sussex");

        servletContextHandler.addServlet(new ServletHolder(new MyServlet(this)), "/consume/*");
        server.setHandler(servletContextHandler);

        if (this.server != null) {
            LOGGER.debug("Starting server");
            try {
                server.start();
            } catch (Exception e) {
                e.printStackTrace();
            }
            LOGGER.info("Server started");
        } else {
            System.out.println("Server has not yet been configured");
        }
    }

    @Override
    protected void doGetNext(JCas jCas) throws IOException, CollectionException {
        Document nextDocument = data.poll();
        DocumentAnnotation da = (DocumentAnnotation) jCas.getDocumentAnnotationFs();
        da.setSourceUri(nextDocument.id.toString());
        jCas.setDocumentText(nextDocument.text);
    }

    @Override
    protected void doClose() throws IOException {
    }

    @Override
    public synchronized boolean doHasNext() throws IOException, CollectionException {
        return !data.isEmpty();
    }

    public synchronized void addData(Document d) {
        this.data.add(d);
    }

    private class MyServlet extends HttpServlet {
        private HttpReader collectionReader;

        public MyServlet(HttpReader collectionReader) {
            this.collectionReader = collectionReader;
        }

        // URL is: http://0.0.0.0:6413/api/1/consume
        // test like so: wget http://0.0.0.0:3124/sussex/consume --post-data='data=[{"text":"hello from www.google.com in Germany","id":"1"}]' -qO-
        @Override
        protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
            List<Document> payload;
            try {
                payload = mapper.readValue(req.getParameter("data"), new TypeReference<List<Document>>() {});
            } catch (IOException e) {
                resp.sendError(500, "JSON is not quite right. Nothing to do here.");
                return;
            }

            final AsyncContext as = req.startAsync();
            as.setTimeout(30_000);//ms
            for(Document d: payload){
                collectionReader.addData(d);
                SussexDataStorage.get().addRawDoc(d.id.toString(), as);
            }
            SussexDataStorage.get().setBatchSize(as, payload.size());
        }
    }

    // Jackson POJO
    private static class Document {
        public String text;
        public Object id;

        public Document() {
        }

        public Document(String text, Object id) {
            this.text = text;
            this.id = id;
        }

        public void setText(String text) {
            this.text = text;
        }

        public void setId(Object id) {
            this.id = id;
        }
    }

}