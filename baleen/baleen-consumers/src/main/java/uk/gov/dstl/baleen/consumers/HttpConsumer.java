package uk.gov.dstl.baleen.consumers;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.uima.UimaContext;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;
import org.apache.uima.jcas.tcas.DocumentAnnotation;
import org.apache.uima.resource.ResourceInitializationException;
import uk.gov.dstl.baleen.collectionreaders.SussexDataStorage;
import uk.gov.dstl.baleen.types.common.Url;
import uk.gov.dstl.baleen.types.semantic.Location;
import uk.gov.dstl.baleen.uima.BaleenConsumer;

import javax.servlet.AsyncContext;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by mmb28 on 04/05/2016.
 */
public class HttpConsumer extends BaleenConsumer {

    private ObjectMapper mapper;

    @Override
    public void doInitialize(UimaContext aContext) throws ResourceInitializationException {
        mapper = new ObjectMapper();
    }

    @Override
    public void doProcess(JCas jcas) throws AnalysisEngineProcessException {
        DocumentAnnotation da = getDocumentAnnotation(jcas);
        final String sourceUri = da.getSourceUri();

        final AnnotatedDocument pojo = new AnnotatedDocument();
        pojo.setText(jcas.getDocumentText());
        pojo.setId(sourceUri);


        Collection<Location> locations = JCasUtil.select(jcas, Location.class);
        final List<String> locationMentions = locations
                .stream()
                .map(loc -> jcas.getDocumentText().substring(loc.getBegin(), loc.getEnd()))
                .collect(Collectors.toList());
        pojo.setLocations(locationMentions);

        Collection<Url> urls = JCasUtil.select(jcas, Url.class);
        final List<String> urlMentions = urls
                .stream()
                .map(x -> jcas.getDocumentText().substring(x.getBegin(), x.getEnd()))
                .collect(Collectors.toList());
        pojo.setUrls(urlMentions);
        //todo extract and add more annotations

        final AsyncContext asyncContext = SussexDataStorage.getAndRemove(sourceUri);
        try {
            asyncContext.getResponse().setContentType("application/json");
            mapper.writeValue(asyncContext.getResponse().getWriter(), pojo);
        } catch (IOException e) {
            e.printStackTrace();
        }
        asyncContext.complete();
    }

    @Override
    public void doDestroy() {

    }

    // POJO that Jackson turns into JSON
    private class AnnotatedDocument {
        private String text;

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        private String id;
        private List<String> locations;

        public List<String> getUrls() {
            return urls;
        }

        public void setUrls(List<String> urls) {
            this.urls = urls;
        }

        public List<String> getLocations() {
            return locations;
        }

        public void setLocations(List<String> locations) {
            this.locations = locations;
        }

        public String getText() {
            return text;
        }

        public void setText(String text) {
            this.text = text;
        }

        private List<String> urls;
    }

}
