package uk.gov.dstl.baleen.consumers;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.uima.UimaContext;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;
import org.apache.uima.jcas.tcas.DocumentAnnotation;
import org.apache.uima.resource.ResourceInitializationException;
import org.eclipse.jetty.io.RuntimeIOException;
import uk.ac.susx.baleen.SussexDataStorage;
import uk.gov.dstl.baleen.types.BaleenAnnotation;
import uk.gov.dstl.baleen.types.common.Organisation;
import uk.gov.dstl.baleen.types.common.Person;
import uk.gov.dstl.baleen.types.semantic.Location;
import uk.gov.dstl.baleen.uima.BaleenConsumer;

import javax.servlet.AsyncContext;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Extract a subset of all annotations, serialise them to JSON and respond to HTTP post.
 * see uk.gov.dstl.baleen.collectionreaders.HttpReader
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
        final String docId = da.getSourceUri();

        AnnotatedDocument pojo = new AnnotatedDocument();
        // if the doc text is long it's wasteful to send it back
        pojo.setId(docId);

        pojo = transferAnnotations(pojo, jcas, Location.class, pojo::setLocations);
        pojo = transferAnnotations(pojo, jcas, Person.class, pojo::setPersons);
        pojo = transferAnnotations(pojo, jcas, Organisation.class, pojo::setOrganisations);

        AsyncContext as = SussexDataStorage.get().addProcessedDoc(docId, pojo);

        if (SussexDataStorage.get().isBatchDone(as)) {
            // done with all docs in this batch
            try {
                as.getResponse().setContentType("application/json");
                mapper.writeValue(as.getResponse().getWriter(), SussexDataStorage.get().getProcessedBatch(as));
            } catch (IOException | RuntimeIOException e) {
                // client stopped listening, whatever
                return;
            }
            as.complete();
        }

    }

    private <T extends BaleenAnnotation> AnnotatedDocument transferAnnotations(AnnotatedDocument doc, JCas jcas, Class<T> clazz, Consumer<List<String>> method) {
        Collection<T> annotations = JCasUtil.select(jcas, clazz);
        final List<String> mentions = annotations
                .stream()
                .map(ann -> jcas.getDocumentText().substring(ann.getBegin(), ann.getEnd()))
                .collect(Collectors.toList());
        method.accept(mentions);

        return doc;
    }

    @Override
    public void doDestroy() {

    }

    // POJO that Jackson turns into JSON
    private class AnnotatedDocument {
        private String id;
        private List<String> persons;
        private List<String> locations;
        private List<String> organisations;

        public List<String> getOrganisations() {
            return organisations;
        }

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public List<String> getLocations() {
            return locations;
        }

        public void setLocations(List<String> locations) {
            this.locations = locations;
        }

        public List<String> getPersons() {
            return persons;
        }

        public void setPersons(List<String> persons) {
            this.persons = persons;
        }

        public void setOrganisations(List<String> organisations) {
            this.organisations = organisations;
        }

    }

}
