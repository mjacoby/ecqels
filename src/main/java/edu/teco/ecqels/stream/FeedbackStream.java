package edu.teco.ecqels.stream;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import edu.teco.ecqels.ECQELSRuntime;
import edu.teco.ecqels.Engine;
import edu.teco.ecqels.continuous.ContinuousConstructListenerBase;
import edu.teco.ecqels.util.Utils;
import java.util.List;

/**
 *
 * @author Michael Jacoby <michael.jacoby@student.kit.edu>
 */
public class FeedbackStream extends ContinuousConstructListenerBase implements RDFStream {

    private boolean stop = false;
    private final String uri;
    private final ECQELSRuntime engine;

    public FeedbackStream(ECQELSRuntime engine, String uri) {
        this.engine = engine;
        this.uri = uri;
    }

    @Override
    public void stop() {
        stop = true;
    }

    @Override
    public void update(List<List<Triple>> graph) {
        if (!stop) {
            graph.stream().forEach(triples -> {
                triples.forEach(triple -> stream(triple));
            });
        }
    }

    @Override
    public String getURI() {
        return uri;
    }
    
    @Override
    public void stream(Triple t) {
        engine.send(Utils.toNode(uri), t.getSubject(), t.getPredicate(), t.getObject());
    }

}
