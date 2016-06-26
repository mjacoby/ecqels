package edu.teco.ecqels.stream;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import edu.teco.ecqels.Engine;

/**
 * @author Danh Le Phuoc
 * @organization DERI Galway, NUIG, Ireland www.deri.ie
 * @author Michael Jacoby
 * @organization Karlsruhe Institute of Technology, Germany www.kit.edu
 * @email danh.lephuoc@deri.org
 * @email michael.jacoby@student.kit.edu
 */
public abstract class AbstractRDFStream implements RDFStream {

    protected Node streamURI;
    protected Engine engine;

    public AbstractRDFStream(Engine engine, String uri) {
        streamURI = Node.createURI(uri);
        this.engine = engine;
    }

    @Override
    public void stream(Triple t) {
        engine.send(streamURI, t.getSubject(), t.getPredicate(), t.getObject());
    }

    @Override
    public String getURI() {
        return streamURI.getURI();
    }
}
