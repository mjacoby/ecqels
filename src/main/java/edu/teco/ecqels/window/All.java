package edu.teco.ecqels.window;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.sparql.core.BasicPattern;
import com.hp.hpl.jena.sparql.core.Quad;
import edu.teco.ecqels.Engine;

public class All extends AbstractWindow {

    public All(Engine engine, Node streamNode, BasicPattern pattern) {
        super(engine, streamNode, pattern);
    }

    @Override
    public void add(final Quad quad) {
        super.add(quad);
        fireDataChanged(evaluate());
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || !(obj instanceof All)) {
            return false;
        }
        return true;
    }

    @Override
    public Window clone() {
        return new All(engine, streamNode, pattern);
    }  

}
