package edu.teco.ecqels.window;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.sparql.core.BasicPattern;
import edu.teco.ecqels.Engine;

/**
 * This class implements the now window - a triple-based window with just newest
 * element
 *
 * @author	Danh Le Phuoc
 * @author Chan Le Van
 * @organization DERI Galway, NUIG, Ireland www.deri.ie
 * @email danh.lephuoc@deri.org
 * @email chan.levan@deri.org
 */
public class Now extends TripleWindow {

    public Now(Engine engine, Node streamNode, BasicPattern pattern) {
        super(engine, streamNode, pattern, 1);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || !(obj instanceof Now)) {
            return false;
        }
        return true;
    }
}
