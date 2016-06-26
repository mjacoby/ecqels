package edu.teco.ecqels.stream;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;

/**
 * @author	Danh Le Phuoc
 * @organization DERI Galway, NUIG, Ireland www.deri.ie
 * @author Michael Jacoby
 * @organization Karlsruhe Institute of Technology, Germany www.kit.edu
 * @email danh.lephuoc@deri.org
 * @email michael.jacoby@student.kit.edu
 */
public interface RDFStream {

    public String getURI();

    public void stop();

    public void stream(Triple t);
}
