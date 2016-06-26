package edu.teco.ecqels.lang.element;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.sparql.syntax.Element;
import com.hp.hpl.jena.sparql.syntax.ElementNamedGraph;
import edu.teco.ecqels.lang.window.Duration;

/**
 * @author Michael Jacoby
 * @organization Karlsruhe Institute of Technology, Germany www.kit.edu
 * @email michael.jacoby@student.kit.edu
 */
public class ElementRefreshableNamedGraph extends ElementNamedGraph {

    private final Duration duration;

    public ElementRefreshableNamedGraph(Node n, Element el, Duration duration) {
        super(n, el);
        this.duration = duration;
    }

    public Duration getDuration() {
        return duration;
    }

}
