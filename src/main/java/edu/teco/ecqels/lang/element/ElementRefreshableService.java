package edu.teco.ecqels.lang.element;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.sparql.syntax.Element;
import com.hp.hpl.jena.sparql.syntax.ElementService;
import edu.teco.ecqels.lang.window.Duration;

/**
 *
 * @author Michael Jacoby
 * @organization Karlsruhe Institute of Technology, Germany www.kit.edu
 * @email michael.jacoby@student.kit.edu
 */
public class ElementRefreshableService extends ElementService {

    private final Duration duration;

    public ElementRefreshableService(String serviceURI, Element el, boolean silent, Duration duration) {
        super(serviceURI, el, silent);
        this.duration = duration;
    }

    public ElementRefreshableService(Node n, Element el, boolean silent, Duration duration) {
        super(n, el, silent);
        this.duration = duration;
    }

    public Duration getDuration() {
        return duration;
    }
}
