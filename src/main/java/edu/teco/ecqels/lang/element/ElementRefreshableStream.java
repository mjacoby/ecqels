package edu.teco.ecqels.lang.element;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.sparql.syntax.Element;
import edu.teco.ecqels.lang.window.Duration;
import edu.teco.ecqels.window.Window;
import edu.teco.ecqels.lang.window.WindowInfo;

/**
 *
 * @author Michael Jacoby
 * @organization Karlsruhe Institute of Technology, Germany www.kit.edu
 * @email michael.jacoby@student.kit.edu
 */
public class ElementRefreshableStream extends ElementStream {

    private final Duration duration;

    public ElementRefreshableStream(Node n, WindowInfo w, Element el, Duration duration) {
        super(n, w, el);
        this.duration = duration;
    }

    public Duration getDuration() {
        return duration;
    }
}
