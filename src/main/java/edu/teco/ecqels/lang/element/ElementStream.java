package edu.teco.ecqels.lang.element;

import edu.teco.ecqels.window.Window;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.sparql.syntax.Element;
import com.hp.hpl.jena.sparql.syntax.ElementNamedGraph;
import edu.teco.ecqels.lang.window.WindowInfo;

public class ElementStream extends ElementNamedGraph {
	private WindowInfo window;
	public ElementStream(Node n, WindowInfo w, Element el) {
		super(n, el);
		window = w;
	}
	
	public WindowInfo getWindowInfo() {	return window; }
}
