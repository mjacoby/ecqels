/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.teco.ecqels.event;

import com.hp.hpl.jena.sparql.engine.QueryIterator;
import edu.teco.ecqels.window.Window;
import java.util.EventObject;

/**
 *
 * @author Michael Jacoby <michael.jacoby@student.kit.edu>
 */
public class DataChangedEvent extends EventObject {

    private final Window window;
    private final QueryIterator result;
    
    public DataChangedEvent(Object source, Window  window, QueryIterator result) {
        super(source);
        this.window = window;
        this.result = result;              
    }

    /**
     * @return the window
     */
    public Window getWindow() {
        return window;
    }

    /**
     * @return the result
     */
    public QueryIterator getResult() {
        return result;
    }
    
}
