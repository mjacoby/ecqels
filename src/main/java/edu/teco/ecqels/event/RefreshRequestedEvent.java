/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.teco.ecqels.event;

import edu.teco.ecqels.refresh.RefreshRequest;
import java.util.EventObject;

/**
 *
 * @author Michael Jacoby <michael.jacoby@student.kit.edu>
 */
public class RefreshRequestedEvent extends EventObject {

    private final RefreshRequest refreshRequest;
    
    public RefreshRequestedEvent(Object source, RefreshRequest refreshRequest) {
        super(source);
        this.refreshRequest = refreshRequest;                
    }
    
    public RefreshRequest getRefreshRequest() {
        return refreshRequest;
    }
    
}
