/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.teco.ecqels.event;

import java.util.EventListener;

/**
 *
 * @author Michael Jacoby <michael.jacoby@student.kit.edu>
 */
public interface RefreshRequestedListener extends EventListener {
    public void refreshRequested(RefreshRequestedEvent e);
}
