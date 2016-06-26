/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.teco.ecqels.op;

/**
 *
 * @author Michael Jacoby <michael.jacoby@student.kit.edu>
 */
public class FilterWithHistoryState {
    private boolean history = false;
    
    public boolean getHistory() {
        return history;
    }
    
    public void setHistory(boolean history) {
        this.history = history;
    }
}
