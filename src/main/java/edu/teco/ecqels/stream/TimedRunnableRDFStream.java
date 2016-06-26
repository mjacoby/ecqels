package edu.teco.ecqels.stream;

import edu.teco.ecqels.Engine;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author Michael Jacoby <michael.jacoby@student.kit.edu>
 */
public abstract class TimedRunnableRDFStream extends AbstractRDFStream implements RunnableRDFStream {

    protected boolean running = false;
    protected boolean stop = false;
    protected long sleep = 10 * 1000;

    public TimedRunnableRDFStream(Engine engine, String uri) {
        super(engine, uri);
    }

    @Override
    public void stop() {
        stop = true;
    }
    
    public boolean isRunning() {
        return running;
    }

    public void setRate(float rate) {
        sleep = (long) (1000 / rate);
    }

    public long getRate() {
        return sleep / 1000;
    }

    @Override
    public void run() {
        running = true;
        while (!stop) {
            execute();
            try {
                Thread.sleep(sleep);
            } catch (InterruptedException ex) {
                Logger.getLogger(TimedRunnableRDFStream.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        stop = false;
        running = false;
    }

    protected abstract void execute();
}
