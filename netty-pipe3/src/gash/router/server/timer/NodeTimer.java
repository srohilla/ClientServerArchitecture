package gash.router.server.timer;

import java.util.Timer;
/**
 * timer class to schedule tasks
 * @author seemarohilla
 *
 */
public class NodeTimer extends Timer {
	private Runnable task = null;
	
	private NodeTask node = null;
	
	public void schedule(Runnable runnable, long delay) {
        task = runnable;
        node = new NodeTask(runnable); 
        this.schedule(node, delay);
    }
	
	public void reschedule(long delay) {
        node.cancel();
        node = new NodeTask(task);
        this.schedule(node, delay);
    }	
}
