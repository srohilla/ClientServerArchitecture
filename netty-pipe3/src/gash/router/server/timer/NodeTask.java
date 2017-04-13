/**
 * 
 */
package gash.router.server.timer;

import java.util.TimerTask;

public class NodeTask extends TimerTask{

	Runnable task = null;
	
	NodeTask(Runnable task) {
		this.task = task;
	}
	
	@Override
	public void run() {
		task.run();
	}

}
