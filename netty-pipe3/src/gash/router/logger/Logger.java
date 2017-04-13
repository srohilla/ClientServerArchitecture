package gash.router.logger;


import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class Logger {
	static FileWriter out;
	public static void init() {
		 try {
			 out = new FileWriter("debug.log",false); 
	
				 out.write("----------DEBUG----------");
			}catch (Exception	 e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static synchronized void DEBUG(String toBeLogged) {

		System.out.println(toBeLogged);
		writeDEBUG(toBeLogged);

	}

	public static synchronized void DEBUG(String toBeLogged, Exception ex) {

		System.out.println(toBeLogged);
		writeDEBUG(toBeLogged);
		ex.printStackTrace();

	}

	public static void writeDEBUG(String debug) {
		try {
			 out = new FileWriter("debug.log",true); 
			 out.write(debug);

		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
