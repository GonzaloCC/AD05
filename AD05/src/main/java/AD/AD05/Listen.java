package AD.AD05;




import java.io.File;
import java.sql.Connection;
import java.util.logging.Level;
import java.util.logging.Logger;
import static AD.AD05.Main.*;

public class Listen extends Thread {
	
	Connection conn;
	File directorio;
	String raiz;
	Main main;

    public Listen (Connection conn,File directorio,String raiz) {
        this.conn = conn;
        this.directorio= directorio;
        this.raiz=raiz;
        main= new Main();
    }

    @Override
    public void run() {



        try {
            while (true) {
            	System.out.println("Got notification: ");
    			main.recorrer(directorio, conn, raiz);
    			//main.getNomeDirectorios(directorio, conn, raiz);
    			//main.getArquivos(directorio, conn, raiz);
                
               

                Thread.sleep(2000);
            }
        } catch (InterruptedException ex) {
            Logger.getLogger(Listen.class.getName()).log(Level.SEVERE, null, ex);
        }

    }

}




