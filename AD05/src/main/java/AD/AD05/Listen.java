package AD.AD05;

import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;
import static AD.AD05.Main.*;

public class Listen extends Thread {

	Connection conn;
	File directorio;
	String raiz;
	Main main;

	public Listen(Connection conn, File directorio, String raiz) {
		this.conn = conn;
		this.directorio = directorio;
		this.raiz = raiz;
		main = new Main();
	}

	@Override
	public void run() {
		try {
			// Tempo en minutos que estara a espera
			Integer tempo = 1;

			// Tempo que espera para cada consulta en milisegundos
			Integer espera = 2000;
			// Variables para controlar o tempo de espera
			boolean flag = true;
			long finishAt = new Date().getTime() + (tempo * 60000);

			while (flag) {
				System.out.println("Got notification: ");
				main.recorrer(directorio, conn, raiz);

				Thread.sleep(espera);

				// Comprobamos se pasou o tempo de espera
				if (new Date().getTime() > finishAt)
					flag = false;
			}
			if (conn != null)
				conn.close();

		} catch (InterruptedException ex) {
			Logger.getLogger(Listen.class.getName()).log(Level.SEVERE, null, ex);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
