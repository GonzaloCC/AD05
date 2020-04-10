package AD.AD05;

import java.io.File;
import java.io.FileOutputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.postgresql.PGConnection;
import org.postgresql.PGNotification;

import static AD.AD05.Main.*;

public class Listener extends Thread {

	Connection conn;
	File directorio;
	String raiz;
	Main main;

	public Listener(Connection conn, File directorio, String raiz) {
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

			// Configuramos para estar a escoita
			PGConnection pgconn = conn.unwrap(PGConnection.class);
			Statement stmt = conn.createStatement();
			stmt.execute("LISTEN novamensaxe");
			stmt.close();
			System.out.println("Esperando novos arquivos...");

			// Variables para controlar o tempo de espera
			boolean flag = true;
			long finishAt = new Date().getTime() + (tempo * 60000);

			// Creamos a consulta que necesitaremos para obter os arquivos
			PreparedStatement sqlMensaxe = conn
					.prepareStatement("SELECT nombre,idDirectorio,binario FROM arquivo WHERE id=?;");
			// Bucle para ir lendo as notificacions

			while (flag) {
				System.out.println("lendo notificacions");
				// main.recorrer(directorio, conn, raiz);
				// main.getNomeDirectorios(directorio, conn, raiz);
				// main.getArquivos(directorio, conn, raiz);
				PGNotification notifications[] = pgconn.getNotifications();
				if (notifications != null) {
					for (int i = 0; i < notifications.length; i++) {
						int id = Integer.parseInt(notifications[i].getParameter());
						sqlMensaxe.setInt(1, id);
						ResultSet rs = sqlMensaxe.executeQuery();
						rs.next();
						System.out.println(rs.getString(1) + ":" + rs.getInt(2));

						Existe existe = new Existe();
						String nome = rs.getString(1);
						int idDirectorio = rs.getInt(2);
						byte[] arqBytes = null;
						if (!existeArquivo(directorio, conn, raiz, nome, idDirectorio, existe)) {
							arqBytes = rs.getBytes(3);

							String ruta = raiz + nomeDirectorio(rs.getInt(2), conn).substring(1)
									+ System.getProperty("file.separator") + rs.getString(1);
							// Creamos o fluxo de datos para gardar o arquivo recuperado
							String ficheiroSaida = new String(ruta);
							File fileOut = new File(ficheiroSaida);
							try {
								FileOutputStream fluxoDatos = new FileOutputStream(fileOut);

								// Gardamos o arquivo recuperado
								if (arqBytes != null) {
									fluxoDatos.write(arqBytes);
								}

								// cerramos o fluxo de datos de saida
								fluxoDatos.close();
							} catch (Exception e) {
								e.printStackTrace();
							}

						}

						rs.close();

					}
				}
				// Esperamos un pouco de tempo entre conexións
				Thread.sleep(espera);

				// Comprobamos se pasou o tempo de espera
				if (new Date().getTime() > finishAt)
					flag = false;

			}

		} catch (InterruptedException ex) {
			Logger.getLogger(Listen.class.getName()).log(Level.SEVERE, null, ex);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
