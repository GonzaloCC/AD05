package AD.AD05;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;
import java.util.Properties;

import org.postgresql.PGConnection;
import org.postgresql.PGNotification;

import com.google.gson.Gson;

public class Main {

	public static void main(String[] args) {

		// Tempo en minutos que estara a espera
		Integer tempo = 1;

		// Tempo que espera para cada consulta en milisegundos
		Integer espera = 1000;

		Configuracion configuracion = new Configuracion();
		File arquivoConf = new File("configuracion.json");

		try {
			// Creamos un fluxo de entrada para o arquivo
			FileReader fluxoDatos;
			fluxoDatos = new FileReader(arquivoConf);

			// Creamos o bufer de entrada
			BufferedReader buferEntrada = new BufferedReader(fluxoDatos);

			// Imos lendo linea a linea
			StringBuilder jsonBuilder = new StringBuilder();
			String linea;
			while ((linea = buferEntrada.readLine()) != null) {
				jsonBuilder.append(linea).append("\n");
			}

			// Temos que cerrar sempre o ficheiro
			buferEntrada.close();

			// Construimos o String con todalas lineas lidas
			String json = jsonBuilder.toString();

			// Pasamos o json a clase ca cal se corresponde
			Gson gson = new Gson();
			configuracion = gson.fromJson(json, Configuracion.class);
		} catch (FileNotFoundException e) {
			System.out.println("Non se encontra o arquivo");
		} catch (IOException e) {
			System.out.println("Erro de entrada saída");
		}

		// TODO Auto-generated method stub
		// URL e base de datos a cal nos conectamos
		String url = new String(configuracion.getDbconnection().getAddress());
		String db = new String(configuracion.getDbconnection().getName());

		// Indicamos as propiedades da conexión
		Properties props = new Properties();
		props.setProperty("user", configuracion.getDbconnection().getUser());
		props.setProperty("password", configuracion.getDbconnection().getPassword());

		// Dirección de conexión a base de datos
		String postgres = "jdbc:postgresql://" + url + "/" + db;

		// Conectamos a base de datos
		try {
			

			Connection conn = DriverManager.getConnection(postgres, props);
			Notifier notifier = new Notifier(conn);

			notifier.start();

			// Creamos a táboa directorio
			String sqlTableCreation = new String(
					"CREATE TABLE IF NOT EXISTS directorio (id serial primary key, nome text not null);");
			// Executamos a sentencia SQL anterior
			CallableStatement createFunction = conn.prepareCall(sqlTableCreation);
			createFunction.execute();
			//System.out.println("tabla creada");
			// createFunction.close();

			// Creamos a táboa arquivo
			String sqlTableCreation1 = new String(
					"CREATE TABLE IF NOT EXISTS arquivo (id serial primary key, nombre text not null,idDirectorio integer references directorio(id),binario bytea not null);");
			// Executamos a sentencia SQL anterior
			createFunction = conn.prepareCall(sqlTableCreation1);
			createFunction.execute();
			createFunction.close();

			crearFuncion(conn);
/*/
			PGConnection pgconn = conn.unwrap(PGConnection.class);
			Statement stmt = conn.createStatement();
			stmt.execute("LISTEN novoArquivo");
			stmt.close();
			System.out.println("Esperando novos arquivos...");

			// Variables para controlar o tempo de espera
			boolean flag = false;
			long finishAt = new Date().getTime() + (tempo * 60000);

			// Creamos a consulta que necesitaremos para obter a mensaxe
			PreparedStatement sqlMensaxe = conn.prepareStatement("SELECT nombre FROM arquivo WHERE id=?;");
			// Bucle para ir lendo as mensaxes
			while (flag) {

				PGNotification notifications[] = pgconn.getNotifications();
				if (notifications != null) {
					for (int i = 0; i < notifications.length; i++) {
						int id = Integer.parseInt(notifications[i].getParameter());
						sqlMensaxe.setInt(1, id);
						ResultSet rs = sqlMensaxe.executeQuery();
						rs.next();
						System.out.println(rs.getString(1) + ":" + rs.getString(2));
						rs.close();
					}
				}
			}
			
			*/
			
			
			File directorio = new File(configuracion.getApp().getDirectory());
			String raiz = directorio.getParent() + directorio.getName();
			String punto = raiz.replace(raiz, ".");
			
			


			recorrer(directorio, conn, raiz);
			getNomeDirectorios(directorio, conn, raiz);
			getArquivos(directorio, conn, raiz);
			
			Connection Lconn = DriverManager.getConnection(postgres, props);
			Listener listener = new Listener(Lconn,directorio,raiz);
			listener.start();
			
			Connection Ltconn = DriverManager.getConnection(postgres, props);
			Listen listen = new Listen(Ltconn,directorio,raiz);
			listen.start();

			// Cerramos a conexión coa base de datos

			// if (conn != null)
			// conn.close();

		} catch (SQLException ex) {
			System.err.println("Error: " + ex.toString());
		}
		


	}// fin do metodo main

	///////////////////////////////////////////////////////////////

	static void recorrer(File fichero, Connection conn, String raiz) {
		if (fichero.isFile()) {
			// System.out.println(fichero.getAbsolutePath());
			String path = fichero.getParent();
			String nomeD = path.replace(raiz, ".");
			int idDirectorio = idDirectorio(conn, fichero.getParent(), raiz);
			String nomeArquivo = fichero.getName();
			//System.out.println("nome: " + nomeArquivo + " id directorio: " + idDirectorio);

			if (!existeArquivoDirectorio(conn, nomeArquivo, idDirectorio)) {

				FileInputStream fis;
				try {
					fis = new FileInputStream(fichero);

					// Creamos a consulta que inserta na base de datos
					String sqlInsert = new String("INSERT INTO arquivo(nombre,idDirectorio,binario) VALUES (?,?,?);");
					PreparedStatement ps;
					ps = conn.prepareStatement(sqlInsert);

					// Engadimos como primeiro parámetro o nome do arquivo
					ps.setString(1, nomeArquivo);
					// Engadimos como segundo parámetro o directorio
					ps.setInt(2, idDirectorio);
					// Engadimos como terceiro parametro o arquivo binario
					ps.setBinaryStream(3, fis, (int) fichero.length());
					// Executamos a consulta
					ps.executeUpdate();
					// Cerrramos a consulta e o arquivo aberto
					ps.close();

					fis.close();
					System.out.println("Novo arquivo añadido");
				} catch (FileNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

		}

		if (fichero.isDirectory()) {
			try {
				String path = fichero.getPath();
				String nome = path.replace(raiz, ".");

				if (!igualNomeDirectorio(conn, nome)) {

					// Creamos a consulta que inserta o directorio na base de datos
					String sqlInsert = new String("INSERT INTO directorio(nome) VALUES (?);");
					PreparedStatement ps;
					ps = conn.prepareStatement(sqlInsert);

					// Engadimos como primeiro parámetro o nome do arquivo
					ps.setString(1, nome);
					// Executamos a consulta
					ps.executeUpdate();
					// Cerrramos a consulta e o arquivo aberto
					ps.close();
				}
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			for (File ficheroHijo : fichero.listFiles()) {
				recorrer(ficheroHijo, conn, raiz);
			}

		}

	}

///////////////////////////////////////////////////////////////////////////////////////
	public static int idDirectorio(Connection conn, String directorio, String raiz) {
		int id = 0;
		String nome = directorio.replace(raiz, ".");
		PreparedStatement stmt;
		try {
			stmt = conn.prepareStatement("SELECT id FROM directorio WHERE nome = ?");
			stmt.setString(1, nome);
			ResultSet rs = stmt.executeQuery();
			if (rs.next()) { // Para leer varias posibles filas se cambia el while por el if
				id = rs.getInt("id");
			}
			;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return id;
	}

	//////////////////////////////////////////////////////////////////////////
	public static boolean igualNomeDirectorio(Connection conn, String nome) {
		boolean existe = false;
		PreparedStatement stmt;
		try {
			stmt = conn.prepareStatement("SELECT nome FROM directorio");
			ResultSet rs = stmt.executeQuery();
			while (rs.next()) {
				if (rs.getString("nome").equals(nome)) {
					existe = true;
				}
			}
			rs.close();
			stmt.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return existe;
	}

	////////////////////////////////////////////////////////////

	public static boolean igualNomeArquivo(Connection conn, String nome) {
		boolean existe = false;
		PreparedStatement stmt;
		try {
			stmt = conn.prepareStatement("SELECT nombre FROM arquivo");
			ResultSet rs = stmt.executeQuery();
			while (rs.next()) {
				if (rs.getString("nombre").equals(nome)) {
					existe = true;
				}
			}
			rs.close();
			stmt.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//System.out.println("Existe nome arquivo: " + existe);
		return existe;
	}

	/////////////////////////////////////////////////////////////////////

	public static boolean igualIdDirectorio(Connection conn, int id, String nomeD) {
		boolean existe = false;
		PreparedStatement stmt;
		try {
			stmt = conn.prepareStatement("SELECT id FROM directorio where nome=?");
			stmt.setString(1, nomeD);
			ResultSet rs = stmt.executeQuery();
			while (rs.next()) {
				//System.out.println("id consulta: " + rs.getInt(1) + " Id: " + id);
				if (rs.getInt("id") == id) {
					existe = true;
				}
			}
			rs.close();
			stmt.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("Existe id directorio: " + existe);
		return existe;
	}

	/////////////////////////////////////////////////////////////

	public static void crearFuncion(Connection conn) {
		String sqlCreateFunction = new String("CREATE OR REPLACE FUNCTION notificar_novoArquivo() "
				+ "RETURNS trigger AS $$ " + "BEGIN " + "PERFORM pg_notify('novamensaxe',NEW.id::text); "
				+ "RETURN NEW; " + "END; " + "$$ LANGUAGE plpgsql; ");

		CallableStatement createFunction;
		try {
			createFunction = conn.prepareCall(sqlCreateFunction);
			createFunction.execute();
			createFunction.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		String sqlCreateTrigger = new String("DROP TRIGGER IF EXISTS not_novo_arquivo ON arquivo; "
				+ "CREATE TRIGGER not_novo_arquivo " + "AFTER INSERT " + "ON arquivo " + "FOR EACH ROW "
				+ "EXECUTE PROCEDURE notificar_novoArquivo(); ");
		CallableStatement createTrigger;
		try {
			createTrigger = conn.prepareCall(sqlCreateTrigger);
			createTrigger.execute();
			createTrigger.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	/////////////////////////////////////////////////////////////////////

	public static void getNomeDirectorios(File fichero, Connection conn, String raiz) {
		// Creamos a consulta que inserta na base de datos
		String sqlInsert = new String("SELECT nome from directorio");
		PreparedStatement ps;
		try {
			ps = conn.prepareStatement(sqlInsert);
			ResultSet rs = ps.executeQuery();
			while (rs.next()) {
				// String path = fichero.getPath();
				// String nomeF = path.replace(raiz, ".");
				//System.out.println("nome: " + rs.getString(1));
				String nome = rs.getString(1);
				Existe existe = new Existe();
				if (!recorreD(fichero, conn, raiz, nome, existe)) {
					System.out.println("No existe, crear directorio");
					File directorio = new File(raiz + "/" + rs.getString(1));
					directorio.mkdirs();
				}

			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	///////////////////////////////////////////////

	public static void getArquivos(File directorio, Connection conn, String raiz) {
		// Creamos a consulta que inserta na base de datos
		String sqlInsert = new String("SELECT nombre,idDirectorio,binario from arquivo");
		PreparedStatement ps;
		try {
			ps = conn.prepareStatement(sqlInsert);
			ResultSet rs = ps.executeQuery();
			while (rs.next()) {
				Existe existe = new Existe();
				String nome = rs.getString("nombre");
				int id = rs.getInt(2);
				//System.out.println("nome: " + nome + " Id: " + id);
				byte[] arqBytes = null;
				if (!existeArquivo(directorio, conn, raiz, nome, id, existe)) {
					//System.out.println("non existe arquivo, crear");

					arqBytes = rs.getBytes(3);

					String ruta = raiz + nomeDirectorio(rs.getInt(2), conn).substring(1)
							+ System.getProperty("file.separator") + rs.getString(1);
					// Creamos o fluxo de datos para gardar o arquivo recuperado
					String ficheiroSaida = new String(ruta);
					File fileOut = new File(ficheiroSaida);
					FileOutputStream fluxoDatos = new FileOutputStream(fileOut);

					// Gardamos o arquivo recuperado
					if (arqBytes != null) {
						fluxoDatos.write(arqBytes);
					}

					// cerramos o fluxo de datos de saida
					fluxoDatos.close();

				}
			}
			rs.close();
			ps.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	//////////////////////////////////////////////////

	static boolean existeArquivo(File fichero, Connection conn, String raiz, String nome, int idDirectorio,
			Existe existe) {
		//System.out.println("Entramos en existeArquivo con " + fichero.getAbsolutePath());
		if (fichero.isFile()) {
			int idDirectorioF = idDirectorio(conn, fichero.getParent(), raiz);
			String nomeF = fichero.getName();
			if (nomeF.equals(nome)) {
				String path = fichero.getParent();
				String nomeD = path.replace(raiz, ".");
				String nomeDir = nomeDirectorio(idDirectorio, conn);
				//System.out.println("nome arquivo igual");
				//System.out.println("nomeD: " + nomeD + " id: " + idDirectorio + " NomeDir: " + nomeDir);
				if (nomeD.equals(nomeDir)) {
					existe.setExiste(true);
				}
			}

		}
		if (fichero.isDirectory()) {
			for (File ficheroHijo : fichero.listFiles()) {
				existeArquivo(ficheroHijo, conn, raiz, nome, idDirectorio, existe);
			}
		}
		// System.out.println("Existe arquivo : " + existe.isExiste());
		return existe.isExiste();
	}
	////////////////////////////////////////////////

	public static String nomeDirectorio(int idDirectorio, Connection conn) {
		String nome = "";
		// Creamos a consulta que inserta na base de datos
		String sqlInsert = new String("SELECT nome from directorio where id=?");
		PreparedStatement ps;
		try {
			ps = conn.prepareStatement(sqlInsert);
			ps.setInt(1, idDirectorio);
			ResultSet rs = ps.executeQuery();
			if (rs.next()) {
				nome = rs.getString(1);

			}

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return nome;

	}
	//////////////////////////////////////////////////////

	private static boolean existeArquivoDirectorio(Connection conn, String nomeArquivo, int idDirectorio) {
		boolean existe = false;
		String sqlInsert = new String("SELECT nombre,idDirectorio from arquivo");
		PreparedStatement ps;
		try {
			ps = conn.prepareStatement(sqlInsert);
			ResultSet rs = ps.executeQuery();
			while (rs.next()) {
				if (nomeArquivo.equals(rs.getString(1)) && idDirectorio == rs.getInt(2)) {
					existe = true;
				}

			}

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		//System.out.println("Existe: " + existe);
		return existe;
	}

	////////////////////////////////////////////////////
	private static boolean recorreD(File fichero, Connection conn, String raiz, String nome, Existe existe) {
		if (fichero.isDirectory()) {
			String path = fichero.getPath();
			String nomeF = path.replace(raiz, ".");
			// System.out.println("nome: "+nome+" NomeF: "+nomeF);
			if (nomeF.equals(nome)) {
				existe.setExiste(true);
			}
			for (File ficheroHijo : fichero.listFiles()) {
				recorreD(ficheroHijo, conn, raiz, nome, existe);
			}

		}
		// System.out.println("NomeF: "+nomeF+" nome: "+nome);
		// System.out.println("Existe directorio: "+existe.isExiste());
		return existe.isExiste();
	}

/////////////////////////////////////////////////////////////////////////////////

}// Acaba a clase main


/*
class Listener extends Thread {
	private Connection conn;
	private org.postgresql.PGConnection pgconn;

	Listener(Connection conn) throws SQLException {
		this.conn = conn;
		this.pgconn = conn.unwrap(org.postgresql.PGConnection.class);
		Statement stmt = conn.createStatement();
		stmt.execute("LISTEN mymessage");
		stmt.close();
	}

	public void run() {
		try {
			while (true) {
				org.postgresql.PGNotification notifications[] = pgconn.getNotifications();

				// If this thread is the only one that uses the connection, a timeout can be
				// used to
				// receive notifications immediately:
				// org.postgresql.PGNotification notifications[] =
				// pgconn.getNotifications(10000);

				if (notifications != null) {
					for (int i = 0; i < notifications.length; i++)
						System.out.println("Got notification: " + notifications[i].getName());
				}

				// wait a while before checking again for new
				// notifications

				Thread.sleep(500);
			}
		} catch (SQLException sqle) {
			sqle.printStackTrace();
		} catch (InterruptedException ie) {
			ie.printStackTrace();
		}
	}
}


*/

class Notifier extends Thread {
	private Connection conn;

	public Notifier(Connection conn) {
		this.conn = conn;
	}

	public void run() {
		while (true) {
			try {
				Statement stmt = conn.createStatement();
				stmt.execute("NOTIFY mymessage");
				stmt.close();
				Thread.sleep(2000);
			} catch (SQLException sqle) {
				sqle.printStackTrace();
			} catch (InterruptedException ie) {
				ie.printStackTrace();
			}
		}
	}
}
