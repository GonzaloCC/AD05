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
import java.util.Properties;

import com.google.gson.Gson;

public class Main {

	public static void main(String[] args) {

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
			System.out.println("Conectado");
			// Creamos a táboa directorio
			String sqlTableCreation = new String(
					"CREATE TABLE IF NOT EXISTS directorio (id serial primary key, nome text not null);");
			// Executamos a sentencia SQL anterior
			CallableStatement createFunction = conn.prepareCall(sqlTableCreation);
			createFunction.execute();
			System.out.println("tabla creada");
			// createFunction.close();

			// Creamos a táboa arquivo
			String sqlTableCreation1 = new String(
					"CREATE TABLE IF NOT EXISTS arquivo (id serial primary key, nombre text not null,idDirectorio integer references directorio(id),binario bytea not null);");
			// Executamos a sentencia SQL anterior
			createFunction = conn.prepareCall(sqlTableCreation1);
			createFunction.execute();
			createFunction.close();

			File directorio = new File(configuracion.getApp().getDirectory());
			String raiz = directorio.getParent() + directorio.getName();
			String punto = raiz.replace(raiz, ".");

			recorrer(directorio, conn, raiz);
			getNomeDirectorios(directorio, conn, raiz);
			getArquivos(directorio, conn, raiz);

			// Cerramos a conexión coa base de datos
			if (conn != null)
				conn.close();

		} catch (SQLException ex) {
			System.err.println("Error: " + ex.toString());
		}

	}

	///////////////////////////////////////////////////////////////

	private static void recorrer(File fichero, Connection conn, String raiz) {
		if (fichero.isFile()) {
			String path = fichero.getParent();
			String nomeD = path.replace(raiz, ".");
			int idDirectorio = idDirectorio(conn, fichero.getParent(), raiz);
			String nomeArquivo = fichero.getName();
			System.out.println("nome: " + nomeArquivo + " id directorio: " + idDirectorio);

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
		System.out.println("Existe nome arquivo: " + existe);
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
				System.out.println("id consulta: " + rs.getInt(1) + " Id: " + id);
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
		// Creamos a sentencia SQL para crear unha función
		// NOTA: nón é moi lóxico crear funcións dende código. Só o fago para despois
		// utilizala
		String sqlCreateFucction = new String("CREATE OR REPLACE FUNCTION inc(val integer) RETURNS integer AS $$ "
				+ "BEGIN " + "RETURN val + 1; " + "END;" + "$$ LANGUAGE PLPGSQL;");
		// Executamos a sentencia SQL anterior
		CallableStatement createFunction;
		try {
			createFunction = conn.prepareCall(sqlCreateFucction);
			createFunction.execute();
			createFunction.close();
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
				if (!existeDirectorio(fichero, conn, raiz, rs.getString(1))) {
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

	public static void getArquivos(File fichero, Connection conn, String raiz) {

		// Creamos a consulta que inserta na base de datos
		String sqlInsert = new String("SELECT nombre,idDirectorio from arquivo");
		PreparedStatement ps;
		try {
			ps = conn.prepareStatement(sqlInsert);
			ResultSet rs = ps.executeQuery();
			while (rs.next()) {
				if (!existeArquivo(fichero, conn, raiz, rs.getString("nombre"), rs.getInt(2))) {
					byte[] arqBytes = null;
						arqBytes = rs.getBytes(2);

					String ruta = raiz + nomeDirectorio(rs.getInt(2), conn).substring(1)
							+ System.getProperty("file.separator") + rs.getString(1);
					System.out.println(ruta);
					File file = new File(ruta);
					FileOutputStream os = new FileOutputStream(file);
					// Gardamos o arquivo recuperado
					if (arqBytes != null) {
						os.write(arqBytes);
					}
					// cerramos o fluxo de datos de saida
					os.close();


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

	///////////////////////////////////////////////

	private static boolean existeDirectorio(File fichero, Connection conn, String raiz, String nome) {
		boolean existe = false;
		if (fichero.isDirectory()) {
			String path = fichero.getPath();
			String nomeF = path.replace(raiz, ".");
			if (nomeF.contentEquals(nome)) {
				existe = true;
			}

			for (File ficheroHijo : fichero.listFiles()) {
				existeDirectorio(ficheroHijo, conn, raiz, nome);
			}
		}
		return existe;
	}
	//////////////////////////////////////////////////

	private static boolean existeArquivo(File fichero, Connection conn, String raiz, String nome, int idDirectorio) {
		boolean existe = false;
		if (fichero.isFile()) {
			int idDirectorioF = idDirectorio(conn, fichero.getParent(), raiz);
			String nomeF = fichero.getName();
			if (nomeF.contentEquals(nome)) {
				String path = fichero.getPath();
				String nomeD = path.replace(raiz, ".");
				if (nomeD.equals(nomeDirectorio(idDirectorio, conn))) {
					existe = true;
				}
			}

			for (File ficheroHijo : fichero.listFiles()) {
				existeArquivo(ficheroHijo, conn, raiz, nome, idDirectorio);
			}
		}
		System.out.println("Existe: " + existe);
		return existe;
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

		System.out.println("Existe: " + existe);
		return existe;
	}

}
