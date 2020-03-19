package AD.AD05;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
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
			String raiz=directorio.getParent()+directorio.getName();
			String punto=raiz.replace(raiz,".");

	
			
			recorrer(directorio, conn,raiz);
			
			//Cerramos a conexión coa base de datos
			if(conn!=null) conn.close();

		} catch (SQLException ex) {
			System.err.println("Error: " + ex.toString());
		}

	}
	
	

	private static void recorrer(File fichero, Connection conn,String raiz) {
		if (fichero.isFile()) {

			FileInputStream fis;
			try {
				fis = new FileInputStream(fichero);

				// Creamos a consulta que inserta  na base de datos
				String sqlInsert = new String("INSERT INTO arquivo(nombre,idDirectorio,binario) VALUES (?,?,?);");
				PreparedStatement ps;
				ps = conn.prepareStatement(sqlInsert);

				// Engadimos como primeiro parámetro o nome do arquivo
				ps.setString(1, fichero.getName());
				// Engadimos como segundo parámetro o directorio
				ps.setInt(2, idDirectorio(conn,fichero.getParent(),raiz));
				// Engadimos como terceiro parametro o arquivo binario
				ps.setBinaryStream(3, fis, (int)fichero.length());
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

		if (fichero.isDirectory() ) {
			try {
				String path=fichero.getPath();
				String nome=path.replace(raiz,".");

				// Creamos a consulta que inserta o directorio na base de datos
				String sqlInsert = new String("INSERT INTO directorio(nome) VALUES (?);");
				PreparedStatement ps;
				ps = conn.prepareStatement(sqlInsert);

				// Engadimos como primeiro parámetro o nome do arquivo
				ps.setString(1, nome);
				// Executamos a consulta
				ps.executeUpdate();
				// Cerrramos a consulta e o arquivo  aberto
				ps.close();
			}catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}


			for (File ficheroHijo : fichero.listFiles()) {
				recorrer(ficheroHijo, conn,raiz);
			}

		}

	}
	
	public static int idDirectorio(Connection conn,String directorio,String raiz) {
		int id=0;
		String nome=directorio.replace(raiz,".");
		 PreparedStatement stmt;
		try {
			stmt = conn.prepareStatement("SELECT id FROM directorio WHERE nome = ?");
			stmt.setString(1,nome);
			ResultSet rs = stmt.executeQuery();
			if (rs.next()) { //Para leer varias posibles filas se cambia el while por el if
	            id = rs.getInt("id");
	        };
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return id;
	}
	


}
