package br.com.peter.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

/**
 * @author thiago peter
 * @since 30/10/2015 POC - Cassandra
 */
public class App {
	
	// https://academy.datastax.com/demos/getting-started-apache-cassandra-and-java-part-i
	
	private static Cluster cluster;
	private static Session session;
	
	public static void main(String[] args) {

		try {
			conectar();
			inserir();
//			atualizar();
//			deletar();
			consultar();
		} finally {
			finalizaConexao();
		}
	}

	public static void conectar() {
		cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
		session = cluster.connect("teste");
		System.out.println("Cassandra connected.");
	}

	public static void finalizaConexao() {
		
		cluster.close();
		System.out.println("Cassandra connection closed.");
	}
	
	public static void inserir() {
		session.execute("INSERT INTO users (lastname, age, city, email, firstname) VALUES ('Jones', 35, 'Austin', 'bob@example.com', 'Bob')");
		System.out.println("user inserted.");
	}

	public static void deletar() {
		session.execute("DELETE FROM users WHERE lastname = 'Jones'");
		System.out.println("user removed.");
	}

	public static void atualizar() {
		session.execute("update users set age = 36 where lastname = 'Jones'");
		System.out.println("user updated.");
	}

	public static void consultar() {
		ResultSet results = session.execute("SELECT * FROM users WHERE lastname='Jones'");
		for (Row row : results) {
			System.out.format("%s %d\n", row.getString("firstname"), row.getInt("age"));
		}
	}
}
