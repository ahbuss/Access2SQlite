package access2sqlite;

import java.io.File;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.DatabaseMetaData;
import java.sql.ResultSetMetaData;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import static net.ucanaccess.jdbc.UcanaccessDriver.URL_PREFIX;
import static org.sqlite.JDBC.PREFIX;

/**
 *
 * @author ahbuss
 */
public class Main {

    private static final Logger LOGGER = Logger.getLogger(Main.class.getName());

    public static final String ACCESS_PREFIX = URL_PREFIX;

    public static final String SQLITE_PREFIX = PREFIX;

    static {
        try {
            Class.forName("net.ucanaccess.jdbc.UcanaccessDriver");
            Class.forName("org.sqlite.JDBC");
        } catch (ClassNotFoundException ex) {
            LOGGER.log(Level.SEVERE, null, ex);
        }
    }

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        String inputDBName = args.length > 0 ? args[0] : "input/output.accdb";
        File inputDB = new File(inputDBName);
        if (!inputDB.exists()) {
            System.err.println("Database not found: " + inputDB.getAbsolutePath());
            return;
        }
        String url = ACCESS_PREFIX.concat(inputDB.getAbsolutePath());
        try {
            Connection accessConnection = DriverManager.getConnection(url);
            DatabaseMetaData dbmd = accessConnection.getMetaData();
            Set<String> tables = getTables(dbmd);
//            System.out.println(tables);
            for (String table : tables) {
                Map<String, String> columns = getColumnsFor(dbmd, table);
//                System.out.println("Table: " + table);
//                System.out.println(columns);
            }
            String sqliteDBName = inputDBName.substring(0, inputDBName.lastIndexOf(".")).concat(".SQLite");
            File sqliteDB = new File(sqliteDBName);
            if (sqliteDB.exists()) {
                LOGGER.warning(sqliteDB.getAbsolutePath() + " exists - will be overwritten");
                sqliteDB.delete();
            }
            String sqliteURL = SQLITE_PREFIX.concat(sqliteDBName);
            Connection sqliteConnection = DriverManager.getConnection(sqliteURL);
            createSQLiteFrom(sqliteConnection, dbmd);
            populateSqLiteFromAccess(accessConnection, sqliteConnection);
            sqliteConnection.close();
            accessConnection.close();
            System.out.println("SQLite Databse created: " + sqliteDB.getAbsolutePath());
        } catch (SQLException ex) {
            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
        }

    }

    public static Set<String> getTables(DatabaseMetaData dbmd) {
        Set<String> tables = new TreeSet<>();
        try {
            ResultSet rs = dbmd.getTables(null, null, null, null);
            while (rs.next()) {
                tables.add(rs.getString("TABLE_NAME"));
            }
            rs.close();
        } catch (SQLException ex) {
            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
        }

        return tables;
    }

    public static Map<String, String> getColumnsFor(DatabaseMetaData dbmd, String table) {
        Map<String, String> columns = new LinkedHashMap<>();
        try {
            ResultSet rs = dbmd.getColumns(null, null, table, null);
            while (rs.next()) {
                String name = rs.getString("COLUMN_NAME");
                String type = rs.getString("TYPE_NAME");
                columns.put(name, type);
            }
            rs.close();
        } catch (SQLException ex) {
            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
        }
        return columns;
    }
    
    public static void createSQLiteFrom(Connection sqliteConnection, DatabaseMetaData dbmd) {
        try {
            Statement statement = sqliteConnection.createStatement();
            Set<String> tables = getTables(dbmd);
            for (String table: tables) {
                StringBuilder builder = new StringBuilder("CREATE TABLE ");
                builder.append(table).append(" (");
                Map<String, String> columns = getColumnsFor(dbmd, table);
                for (String columnName: columns.keySet()) {
                    builder.append(columnName).append(" ");
                    String columnType = columns.get(columnName);
                    builder.append(columnType).append(", ");
                }
                builder.deleteCharAt(builder.length() - 2);
                builder.append(")");
                
                statement.executeUpdate(builder.toString());
//                System.out.println(builder);
            }
            statement.close();
        } catch (SQLException ex) {
            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    public static void populateSqLiteFromAccess(Connection accessConnection, Connection sqliteConnection) {
        try {
            Statement accessStatement = accessConnection.createStatement();
            Statement sqliteStatement = sqliteConnection.createStatement();
            Set<String> tables = getTables(accessConnection.getMetaData());
            for (String table: tables) {
                ResultSet rs = accessStatement.executeQuery("SELECT * FROM " + table);
                ResultSetMetaData rsmd = rs.getMetaData();
                while (rs.next()) {
                    StringBuilder builder = new StringBuilder("INSERT INTO ");
                    builder.append(table).append(" (");
                    for (int column = 1; column <= rsmd.getColumnCount(); ++column) {
                        String columnName = rsmd.getColumnName(column);
                        builder.append(columnName).append(",");
                    }
                    builder.deleteCharAt(builder.length() - 1);
                    builder.append(") VALUES (");
                    for (int column = 1; column <= rsmd.getColumnCount(); ++column) {
                        Object value = rs.getObject(column);
                        if (value instanceof String) {
                            value = "'" + value + "'";
                        }
                        builder.append(value).append(",");
                    }
                    builder.deleteCharAt(builder.length() - 1);
                    builder.append(")");
                    String query = builder.toString();
                    sqliteStatement.executeUpdate(builder.toString());
                }
            }
        } catch (SQLException ex) {
            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
}
