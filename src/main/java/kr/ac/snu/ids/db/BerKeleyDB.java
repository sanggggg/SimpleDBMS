package kr.ac.snu.ids.db;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

public class BerKeleyDB {

    private static Database INSTANCE = null;

    private static Environment envDb = null;

    private static HashMap<String, Database> tableInstance = new HashMap<>();

    public static Database getTableInstance(String schemaName) {
        Database ret = tableInstance.get(schemaName);
        if (ret == null) {
            tableInstance.put(schemaName, generateDatabase(schemaName));
        }
        return tableInstance.get(schemaName);
    }

    public static Database getInstance() {
        if (INSTANCE == null) {
            INSTANCE = generateDatabase("schema");
        }
        return INSTANCE;
    }

    public static void closeDatabase() {
        if (INSTANCE != null) INSTANCE.close();

        for (Map.Entry<String, Database> elem : tableInstance.entrySet()) {
            elem.getValue().close();
        }

        if (envDb != null) envDb.close();

        INSTANCE = null;
        envDb = null;
    }

    private static Database generateDatabase(String dbName) {
        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setAllowCreate(true);

        if (envDb == null) envDb = new Environment(new File("db/"), envConfig);

        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);
        dbConfig.setSortedDuplicates(true);

        return envDb.openDatabase(null, dbName, dbConfig);
    }
}
