import java.io.*;
import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Properties;

public class DBApp implements DBAppInterface {
    static final int N = 200;
    HashMap<String, HashMap<String, String>> tableData = new HashMap<String, HashMap<String, String>>();

    public static void main(String[] args) throws DBAppException {
//        String strTableName = "Student";
        DBApp dbApp = new DBApp();
//        dbApp.init();
//        Hashtable htblColNameType = new Hashtable();
//
//        htblColNameType.put("id", "java.lang.Integer");
//        htblColNameType.put("name", "java.lang.String");
//        htblColNameType.put("gpa", "java.lang.double");
//
//        Hashtable min = new Hashtable();
//        min.put("id", "0");
//        min.put("name", "a");
//        min.put("gpa", "0.7");
//
//        Hashtable max = new Hashtable();
//        max.put("id", "9");
//        max.put("name", "z");
//        max.put("gpa", "4");
//
//
//        dbApp.createTable(strTableName, "id", htblColNameType, min, max);
    }

    @Override
    public void init() {
        try {
            FileWriter csvWriter = new FileWriter("src/main/resources/metadata.csv", true);
            csvWriter.append("Table Name");
            csvWriter.append(",");
            csvWriter.append("Column Name");
            csvWriter.append(",");
            csvWriter.append("Column Type");
            csvWriter.append(",");
            csvWriter.append("ClusteringKey");
            csvWriter.append(",");
            csvWriter.append("Indexed");
            csvWriter.append(",");
            csvWriter.append("min");
            csvWriter.append(",");
            csvWriter.append("max");
            csvWriter.append("\n");
            csvWriter.flush();
            csvWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Override
    public void createTable(String tableName, String clusteringKey, Hashtable<String, String> colNameType, Hashtable<String, String> colNameMin, Hashtable<String, String> colNameMax) throws DBAppException {
        if (!tableExists(tableName)) {
            try {
                FileWriter csvWriter = new FileWriter("src/main/resources/metadata.csv", true);
                for (String s : colNameType.keySet()) {
                    csvWriter.append(tableName);
                    csvWriter.append(",");
                    csvWriter.append(s);
                    csvWriter.append(",");
                    csvWriter.append(colNameType.get(s));
                    csvWriter.append(",");
                    csvWriter.append(clusteringKey.equals(s) ? "true" : "false");
                    csvWriter.append(",");
                    csvWriter.append("false");
                    csvWriter.append(",");
                    csvWriter.append(colNameMin.get(s));
                    csvWriter.append(",");
                    csvWriter.append(colNameMax.get(s));
                    csvWriter.append("\n");
                }
                csvWriter.flush();
                csvWriter.close();

            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        else
            throw new DBAppException("Table name exists before");

    }

    private boolean tableExists(String tableName) {
        try {
            BufferedReader br = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
            String current = br.readLine();
            while (current != null) {
                String[] line = current.split(",");
                if (line[0].equals(tableName)) {
                    return true;
                }
                current = br.readLine();
            }
            br.close();
        } catch (FileNotFoundException e) {
            System.out.println("File is not right :(");
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    @Override
    public void createIndex(String tableName, String[] columnNames) throws DBAppException {

    }

    @Override
    public void insertIntoTable(String tableName, Hashtable<String, Object> colNameValue) throws DBAppException {
        if (tableExists(tableName)) {
            Hashtable<String, String> colType = new Hashtable();
            Hashtable<String, String> colmin = new Hashtable();
            Hashtable<String, String> colmax = new Hashtable();
            String primaryKey = "";

            try {
                BufferedReader br = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
                String current = br.readLine();
                while (current != null) {
                    String[] line = current.split(",");
                    if (line[0].equals(tableName)) {
                        do {
                            if(line[3].equals(true)){
                                primaryKey=line[1];
                            }
                            colType.put(line[1], line[2]);
                            colmin.put(line[1], line[6]);
                            colmax.put(line[1], line[7]);
                            current = br.readLine();
                            if (current != null) {
                                line = current.split(",");
                            }
                        } while (current != null && line[0].equals(tableName));
                        break;
                    }
                    current = br.readLine();
                }
                br.close();

            } catch (FileNotFoundException e) {
                System.out.println("File is not right :(");
            } catch (IOException e) {
                e.printStackTrace();
            }
            if(!colNameValue.keySet().contains(primaryKey)){
                throw new DBAppException("Primary Key Should Have A Value");
            }
            for(String columnName : colNameValue.keySet()){
                Object value =colNameValue.get(columnName);
                if(colType.get(columnName)==null){
                   throw new DBAppException("Column Does not Exist");
                }
                Class type = value.getClass();
                if(!(type.getName().equals(colType.get(columnName)))){
                    throw new DBAppException("Type Miss-match of Column: " + columnName);
                }
                Object min, max ;
               try {
                   Constructor constr = type.getConstructor(String.class);
                    min = constr.newInstance(colmin.get(columnName));
                    max = constr.newInstance(colmax.get(columnName));
               }
               catch(Exception e){

               }
               //if(value.compareTo())
            }

        }else{
            throw new DBAppException("Table Does Not Exist");
        }
    }

    @Override
    public void updateTable(String tableName, String clusteringKeyValue, Hashtable<String, Object> columnNameValue) throws DBAppException {

    }

    @Override
    public void deleteFromTable(String tableName, Hashtable<String, Object> columnNameValue) throws DBAppException {

    }

    @Override
    public Iterator selectFromTable(SQLTerm[] sqlTerms, String[] arrayOperators) throws DBAppException {
        return null;
    }

}