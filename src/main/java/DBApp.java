import java.io.*;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Properties;

public class DBApp implements DBAppInterface{
    static final int N= 200;
    HashMap<String,HashMap<String, String>>tableData = new HashMap<String,HashMap<String, String>>();
    @Override
    public void   init() {
//        try {
//            BufferedReader br = new BufferedReader(new FileReader("resources/metadata.csv"));
//            String current = br.readLine();
//            while (current != null) {
//                String[] line = current.split(",");
//            }
//            br.close();
//
//        } catch (FileNotFoundException e) {
//            System.out.println("File is not right :(");
//        } catch (IOException e) {
//            e.printStackTrace();
//        }


        try {
            FileWriter csvWriter  = new FileWriter("resources/metadata.csv");
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
        try {
            FileWriter csvWriter  = new FileWriter("resources/metadata.csv");
            for(String s : colNameType.keySet()){
                csvWriter.append(tableName);
                csvWriter.append(",");
                csvWriter.append(s);
                csvWriter.append(",");
                csvWriter.append(colNameType.get(s));
                csvWriter.append(",");
                csvWriter.append(clusteringKey.equals(s) ? "true":"false");
                csvWriter.append(",");
                csvWriter.append("free");
                csvWriter.append(",");
                csvWriter.append(colNameMin.get(s));
                csvWriter.append(",");
                csvWriter.append(colNameMax.get(s));
                csvWriter.append("\n");
                csvWriter.flush();
                csvWriter.close();
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Override
    public void createIndex(String tableName, String[] columnNames) throws DBAppException {

    }

    @Override
    public void insertIntoTable(String tableName, Hashtable<String, Object> colNameValue) throws DBAppException {

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
