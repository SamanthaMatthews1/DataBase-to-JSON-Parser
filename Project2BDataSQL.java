package project2bdatasql;

import java.sql.*;
import java.util.ArrayList;
import org.json.simple.*;
/**
 *
 * @author Sam
 */
public class Project2BDataSQL {

    public static void main(String[] args){
        
        JSONArray array = getJSONData();
        System.out.println("Getting Results...");
        System.out.println(array);
        
        
    }
    
    public static JSONArray getJSONData(){
        
        
        Connection conn = null;
        PreparedStatement pstSelect = null, pstUpdate = null;
        ResultSet resultset = null;
        ResultSetMetaData metadata = null;
        
        JSONArray records = new JSONArray();
        
        String query;
        String value;
        
        ArrayList<String> key = new ArrayList<>();
        
        boolean results;
        int result = 0;
        int columns = 0;
        
         try{
            
            /* Identify the Server */
            
            String server = ("jdbc:mysql://localhost/p2_test");
            String username = "root";
            String password = "CS488";
            System.out.println("Connecting to " + server + "...");
            
            /* Load the MySQL JDBC Driver */
            
            Class.forName("com.mysql.jdbc.Driver").newInstance();
            
            /* Open Connection */

            conn = DriverManager.getConnection(server, username, password);

            /* Test Connection */
            
            if (conn.isValid(0)) {
                
                /* Connection Open! */
                
                System.out.println("Connected Successfully!");
                
                query = "SELECT * FROM people";
                
                pstSelect = conn.prepareStatement(query);
                
                results = pstSelect.execute();
                
                while( results || pstSelect.getUpdateCount() != -1){
                    
                    if(results){
                        
                        resultset = pstSelect.getResultSet();
                        metadata = resultset.getMetaData();
                        columns = metadata.getColumnCount();
                        
                        
                        for(int i = 2; i <= columns; i++){
                            
                            key.add(metadata.getColumnLabel(i));
                            
                        }
                    
                        while(resultset.next()){
                            
                            JSONObject object = new JSONObject();
                            
                           for(int i = 2; i <= columns; i++){
                               
                               JSONObject jsonObject = new JSONObject();
                               value = resultset.getString(i);
                               
                               if(resultset.wasNull()){
                                   
                                   jsonObject.put(key.get(i - 2), "NULL");
                                   jsonObject.toJSONString();
                                   
                               }
                               else{
                                   
                                   jsonObject.put(key.get(i-2), value);
                                   jsonObject.toString();
                                   
                               }
                               
                               object.putAll(jsonObject);
                           }
                            
                           records.add(object);
                        }
                    }
                    else{
                        
                        result = pstSelect.getUpdateCount();
                        
                        if(result == -1){
                            break;
                            
                        }
                    }
                    results = pstSelect.getMoreResults();
                }        
        }
        
            
        conn.close();
      
                  
        }
         
         catch (Exception e) {
            
             System.err.println(e.toString());
             
         }
         finally {
            
            if (resultset != null) { try { resultset.close(); resultset = null; } catch (Exception e) {} }
            
            if (pstSelect != null) { try { pstSelect.close(); pstSelect = null; } catch (Exception e) {} }
            
            if (pstUpdate != null) { try { pstUpdate.close(); pstUpdate = null; } catch (Exception e) {} }
            
        }
         
         return records;
    
}
}