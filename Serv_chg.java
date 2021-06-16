/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package serv_chg;


import javaBS.utilBS.Edit_log;

        ;
import java.io.File;
import java.io.IOException;




import org.ini4j.*;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;

 import java.io.File ;
import java.io.FileFilter;
import java.io.FileNotFoundException;
 import java.text.DateFormat ;
 import java.text.SimpleDateFormat ;
 import java.util.Date ;
 import java.util.Calendar ;
 import java.io.FileWriter ;
import java.io.FilenameFilter;
import static java.lang.Thread.sleep;
import java.nio.file.Files;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

 



/**
 *
 * @author fouzi.bensemmane
 */
public class Serv_chg {

    /**
     * @param args the command line arguments
     */
  public static String p_out;
  public static String p_in;
  public static String p_archiv ;
  public static String p_trace ;
  
  
  public static String chaine_conn  ;
  public static String user_conn  ; 
  public static String psw_conn  ;
  
   public static int  rep_chg  ;
     
  public static final String FILENAME = "Serv_chg.ini";
 
  public static  Connection conn = null ; 
  
   public  static  List<String> liste_etat =  new ArrayList<String>();
 
 
  
 
 
   
 public static void main(String[] args) throws IOException, SQLException
    {
       
  
  //      
           List<String> liste_etat ;
         
         
        init_var(null) ;

          
   
  

                long start = System.currentTimeMillis();
                long end = start + 20 * 1000;
  String re = "1" ;
                while ( re=="1" )
                {
                    
                     javaBS.utilBS.Edit_log.create("file_log", "--> Début Scan  "  ) ;
                       
                      try
                      {
                          Scan_IN(null);
                      } 
                      catch(Exception e){
                                Edit_log.create("file_log", "**   Anomalie dans SCAN IN  " + e.getMessage()  );
            
                           } 
                      
                           
                     javaBS.utilBS.Edit_log.create("file_log", "--> Fin  Scan  "  ) ;
                //  Date date = new Date();
                //    System.out.println("Time: " + date.toString());

                    try
                    {
                        sleep(30000);
                    }
                    catch (Exception e)
                    {
                        e.printStackTrace();
                       javaBS.utilBS.Edit_log.create("file_log", "--> Anomalie   Scan  "  + e.getMessage()  ) ;
                    }
                }

            }
     
  
   public static  void init_var(String[] args) throws IOException, FileNotFoundException, SQLException  {
        // Initialisation des varaibles 
     
      try{
            Wini ini = new Wini(new File("Serv_chg.ini"));
           
             p_in      =  ini.get("Chemin", "in")  ;
             p_out     = ini.get("Chemin", "out") ;
             p_archiv  = ini.get("Chemin", "archiv");
             p_trace  = ini.get("Chemin", "trace") ; 
             chaine_conn  = ini.get("Base", "chaine");
             user_conn  = ini.get("Base", "user");
             psw_conn  = ini.get("Base", "psw");
  
     // ***********************************  
     //    Initialation de la liste liste_etat 
     //          qui envoi quoi   
     //      DCP_001  dcp est envoyé par 001
        conn = javaBS.utilBS.Connect.getConnection(chaine_conn,user_conn , psw_conn ) ;
     String  stm =  "select  a.fich_ass || '_' || a.bef_ass n_fichier   from ass_fich_org a ,   ref_tp_fichier  b\n" +
                     "where  a.fich_ass = b.code and  b.code_prg_chg = 1  " ;
                   
         ResultSet rs = null;
      PreparedStatement pst = null;      
      try {   
         pst = conn.prepareStatement(stm);
         pst.execute();
         rs = pst.getResultSet();
      
         while(rs.next()) {
           
              liste_etat.add(rs.getString(1) );
             
               }
      } catch (SQLException e) {
            javaBS.utilBS.Edit_log.create("catsh", e.getMessage()) ;
         e.printStackTrace();
      }  
      conn.close(); conn = null ; 
      System.out.println("  liste etat ");
      System.out.println(liste_etat);
       //*********************** 
        
         
     
                 
       
         Edit_log.create("file_log", "***************************************");
         
         Edit_log.create("file_log", "**   lancement du service de chargement");
         Edit_log.create("file_log", "**   Version  1.0 du 08/03/2021        ");
         Edit_log.create("file_log", "**   ");
              
         Edit_log.create("file_log", "**   chemin in      : " + p_in );
        
         Edit_log.create("file_log", "**   chemin out     : " + p_out );
         Edit_log.create("file_log", "**   chemin archiv  : " + p_archiv );
         Edit_log.create("file_log", "**   chaine conn   : " + chaine_conn );
         Edit_log.create("file_log", "**   user    : " + user_conn   );
       

     
         
        }catch(Exception e){
            Edit_log.create("file_log", "**   Anomalie dans la lecture du fichier INI " + e.getMessage()  );
            
        }  
        
   }
    
 
    
    public static  void Scan_IN(String[] args) throws IOException, FileNotFoundException, SQLException  {
        // TODO code application logic here
     
    /**
     *
     */
      
   
    
        Path result = null;          
        String[] pathnames;

        
        File f = new File( p_in );  
        
        
		FilenameFilter textFilter = new FilenameFilter() {
                    
                   
			public boolean accept(File dir, String name) {
                           
             
             
                     File Xfile = new File(dir + "/" + name ) ;
                     if ( !Xfile.isFile())
                     {
                       //  Is directory     
                        return false;
                     }
                                   
                                    
                            
				String lowercaseName = name.toLowerCase();
                               
                                
                                     
                                 boolean oui = liste_etat.contains(name.substring(0,7));
				if( (lowercaseName.endsWith(".xls")) & (oui)) {
					return true;
				};
                                if ( (lowercaseName.endsWith(".xlsx")) & (oui) 
                                        ) {
					return true;
				} 
                                
                                 if  (lowercaseName.endsWith(".xml")) 
                                         {
					return true;
				} 
                                else
                                
					return false;
				}
                };	
		

        //**************** 
        
      
        pathnames = f.list(textFilter) ;
        
       
        

    
      
      
        for (String file  : pathnames) {
      
            // ****************************************************************************
            //
            
             
     while   ( conn == null )   { 
       
      
        try 
          {
              Edit_log.create("file_log", "**   Connexion  base de donnée "  );
            
            conn = javaBS.utilBS.Connect.getConnection(chaine_conn,user_conn , psw_conn ) ;
            Edit_log.create("file_log", "**   Connexion  base de donnée  Ok"  );
            
          }
         catch (Exception e) {
            javaBS.utilBS.Edit_log.create("file_log", "--> Anomalie probleme de connexion " + e.getMessage()  ) ;
              System.out.print("  probleme de connexion "  + "\t\t"); 
            
         }
        
       
     }
     
        
          
               

            //
            //*************************************************************************
               
               Edit_log.create("file_log", "**             fichier    : " +  p_in  + file  );
              System.out.print(" fichier  :  "  + file  ); 
                            
            
               try {
                   rep_chg = 0 ;
                   switch(file.substring(0,3)) {
                          case "DPC" :
                                   class_chg.meth_chg_DPC.chg_DPC(file,p_in );
                                   break;
                          case  "R20" :
                                   class_chg.meth_chg_R20.chg_R20(file,p_in );
                                   break;
                          case  "SC6" :
                                   class_chg.meth_chg_SC6.chg_SC6(file,p_in );
                                   break;   
                           case  "SDT" :
                                   class_chg.meth_chg_SDT.chg_SDT(file,p_in );
                                   break;            
                            case  "DGR" :
                                   class_chg.meth_chg_DGR.chg_DGR(file,p_in );
                                   break;     
                                   
                              case  "EQL" :
                                   class_chg.meth_chg_EQL.chg_EQL(file,p_in );
                                   break;    
                                   
                                case  "CRP" :
                                   class_chg.meth_chg_CRP.chg_CRP(file,p_in );
                                   break;      
                                   
                                  case  "LQD" :
                                   class_chg.meth_chg_LQD.chg_LQD(file,p_in );
                                   break;       
                                    
                                 case  "RRO" :
                                   class_chg.meth_chg_RRO.chg_RRO(file,p_in );
                                   break;       
                                   
                                  case  "DEE" :
                                   class_chg.meth_chg_DEE.chg_DEE(file,p_in );
                                   break;          
                          default:
                                   Edit_log.create("file_log", "**   Anomalie FICHIER non prise en charge "  );
                                   break;
            
                           }

                   
                    
               }
                catch(Exception e){
              Edit_log.create("file_log", "**   Anomalie dans la lecture du fichier  " + e.getMessage()  );
            
                     }  
            
              class_chg.arch_file.Archiv(file, p_in, p_archiv ) ;
 
                   Edit_log.create("file_log", "");
                   Edit_log.create("file_log", "***************************************");
                   Edit_log.create("file_log", "");
       
                   
                   
                   
  
    }
      try  {  
                conn.close();
                conn =  null ;
          } 
         catch(Exception e){
                   //  System.out.print("  base donnée non connectée  "  + "\t\t"); 
       
             }
    }
   
}    


    

