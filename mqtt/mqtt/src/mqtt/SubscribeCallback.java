/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package mqtt;

import org.eclipse.paho.client.mqttv3.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;


public class SubscribeCallback implements MqttCallback {

    //private static final Logger log = LoggerFactory.getLogger(WildcardSubscriber.class);

    private static final long RECONNECT_INTERVAL = TimeUnit.SECONDS.toMillis(10);

    private static final String JDBC_URL = "jdbc:mysql://10.210.3.90:3306/mqtt?user=root&password=grostirolla";

    private static final String SQL_INSERT = "INSERT INTO `Messages` (`message`,`topic`,`quality_of_service`) VALUES (?,?,?)";


    private final MqttClient mqttClient;

    private PreparedStatement statement;

    public SubscribeCallback(final MqttClient mqttClient) {
        this.mqttClient = mqttClient;

        try {
            Class.forName("com.mysql.jdbc.Driver"); 
            final Connection conn = DriverManager.getConnection(JDBC_URL);
            statement = conn.prepareStatement(SQL_INSERT);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    @Override
    public void connectionLost(Throwable cause) {
        try {
            mqttClient.connect();

        } catch (MqttException e) {
            e.printStackTrace();
            try {
                Thread.sleep(RECONNECT_INTERVAL);
            } catch (InterruptedException e1) {
                Thread.currentThread().interrupt();
            }
            connectionLost(e);
        }
    }

    @Override
    public void messageArrived(String string, MqttMessage mm) throws Exception {
         
        try {
            statement.setString(1,  mm.toString());
            statement.setString(2,string);
            statement.setInt(3, mm.getQos());

            System.out.println("mm1: "+mm.toString());            
            System.out.println("mm1: "+mm.getPayload());
            System.out.println("mm1: "+mm.getQos());
            System.out.println("mm1: "+mm.isDuplicate());
            System.out.println("mm1: "+mm.isRetained());
            System.out.println("str"+string);

            //Ok, let's persist to the database
            statement.executeUpdate();
        } catch (SQLException e) {
            //log.error("Error while inserting", e);
            e.printStackTrace();
        }
    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken imdt) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
}
