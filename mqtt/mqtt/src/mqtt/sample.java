/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package mqtt;

/**
 *
 * @author grostirolla
 */
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

public class sample {

    public static void main(String[] args) {

        String topic        = "#";
        int qos             = 2;
        String broker       = "tcp://10.210.3.90:1883";
        String clientId     = "middleware-crazy";
        MemoryPersistence persistence = new MemoryPersistence();

        try {
            MqttClient clientconn = new MqttClient(broker, clientId, persistence);
            MqttConnectOptions connOpts = new MqttConnectOptions();
            connOpts.setCleanSession(true);
            clientconn.setCallback(new SubscribeCallback(clientconn));
            System.out.println("Connecting to broker: "+broker);
            clientconn.connect(connOpts);
            System.out.println("Connected");
            clientconn.subscribe(topic, qos);
            
            
        } catch(MqttException me) {
            System.out.println("reason "+me.getReasonCode());
            System.out.println("msg "+me.getMessage());
            System.out.println("loc "+me.getLocalizedMessage());
            System.out.println("cause "+me.getCause());
            System.out.println("excep "+me);
            me.printStackTrace();
        }
    }
}