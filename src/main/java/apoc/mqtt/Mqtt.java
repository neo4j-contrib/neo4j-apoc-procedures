package apoc.mqtt;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.stream.Stream;
import java.util.LinkedList;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Procedure;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;

import apoc.result.MapResult;
import apoc.util.Util;
import apoc.result.VirtualNode;
import apoc.result.VirtualRelationship;
import apoc.result.VirtualPathResult;
import org.neo4j.graphdb.RelationshipType;
import apoc.result.GraphResult;

/**
 * @author bz
 * @since 29.05.16
 *
 * https://www.eclipse.org/paho/clients/java/#
 * https://www.eclipse.org/paho/files/javadoc/index.html
 *
 */
public class Mqtt {

    public static final MapProcess mqttBrokersMap = new MapProcess();

    @Context
    public Log log;

    @Context
    public GraphDatabaseService db;

    // ----------------------------------------------------------------------------------
    // list
    // ----------------------------------------------------------------------------------
    @UserFunction
    @Description("RETURN apoc.mqtt.listBrokers()")
    public List< Map<String, Object>> listBrokers() {
        log.debug("apoc.mqtt.listBrokers request");
        List< Map<String, Object>> brokerList = mqttBrokersMap.getListFromMapAllClean();
        log.debug("apoc.mqtt.listBrokers: " + brokerList.toString());
        return brokerList;
    }

    @UserFunction
    @Description("RETURN apoc.mqtt.listSubscriptions()")
    public List< Map<String, Object>> listSubscriptions(
            @Name(value = "brokerName", defaultValue = "all") String brokerName
    ) {
        log.debug("apoc.mqtt.listSubscriptions request for: " + brokerName);

        List<Map<String, Object>> subscribeList = new ArrayList<Map<String, Object>>();

        for (int i = 0; i < mqttBrokersMap.getListFromMapAllClean().size(); i++) {
            // --- get broker
            Map<String, Object> broker = mqttBrokersMap.getListFromMapAllClean().get(i);
            log.debug("apoc.mqtt.listSubscriptions broker: " + broker.toString());

            // --- get subscriptions
            Map<String, Object> brokerSubscriptions = (Map<String, Object>) broker.get("subscribeList");
            for (Map.Entry<String, Object> entry : brokerSubscriptions.entrySet()) {
                String key = entry.getKey();
                Object value = entry.getValue();

                Map<String, Object> subscriptionMap = new HashMap<String, Object>();
                subscriptionMap.put("name", broker.get("name") + "-" + key);
                subscriptionMap.put("type", "MqttSubscription");
                subscriptionMap.put("mqttBrokerName", broker.get("name"));
                subscriptionMap.put("topic", key);
                subscriptionMap.put("cypher", value);
                log.debug("apoc.mqtt.listSubscriptions broker subscription " + brokerName + ": " + subscriptionMap.toString());
                if (brokerName.equals("all")) {
                    subscribeList.add(subscriptionMap);
                } else if (broker.get("name").equals(brokerName)) {
                    subscribeList.add(subscriptionMap);
                } else {
                    log.debug("apoc.mqtt.listSubscriptions ignoring : " + brokerName + "  " + broker.get("name"));
                }

            }
        }
        log.debug("apoc.mqtt.listSubscriptions for " + brokerName + ": " + subscribeList.toString());
        return subscribeList;
    }

    @UserFunction
    @Description("RETURN apoc.mqtt.listBrokersAsVnode()")
    public List<Node> listBrokersAsVnode() {
        log.debug("apoc.mqtt.listBrokersAsVnode request");
        List< Map<String, Object>> brokerList = mqttBrokersMap.getListFromMapAllClean();

        // --- vNode list
        List<Node> listNodes = new ArrayList<Node>();
        for (int i = 0; i < brokerList.size(); i++) {
            // --- vNode labels
            List<String> vnodeLabels = new ArrayList<String>();
            vnodeLabels.add("MqttBroker");
            // --- vNode properties
            Map<String, Object> vNodeProps = brokerList.get(i);
            vNodeProps.put("type", "MqttBroker");
            listNodes.add(new VirtualNode(Util.labels(vnodeLabels), vNodeProps, db));
        }
        log.debug("apoc.mqtt.listBrokersAsVnode: " + listNodes.toString());
        return listNodes;
    }

    @UserFunction
    @Description("RETURN apoc.mqtt.listSubscriptionsAsVnode()")
    public List<Node> listSubscriptionsAsVnode(
            @Name(value = "brokerName", defaultValue = "all") String brokerName
    ) {
        log.debug("apoc.mqtt.listSubscriptionsAsVnode request for: " + brokerName);
        // --- vNode list
        List<Node> listNodes = new ArrayList<Node>();

        for (int i = 0; i < mqttBrokersMap.getListFromMapAllClean().size(); i++) {

            // --- get broker
            Map<String, Object> broker = mqttBrokersMap.getListFromMapAllClean().get(i);
            Map<String, Object> subscribeList = (Map<String, Object>) broker.get("subscribeList");

            // --- get subscriptions
            for (Map.Entry<String, Object> entry : subscribeList.entrySet()) {
                String key = entry.getKey();
                Object value = entry.getValue();

                // --- vNode labels
                List<String> vnodeLabels = new ArrayList<String>();
                vnodeLabels.add("MqttSubscription");

                // --- vNode properties
                Map<String, Object> vNodeProps = new HashMap<String, Object>();
                vNodeProps.put("name", broker.get("name") + "-" + key);
                vNodeProps.put("type", "MqttSubscription");
                vNodeProps.put("mqttBrokerName", broker.get("name"));
                vNodeProps.put("topic", key);
                vNodeProps.put("cypher", value);
                log.debug("apoc.mqtt.listSubscriptions broker subscription " + brokerName + ": " + vNodeProps.toString());

                // --- create vNode
                Node mqttBrokerSubscription = new VirtualNode(Util.labels(vnodeLabels), vNodeProps, db);

                // --- filter nodes
                if (brokerName.equals("all")) {
                    listNodes.add(mqttBrokerSubscription);
                } else if (broker.get("name").equals(brokerName)) {
                    listNodes.add(mqttBrokerSubscription);
                } else {
                    log.debug("apoc.mqtt.listSubscriptionsAsVnode ignoring : " + brokerName + "  " + broker.get("name"));
                }
            }
        }
        log.debug("apoc.mqtt.listSubscriptionsAsVnode for " + brokerName + ": " + listNodes.toString());
        return listNodes;
    }

    @UserFunction
    @Description("RETURN apoc.mqtt.listBrokersAsVgraph()")
    public List<Relationship> listBrokersAsVgraph() {  // List<VirtualPathResult> listBrokersAsVgraph()    Stream<GraphResult> listBrokersAsVgraph()
        log.debug("apoc.mqtt.listBrokersAsVgraph: " + mqttBrokersMap.getListFromMapAllClean().toString());

        // --- vNode list
        List<Node> listNodes = new ArrayList<Node>(); // new LinkedList<>(); //
        List<Relationship> listRelationships = new ArrayList<Relationship>();// new LinkedList<>(); //
        List<VirtualPathResult> listPaths = new ArrayList<VirtualPathResult>();

        for (int i = 0; i < mqttBrokersMap.getListFromMapAllClean().size(); i++) {
            // --- vNode labels
            List<String> vnodeLabels = new ArrayList<String>();
            vnodeLabels.add("MqttBroker");
            // --- vNode properties
            Map<String, Object> vNodeProps = mqttBrokersMap.getListFromMapAllClean().get(i);
            Node mqttBroker = new VirtualNode(Util.labels(vnodeLabels), vNodeProps, db);
            listNodes.add(mqttBroker);

            Map<String, Object> subscribeList = (Map<String, Object>) vNodeProps.get("subscribeList");

            for (Map.Entry<String, Object> entry : subscribeList.entrySet()) {
                String key = entry.getKey();
                Object value = entry.getValue();

                // --- vNode labels
                List<String> vnodeLabels2 = new ArrayList<String>();
                vnodeLabels2.add("MqttSubscription");
                Map<String, Object> vNodeProps2 = new HashMap<String, Object>();
                vNodeProps2.put("mqttBrokerName", vNodeProps.get("name"));
                vNodeProps2.put("topic", key);
                vNodeProps2.put("cypher", value);
                log.info("apoc.mqtt.listBrokersAsVgraph: " + mqttBroker.toString());

                Node mqttBrokerSubscription = new VirtualNode(Util.labels(vnodeLabels2), vNodeProps2, db);
                listNodes.add(mqttBrokerSubscription);

                VirtualRelationship relation = new VirtualRelationship(mqttBroker, mqttBrokerSubscription, RelationshipType.withName("SUBSCRIPTION"));

                listRelationships.add(relation);
                log.info("apoc.mqtt.listBrokersAsVgraph \n\n---mqttBroker:\n" + mqttBroker.toString() + "\n\n---relation\n" + relation.toString() + "\n\n---mqttBrokerSubscription\n" + mqttBrokerSubscription.toString());

                VirtualPathResult pathResult = new VirtualPathResult(mqttBroker, relation, mqttBrokerSubscription);

                listPaths.add(pathResult);
                //System.out.println(pair.getKey() + " = " + pair.getValue());

            }

            //System.out.println(cronMap.getListFromMapAllClean().get(i));
        }
        System.out.println(listPaths);
        GraphResult graphResult = new GraphResult(listNodes, listRelationships);
        //return Stream.of( graphResult );
        //return Stream.of(graphResult).map(GraphResult::new);
        return listRelationships; //System.out.println(cronMap.getListFromMapAllClean().get(i));; //new VirtualNode(Util.labels(labelNames), props, db);
        //return Stream.of(listPaths).map(VirtualPathResult::new);
    }

    // ----------------------------------------------------------------------------------
    // add
    // ----------------------------------------------------------------------------------
    @UserFunction
    @Description("RETURN apoc.mqtt.addBroker('mqttBrokerName', {serverURI:'tcp://iot.eclipse.org:1883' ,clientId:'neo4jTestMqttClient01'  })")
    public Map<String, Object> addBroker(
            @Name("name") String name,
            @Name("mqtt") Map<String, Object> mqttConnectOptions
    ) {
        log.debug("apoc.mqtt.addBroker: " + name + " " + mqttConnectOptions.toString());

        // --- check if exists
        Map<String, Object> mqttBroker = mqttBrokersMap.getMapElementByName(name);
        if (!(mqttBroker == null)) {
            Map<String, Object> errorMessage = new HashMap<String, Object>();
            errorMessage.put("error", "Failed to Connect MqTT Broker");
            errorMessage.put("broker name", name);
            errorMessage.put("broker uri", mqttConnectOptions.get("serverURI"));
            errorMessage.put("err message", "Broker with this name exists!");
            log.error("apoc.mqtt.addBroker - error: " + name + " " + mqttConnectOptions.get("serverURI") + " " + mqttConnectOptions.get("clientId") + " " + errorMessage);
            return errorMessage;
        }

        /*

MqttClient(java.lang.String serverURI, java.lang.String clientId, MqttClientPersistence persistence)
Create an MqttClient that can be used to communicate with an MQTT server.
void	setAutomaticReconnect(boolean automaticReconnect)
Sets whether the client will automatically attempt to reconnect to the server if the connection is lost.
void	setCleanSession(boolean cleanSession)
Sets whether the client and server should remember state across restarts and reconnects.
void	setConnectionTimeout(int connectionTimeout)
Sets the connection timeout value.
void	setKeepAliveInterval(int keepAliveInterval)
Sets the "keep alive" interval.
void	setMaxInflight(int maxInflight)
Sets the "max inflight".
void	setMqttVersion(int MqttVersion)
Sets the MQTT version.
void	setPassword(char[] password)
Sets the password to use for the connection.
void	setServerURIs(java.lang.String[] array)
Set a list of one or more serverURIs the client may connect to.
void	setSocketFactory(javax.net.SocketFactory socketFactory)
Sets the SocketFactory to use.
void	setSSLProperties(java.util.Properties props)
Sets the SSL properties for the connection.
void	setUserName(java.lang.String userName)
Sets the user name to use for the connection.
void	setWill(MqttTopic topic, byte[] payload, int qos, boolean retained)
Sets the "Last Will and Testament" (LWT) for the connection.
void	setWill(java.lang.String topic, byte[] payload, int qos, boolean retained)
Sets the "Last Will and Testament" (LWT) for the connection.



        public void publish(java.lang.String topic,
                    byte[] payload,
                    int qos,
                    boolean retained)
             throws MqttException,
                    MqttPersistenceException
Description copied from interface: IMqttClient
Publishes a message to a topic on the server and return once it is delivered.
This is a convenience method, which will create a new MqttMessage object with a byte array payload and the specified QoS, and then publish it. All other values in the message will be set to the defaults.

Specified by:
publish in interface IMqttClient
Parameters:
topic - to deliver the message to, for example "finance/stock/ibm".
payload - the byte array to use as the payload
qos - the Quality of Service to deliver the message at. Valid values are 0, 1 or 2.
retained - whether or not this message should be retained by the server.
Throws:
MqttPersistenceException - when a problem with storing the message
MqttException - for other errors encountered while publishing the message. For instance client not connected.
See Also:
IMqttClient.publish(String, MqttMessage), MqttMessage.setQos(int), MqttMessage.setRetained(boolean)

         */
        mqttConnectOptions.put("name", (String) name);
        // --- set defaults for mqtt connection
        // --- serverURI
        if (!mqttConnectOptions.containsKey("serverURI")) {
            mqttConnectOptions.put("serverURI", "tcp://localhost:1883");
        }
        // --- clientId
        if (!mqttConnectOptions.containsKey("clientId")) {
            mqttConnectOptions.put("clientId", name + "Client" + new Random().nextInt());
        }
        // --- qos
        if (!mqttConnectOptions.containsKey("qos")) {
            mqttConnectOptions.put("qos", (int) 0);
        }
        log.debug("apoc.mqtt.addBroker  - mqttConnectOptions: " + mqttConnectOptions.toString());
        // --- automaticReconnec
        if (!mqttConnectOptions.containsKey("automaticReconnect")) {
            mqttConnectOptions.put("automaticReconnect", (Boolean) true);
        }
        // --- cleanSession
        if (!mqttConnectOptions.containsKey("cleanSession")) {
            mqttConnectOptions.put("cleanSession", (Boolean) false);
        }
        // --- connectionTimeout
        if (!mqttConnectOptions.containsKey("connectionTimeout")) {
            mqttConnectOptions.put("connectionTimeout", (int) 1);
        }
        log.debug("apoc.mqtt.addBroker  - mqttConnectOptions: " + mqttConnectOptions.toString());

        // ---
        try {
            // --- register boker
            MqttClientNeo mqttBrokerNeo4jClient = new MqttClientNeo(mqttConnectOptions);

            Map<String, Object> subscribeList = new HashMap<String, Object>();

            log.debug("apoc.mqtt.addBroker -  connect ok: " + name + " " + mqttConnectOptions.get("serverURI") + " " + mqttConnectOptions.get("clientId"));

            // --- store broker info
            mqttConnectOptions.put("connected", true);
            mqttConnectOptions.put("messageSendOk", 0);
            mqttConnectOptions.put("messageSendError", 0);
            mqttConnectOptions.put("messageSubscribeOk", 0);
            mqttConnectOptions.put("messageSubscribeError", 0);
            mqttConnectOptions.put("messageSubscribeReceived", 0);
            mqttConnectOptions.put("mqttBrokerNeo4jClient", mqttBrokerNeo4jClient);
            mqttConnectOptions.put("subscribeList", subscribeList);

            mqttBrokersMap.addToMap(name, mqttConnectOptions);

            log.info("apoc.mqtt.addBroker - ok: " + name + " " + mqttConnectOptions.get("serverURI") + " " + mqttConnectOptions.get("clientId"));
            return mqttBrokersMap.getMapElementByNameClean(name);
        } catch (Exception ex) {
            Map<String, Object> errorMessage = new HashMap<String, Object>();
            errorMessage.put("error", "Failed to Connect MqTT Broker");
            errorMessage.put("broker name", name);
            errorMessage.put("broker uri", mqttConnectOptions.get("serverURI"));
            errorMessage.put("err message", ex.getMessage());
            log.error("apoc.mqtt.addBroker - error: " + name + " " + mqttConnectOptions.get("serverURI") + " " + mqttConnectOptions.get("clientId") + " " + errorMessage);
            return errorMessage;
        }
    }

    // ----------------------------------------------------------------------------------
    // delete
    // ----------------------------------------------------------------------------------
    @UserFunction
    @Description("RETURN apoc.mqtt.deleteBroker('mqttBrokerName')")
    public Map<String, Object> deleteBroker(
            @Name("name") String name
    ) {
        // --- delete broker if exists
        log.debug("apoc.mqtt -  deleteBroker try: " + name);
        Map<String, Object> mqttBroker = mqttBrokersMap.getMapElementByName(name);
        if (!(mqttBroker == null)) {
            if (!(mqttBroker.get("mqttBrokerNeo4jClient") == null)) {
                MqttClientNeo mqttBrokerNeo4jClient = (MqttClientNeo) mqttBroker.get("mqttBrokerNeo4jClient");
                mqttBrokerNeo4jClient.unsubscribeAll();
                mqttBrokerNeo4jClient.disconnect();
                mqttBrokersMap.removeFromMap(name);
                log.info("apoc.mqtt - deleteBroker: " + name);
            }
        }
        return null;
    }

    // ----------------------------------------------------------------------------------
    // publish
    // ----------------------------------------------------------------------------------
    @Procedure(mode = Mode.WRITE)
    @Description("CALL apoc.mqtt.publish('mqttBrokerName', 'mqtt/topic/path', {message:123})")
    public Stream<MapResult> publish(
            @Name("name") String name,
            @Name("topic") String topic,
            @Name("message") Object message
    ) {
        // --- get broker
        log.debug("apoc.mqtt.publish request: " + name + " " + topic + " " + message);
        Map<String, Object> mqttBroker = mqttBrokersMap.getMapElementByName(name);
        if (mqttBroker == null) {
            Map<String, Object> errorMessage = new HashMap<String, Object>();
            errorMessage.put("error", "MqTT Publish Failed");
            errorMessage.put("broker name", name);
            errorMessage.put("broker topic", topic);
            errorMessage.put("broker message", message);
            errorMessage.put("err message", "Failed to find MqTT Broker - Check Connection");
            log.debug("apoc.mqtt -  publish failed: " + errorMessage);
            return Stream.of(errorMessage).map(MapResult::new);

        } else {

            log.debug("apoc.mqtt - mqttBroker " + mqttBroker);
            MqttClientNeo mqttBrokerNeo4jClient = (MqttClientNeo) mqttBroker.get("mqttBrokerNeo4jClient");
            log.debug("apoc.mqtt - mqttBrokerNeo4jClient " + mqttBrokerNeo4jClient);
            // --- send message
            String mqttMesageString = "";
            ObjectMapper mapper = new ObjectMapper();
            Map<String, Object> jsonParams = new HashMap<String, Object>();

            try {
                if (message instanceof Map) {
                    jsonParams = (Map<String, Object>) message;
                    mqttMesageString = mapper.writeValueAsString(jsonParams).toString();
                } else if (message instanceof Node) {
                    //jsonParams = (Map<String, Object>) ((Node) message).getAllProperties();
                    jsonParams.put("id", ((Node) message).getId());
                    jsonParams.put("labels", ((Node) ((List<String>) message)).getLabels());
                    jsonParams.put("properties", (Map<String, Object>) ((Node) message).getAllProperties());
                    mqttMesageString = mapper.writeValueAsString(jsonParams).toString();
                } else if (message instanceof Relationship) {
                    //jsonParams = (Map<String, Object>) ((Relationship) message).getAllProperties();
                    jsonParams.put("id", ((Relationship) message).getId());
                    jsonParams.put("startNodeId", ((Relationship) message).getStartNodeId());
                    jsonParams.put("endNodeId", ((Relationship) message).getEndNodeId());
                    jsonParams.put("type", ((Relationship) ((List<String>) message)).getType());
                    jsonParams.put("properties", (Map<String, Object>) ((Relationship) message).getAllProperties());
                    mqttMesageString = mapper.writeValueAsString(jsonParams).toString();
                } else if (message instanceof String) {
                    mqttMesageString = (String) message;
                } else {
                    mqttMesageString = message.toString();
                }
                // ---
                mqttBrokerNeo4jClient.publish(topic, mqttMesageString);

                // --- 
                mqttBroker.put("messageSendOk", 1 + (int) mqttBroker.get("messageSendOk"));
                log.debug("apoc.mqtt - publishJson ok:\n" + name + "\n" + topic + "\n" + mqttMesageString);
                return Stream.of(mqttBrokersMap.getMapElementByNameClean(name)).map(MapResult::new);
            } catch (Exception ex) {
                mqttBroker.put("messageSendError", 1 + (int) mqttBroker.get("messageSendError"));
                mqttBroker.put("messageSendErrorMessage", "apoc.mqtt - publish error: " + name + " " + topic + " " + mqttMesageString + " " + ex.getMessage());

                Map<String, Object> errorMessage = new HashMap<String, Object>();
                errorMessage.put("error", "MqTT Publish Error");
                errorMessage.put("broker name", name);
                errorMessage.put("broker topic", topic);
                errorMessage.put("broker message", message);
                errorMessage.put("err message", ex.getMessage());
                log.error("apoc.mqtt -  publish failed: " + errorMessage);
                return Stream.of(errorMessage).map(MapResult::new);
            }

        }
    }

    // ----------------------------------------------------------------------------------
    // subscribe
    // ----------------------------------------------------------------------------------
    @Procedure(mode = Mode.WRITE)
    @Description("CALL apoc.mqtt.subscribe('mqttBrokerName', 'mqtt/topic/path','MERGE (n:mqttTest) ON CREATE SET n.count=1, n.message=$message ON MATCH SET n.count = n.count +1, n.message=$message ') ")
    public Stream<MapResult> subscribe(
            @Name("name") String name,
            @Name("topic") String topic,
            @Name("query") String query
    ) {
        // --- remove subscription if exist
        log.debug("apoc.mqtt -  subscribe unSubscribe: ");
        this.unSubscribe(name, topic);
        log.debug("apoc.mqtt -  subscribe unSubscribe: ");
        // --- get broker
        Map<String, Object> mqttBroker = mqttBrokersMap.getMapElementByName(name);

        if (mqttBroker == null) {
            Map<String, Object> errorMessage = new HashMap<String, Object>();
            errorMessage.put("error", "MqTT Subscribe Failed");
            errorMessage.put("broker name", name);
            errorMessage.put("broker topic", topic);
            errorMessage.put("broker query", query);
            errorMessage.put("err message", "Failed to find MqTT Broker - Check Connection");
            log.debug("apoc.mqtt -  subscribe failed: " + errorMessage);
            return Stream.of(errorMessage).map(MapResult::new);

        } else {
            log.debug("apoc.mqtt -  subscribe mqttBroker ok: ");
            MqttClientNeo mqttBrokerNeo4jClient = (MqttClientNeo) mqttBroker.get("mqttBrokerNeo4jClient");

            try {
                // --- set processor
                ProcessMqttMessage task = new ProcessMqttMessage("json", query);
                // --- subscribe
                mqttBrokerNeo4jClient.subscribe(topic, query, task);
                // --- add to subscription list
                Map<String, Object> subscribeList = (Map<String, Object>) mqttBroker.get("subscribeList");
                subscribeList.put(topic, query);
                mqttBroker.put("messageSubscribeOk", 1 + (int) mqttBroker.get("messageSubscribeOk"));

                log.debug("apoc.mqtt -  subscribe ok: \n" + name + "\n" + topic);
                return Stream.of(mqttBrokersMap.getMapElementByNameClean(name)).map(MapResult::new);
            } catch (Exception ex) {
                mqttBroker.put("messageSubscribeError", 1 + (int) mqttBroker.get("messageSubscribeError"));
                //mqttBroker.put("messageSubscribeErrorMessage", "apoc.mqtt -  subscribe error: " + name + " " + topic + " " + query + " " + ex.toString());
                log.error("apoc.mqtt -  subscribeJson error: \n" + name + "\n" + topic + "\n" + "\n" + ex.toString());
                Map<String, Object> errorMessage = new HashMap<String, Object>();
                errorMessage.put("error", "MqTT Subscribe Failed");
                errorMessage.put("broker name", name);
                errorMessage.put("broker topic", topic);
                errorMessage.put("broker query", query);
                errorMessage.put("err message", ex.getMessage());
                return Stream.of(errorMessage).map(MapResult::new);
            }
        }

    }

    // ----------------------------------------------------------------------------------
    // unsubscribe
    // ----------------------------------------------------------------------------------
    @UserFunction
    @Description("RETURN apoc.mqtt.unSubscribe('mqttBrokerName', 'mqtt/topic/path' )")
    public Object unSubscribe(
            @Name("name") String name,
            @Name("topic") String topic
    ) {
        log.debug("apoc.mqtt - unSubscribeTopic: " + name + " " + topic);
        // --- get broker
        Map<String, Object> mqttBroker = mqttBrokersMap.getMapElementByName(name);
        log.debug("apoc.mqtt - unSubscribeTopic mqttBroker: " + name + " " + topic + " " + mqttBroker + " " + mqttBroker);
        if (mqttBroker != null) {
            log.debug("apoc.mqtt - unSubscribeTopic: " + name + " " + topic);
            MqttClientNeo mqttBrokerNeo4jClient = (MqttClientNeo) mqttBroker.get("mqttBrokerNeo4jClient");
            mqttBrokerNeo4jClient.unsubscribe(topic);

            Map<String, Object> subscribeList = (Map<String, Object>) mqttBroker.get("subscribeList");
            subscribeList.remove(topic);
            log.debug("apoc.mqtt - unSubscribeTopic: " + name + " " + topic);

        } else {
            log.debug("apoc.mqtt - no broker to unSubscribeTopic: " + name + " " + topic);
        }
        return null;
    }

    // ----------------------------------------------------------------------------------
    // util
    // ----------------------------------------------------------------------------------
    /**
     * JSONUtils checkJson = new JSONUtils();
     * System.out.print(checkJson.jsonStringToMap(validJson));
     */
    public final static class JSONUtils {

        private final Gson gson = new Gson();

        private JSONUtils() {
        }

        public Object jsonStringToMap(String jsonInString) {
            try {
                Map<String, Object> retMap = new Gson().fromJson(jsonInString.toString(), new TypeToken<HashMap<String, Object>>() {
                }.getType());
                //gson.fromJson(jsonInString, Object.class);
                return retMap;
            } catch (JsonSyntaxException ex) {
                //System.out.println("This is finally block");
                return null;
            }
        }
    }

    // ----------------------------------------------------------------------------------
    // ProcessMqttMessage
    // ----------------------------------------------------------------------------------
    public class ProcessMqttMessage {

        String processType = "";
        String cypherQuery = "";

        public ProcessMqttMessage(String messageType, String cypherQueryInput) {

            this.processType = messageType;
            this.cypherQuery = cypherQueryInput;
            log.info("apoc.mqtt - ProcessMqttMessage registration: " + this.processType + this.cypherQuery);
        }

        public void run(String topic, String message) {
            log.info("apoc.mqtt - ProcessMqttMessage run:\n" + this.cypherQuery + "\n" + message + "\n" + this.processType);

            Map<String, Object> cypherParams = new HashMap();
            if (this.processType == "json") {
                JSONUtils checkJson = new JSONUtils();
                cypherParams = (Map<String, Object>) checkJson.jsonStringToMap(message);
            } else if (this.processType == "value") {
                cypherParams.put("value", message);
            } else {
                cypherParams = new HashMap();
            }

            log.info("apoc.mqtt - message received: \n" + this.cypherQuery + "\n" + message + "\n" + this.processType + "\n" + cypherParams.toString());
            try (Transaction tx = db.beginTx()) {
                Result dbResult = db.execute(this.cypherQuery, cypherParams);
                log.info("apoc.mqtt - cypherQuery results:\n" + "\n" + dbResult.resultAsString());
                tx.success();
            } catch (Exception ex) {
                log.error("apoc.mqtt - cypherQuery error:\n" + ex.toString());
            }
        }

    }

    // ----------------------------------------------------------------------------------
    // MqttClientNeo
    // ----------------------------------------------------------------------------------
    public class MqttClientNeo {

        int qos = 0;
        public MqttClient neo4jMqttClient;
        Map<String, Object> mapMqttTopicTask = new HashMap<>();

        public MqttClientNeo(Map<String, Object> mqttConnectOptions) throws MqttException {
            log.debug("apoc.mqtt - MqttClientNeo: " + mqttConnectOptions.toString());

            qos = (int) mqttConnectOptions.get("qos");
            log.debug("apoc.mqtt - MqttClientNeo connOpts: ");
            neo4jMqttClient = new MqttClient((String) mqttConnectOptions.get("serverURI"), (String) mqttConnectOptions.get("clientId"), new MemoryPersistence());
            // --- connOpts
            log.debug("apoc.mqtt - MqttClientNeo connOpts: ");
            MqttConnectOptions connOpts = new MqttConnectOptions();
            connOpts.setAutomaticReconnect((Boolean) mqttConnectOptions.get("automaticReconnect"));
            connOpts.setCleanSession((Boolean) mqttConnectOptions.get("cleanSession"));
            connOpts.setConnectionTimeout((int) mqttConnectOptions.get("connectionTimeout"));
            log.debug("apoc.mqtt - MqttClientNeo connOpts: " + connOpts.toString());

            // --- connect
            neo4jMqttClient.connect(connOpts);

            neo4jMqttClient.setCallback(new MqttCallback() {
                public void connectionLost(Throwable cause) {
                    log.debug("apoc.mqtt - connectionLost");
                }

                public void messageArrived(String topic, MqttMessage message) {
                    log.debug("apoc.mqtt - messageArrived " + topic + " " + message.toString());
                    ProcessMqttMessage task = (ProcessMqttMessage) mapMqttTopicTask.get(topic);
                    //log.debug("aaa" + task.toString() + task.cypherQuery + task.processType);
                    task.run(topic, message.toString());
                }

                public void deliveryComplete(IMqttDeliveryToken token) {
                    log.debug("apoc.mqtt - deliveryComplete");
                }
            });

            // --- send connect message
            log.info("apoc.mqtt - connecting to broker: " + mqttConnectOptions.get("serverURI") + " as " + mqttConnectOptions.get("clientId"));

        }

        private void publish(String topic, String content) throws MqttException {
            log.debug("apoc.mqtt - publish" + topic, content);
            MqttMessage message = new MqttMessage(content.getBytes());
            message.setQos(qos);
            String clientId = this.neo4jMqttClient.getClientId();
            String broker = this.neo4jMqttClient.getServerURI();

            this.neo4jMqttClient.publish(topic, message);

            log.debug("apoc.mqtt - publish" + mapMqttTopicTask.toString());

        }

        private void unsubscribeAll() {
            String clientId = this.neo4jMqttClient.getClientId();
            String broker = this.neo4jMqttClient.getServerURI();
            mapMqttTopicTask = null;
            try {
                this.neo4jMqttClient.unsubscribe("#");
                log.info("apoc.mqtt -  unsubscribeAll ok: " + clientId + " " + broker);
            } catch (MqttException ex) {
                log.error("apoc.mqtt -  unsubscribeAll error: " + clientId + " " + broker + " " + ex.toString());
            }
        }

        private void unsubscribe(String topic) {
            String clientId = this.neo4jMqttClient.getClientId();
            String broker = this.neo4jMqttClient.getServerURI();
            mapMqttTopicTask.remove(topic);
            try {
                this.neo4jMqttClient.unsubscribe(topic);
                log.info("apoc.mqtt -  unsubscribe ok: " + topic + " " + clientId + " " + broker);
            } catch (MqttException ex) {
                log.error("apoc.mqtt -  unsubscribe error: " + topic + " " + clientId + " " + broker + " " + ex.toString());
            }
        }

        private void disconnect() {
            String clientId = this.neo4jMqttClient.getClientId();
            String broker = this.neo4jMqttClient.getServerURI();
            mapMqttTopicTask = null;
            try {
                this.neo4jMqttClient.disconnect();
                log.info("apoc.mqtt -  disconnect ok: " + clientId + " " + broker);
            } catch (MqttException ex) {
                log.error("apoc.mqtt -  disconnect error: " + clientId + " " + broker + " " + ex.toString());
            }
        }

        public void subscribe(String topic, String query, ProcessMqttMessage task) throws MqttException {
            String clientId = this.neo4jMqttClient.getClientId();
            String broker = this.neo4jMqttClient.getServerURI();
            log.info("apoc.mqtt - subscribe: " + topic + " " + clientId + " " + broker + " " + query);

            mapMqttTopicTask.put(topic, task);
            this.neo4jMqttClient.subscribe(topic);
        }
    }

    // ----------------------------------------------------------------------------------
    // MqttClientNeo
    // ----------------------------------------------------------------------------------
    public final static class MapProcess {

        public final Map<String, Object> map;
        MapProcess.CleanObject cleanObject = new MapProcess.CleanObject();

        public MapProcess() {
            map = new HashMap();
        }

        public Map<String, Object> getMapAll() {
            return map;
        }

        public Map<String, Object> addToMap(String name, Map<String, Object> mapTmp) {
            map.put(name, mapTmp);
            return mapTmp;
        }

        public void removeFromMap(String name) {
            map.remove(name);
        }

        public Map<String, Object> getMapElementByName(String name) {
            Map<String, Object> mapTmp = (Map) map.get(name);
            return mapTmp;
        }

        public Map<String, Object> getMapElementByNameClean(String name) {
            Map<String, Object> mapTmp = (Map) map.get(name);
            return cleanObject.cleanMap(mapTmp);
        }

        public ArrayList<Map<String, Object>> getListFromMapAll() {
            List<String> mapKeys = new ArrayList(map.keySet());
            ArrayList<Map<String, Object>> listMap = new ArrayList();

            for (int i = 0; i < mapKeys.size(); i++) {
                Map<String, Object> mapTmp = (Map) map.get(mapKeys.get(i));

                listMap.add(mapTmp);
            }
            return listMap;
        }

        public ArrayList<Map<String, Object>> getListFromMapAllClean() {
            List<String> mapKeys = new ArrayList(map.keySet());
            ArrayList<Map<String, Object>> listMap = new ArrayList();

            for (int i = 0; i < mapKeys.size(); i++) {
                Map<String, Object> mapTmp = (Map) map.get(mapKeys.get(i));

                listMap.add(cleanObject.cleanMap(mapTmp));
            }
            return listMap;
        }

        public ArrayList<Node> getListVnodesFromMapAll(String label, GraphDatabaseService db) {

            List<String> mapKeys = new ArrayList(map.keySet());
            ArrayList<Node> listMap = new ArrayList();

            for (int i = 0; i < mapKeys.size(); i++) {
                Map<String, Object> mapTmp = (Map) map.get(mapKeys.get(i));

                List<String> labelNames = new ArrayList();
                labelNames.add(label); // ['Label'];
                Map<String, Object> props = cleanObject.cleanMap(mapTmp);
                props.put("name", mapTmp);
                props.put("type", label);
                listMap.add(new VirtualNode(Util.labels(labelNames), props, db));

            }
            return listMap;
        }

        public ArrayList<Map<String, Object>> getListFromMap() {
            List<String> mapKeys = new ArrayList(map.keySet());
            ArrayList<Map<String, Object>> listMap = new ArrayList();
            for (int i = 0; i < mapKeys.size(); i++) {
                Map<String, Object> mapTmp = (Map) map.get(mapKeys.get(i));

                listMap.add(mapTmp);
            }
            return listMap;
        }

        public class CleanObject {

            public CleanObject() {

            }

            public Map<String, Object> cleanMap(final Map<String, Object> mapInput) {
                final Map<String, Object> mapTmp = new HashMap<String, Object>();
                final List<String> mapKeys = new ArrayList<String>(mapInput.keySet());
                for (int i = 0; i < mapKeys.size(); ++i) {
                    final Object mapObject = mapInput.get(mapKeys.get(i));
                    if (mapObject instanceof String
                            || mapObject instanceof Integer
                            || mapObject instanceof Boolean
                            || mapObject instanceof Float
                            || mapObject instanceof Double
                            || mapObject instanceof Map) {
                        mapTmp.put(mapKeys.get(i), mapObject);
                    }
                }
                return mapTmp;
            }

            public ArrayList<Node> cleanNodeList(final Map<String, Object> mapInput, List<String> labelNames, GraphDatabaseService db) {
                final Map<String, Object> mapTmp = new HashMap<String, Object>();
                final List<String> mapKeys = new ArrayList<String>(mapInput.keySet());
                final ArrayList<Node> nodes = new ArrayList();
                for (int i = 0; i < mapKeys.size(); ++i) {
                    final Object mapObject = mapInput.get(mapKeys.get(i));
                    if (mapObject instanceof String
                            || mapObject instanceof Integer
                            || mapObject instanceof Boolean
                            || mapObject instanceof Float
                            || mapObject instanceof Double
                            || mapObject instanceof Map) {
                        mapTmp.put(mapKeys.get(i), mapObject);

                    }
                    nodes.add(new VirtualNode(Util.labels(labelNames), mapTmp, db));
                }
                return nodes;
            }
        }

    }

}
