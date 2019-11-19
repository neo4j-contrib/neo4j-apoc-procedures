package apoc.mqtt;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Stream;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.eclipse.paho.client.mqttv3.IMqttMessageListener;

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
import org.neo4j.graphdb.RelationshipType;

import apoc.result.MapResult;
import apoc.util.Util;
import apoc.result.VirtualNode;
import static apoc.util.Util.labelString;
import apoc.result.VirtualRelationship;
import apoc.result.VirtualPathResult;
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
    // list brokers
    // ----------------------------------------------------------------------------------
    @UserFunction
    @Description("RETURN apoc.mqtt.listBrokers()")
    public List< Map<String, Object>> listBrokers() {
        List< Map<String, Object>> brokerList = mqttBrokersMap.getListFromMapAllClean();
        log.debug("apoc.mqtt.listBrokers: " + brokerList.toString());
        return brokerList;
    }

    @UserFunction
    @Description("RETURN apoc.mqtt.listBrokersAsVnode()")
    public List<Node> listBrokersAsVnode() {
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

    // ----------------------------------------------------------------------------------
    // list subscriptions
    // ----------------------------------------------------------------------------------
    @UserFunction
    @Description("RETURN apoc.mqtt.listSubscriptions('optional mqttBrokerId default to all')")
    public List< Map<String, Object>> listSubscriptions(
            @Name(value = "mqttBrokerId", defaultValue = "all") String mqttBrokerId
    ) {
        // --- subscriptions list
        List<Map<String, Object>> subscribeList = new ArrayList<Map<String, Object>>();
        // --- loop brokers
        for (int i = 0; i < mqttBrokersMap.getListFromMapAllClean().size(); i++) {
            // --- get broker
            Map<String, Object> broker = mqttBrokersMap.getListFromMapAllClean().get(i);
            // --- return requested
            if ((mqttBrokerId.equals("all")) || (broker.get("name").equals(mqttBrokerId))) {
                // --- get subscriptions
                Map<String, Object> brokerSubscriptions = (Map<String, Object>) broker.get("subscribeList");
                for (Map.Entry<String, Object> entry : brokerSubscriptions.entrySet()) {
                    String topic = entry.getKey();
                    Object subscribeOptions = entry.getValue();

                    Map<String, Object> subscriptionMap = new HashMap<String, Object>();
                    subscriptionMap.put("mqttBrokerId", broker.get("mqttBrokerId") + "-" + topic);
                    subscriptionMap.put("type", "MqttSubscription");
                    subscriptionMap.put("mqttBrokerId", broker.get("mqttBrokerId"));
                    subscriptionMap.put("topic", topic);
                    subscriptionMap.put("subscribeOptions", subscribeOptions);
                    subscribeList.add(subscriptionMap);
                }
            } else {
                log.debug("apoc.mqtt.listSubscriptions ignoring : " + mqttBrokerId + "  " + broker.get("name"));
            }
        }
        log.debug("apoc.mqtt.listSubscriptions for " + mqttBrokerId + ": " + subscribeList.toString());
        return subscribeList;
    }

    @UserFunction
    @Description("RETURN apoc.mqtt.listSubscriptionsAsVnode('optional mqttBrokerId default to all')")
    public List<Node> listSubscriptionsAsVnode(
            @Name(value = "mqttBrokerId", defaultValue = "all") String mqttBrokerId
    ) {
        // --- vNode list
        List<Node> listNodes = new ArrayList<Node>();
        // --- loop brokers
        for (int i = 0; i < mqttBrokersMap.getListFromMapAllClean().size(); i++) {

            // --- get broker
            Map<String, Object> broker = mqttBrokersMap.getListFromMapAllClean().get(i);
            // --- return requested
            if ((mqttBrokerId.equals("all")) || (broker.get("name").equals(mqttBrokerId))) {
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
                    // --- create vNode
                    Node mqttBrokerSubscription = new VirtualNode(Util.labels(vnodeLabels), vNodeProps, db);
                    listNodes.add(mqttBrokerSubscription);
                }
            } else {
                log.debug("apoc.mqtt.listSubscriptionsAsVnode ignoring : " + mqttBrokerId + "  " + broker.get("mqttBrokerId"));
            }
        }
        log.debug("apoc.mqtt.listSubscriptionsAsVnode for " + mqttBrokerId + ": " + listNodes.toString());
        return listNodes;
    }

    /**
     * @UserFunction @Description("RETURN apoc.mqtt.listBrokersAsVgraph()")
     * public List<Relationship> listBrokersAsVgraph() { //
     * List<VirtualPathResult> listBrokersAsVgraph() Stream<GraphResult>
     * listBrokersAsVgraph() log.debug("apoc.mqtt.listBrokersAsVgraph: " +
     * mqttBrokersMap.getListFromMapAllClean().toString());
     *
     * // --- vNode list List<Node> listNodes = new ArrayList<Node>(); // new
     * LinkedList<>(); // List<Relationship> listRelationships = new
     * ArrayList<Relationship>();// new LinkedList<>(); //
     * List<VirtualPathResult> listPaths = new ArrayList<VirtualPathResult>();
     *
     * for (int i = 0; i < mqttBrokersMap.getListFromMapAllClean().size(); i++)
     * { // --- vNode labels List<String> vnodeLabels = new ArrayList<String>();
     * vnodeLabels.add("MqttBroker"); // --- vNode properties
     * Map<String, Object> vNodeProps =
     * mqttBrokersMap.getListFromMapAllClean().get(i); Node mqttBroker = new
     * VirtualNode(Util.labels(vnodeLabels), vNodeProps, db);
     * listNodes.add(mqttBroker);
     *
     * Map<String, Object> subscribeList = (Map<String, Object>)
     * vNodeProps.get("subscribeList");
     *
     * for (Map.Entry<String, Object> entry : subscribeList.entrySet()) { String
     * key = entry.getKey(); Object value = entry.getValue();
     *
     * // --- vNode labels List<String> vnodeLabels2 = new ArrayList<String>();
     * vnodeLabels2.add("MqttSubscription"); Map<String, Object> vNodeProps2 =
     * new HashMap<String, Object>(); vNodeProps2.put("mqttBrokerId",
     * vNodeProps.get("mqttBrokerId")); vNodeProps2.put("topic", key);
     * vNodeProps2.put("cypher", value);
     * log.info("apoc.mqtt.listBrokersAsVgraph: " + mqttBroker.toString());
     *
     * Node mqttBrokerSubscription = new VirtualNode(Util.labels(vnodeLabels2),
     * vNodeProps2, db); listNodes.add(mqttBrokerSubscription);
     *
     * VirtualRelationship relation = new VirtualRelationship(mqttBroker,
     * mqttBrokerSubscription, RelationshipType.withName("SUBSCRIPTION"));
     *
     * listRelationships.add(relation); log.info("apoc.mqtt.listBrokersAsVgraph
     * \n\n---mqttBroker:\n" + mqttBroker.toString() + "\n\n---relation\n" +
     * relation.toString() + "\n\n---mqttBrokerSubscription\n" +
     * mqttBrokerSubscription.toString());
     *
     * VirtualPathResult pathResult = new VirtualPathResult(mqttBroker,
     * relation, mqttBrokerSubscription);
     *
     * listPaths.add(pathResult); //System.out.println(pair.getKey() + " = " +
     * pair.getValue());
     *
     * }
     *
     * //System.out.println(cronMap.getListFromMapAllClean().get(i)); }
     * System.out.println(listPaths); GraphResult graphResult = new
     * GraphResult(listNodes, listRelationships); //return Stream.of(
     * graphResult ); //return Stream.of(graphResult).map(GraphResult::new);
     * return listRelationships;
     * //System.out.println(cronMap.getListFromMapAllClean().get(i));; //new
     * VirtualNode(Util.labels(labelNames), props, db); //return
     * Stream.of(listPaths).map(VirtualPathResult::new); }
     */
    // ----------------------------------------------------------------------------------
    // add mqtt broker
    // ----------------------------------------------------------------------------------
    @UserFunction
    @Description("RETURN apoc.mqtt.connectBroker('mqttBrokerId', {serverURI:'tcp://localhost:1883',clientId:'mqttBrokerIdClient+random',qos:0, automaticReconnect:true, cleanSession:false, connectionTimeout:1, keepAliveInterval:3  })")
    public Map<String, Object> connectBroker(
            @Name("mqttBrokerId") String mqttBrokerId,
            @Name("mqttConnectOptions") Map<String, Object> mqttConnectOptions
    ) {
        log.debug("apoc.mqtt.connectBroker - initialization: " + mqttBrokerId + " " + mqttConnectOptions.toString());

        // --- check if already exists
        Map<String, Object> mqttBroker = mqttBrokersMap.getMapElementById(mqttBrokerId);
        if (!(mqttBroker == null)) {
            Map<String, Object> returnMessage = new HashMap<String, Object>();
            returnMessage.put("status", "error");
            returnMessage.put("mqttBrokerId", mqttBrokerId);
            returnMessage.put("serverURI", mqttConnectOptions.get("serverURI"));
            returnMessage.put("statusMessage", "Broker with this name exists!");
            log.error("apoc.mqtt.connectBroker: " + returnMessage);
            return returnMessage;
        }
        // --- set connections options
        mqttConnectOptions.put("mqttBrokerId", (String) mqttBrokerId);
        // --- set defaults for mqtt connection
        // --- serverURI
        if (!mqttConnectOptions.containsKey("serverURI")) {
            mqttConnectOptions.put("serverURI", "tcp://localhost:1883");
        }
        // --- clientId
        if (!mqttConnectOptions.containsKey("clientId")) {
            mqttConnectOptions.put("clientId", mqttBrokerId + "-" + new Random().nextInt());
        }
        // --- qos
        if (!mqttConnectOptions.containsKey("qos")) {
            mqttConnectOptions.put("qos", (int) 0);
        }
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
        // --- setKeepAliveInterval
        if (!mqttConnectOptions.containsKey("keepAliveInterval")) {
            mqttConnectOptions.put("keepAliveInterval", (int) 3);
        }

        log.debug("apoc.mqtt.connectBroker - mqttConnectOptions: " + mqttConnectOptions.toString() + "\n");

        /*    TODO ...
https://www.eclipse.org/paho/files/javadoc/org/eclipse/paho/client/mqttv3/MqttConnectOptions.html
        
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
        // ---
        try {
            // --- register boker
            MqttClientNeo mqttBrokerNeo4jClient = new MqttClientNeo(mqttConnectOptions);

            log.debug("apoc.mqtt.connectBroker - connect ok: " + mqttBrokerId + " " + mqttConnectOptions.get("serverURI") + " " + mqttConnectOptions.get("clientId"));

            // --- store broker info
            mqttConnectOptions.put("connected", true);
            mqttConnectOptions.put("messagePublishLastStatus", "");
            mqttConnectOptions.put("messagePublishOk", 0);
            mqttConnectOptions.put("messagePublishError", 0);
            mqttConnectOptions.put("messageSubscribeOk", 0);
            mqttConnectOptions.put("messageSubscribeError", 0);
            mqttConnectOptions.put("mqttBrokerNeo4jClient", mqttBrokerNeo4jClient);
            mqttConnectOptions.put("subscribeList", new HashMap<String, Object>());
            // --- add to map
            mqttBrokersMap.addToMap(mqttBrokerId, mqttConnectOptions);
            // --- return object
            Map<String, Object> returnMessage = new HashMap<String, Object>();
            returnMessage.put("status", "ok");
            returnMessage.put("mqttBrokerId", mqttBrokerId);
            returnMessage.put("serverURI", mqttConnectOptions.get("serverURI"));
            returnMessage.put("statusMessage", "Connect MqTT OK");
            log.info("apoc.mqtt.connectBroker: " + returnMessage);
            return returnMessage;
        } catch (Exception ex) {
            Map<String, Object> returnMessage = new HashMap<String, Object>();
            returnMessage.put("status", "error");
            returnMessage.put("mqttBrokerId", mqttBrokerId);
            returnMessage.put("serverURI", mqttConnectOptions.get("serverURI"));
            returnMessage.put("statusMessage", "Failed to Connect MqTT Broker: " + ex.getMessage());
            log.error("apoc.mqtt.connectBroker: " + returnMessage);
            return returnMessage;
        }
    }

    // ----------------------------------------------------------------------------------
    // delete mqtt broker
    // ----------------------------------------------------------------------------------
    @UserFunction
    @Description("RETURN apoc.mqtt.disconnectBroker('mqttBrokerId')")
    public Map<String, Object> disconnectBroker(
            @Name("mqttBrokerId") String mqttBrokerId
    ) {
        // --- delete broker if exists
        log.debug("apoc.mqtt.disconnectBroker initialization: " + mqttBrokerId);
        Map<String, Object> mqttBroker = mqttBrokersMap.getMapElementById(mqttBrokerId);
        if (!(mqttBroker == null)) {
            if (!(mqttBroker.get("mqttBrokerNeo4jClient") == null)) {
                MqttClientNeo mqttBrokerNeo4jClient = (MqttClientNeo) mqttBroker.get("mqttBrokerNeo4jClient");
                mqttBrokerNeo4jClient.unsubscribeAll();
                mqttBrokerNeo4jClient.disconnect();
            }
            mqttBrokersMap.removeFromMap(mqttBrokerId);
            // --- return info
            Map<String, Object> returnMessage = new HashMap<String, Object>();
            returnMessage.put("status", "ok");
            returnMessage.put("mqttBrokerId", mqttBrokerId);
            returnMessage.put("statusMessage", "Broker removed.");
            log.info("apoc.mqtt.disconnectBroker: " + returnMessage);
            return returnMessage;
        } else {
            // --- return info
            Map<String, Object> returnMessage = new HashMap<String, Object>();
            returnMessage.put("status", "ok");
            returnMessage.put("mqttBrokerId", mqttBrokerId);
            returnMessage.put("statusMessage", "NO broker to remove.");
            log.info("apoc.mqtt.disconnectBroker: " + returnMessage);
            return returnMessage;
        }
    }

    // ----------------------------------------------------------------------------------
    // publish
    // ----------------------------------------------------------------------------------
    @UserFunction
    @Description("RETURN apoc.mqtt.publish('mqttBrokerId', 'mqtt/topic/path', {message:123}, {optionalOptions:null})")
    public Map<String, Object> publish(
            @Name("mqttBrokerId") String mqttBrokerId,
            @Name("topic") String topic,
            @Name("message") Object message,
            @Name(value = "options", defaultValue = "{debug:false}") Map<String, Object> options
    ) {
        // --- get broker

        String debugMessage = "apoc.mqtt.publish - initialization: mqttBrokerId: " + mqttBrokerId + " topic: " + topic + " message: " + message.toString();
        if (options.get("debug").equals(true)) {
            log.info(debugMessage);
        } else {
            log.debug(debugMessage);
        }

        Map<String, Object> mqttBroker = mqttBrokersMap.getMapElementById(mqttBrokerId);
        // --- no broker found
        if (mqttBroker == null) {
            // --- return info
            Map<String, Object> returnMessage = new HashMap<String, Object>();
            returnMessage.put("status", "error");
            returnMessage.put("mqttBrokerId", mqttBrokerId);
            returnMessage.put("topic", topic);
            returnMessage.put("message", message);
            returnMessage.put("options", options);
            returnMessage.put("statusMessage", "Failed to find MqTT Broker - Check Connection");
            if (options.get("debug").equals(true)) {
                String lastMessagePublished = ""
                        + "\n mqttBrokerId: " + mqttBrokerId
                        + "\n toppic: " + topic
                        + "\n message: " + message.toString()
                        + "\n publish status: ERROR - " + "Failed to find MqTT Broker - Check Connection"
                        + "\n";
                log.info("apoc.mqtt.publish: " + lastMessagePublished);
            }
            log.error("apoc.mqtt.publish: " + returnMessage);
            return returnMessage;
        } else {
            // --- get
            MqttClientNeo mqttBrokerNeo4jClient = (MqttClientNeo) mqttBroker.get("mqttBrokerNeo4jClient");
            log.debug("apoc.mqtt.publish - mqttBrokerNeo4jClient " + mqttBrokerNeo4jClient);
            // --- send message
            String mqttMesageString = "";
            ObjectMapper mapper = new ObjectMapper();
            Map<String, Object> jsonParams = new HashMap<String, Object>();

            try {
                // --- detect message object type and setup message
                if (message instanceof Map) {
                    jsonParams = (Map<String, Object>) message;
                    mqttMesageString = mapper.writeValueAsString(jsonParams).toString();
                } else if (message instanceof Node) {
                    jsonParams.put("id", (int) ((Node) message).getId());
                    jsonParams.put("labels", (List) ((Node) message).getLabels()); // (String)((Node) message).getLabels().toString());
                    jsonParams.put("properties", (Map<String, Object>) ((Node) message).getAllProperties());
                    mqttMesageString = mapper.writeValueAsString(jsonParams).toString();
                } else if (message instanceof Relationship) {
                    jsonParams.put("id", (int) ((Relationship) message).getId());
                    jsonParams.put("startNodeId", (int) ((Relationship) message).getStartNodeId());
                    jsonParams.put("endNodeId", (int) ((Relationship) message).getEndNodeId());
                    jsonParams.put("type", (String) ((Relationship) message).getType().toString());
                    jsonParams.put("properties", (Map<String, Object>) ((Relationship) message).getAllProperties());
                    mqttMesageString = mapper.writeValueAsString(jsonParams).toString();
                } else if (message instanceof String) {
                    mqttMesageString = (String) message;
                } else {
                    mqttMesageString = message.toString();
                }
                // --- publish

                mqttBrokerNeo4jClient.publish(topic, mqttMesageString);
                // ---  set statistics
                mqttBroker.put("messagePublishLastStatus", "OK");
                mqttBroker.put("messagePublishOk", 1 + (int) mqttBroker.get("messagePublishOk"));
                // --- return info
                Map<String, Object> returnMessage = new HashMap<String, Object>();
                returnMessage.put("status", "ok");
                returnMessage.put("mqttBrokerId", mqttBrokerId);
                returnMessage.put("topic", topic);
                returnMessage.put("message", message);
                returnMessage.put("options", options);
                returnMessage.put("statusMessage", "MqTT Publish OK");
                if (options.get("debug").equals(true)) {
                    String lastMessagePublished = ""
                            + "\n mqttBrokerId: " + mqttBrokerId
                            + "\n toppic: " + topic
                            + "\n message: " + message.toString()
                            + "\n publish status: OK"
                            + "\n";
                    log.info("apoc.mqtt.publish: " + lastMessagePublished);
                }
                log.debug("apoc.mqtt.publish: " + returnMessage);
                return returnMessage;
            } catch (Exception ex) {
                // ---  set statistics
                mqttBroker.put("messagePublishLastStatus", "ERROR: " + ex.getMessage());
                mqttBroker.put("messagePublishError", 1 + (int) mqttBroker.get("messagePublishError"));
                // --- return info
                Map<String, Object> returnMessage = new HashMap<String, Object>();
                returnMessage.put("status", "error");
                returnMessage.put("mqttBrokerId", mqttBrokerId);
                returnMessage.put("topic", topic);
                returnMessage.put("message", message);
                returnMessage.put("options", options);
                returnMessage.put("statusMessage", "MqTT Publish Error: " + ex.getMessage());
                log.error("apoc.mqtt.publish: debug" + options.get("debug"));
                if (options.get("debug").equals(true)) {
                    String lastMessagePublished = ""
                            + "\n mqttBrokerId: " + mqttBrokerId
                            + "\n toppic: " + topic
                            + "\n message: " + message.toString()
                            + "\n publish status: ERROR - " + ex.getMessage()
                            + "\n";
                    log.info("apoc.mqtt.publish: " + lastMessagePublished);
                }
                log.error("apoc.mqtt.publish: " + returnMessage);
                return returnMessage;
            }
        }
    }

    // ----------------------------------------------------------------------------------
    // subscribe
    // ----------------------------------------------------------------------------------
    @Procedure(mode = Mode.WRITE)
    @Description("CALL apoc.mqtt.subscribe('mqttBrokerId', 'mqtt/topic/path','MERGE (n:mqttTest) ON CREATE SET n.count=1, n.message=$message ON MATCH SET n.count = n.count +1, n.message=$message RETURN n', {messageType:'json'}) ")
    public Stream<MapResult> subscribe(
            @Name("mqttBrokerId") String mqttBrokerId,
            @Name("topic") String topic,
            @Name("query") String query,
            @Name(value = "options", defaultValue = "{debug:false}") Map<String, Object> options
    ) {
        log.debug("apoc.mqtt.subscribe - initialization: " + mqttBrokerId + " " + topic + " " + query + " " + options);
        // --- remove subscription if exist
        this.unSubscribe(mqttBrokerId, topic);
        // --- get broker
        Map<String, Object> mqttBroker = mqttBrokersMap.getMapElementById(mqttBrokerId);
        if (mqttBroker == null) {
            Map<String, Object> returnMessage = new HashMap<String, Object>();
            returnMessage.put("status", "error");
            returnMessage.put("mqttBrokerId", mqttBrokerId);
            returnMessage.put("topic", topic);
            returnMessage.put("query", query);
            returnMessage.put("options", options);
            returnMessage.put("statusMessage", "Failed to find MqTT Broker - Check Connection");
            if (options.get("debug").equals(true)) {
                String lastMessagePublished = ""
                        + "\n mqttBrokerId: " + mqttBrokerId
                        + "\n toppic: " + topic
                        + "\n query: " + query.toString()
                        + "\n publish status: ERROR " + "Failed to find MqTT Broker - Check Connection"
                        + "\n";
                log.error("apoc.mqtt.publish: " + lastMessagePublished);
            }
            log.error("apoc.mqtt.subscribe: " + returnMessage);
            return Stream.of(returnMessage).map(MapResult::new);
        } else {
            log.debug("apoc.mqtt.subscribe - mqttBroker ok");
            MqttClientNeo mqttBrokerNeo4jClient = (MqttClientNeo) mqttBroker.get("mqttBrokerNeo4jClient");
            // --- subscribe
            try {
                Map<String, Object> subscribeOptions = new HashMap<String, Object>();
                // --- set options
                subscribeOptions.put("mqttBrokerId", mqttBrokerId);
                subscribeOptions.put("query", query);
                subscribeOptions.put("debug", options.get("debug"));
                subscribeOptions.put("lastMessageReceived", "subscribed");
                subscribeOptions.put("messageReceivedOk", 0);
                subscribeOptions.put("messageReceivedError", 0);
                // --- create cypher task
                ProcessMqttMessage task = new ProcessMqttMessage(subscribeOptions);
                // --- subscribe to mqtt
                mqttBrokerNeo4jClient.subscribe(topic, task);
                // --- add to subscription list
                Map<String, Object> subscribeList = (Map<String, Object>) mqttBroker.get("subscribeList");
                subscribeList.put(topic, subscribeOptions);
                // --- statistics
                mqttBroker.put("messageSubscribeOk", 1 + (int) mqttBroker.get("messageSubscribeOk"));
                // --- return
                Map<String, Object> returnMessage = new HashMap<String, Object>();
                returnMessage.put("status", "ok");
                returnMessage.put("mqttBrokerId", mqttBrokerId);
                returnMessage.put("topic", topic);
                returnMessage.put("query", query);
                returnMessage.put("options", options);
                returnMessage.put("statusMessage", "MqTT Subscribe ok");
                log.info("apoc.mqtt.subscribe: " + returnMessage);
                return Stream.of(returnMessage).map(MapResult::new);
            } catch (Exception ex) {
                // --- statistics
                mqttBroker.put("messageSubscribeError", 1 + (int) mqttBroker.get("messageSubscribeError"));
                // --- return
                Map<String, Object> returnMessage = new HashMap<String, Object>();
                returnMessage.put("status", "error");
                returnMessage.put("mqttBrokerId", mqttBrokerId);
                returnMessage.put("topic", topic);
                returnMessage.put("query", query);
                returnMessage.put("options", options);
                returnMessage.put("statusMessage", "MqTT Subscribe Failed: " + ex.getMessage());
                log.error("apoc.mqtt.subscribe: " + returnMessage);
                return Stream.of(returnMessage).map(MapResult::new);
            }
        }
    }

    // ----------------------------------------------------------------------------------
    // unsubscribe
    // ----------------------------------------------------------------------------------
    @UserFunction
    @Description("RETURN apoc.mqtt.unSubscribe('mqttBrokerId', 'mqtt/topic/path' )")
    public Object unSubscribe(
            @Name("mqttBrokerId") String mqttBrokerId,
            @Name("topic") String topic
    ) {
        log.debug("apoc.mqtt.unSubscribe: " + mqttBrokerId + " " + topic);
        // --- get broker
        Map<String, Object> mqttBroker = mqttBrokersMap.getMapElementById(mqttBrokerId);
        if (mqttBroker != null) {
            MqttClientNeo mqttBrokerNeo4jClient = (MqttClientNeo) mqttBroker.get("mqttBrokerNeo4jClient");
            // --- unsubscribe
            mqttBrokerNeo4jClient.unsubscribe(topic);
            Map<String, Object> subscribeList = (Map<String, Object>) mqttBroker.get("subscribeList");
            // --- remove from list
            subscribeList.remove(topic);
            // --- statistics
            mqttBroker.put("messageSubscribeOk", (int) mqttBroker.get("messageSubscribeOk") - 1);
            // --- return
            Map<String, Object> returnMessage = new HashMap<String, Object>();
            returnMessage.put("status", "ok");
            returnMessage.put("mqttBrokerId", mqttBrokerId);
            returnMessage.put("broker topic", topic);
            returnMessage.put("statusMessage", "UnSubscribe ok");
            log.info("apoc.mqtt.unSubscribe: " + returnMessage);
            return returnMessage;
        } else {
            // --- return
            Map<String, Object> returnMessage = new HashMap<String, Object>();
            returnMessage.put("status", "ok");
            returnMessage.put("mqttBrokerId", mqttBrokerId);
            returnMessage.put("broker topic", topic);
            returnMessage.put("statusMessage", "No broker to unSubscribe");
            log.info("apoc.mqtt.unSubscribe: " + returnMessage);
            return returnMessage;
        }
    }

    // ----------------------------------------------------------------------------------
    // utils
    // ----------------------------------------------------------------------------------
    // --- JSONUtils
    /**
     * JSONUtils checkJson = new JSONUtils();
     * System.out.print(checkJson.jsonStringToMap(validJson));
     */
    public final static class JSONUtils {

        private JSONUtils() {
        }

        public Object jsonStringToMap(String jsonInString) {
            try {
                Map<String, Object> retMap = new Gson().fromJson(jsonInString.toString(), new TypeToken<HashMap<String, Object>>() {
                }.getType());
                return retMap;
            } catch (JsonSyntaxException ex) {
                // --- set as object
                Object input = (Object) jsonInString;
                Map<String, Object> returnMap = new HashMap<String, Object>();
                // --- map values
                if (input instanceof String) {
                    returnMap.put("value", (String) jsonInString);
                } else if (input instanceof Integer) {
                    returnMap.put("value", (Integer) input);
                } else if (input instanceof Boolean) {
                    returnMap.put("value", (Boolean) input);
                } else if (input instanceof Float) {
                    returnMap.put("value", (Float) input);
                } else if (input instanceof Double) {
                    returnMap.put("value", (Double) input);
                } else if (input instanceof Map) {
                    returnMap.put("value", (String) input);
                } else {
                    returnMap.put("value", (String) input);
                }
                return returnMap;
            }
        }
    }

    // --- ProcessMqttMessage
    public class ProcessMqttMessage {

        public Map<String, Object> options = new HashMap<String, Object>();

        public ProcessMqttMessage(Map<String, Object> optionsIn) {
            options = optionsIn;
            log.debug("apoc.mqtt - ProcessMqttMessage registration: " + options.toString());
        }

        public void run(String topic, String message) {
            options.remove("lastMessageProcessedResults");
            log.debug("apoc.mqtt - ProcessMqttMessage run: " + options.toString());
            // --- check message - get key-value
            JSONUtils checkJson = new JSONUtils();
            Map<String, Object> cypherParams = (Map<String, Object>) checkJson.jsonStringToMap(message);
            log.debug("apoc.mqtt - ProcessMqttMessage cypherParams: " + cypherParams.toString());
            // --- run cypher
            try (Transaction tx = db.beginTx()) {
                Result dbResult = db.execute((String) options.get("query"), cypherParams);
                String dbResultString = dbResult.resultAsString();
                // --- statistics
                options.put("messageReceivedOk", (int) options.get("messageReceivedOk") + 1);
                options.put("lastMessageReceived", message.toString());
                options.put("lastMessageProcessedResults", dbResultString);
                // --- log
                String lastMessageProcessedResults = "apoc.mqtt - ProcessMqttMessage "
                        + "\n mqttBrokerId: " + options.get("mqttBrokerId")
                        + "\n toppic: " + topic
                        + "\n message: " + message.toString()
                        + "\n cypherQuery: \n  " + options.get("query")
                        + "\n cypherParams: \n  " + cypherParams.toString()
                        + "\n cypherQuery results:\n  " + dbResultString
                        + "\n";
                if (options.get("debug").equals(true)) {
                    log.info(lastMessageProcessedResults
                    );
                } else {
                    log.debug(lastMessageProcessedResults);
                }
                tx.success();
            } catch (Exception ex) {
                // --- statistics
                options.put("messageReceivedError", (int) options.get("messageReceivedError") + 1);
                options.put("lastMessageReceived", message.toString());
                options.put("lastMessageProcessedResults", (String) ex.toString());
                // --- log
                String lastMessageProcessedResults = "apoc.mqtt - ProcessMqttMessage "
                        + "\n mqttBrokerId: " + options.get("mqttBrokerId")
                        + "\n toppic: " + topic
                        + "\n message: " + message.toString()
                        + "\n cypherQuery: \n  " + options.get("query")
                        + "\n cypherParams: \n  " + cypherParams.toString()
                        + "\n cypherQuery results:\n  " + ex.toString()
                        + "\n";
                log.error(lastMessageProcessedResults);
            }
        }
    }

    // --- MqttClientNeo
    public class MqttClientNeo {

        int qos = 0;
        public MqttClient neo4jMqttClient;

        public MqttClientNeo(Map<String, Object> mqttConnectOptions) throws MqttException {
            log.debug("apoc.mqtt - MqttClientNeo initialization: " + mqttConnectOptions.toString());

            qos = (int) mqttConnectOptions.get("qos");
            neo4jMqttClient = new MqttClient((String) mqttConnectOptions.get("serverURI"), (String) mqttConnectOptions.get("clientId"), new MemoryPersistence());
            // --- connOpts
            MqttConnectOptions connOpts = new MqttConnectOptions();
            connOpts.setAutomaticReconnect((Boolean) mqttConnectOptions.get("automaticReconnect"));
            connOpts.setCleanSession((Boolean) mqttConnectOptions.get("cleanSession"));
            connOpts.setConnectionTimeout((int) mqttConnectOptions.get("connectionTimeout"));
            connOpts.setKeepAliveInterval((int) mqttConnectOptions.get("keepAliveInterval"));
            
            // --- connect to broker
            log.debug("apoc.mqtt - MqttClientNeo connOpts: " + connOpts.toString());
            // --- connect
            neo4jMqttClient.connect(connOpts);

            neo4jMqttClient.setCallback(new MqttCallback() {
                public void connectionLost(Throwable cause) {
                    log.info("apoc.mqtt - connectionLost");
                }

                public void messageArrived(String topic, MqttMessage message) {
                    log.debug("apoc.mqtt - messageArrived " + topic + " " + message.toString());
                }

                public void deliveryComplete(IMqttDeliveryToken token) {
                    log.debug("apoc.mqtt - deliveryComplete");
                }
            });
            // --- send connect message
            log.info("apoc.mqtt - MqttClientNeo OK: " + mqttConnectOptions.get("serverURI") + " as " + mqttConnectOptions.get("clientId"));
        }

        private void publish(String topic, String content) throws MqttException {
            log.debug("apoc.mqtt - MqttClientNeo publish initialization: " + topic, content);
            MqttMessage message = new MqttMessage(content.getBytes());
            message.setQos(qos);
            String clientId = neo4jMqttClient.getClientId();
            String serverURI = neo4jMqttClient.getServerURI();
            // --- publish
            neo4jMqttClient.publish(topic, message);
            log.debug("apoc.mqtt - MqttClientNeo publish finished: " + clientId + " " + serverURI + " " + topic + " " + content.toString());
        }

        private void unsubscribeAll() {
            String clientId = neo4jMqttClient.getClientId();
            String serverURI = neo4jMqttClient.getServerURI();

            try {
                neo4jMqttClient.unsubscribe("#");
                log.debug("apoc.mqtt -  MqttClientNeo unsubscribeAll ok: " + clientId + " " + serverURI);
            } catch (MqttException ex) {
                log.error("apoc.mqtt - MqttClientNeo unsubscribeAll error: " + clientId + " " + serverURI + " " + ex.toString());
            }
        }

        private void unsubscribe(String topic) {
            String clientId = neo4jMqttClient.getClientId();
            String serverURI = neo4jMqttClient.getServerURI();

            try {
                neo4jMqttClient.unsubscribe(topic);
                log.debug("apoc.mqtt - MqttClientNeo unsubscribe ok: " + topic + " " + clientId + " " + serverURI);
            } catch (MqttException ex) {
                log.error("apoc.mqtt - MqttClientNeo unsubscribe error: " + topic + " " + clientId + " " + serverURI + " " + ex.toString());
            }
        }

        private void disconnect() {
            String clientId = neo4jMqttClient.getClientId();
            String serverURI = neo4jMqttClient.getServerURI();

            try {
                neo4jMqttClient.unsubscribe("#");
                neo4jMqttClient.disconnect();
                log.debug("apoc.mqtt - MqttClientNeo disconnect ok: " + clientId + " " + serverURI);
            } catch (MqttException ex) {
                log.error("apoc.mqtt - MqttClientNeo disconnect error: " + clientId + " " + serverURI + " " + ex.toString());
            }
        }

        public void subscribe(String topic, ProcessMqttMessage task) throws MqttException {
            String clientId = neo4jMqttClient.getClientId();
            String serverURI = neo4jMqttClient.getServerURI();
            log.debug("apoc.mqtt - MqttClientNeo subscribe initialization: " + topic + " " + clientId + " " + serverURI + " " + task.options);
            // --- set processor
            IMqttMessageListener topicProcesor = new IMqttMessageListener() {
                @Override
                public void messageArrived(String topic, MqttMessage message) throws Exception {
                    task.run(topic, message.toString());
                }
            };
            // --- subscribe
            neo4jMqttClient.subscribe(topic, qos, topicProcesor);
        }
    }

    // --- MapProcess
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

        public Map<String, Object> getMapElementById(String id) {
            Map<String, Object> mapTmp = (Map) map.get(id);
            return mapTmp;
        }

        public Map<String, Object> getMapElementByIdClean(String id) {
            Map<String, Object> mapTmp = (Map) map.get(id);
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
