package apoc.atomic;

import apoc.atomic.util.AtomicUtils;
import apoc.util.ArrayBackedList;
import apoc.util.MapUtil;
import org.apache.commons.lang3.ArrayUtils;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Lock;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.helpers.TransactionTemplate;
import org.neo4j.procedure.*;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * @author AgileLARUS
 * @since 20-06-17
 */
public class Atomic {

    @Context
    public GraphDatabaseService db;

    /**
     * increment a property's value
     */
    @Procedure(mode = Mode.WRITE)
    @Description("apoc.atomic.add(node/relatonship,propertyName,number) Sums the property's value with the 'number' value ")
    public Stream<AtomicResults> add(@Name("container") Object container, @Name("propertyName") String property, @Name("number") Number number, @Name(value = "times", defaultValue = "5") Long times) {
        checkIsPropertyContainer(container);
        PropertyContainer propertyContainer;
        final Number[] newValue = new Number[1];
        final Number[] oldValue = new Number[1];
        propertyContainer = (PropertyContainer) container;

        final ExecutionContext executionContext = new ExecutionContext(db, propertyContainer, property);
        retry(executionContext, (context) -> {
            oldValue[0] = (Number) propertyContainer.getProperty(property);
            newValue[0] = AtomicUtils.sum((Number) propertyContainer.getProperty(property), number);
            propertyContainer.setProperty(property, newValue[0]);
            return context.propertyContainer.getProperty(property);
        }, times);

        return Stream.of(new AtomicResults(propertyContainer,property, oldValue[0], newValue[0]));
    }

    /**
     * decrement a property's value
     */
    @Procedure(mode = Mode.WRITE)
    @Description("apoc.atomic.subtract(node/relatonship,propertyName,number) Subtracts the 'number' value to the property's value")
    public Stream<AtomicResults> subtract(@Name("container") Object container, @Name("propertyName") String property, @Name("number") Number number, @Name(value = "times", defaultValue = "5") Long times) {
        checkIsPropertyContainer(container);
        PropertyContainer propertyContainer;
        final Number[] newValue = new Number[1];
        final Number[] oldValue = new Number[1];
        propertyContainer = (PropertyContainer) container;

        final ExecutionContext executionContext = new ExecutionContext(db, propertyContainer, property);
        retry(executionContext, (context) -> {
            oldValue[0] = (Number) propertyContainer.getProperty(property);
            newValue[0] = AtomicUtils.sub((Number) propertyContainer.getProperty(property), number);
            propertyContainer.setProperty(property, newValue[0]);
            return context.propertyContainer.getProperty(property);
        }, times);

        return Stream.of(new AtomicResults(propertyContainer, property, oldValue[0], newValue[0]));
    }

    /**
     * concat a property's value
     */
    @Procedure(mode = Mode.WRITE)
    @Description("apoc.atomic.concat(node/relatonship,propertyName,string) Concats the property's value with the 'string' value")
    public Stream<AtomicResults> concat(@Name("container") Object container, @Name("propertyName") String property, @Name("string") String string, @Name(value = "times", defaultValue = "5") Long times) {
        checkIsPropertyContainer(container);
        PropertyContainer propertyContainer;
        final String[] newValue = new String[1];
        final String[] oldValue = new String[1];
        propertyContainer = (PropertyContainer) container;

        final ExecutionContext executionContext = new ExecutionContext(db, propertyContainer, property);
        retry(executionContext, (context) -> {
            oldValue[0] = propertyContainer.getProperty(property).toString();
            newValue[0] = oldValue[0].toString().concat(string);
            propertyContainer.setProperty(property, newValue[0]);

            return context.propertyContainer.getProperty(property);
        }, times);

        return Stream.of(new AtomicResults(propertyContainer, property, oldValue[0], newValue[0]));
    }

    /**
     * insert a value into an array property value
     */
    @Procedure(mode = Mode.WRITE)
    @Description("apoc.atomic.insert(node/relatonship,propertyName,position,value) insert a value into the property's array value at 'position'")
    public Stream<AtomicResults> insert(@Name("container") Object container, @Name("propertyName") String property, @Name("position") Long position, @Name("value") Object value, @Name(value = "times", defaultValue = "5") Long times) throws ClassNotFoundException {
        checkIsPropertyContainer(container);
        PropertyContainer propertyContainer;
        final Object[] oldValue = new Object[1];
        final Object[] newValue = new Object[1];
        propertyContainer = (PropertyContainer) container;
        final ExecutionContext executionContext = new ExecutionContext(db, propertyContainer, property);

        retry(executionContext, (context) -> {
            oldValue[0] = propertyContainer.getProperty(property);
            List<Object> values = insertValueIntoArray(propertyContainer.getProperty(property), position, value);
            Class clazz;
            try {
                clazz = Class.forName(values.toArray()[0].getClass().getName());
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
            newValue[0] = Array.newInstance(clazz, values.size());
            try {
                System.arraycopy(values.toArray(), 0, newValue[0], 0, values.size());
            } catch (Exception e) {
                String message = "Property's array value has type: " + values.toArray()[0].getClass().getName() + ", and your value to insert has type: " + value.getClass().getName();
                throw new ArrayStoreException(message);
            }
            propertyContainer.setProperty(property, newValue[0]);
            return context.propertyContainer.getProperty(property);
        }, times);

        return Stream.of(new AtomicResults(propertyContainer, property, oldValue[0], newValue[0]));
    }

    /**
     * remove a value into an array property value
     */
    @Procedure(mode = Mode.WRITE)
    @Description("apoc.atomic.remove(node/relatonship,propertyName,position) remove the element at position 'position'")
    public Stream<AtomicResults> remove(@Name("container") Object container, @Name("propertyName") String property, @Name("position") Long position, @Name(value = "times", defaultValue = "5") Long times) throws ClassNotFoundException {
        checkIsPropertyContainer(container);
        PropertyContainer propertyContainer;
        final Object[] oldValue = new Object[1];
        final Object[] newValue = new Object[1];
        propertyContainer = (PropertyContainer) container;
        final ExecutionContext executionContext = new ExecutionContext(db, propertyContainer, property);

        retry(executionContext, (context) -> {
            Object[] arrayBackedList = new ArrayBackedList(propertyContainer.getProperty(property)).toArray();
            oldValue[0] = arrayBackedList;
            if(position > arrayBackedList.length || position < 0) {
                throw new RuntimeException("Attention your position out of range or higher than array length, that is " + arrayBackedList.length);
            }
            Object[] newArray = ArrayUtils.addAll(Arrays.copyOfRange(arrayBackedList, 0, position.intValue()), Arrays.copyOfRange(arrayBackedList, position.intValue() +1, arrayBackedList.length));
            Class clazz;
            try {
                clazz = Class.forName(arrayBackedList[0].getClass().getName());
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
            /*it's not possible to return directly the newArray, we have to create a new array with the specific class*/
            newValue[0] = Array.newInstance(clazz, newArray.length);
            System.arraycopy(newArray, 0, newValue[0], 0, newArray.length);
            propertyContainer.setProperty(property, newValue[0]);

            return context.propertyContainer.getProperty(property);
        }, times);

        return Stream.of(new AtomicResults(propertyContainer, property, oldValue[0], newValue[0]));
    }

    /**
     * update the property's value
     */
    @Procedure(mode = Mode.WRITE)
    @Description("apoc.atomic.update(node/relatonship,propertyName,updateOperation) update a property's value with a cypher operation (ex. \"n.prop1+n.prop2\")")
    public Stream<AtomicResults> update(@Name("container") Object container, @Name("propertyName") String property, @Name("operation") String operation, @Name(value = "times", defaultValue = "5") Long times) throws InterruptedException {
        checkIsPropertyContainer(container);
        PropertyContainer propertyContainer = (PropertyContainer) container;
        final Object[] oldValue = new Object[1];
        final ExecutionContext executionContext = new ExecutionContext(db, propertyContainer, property);

        retry(executionContext, (context) -> {
            oldValue[0] = propertyContainer.getProperty(property);
            String statement = "WITH {container} as n with n set n." + property + "=" + operation + ";";
            Map<String, Object> properties = MapUtil.map("container", propertyContainer);
            return context.db.execute(statement, properties);
        }, times);

        return Stream.of(new AtomicResults(propertyContainer,property,oldValue[0],propertyContainer.getProperty(property)));
    }

    private static class ExecutionContext {
        private final GraphDatabaseService db;

        private final PropertyContainer propertyContainer;

        private final String propertyName;

        public ExecutionContext(GraphDatabaseService db, PropertyContainer propertyContainer, String propertyName){
            this.db = db;
            this.propertyContainer = propertyContainer;
            this.propertyName = propertyName;
        }
    }

    private List<Object> insertValueIntoArray(Object oldValue, Long position, Object value) {
        List<Object> values = new ArrayList<>();
        if (oldValue.getClass().isArray())
            values.addAll(new ArrayBackedList(oldValue));
        else
            values.add(oldValue);
        if (position > values.size())
            values.add(value);
        else
            values.add(position.intValue(), value);
        return values;
    }

    private void retry(ExecutionContext executionContext, Function<ExecutionContext, Object> work, Long times){
        TransactionTemplate template = new TransactionTemplate().retries(times.intValue()).backoff(10, TimeUnit.MILLISECONDS);
        template.with(db).execute(tx -> {
            Lock lock = tx.acquireWriteLock(executionContext.propertyContainer);
            work.apply(executionContext);
            lock.release();
        });
    }

    private void checkIsPropertyContainer(Object container) {
        if (!(container instanceof PropertyContainer)) throw new RuntimeException("You Must pass Node or Relationship");
    }

    public class AtomicResults {
        public Object container;
        public String property;
        public Object oldValue;
        public Object newValue;

        public AtomicResults(Object container, String property, Object oldValue, Object newValue) {
            this.container = container;
            this.property = property;
            this.oldValue = oldValue;
            this.newValue = newValue;
        }
    }
}
