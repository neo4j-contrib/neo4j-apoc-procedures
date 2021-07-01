package org.neo4j.test.rule;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

/**
 * A better version of {@link org.junit.rules.ExternalResource} that properly handles exceptions in {@link
 * #after(boolean)}.
 */
@Deprecated
public abstract class ExternalResource implements TestRule
{
    @Override
    public Statement apply( final Statement base, Description description )
    {
        return new Statement()
        {
            @Override
            public void evaluate() throws Throwable
            {
                before();
                Throwable failure = null;
                try
                {
                    base.evaluate();
                }
                catch ( Throwable e )
                {
                    failure = e;
                }
                finally
                {
                    try
                    {
                        after( failure == null );
                    }
                    catch ( Throwable e )
                    {
                        if ( failure != null )
                        {
                            failure.addSuppressed( e );
                        }
                        else
                        {
                            failure = e;
                        }
                    }
                }
                if ( failure != null )
                {
                    throw failure;
                }
            }
        };
    }

    /**
     * Override to set up your specific external resource.
     *
     * @throws Throwable if setup fails (which will disable {@code after}
     */
    protected void before() throws Throwable
    {
        // do nothing
    }

    /**
     * Override to tear down your specific external resource.
     */
    protected void after( boolean successful ) throws Throwable
    {
        // do nothing
    }
}
