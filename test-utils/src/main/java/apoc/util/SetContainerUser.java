package apoc.util;

import com.github.dockerjava.api.command.CreateContainerCmd;
import com.sun.security.auth.module.UnixSystem;
import org.testcontainers.containers.GenericContainer;
import java.util.function.Consumer;

public class SetContainerUser
{

    public static void nonRootUser( GenericContainer container )
    {
        container.withCreateContainerCmdModifier( (Consumer<CreateContainerCmd>) cmd -> cmd.withUser( getNonRootUserString() ) );
    }

    public static String getNonRootUserString()
    {
        // check if the non root user environment variable is set, if so use that. Otherwise use current user.
        String user = System.getenv( "NON_ROOT_USER_ID" );
        if(user == null)
        {
            return getCurrentlyRunningUser();
        }
        else
        {
            return user;
        }
    }

    private static String getCurrentlyRunningUser()
    {
        UnixSystem fs = new UnixSystem();
        return fs.getUid() + ":" + fs.getGid();
    }
}
