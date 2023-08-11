package apoc.util;

import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.shaded.org.apache.commons.io.IOUtils;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

/**JUnit extension to compress temporary folders after each test class runs.
 *
 * This utility will create temporary folders and compress them after each test class executes.
 * Starting a clean neo4j pre-allocates 500MB of space for the data folder. With all these docker tests
 * this ends up allocating a huge amount of empty space that fills the test machine memory.
 * There are enough unit tests now, that we frequently get test failures just because
 * the machine running the tests ran out of space.
 * This empty space can easily be freed by compressing the mounted folders once we are finished with them.
 *
 * To use this utility, create an object as a class field, and use @RegisterExtension annotation.
 * */
public class TemporaryFolderManager implements AfterAllCallback, BeforeEachCallback
{
    private static final Logger log = LoggerFactory.getLogger( TemporaryFolderManager.class );
    // if we ever run parallel tests, random number generator and
    // list of folders to compress need to be made thread safe
    private Random rng = new Random(  );
    private Set<Path> toCompressAfterAll = new HashSet<>();
    private final Path folderRoot;
    public static final Path TEST_TMP_FOLDER = Paths.get("local-mounts" );


    public TemporaryFolderManager( )
    {
        this(TEST_TMP_FOLDER);
    }
    public TemporaryFolderManager( Path testOutputParentFolder )
    {
        this.folderRoot = testOutputParentFolder;
    }

    @Override
    public void beforeEach( ExtensionContext extensionContext ) throws Exception
    {
        String outputFolderNamePrefix = extensionContext.getTestClass().get().getName() + "_" +
                extensionContext.getTestMethod().get().getName();
        if(!extensionContext.getDisplayName().startsWith( extensionContext.getTestMethod().get().getName() ))
        {
            outputFolderNamePrefix += "_" + extensionContext.getDisplayName() + "-";
        }
        else
        {
            outputFolderNamePrefix += "-";
        }
        log.info( "Recommended folder prefix is " + outputFolderNamePrefix );
    }

    @Override
    public void afterAll( ExtensionContext extensionContext ) throws Exception
    {
 //       if(TestSettings.SKIP_MOUNTED_FOLDER_TARBALLING)
 //       {
 //           log.info( "Cleanup of test artifacts skipped by request" );
 //           return;
 //       }
        log.info( "Performing cleanup of {}", folderRoot );
        // create tar archive of data
        for(Path p : toCompressAfterAll)
        {
            String tarOutName = p.getFileName().toString() + ".tar.gz";
            try ( OutputStream fo = Files.newOutputStream( p.getParent().resolve( tarOutName ) );
                  OutputStream gzo = new GzipCompressorOutputStream( fo );
                  TarArchiveOutputStream archiver = new TarArchiveOutputStream( gzo ) )
            {
                archiver.setLongFileMode( TarArchiveOutputStream.LONGFILE_POSIX );
                List<Path> files = Files.walk( p ).collect( Collectors.toList());
                for(Path fileToBeArchived : files)
                {
                    // don't archive directories...
                    if(fileToBeArchived.toFile().isDirectory()) continue;
                    try( InputStream fileStream = Files.newInputStream( fileToBeArchived ))
                    {
                        ArchiveEntry entry = archiver.createArchiveEntry( fileToBeArchived, folderRoot.relativize( fileToBeArchived ).toString() );
                        archiver.putArchiveEntry( entry );
                        IOUtils.copy( fileStream, archiver );
                        archiver.closeArchiveEntry();
                    } catch (IOException ioe)
                    {
                        // consume the error, because sometimes, file permissions won't let us copy
                        log.warn( "Could not archive "+ fileToBeArchived, ioe);
                    }
                }
                archiver.finish();
            }
        }
        // delete original folders
        log.debug( "Re owning folders: {}", toCompressAfterAll.stream().map( Path::toString ).collect( Collectors.joining(", ")));
        setFolderOwnerTo( SetContainerUser.getNonRootUserString(),
                toCompressAfterAll.toArray(new Path[toCompressAfterAll.size()]) );

        if(extensionContext != null) log.info( "Deleting test folders for {}", extensionContext.getDisplayName() );
        for(Path p : toCompressAfterAll)
        {
            log.debug( "Deleting test output folder {}", p.getFileName().toString() );
            FileUtils.deleteDirectory( p.toFile() );
        }
        toCompressAfterAll.clear();
    }

    public Path createTempFolderAndMountAsVolume( GenericContainer container, String hostFolderNamePrefix,
                                                  String containerMountPoint ) throws IOException
    {
        return createTempFolderAndMountAsVolume( container, hostFolderNamePrefix, containerMountPoint,
                folderRoot );
    }

    public Path createTempFolderAndMountAsVolume( GenericContainer container, String hostFolderNamePrefix,
                                                  String containerMountPoint, Path parentFolder ) throws IOException
    {
        Path hostFolder = createTempFolder( hostFolderNamePrefix, parentFolder );
        mountHostFolderAsVolume( container, hostFolder, containerMountPoint );
        return hostFolder;
    }

    public void mountHostFolderAsVolume(GenericContainer container, Path hostFolder, String containerMountPoint)
    {
        container.withFileSystemBind( hostFolder.toAbsolutePath().toString(),
                containerMountPoint,
                BindMode.READ_WRITE );
    }

    public Path createTempFolder( String folderNamePrefix ) throws IOException
    {
        return createTempFolder( folderNamePrefix, folderRoot );
    }

    public Path createTempFolder( String folderNamePrefix, Path parentFolder ) throws IOException
    {
        String randomStr = String.format( "%04d", rng.nextInt(10000 ) );  // random 4 digit number
        Path hostFolder = parentFolder.resolve( folderNamePrefix + randomStr);
        try
        {
            Files.createDirectories( hostFolder );
        }
        catch ( IOException e )
        {
            log.error( "could not create directory: {}", hostFolder.toAbsolutePath() );
            e.printStackTrace();
            throw e;
        }
        log.info( "Created folder {}", hostFolder );
        if(parentFolder.equals( folderRoot ))
        {
            toCompressAfterAll.add( hostFolder );
        }
        return hostFolder;
    }

    public void setFolderOwnerToCurrentUser(Path file) throws Exception
    {
        setFolderOwnerTo( SetContainerUser.getNonRootUserString(), file );
    }

    public void setFolderOwnerToNeo4j( Path file) throws Exception
    {
        setFolderOwnerTo( "7474:7474", file );
    }

    private void setFolderOwnerTo(String userAndGroup, Path ...files) throws Exception
    {
        // uses docker privileges to set file owner, since probably the current user is not a sudoer.

        // Using nginx because it's easy to verify that the image started.
        try(GenericContainer container = new GenericContainer( DockerImageName.parse( "nginx:latest")))
        {
            container.withExposedPorts( 80 )
                    .waitingFor( Wait.forHttp( "/" ).withStartupTimeout( Duration.ofSeconds( 20 ) ) );
            for(Path p : files)
            {
                mountHostFolderAsVolume( container, p, p.toAbsolutePath().toString() );
            }
            container.start();
            for(Path p : files)
            {
                Container.ExecResult x =
                        container.execInContainer( "chown", "-R", userAndGroup,
                                p.toAbsolutePath().toString() );
            }
            container.stop();
        }
    }
}
