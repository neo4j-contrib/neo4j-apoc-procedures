package apoc.util;

import org.apache.commons.lang.ArrayUtils;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

public class CompressionUtils
{

    public static List<Long> bytesWithCharSet( String text, String charSetString )
    {
        if ( Charset.isSupported( charSetString ) )
        {

            try
            {
                byte[] bytes = text.getBytes( charSetString );
                List<Long> result = new ArrayList<>( bytes.length );
                for ( byte b : bytes )
                {
                    result.add( (long) b & 0xFFL );
                }
                return result;
            }
            catch ( UnsupportedEncodingException e )
            {
                throw new RuntimeException( "Encoding problem. ", e );
            }
        }
        throw new RuntimeException( "CharSet '" + charSetString + "' is not supported." );
    }

    public static String decompressWithCharSet( List<Long> longBytes, String charSetString )
    {
        if ( Charset.isSupported( charSetString ) )
        {

            try
            {
                // Turn long bytes into their real bytes
                List<Byte> bytes = new ArrayList<>( longBytes.size() );
                for ( long b : longBytes )
                {
                    bytes.add( (byte) (b & 0xFF) );
                }

                byte[] byteArray = ArrayUtils.toPrimitive( bytes.toArray( new Byte[bytes.size()] ) );

                return Charset.forName( charSetString ).newDecoder().decode( ByteBuffer.wrap( byteArray ) ).toString();
            }
            catch ( CharacterCodingException e )
            {
                throw new RuntimeException( "Decoding problem. ", e );
            }
        }
        throw new RuntimeException( "CharSet '" + charSetString + "' is not supported." );
    }
}
