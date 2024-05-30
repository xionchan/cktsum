----------- Oracle database -------------
// JAVA 计算字符串, clob, blob的crc32的值
CREATE OR REPLACE AND COMPILE JAVA SOURCE NAMED CRC32CAL AS
import java.util.zip.CRC32;
import java.sql.Blob;
import java.io.InputStream;
import java.sql.Clob;
import java.io.BufferedReader;
import java.io.Reader;

public class CRC32CAL {
    public static long CRC32STR(String strData) throws Exception {
        if (strData == null) {
                return 0;
}

        CRC32 crc32 = new CRC32();
        byte[] bytes = strData.getBytes();
        crc32.update(bytes);
return crc32.getValue();
}

    public static long CRC32BLOB(Blob blobData) throws Exception {
        if (blobData == null) {
                return 0;
}

        CRC32 crc32 = new CRC32();
        byte[] buffer = new byte[8192]; // 8KB buffer size
        int bytesRead;

        InputStream inputStream = blobData.getBinaryStream();
        while ((bytesRead = inputStream.read(buffer)) != -1) {
                crc32.update(buffer, 0, bytesRead);
}

        return crc32.getValue();
}

    public static long CRC32CLOB(Clob clobData) throws Exception {
        if (clobData == null) {
                return 0;
}

        CRC32 crc32 = new CRC32();
char[] buffer = new char[8192];
int bytesRead;

        Reader reader = clobData.getCharacterStream();
        BufferedReader bufferedReader = new BufferedReader(reader);
        while ((bytesRead = bufferedReader.read(buffer)) != -1) {
                crc32.update(new String(buffer, 0, bytesRead).getBytes());
}

        return crc32.getValue();
}
}
/

// 计算字符串的CRC32, 将数字,字符,日期类型拼接到一起计算然后计算crc32
CREATE OR REPLACE FUNCTION CAL_STR_CRC(concatstr in varchar2) RETURN NUMBER IS
LANGUAGE JAVA NAME 'CRC32CAL.CRC32STR(java.lang.String) return long';
/

// 计算clob的crc32
CREATE OR REPLACE FUNCTION CAL_CLOB_CRC(clobdata in clob) RETURN NUMBER IS
LANGUAGE JAVA NAME 'CRC32CAL.CRC32CLOB(java.sql.Clob) return long';
/

// 计blob的crc32
CREATE OR REPLACE FUNCTION CAL_BLOB_CRC(blobdata in blob) RETURN NUMBER IS
LANGUAGE JAVA NAME 'CRC32CAL.CRC32BLOB(java.sql.Blob) return long';
/