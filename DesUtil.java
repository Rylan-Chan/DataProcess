import org.apache.commons.lang3.StringUtils;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.DESKeySpec;
import java.io.UnsupportedEncodingException;
import java.security.SecureRandom;
import java.util.Base64;



/**
 * DesUtil.
 */
public class DesUtil {

    private static String password = "b0b822tg15d6c15b0sda08";

    private static Object lock = new Object();

    private static DesUtil instance;

    /**
     * DesUtil.
     */
    public DesUtil() {

    }

    /**
     * getInstance.
     * @return DesUtil
     */
    public static DesUtil getInstance() {
        synchronized (lock) {
            if (instance == null) {
                init();
            }
        }

        return instance;
    }

    /**
     * init.
     */
    private static void init() {
        instance = new DesUtil();
    }

    /**
     *
     *  encrypt.
     * @param data data
     * @return string
     */
    public  String encrypt(final String data) {
        if (StringUtils.isEmpty(data)) {
            return null;
        }
        try {
            final byte[] bt = encryptByKey(data.getBytes("utf-8"), password);
            final Base64.Encoder base64en = Base64.getEncoder();
            final String strs;
            strs = new String(base64en.encode(bt), "utf-8");
            return strs;
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     *
     *  decryptor.
     * @param data data
     * @return string
     */
    public String decryptor(final String data)  {
        try {
            final Base64.Decoder base64en = Base64.getDecoder();
            final byte[] bt = decrypt(base64en.decode(data), password);
            final String strs = new String(bt, "utf-8");
            return strs;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * encryptByKey.
     * @param datasource byte[]
     * @return byte[]
     */
    private byte[] encryptByKey(final byte[] datasource, final String key) {
        try {
            final SecureRandom random = new SecureRandom();

            final DESKeySpec desKey = new DESKeySpec(key.getBytes("utf-8"));
            final SecretKeyFactory keyFactory = SecretKeyFactory.getInstance("DES");
            final SecretKey securekey = keyFactory.generateSecret(desKey);
            final Cipher cipher = Cipher.getInstance("DES");
            cipher.init(Cipher.ENCRYPT_MODE, securekey, random);
            return cipher.doFinal(datasource);
        } catch (Throwable e) {
            e.printStackTrace();
        }
        return null;
    }


    /**
     * decrypt.
     * @param src byte[]
     * @return byte[]
     * @throws Exception
     */
    private  byte[] decrypt(final byte[] src, final String key) throws Exception {
        final SecureRandom random = new SecureRandom();
        final DESKeySpec desKey = new DESKeySpec(key.getBytes("utf-8"));
        final SecretKeyFactory keyFactory = SecretKeyFactory.getInstance("DES");
        final SecretKey securekey = keyFactory.generateSecret(desKey);
        final Cipher cipher = Cipher.getInstance("DES");
        cipher.init(Cipher.DECRYPT_MODE, securekey, random);
        return cipher.doFinal(src);
    }

    /**
     * Test.
     * @param args args.
     */
    public static void main(final String[] args) {
        System.out.println(DesUtil.getInstance().decryptor("CMCwbILQkvHY7AqH87LFu2MIHr0/di7N"));
    }


}