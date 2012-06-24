package com.orientechnologies.orient.test.internal.index;

import java.util.Random;

import org.testng.annotations.Test;

import com.orientechnologies.common.test.SpeedTestMonoThread;
import com.orientechnologies.common.types.OBinaryConverter;
import com.orientechnologies.common.types.OUnsafeBinaryConverter;

/**
 * @author LomakiA <a href="mailto:Andrey.Lomakin@exigenservices.com">Andrey Lomakin</a>
 * @since 19.06.12
 */
@Test
public class BinaryConverterSpeedTest extends SpeedTestMonoThread {
  private static final OBinaryConverter CONVERTER = new OUnsafeBinaryConverter();
  private final Random                  random    = new Random();
  private final byte[]                  buffer    = new byte[10000];

  public BinaryConverterSpeedTest() {
    super(1000000000);
  }

  @Test(enabled = false)
  @Override
  public void cycle() throws Exception {
    CONVERTER.putInt(buffer, 100, 0, 100);
    CONVERTER.getInt(buffer, 0, 100);
    // int t = 100;
    // if (t > Integer.MAX_VALUE)
    // System.out.println(1);

  }
}
