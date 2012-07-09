package com.orientechnologies.orient.core.storage.impl.local;

import com.orientechnologies.common.exception.OException;

/**
 * @author LomakiA <a href="mailto:Andrey.Lomakin@exigenservices.com">Andrey Lomakin</a>
 * @since 09.07.12
 */
public class ORecordCacheException extends OException {
  public ORecordCacheException() {
  }

  public ORecordCacheException(String message) {
    super(message);
  }

  public ORecordCacheException(Throwable cause) {
    super(cause);
  }

  public ORecordCacheException(String message, Throwable cause) {
    super(message, cause);
  }
}
