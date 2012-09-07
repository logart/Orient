package com.orientechnologies.orient.core.storage;

import com.orientechnologies.orient.core.id.ORID;

/**
 * @author <a href="mailto:enisher@exigenservices.com">Artem Orobets</a>
 * @since 9/5/12
 */
public class ORecordDuplicatedException extends RuntimeException {
  private final ORID iRid;

  public ORecordDuplicatedException( String message, ORID iRid ) {
    super( message );
    this.iRid = iRid;
  }
}
