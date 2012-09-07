package com.orientechnologies.orient.test.database.sharding;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecordInternal;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 * @author <a href="mailto:enisher@gmail.com">Artem Orobets</a>
 * @since 8/31/12
 */
@Test
public class AutoshardingTest {
  @Test
  public void testCreateRecord() throws Exception {
    ODatabaseDocumentTx database1 = new ODatabaseDocumentTx("remote:localhost:2424/distributedStorage");
    database1.open( "admin", "admin" );

    List<ORID> documents  = new ArrayList<ORID>( 10 );
    for (int i = 0; i < 10; i++) {
      final ODocument doc = new ODocument();
      doc.field("f1", i);
      doc.save();
      documents.add( doc.getIdentity() );
    }

    database1.close();

    ODatabaseDocumentTx database2 = new ODatabaseDocumentTx("remote:localhost:2425/distributedStorage");
    database2.open("admin", "admin");

    for ( ORID id : documents ) {
      final ODocument document = database2.load( id );
      System.out.println(id + " - " + document.field( "f1" ));
    }

    database2.close();
  }
}
