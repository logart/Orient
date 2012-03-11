package com.orientechnologies.orient.test.database.speed;

import com.orientechnologies.orient.client.db.ODatabaseHelper;
import com.orientechnologies.orient.client.remote.OServerAdmin;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.*;

/**
 * @author EniSh
 *         Date: 17.02.12
 */
@Test
public class DeadlockTest {

	public static final int READER_COUNT = 20;
	public static final int WRITER_COUNT = 20;
	public static final int DOC_COUNT = 1500;
	public static final String DATABASE_URL = "local:concurrentTest";
//	public static final String DATABASE_URL = "remote:localhost/concurrentTest";

	private OClass carClass;
	private List<ORID> identities = new ArrayList<ORID>(10);
	private List<Future<?>> results = new ArrayList<Future<?>>();

	@BeforeMethod
	public void setUp() throws Exception {
		OGlobalConfiguration.MVRBTREE_ENTRYPOINTS.setValue(1);
		OGlobalConfiguration.MVRBTREE_OPTIMIZE_ENTRYPOINTS_FACTOR.setValue(1.);
		OGlobalConfiguration.MVRBTREE_TIMEOUT.setValue(0);
		OGlobalConfiguration.STORAGE_RECORD_LOCK_TIMEOUT.setValue(-1);

		final ODatabaseDocumentTx database = new ODatabaseDocumentTx(DATABASE_URL);
		if (database.exists()) {
			database.drop();
		}
		database.create();

		final OSchema schema = database.getMetadata().getSchema();
		carClass = schema.createClass("Car");
		carClass.createProperty("prise", OType.INTEGER).createIndex(OClass.INDEX_TYPE.NOTUNIQUE);
		schema.save();
		for (int i = 0; i < DOC_COUNT; i++) {
			final ODocument car = database.newInstance("Car");
			car.field("prise", i);
			car.save();
			identities.add(car.getIdentity());
		}

		database.close();
	}

	@Test
	public void testDeadlock() throws Exception {
		ThreadPoolExecutor ex = new ThreadPoolExecutor(Math.max(10, READER_COUNT + WRITER_COUNT), Math.max(10, READER_COUNT + WRITER_COUNT), 1000, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(READER_COUNT + WRITER_COUNT));
		final CountDownLatch latch = new CountDownLatch(READER_COUNT + WRITER_COUNT);

		for (int i = 0; i < READER_COUNT; i++) {
			final int num = i;
			Runnable reader = new Runnable() {
				Random r = new Random();

				public void run() {
					final ODatabaseDocumentTx database = new ODatabaseDocumentTx(DATABASE_URL).open("admin", "admin");
					final OIndex<?> carIndex = database.getMetadata().getIndexManager().getClassIndex("Car", "Car.prise");
					latch.countDown();
					try {
						latch.await();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					for (int i = 0; i < 1000000; i++) {
						//database.begin();
						final int key = r.nextInt(DOC_COUNT);
						carIndex.get(key);
						//database.commit();
						if(i % 1000 == 0)
							System.out.println("Reader " + num + " : " + i);
					}
					database.close();
				}
			};
			results.add(ex.submit(reader));
		}

		for (int i = 0; i < WRITER_COUNT; i++) {
			final int num = i;
			Runnable writer = new Runnable() {
				Random r = new Random();

				public void run() {
					final ODatabaseDocumentTx database = new ODatabaseDocumentTx(DATABASE_URL).open("admin", "admin");
					latch.countDown();
					try {
						latch.await();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					for (int i = 0; i < 1000000; i++) {
						try {
							//database.begin();
							final ODocument car = database.load(identities.get(r.nextInt(DOC_COUNT)));
							car.field("prise", r.nextInt(DOC_COUNT));
							car.save();
							//database.commit();
						} catch (OConcurrentModificationException e) {
							database.rollback();
							i--;
						}
						if(i % 1000 == 0)
							System.out.println("Writer " + num + " : " + i);
					}

					database.close();
				}
			};
			results.add(ex.submit(writer));
		}

		ex.shutdown();

		ex.awaitTermination(50, TimeUnit.SECONDS);

		for (Future<?> result : results) {
			final Object o = result.get();
			if (o != null) {
				throw (ExecutionException) o;
			}
		}
	}
}
