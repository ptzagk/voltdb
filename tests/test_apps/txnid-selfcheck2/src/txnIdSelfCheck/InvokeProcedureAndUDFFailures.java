/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package txnIdSelfCheck;

import org.voltdb.VoltTable;
import org.voltdb.client.*;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;


public class InvokeProcedureAndUDFFailures extends BenchmarkThread {

    Random r = new Random(8278923);
    long counter = 0;
    final long MAX_SIMPLE_UDF_EMPTY_TABLES = 1000;
    final Client client;
    final AtomicBoolean m_shouldContinue = new AtomicBoolean(true);
    final AtomicBoolean m_needsBlock = new AtomicBoolean(false);
    final Semaphore txnsOutstanding = new Semaphore(3);

    public InvokeProcedureAndUDFFailures(Client client) {
        setName("InvokeProcedureAndUDFFailures");
        this.client = client;
    }

    void shutdown() {
        m_shouldContinue.set(false);
        this.interrupt();
    }

    class InvokeDroppedCallback implements ProcedureCallback {
        private boolean m_expectFailure;
        InvokeDroppedCallback() {
            this(false);
        }
        InvokeDroppedCallback(boolean expectFailure) {
            super();
            m_expectFailure = expectFailure;
        }
        @Override
        public void clientCallback(ClientResponse clientResponse) throws Exception {
            txnsOutstanding.release();
            log.info("InvokeProcedureAndUDFFailures response '" + clientResponse.getStatusString() + "' (" +  clientResponse.getStatus() + ")");
            // (! m_expectFailure) == "expect success".  So, if
            // we expect success but the result is not successful then we want
            // to crash.
            if ((! m_expectFailure) != (clientResponse.getStatus() == ClientResponse.SUCCESS)) {
                Benchmark.txnCount.incrementAndGet();
                hardStop(String.format("InvokeProcedureAndUDFFailures returned an unexpected status %d, %s failure.",
                                       clientResponse.getStatus(),
                                       (m_expectFailure ? "expected" : "did not expect")));
                //The procedure/udf may be dropped so we don't really care, just want to test the server with dropped procedure invocations in flight
            } else {
                validate(clientResponse);
            }
        }
        public void validate(ClientResponse cr) {
            // Do nothing here.
        }
    }
    /*
     * Keep track of how many failures we have in a row.
     * Failures here means that a simpleUDF call produced
     * no rows.
     */
    final AtomicInteger failures = new AtomicInteger();

    private abstract class TestCase {
        final String m_description;
        final boolean m_expectFailure;

        TestCase(String description, boolean expectFailure) {
            m_description = description;
            m_expectFailure = expectFailure;
        }

        abstract void run(Client client, Random r) throws Exception;
        public String toString() {
            return m_description + " - expect "
                    + (m_expectFailure ? "failure" : "success");
        }
        public boolean isFailureExpected() {
            return m_expectFailure;
        }
    }

    private class SimpleUDFTestCase extends TestCase {
        int m_exponent;

        SimpleUDFTestCase(String description, boolean expectsFailure, int exponent) {
            super(description, expectsFailure);
            m_exponent = exponent;
        }

        void run(Client client, Random r) throws Exception {
            /*
             * The bound here is not really necessary, but we
             * would not want overflow.
             */
            final long t = (long) r.nextInt(1000);
            final long expected = getExpected(t, m_exponent);
            if (2 <= m_exponent && m_exponent <= 10) {
                client.callProcedure(
                        new InvokeDroppedCallback(isFailureExpected()) {
                            @Override
                            public void validate(ClientResponse cr) {
                                if (cr.getStatus() != ClientResponse.SUCCESS) {
                                    hardStop(String.format("simpleUDF(%d, %d) failed with status %d", t, m_exponent, cr.getStatus()));
                                }
                                VoltTable vt = cr.getResults()[0];
                                // I think it's ok if there are no rows.  That
                                // just means the table is not populated.  But if
                                // it happens too often, that means there is some
                                // problem with the test, and the test should fail.
                                if (vt.advanceRow()) {
                                    long computed = vt.getLong(0);
                                    if (computed != expected) {
                                        hardStop(String.format("simpleUDF(%d, %d): expected %d, got %d.",
                                                t, m_exponent, expected, computed));
                                    }
                                    failures.set(0);
                                }
                                else {
                                    int numFailures = failures.incrementAndGet();
                                    if (numFailures >= MAX_SIMPLE_UDF_EMPTY_TABLES) {
                                        hardStop("Too many empty tables for simpleUDF calls.");
                                    }
                                }
                            }
                        },
                        "SimpleUDF",
                        t,
                        m_exponent);
            }
        }
    }

    TestCase[] testCases = new TestCase[] {
            new TestCase("dropped Read procedure", true) {
                @Override
                void run(Client client, Random r) throws Exception {
                    // try to run a read procedure that has been droppped (or does not exist)
                    client.callProcedure(new InvokeDroppedCallback(isFailureExpected()), "droppedRead", r.nextInt());
                }
            },
            new TestCase("dropped Write procedure", true) {
                @Override
                void run(Client client, Random r) throws Exception {
                    // try to run a write procedure that has been dropped (or does not exist)
                    client.callProcedure(new InvokeDroppedCallback(isFailureExpected()), "droppedWrite", r.nextInt());
                }
            },
            new TestCase("UDF that throws a SQL exception", true) {
                @Override
                void run(Client client, Random r) throws Exception {
                    // run a udf that throws an exception
                    client.callProcedure(new InvokeDroppedCallback(isFailureExpected()), "exceptionUDF");
                }
            },
            new TestCase("undefined UDF", true) {
                @Override
                void run(Client client, Random r) throws Exception {
                    // run a statement using a function that is non-existent/dropped
                    try {
                        ClientResponse cr = TxnId2Utils.doAdHoc(client,
                                "select missingUDF(cid) FROM partitioned where cid=? order by cid, rid desc");
                    } catch (ProcCallException e) {
                        log.info(e.getClientResponse().getStatus());
                        if (e.getClientResponse().getStatus() != ClientResponse.GRACEFUL_FAILURE)
                            hardStop(e);
                    }
                }
            },
            new TestCase("Invalid drop function", true) {
                @Override
                void run(Client client, Random r) throws Exception {
                    // try to drop a function which is used in the schema
                    try {
                        ClientResponse cr = client.callProcedure("@AdHoc", "drop function add2Bigint;");
                        log.info(cr.getStatusString() + " (" + cr.getStatus() + ")");
                        if (cr.getStatus() == ClientResponse.SUCCESS)
                            hardStop("Should not succeed, the function is used in a stored procedure");
                    } catch (Exception e) {
                        log.info("exception: ", e);
                    }
                }
            },
            new SimpleUDFTestCase("call simpleUDF2", false, 2),
            new SimpleUDFTestCase("call simpleUDF3", false, 3),
            new SimpleUDFTestCase("call simpleUDF4", false, 4),
            new SimpleUDFTestCase("call simpleUDF5", false, 5),
            new SimpleUDFTestCase("call simpleUDF6", false, 6),
            new SimpleUDFTestCase("call simpleUDF7", false, 7),
            new SimpleUDFTestCase("call simpleUDF8", false, 8),
            new SimpleUDFTestCase("call simpleUDF9", false, 9),
            new SimpleUDFTestCase("call simpleUDF10", false, 10),
    };

    @Override
    public void run() {
        while (m_shouldContinue.get()) {
            // if not, connected, sleep
            if (m_needsBlock.get()) {
                do {
                    try { Thread.sleep(3000); } catch (Exception e) {} // sleep for 3s
                    // bail on wakeup if we're supposed to bail
                    if (!m_shouldContinue.get()) {
                        return;
                    }
                }
                while (client.getConnectedHostList().size() == 0);
                m_needsBlock.set(false);
            } else {
                try { Thread.sleep(1); } catch (Exception e) {}
            }

            // get a permit to send a transaction
            try {
                txnsOutstanding.acquire();
            } catch (InterruptedException e) {
                hardStop("InvokeProcedureAndUDFFailures interrupted while waiting for permit. Will end.", e);
            }

            // call a transaction
            try {
                // We have SIMPLE_UDF_BASE cases for the non-simpleUDF calls,
                // Plus (MAXIMUM_EXPONENT - MINIMUM_EXPONENT + 1) exponents
                // for simpleUDF cases.
                final int caseNumber = r.nextInt(testCases.length);
                TestCase testCase = testCases[caseNumber];
                log.info(String.format("InvokeProcedureAndUDFFailures running case %d: %s",
                                       caseNumber,
                                       testCase));
                testCase.run(client, r);

                // don't flood the system with these
                Thread.sleep(r.nextInt(1000));
                txnsOutstanding.release();
            }
            catch (NoConnectionsException e) {
                log.warn("InvokeProcedureAndUDFFailures got NoConnectionsException on proc call. Will sleep.");
                m_needsBlock.set(true);
            }
            catch (Exception e) {
                hardStop("InvokeProcedureAndUDFFailures failed to run client. Will exit.", e);
            }
        }
    }

    private long getExpected(long t, int exponent) {
        return Math.abs(t+2) * (long)Math.pow(10, exponent);
    }
}
