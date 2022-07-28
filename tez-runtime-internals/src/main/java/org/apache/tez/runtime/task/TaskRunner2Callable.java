/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tez.runtime.task;

import java.lang.reflect.UndeclaredThrowableException;
import java.security.PrivilegedExceptionAction;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.tez.common.CallableWithNdc;
import org.apache.tez.common.TezCommonUtils;
import org.apache.tez.common.TezUtilsInternal;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.runtime.LogicalIOProcessorRuntimeTask;
import org.apache.tez.runtime.api.impl.TezUmbilical;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is responsible for running a {@link LogicalIOProcessorRuntimeTask}.
 * It does not worry about reporting errors, heartbeats etc.
 *
 * Returns success / interrupt / failure status via it's return parameter.
 *
 * It's the responsibility of the invoker to handle whatever exceptions may be generated by this.
 */
public class TaskRunner2Callable extends CallableWithNdc<TaskRunner2Callable.TaskRunner2CallableResult> {

  private static final Logger LOG = LoggerFactory.getLogger(TaskRunner2Callable.class);

  private final LogicalIOProcessorRuntimeTask task;
  private final UserGroupInformation ugi;
  private final AtomicBoolean stopRequested = new AtomicBoolean(false);
  private final AtomicBoolean interruptAttempted = new AtomicBoolean(false);

  private volatile Thread ownThread;

  /**
   * Protocol to send the events.
   */
  private final TezUmbilical tezUmbilical;

  public TaskRunner2Callable(LogicalIOProcessorRuntimeTask task,
      final UserGroupInformation ugi, final TezUmbilical umbilical) {
    this.task = task;
    this.ugi = ugi;
    this.tezUmbilical = umbilical;
  }

  @Override
  public TaskRunner2CallableResult callInternal() throws Exception {
    ownThread = Thread.currentThread();
    if (stopRequested.get()) {
      return new TaskRunner2CallableResult(null);
    }
    try {
      return ugi.doAs(new PrivilegedExceptionAction<TaskRunner2CallableResult>() {
        @Override
        public TaskRunner2CallableResult run() throws Exception {
          if (stopRequested.get() || Thread.currentThread().isInterrupted()) {
            return new TaskRunner2CallableResult(null);
          }
          LOG.error("Temp", new RuntimeException());
          TezUtilsInternal.setHadoopCallerContext(task.getHadoopShim(), task.getTaskAttemptID());
          TezCommonUtils.logCredentials(LOG, ugi.getCredentials(), "taskInit");
          task.initialize();

          if (!stopRequested.get() && !Thread.currentThread().isInterrupted()) {
            LOG.error("Temp", new RuntimeException());
            task.run();
          } else {
            LOG.info("Stopped before running the processor taskAttemptId={}",
                task.getTaskAttemptID());
            task.setFrameworkCounters();
            return new TaskRunner2CallableResult(null);
          }

          if (!stopRequested.get() && !Thread.currentThread().isInterrupted()) {
            LOG.error("Temp", new RuntimeException());
            task.close();
            task.setFrameworkCounters();
          } else {
            LOG.error("Temp", new RuntimeException());
            task.setFrameworkCounters();
            return new TaskRunner2CallableResult(null);
          }
          LOG.error("Temp", new RuntimeException());


          return new TaskRunner2CallableResult(null);
        }
      });
    } catch (Throwable t) {
      if (t instanceof UndeclaredThrowableException) {
        t = t.getCause();
      }
      task.setFrameworkCounters();
      return new TaskRunner2CallableResult(t);
    } finally {
      // If a stop was requested. Make sure the interrupt status is set during the cleanup.

      // One drawback of not communicating out from here is that task complete messages will only
      // be sent out after cleanup is complete.
      // For a successful task, however, this should be almost no delay since close has already happened.
      maybeFixInterruptStatus();
      LOG.error("Temp", new RuntimeException());
      task.getOutputContexts().forEach(outputContext
          -> outputContext.trapEvents(new TezTrapEventHandler(outputContext,
          this.tezUmbilical)));
      task.cleanup();
    }
  }

  private void maybeFixInterruptStatus() {
    if (stopRequested.get() && !Thread.currentThread().isInterrupted()) {
      Thread.currentThread().interrupt();
    }
  }


  public void abortTask() {
    if (!stopRequested.getAndSet(true)) {
      task.abortTask();
    }
  }

  public void interruptTask() {
    if (!interruptAttempted.getAndSet(true)) {
      LogicalIOProcessorRuntimeTask localTask = task;
      // Send an interrupt only if the task is not done.
      if (ownThread != null && (localTask != null && !localTask.isTaskDone())) {
        ownThread.interrupt();
      }
    }
  }

  public static class TaskRunner2CallableResult {
    final Throwable error;

    public TaskRunner2CallableResult(Throwable error) {
      this.error = error;
    }
  }

  public TezCounters addAndGetTezCounter(final String name) {
    return task.addAndGetTezCounter(name);
  }
}
