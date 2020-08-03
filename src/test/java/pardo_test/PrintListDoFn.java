package pardo_test;

import org.apache.beam.sdk.transforms.DoFn;

import java.util.concurrent.locks.ReentrantLock;

public class PrintListDoFn<T extends Iterable> extends DoFn<T, Void> {
  public static ReentrantLock lock = new ReentrantLock();

  private static final String indent = "    ";

  @DoFn.ProcessElement
  public void processElement(ProcessContext c) {
    lock.lock();
    try {
      System.out.println("Iterable<" + c.element().getClass().getName() + ">: ----------------------------");
      for (Object event : c.element()) {
        System.out.println(indent + event.toString().replace("\n", "\n"+indent));
      }
    } finally {
      lock.unlock();
    }
  }
}
