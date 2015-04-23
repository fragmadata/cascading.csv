package com.datascience.hadoop;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Array;
import java.util.*;

/**
 * Hadoop writable for lists containing writable fields.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class ListWritable<T extends Writable> implements List<T>, Writable {
  private final Class<T> type;
  private final List<T> elements = new ArrayList<>();
  private T[] pool;

  @SuppressWarnings("unchecked")
  public ListWritable(Class<T> type) {
    if (type == null)
      throw new NullPointerException("type cannot be null");
    this.type = type;
    this.pool = (T[]) Array.newInstance(type, 1024);
  }

  @Override
  public int size() {
    return elements.size();
  }

  @Override
  public boolean isEmpty() {
    return elements.isEmpty();
  }

  @Override
  public boolean contains(Object o) {
    return elements.contains(o);
  }

  @Override
  public Iterator<T> iterator() {
    return elements.iterator();
  }

  @Override
  public Object[] toArray() {
    return elements.toArray();
  }

  @Override
  public <T1> T1[] toArray(T1[] a) {
    return elements.toArray(a);
  }

  @Override
  public boolean add(T t) {
    return elements.add(t);
  }

  @Override
  public boolean remove(Object o) {
    return elements.remove(o);
  }

  @Override
  public boolean containsAll(Collection<?> c) {
    return elements.containsAll(c);
  }

  @Override
  public boolean addAll(Collection<? extends T> c) {
    return elements.addAll(c);
  }

  @Override
  public boolean addAll(int index, Collection<? extends T> c) {
    return elements.addAll(index, c);
  }

  @Override
  public boolean removeAll(Collection<?> c) {
    return elements.removeAll(c);
  }

  @Override
  public boolean retainAll(Collection<?> c) {
    return elements.retainAll(c);
  }

  @Override
  public void clear() {
    elements.clear();
  }

  @Override
  public T get(int index) {
    return elements.get(index);
  }

  @Override
  public T set(int index, T element) {
    return elements.set(index, element);
  }

  @Override
  public void add(int index, T element) {
    elements.add(index, element);
  }

  @Override
  public T remove(int index) {
    return elements.remove(index);
  }

  @Override
  public int indexOf(Object o) {
    return elements.indexOf(o);
  }

  @Override
  public int lastIndexOf(Object o) {
    return elements.lastIndexOf(o);
  }

  @Override
  public ListIterator<T> listIterator() {
    return elements.listIterator();
  }

  @Override
  public ListIterator<T> listIterator(int index) {
    return elements.listIterator(index);
  }

  @Override
  public List<T> subList(int fromIndex, int toIndex) {
    return elements.subList(fromIndex, toIndex);
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    dataOutput.writeInt(elements.size());
    for (T element : elements) {
      element.write(dataOutput);
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public void readFields(DataInput dataInput) throws IOException {
    elements.clear();
    int size = dataInput.readInt();
    for (int i = 0; i < size; i++) {
      if (pool.length <= i) {
        T[] newPool = (T[]) Array.newInstance(type, pool.length << 1);
        System.arraycopy(pool, 0, newPool, 0, pool.length);
      }
      if (pool[i] == null) {
        try {
          pool[i] = type.newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
          throw new RuntimeException("failed to construct list element instance");
        }
      }
      T object = pool[i];
      object.readFields(dataInput);
      elements.add(object);
    }
  }

}
