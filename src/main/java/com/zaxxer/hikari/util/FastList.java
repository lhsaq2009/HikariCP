/*
 * Copyright (C) 2013, 2014 Brett Wooldridge
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.zaxxer.hikari.util;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.NoSuchElementException;
import java.util.RandomAccess;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

/**
 * 底层基于数组实现，目的是提高 List 操作的性能，主要用于 HikariCP 中缓存 Statement 实例和链接。
 * 与 JDK 自带的 ArrayList 的主要优化：
 *      1. 去掉了 add、get、remove 等操作时的范围检查。源码中 FastList 的注释为：Fast list without range checking
 * <p>
 * Fast list without range checking.
 * <p>
 * FastList 实现了 List 接口，但并没有将所有方法都实现出来，对 HikariCP 中用不到的方法直接抛出了 UnsupportedOperationException
 *
 * @author Brett Wooldridge
 */
public final class FastList<T> implements List<T>, RandomAccess, Serializable {
    private static final long serialVersionUID = -4598088075242913858L;

    private final Class<?> clazz;
    private T[] elementData;
    private int size;

    /**
     * Construct a FastList with a default size of 32.
     *
     * @param clazz the Class stored in the collection
     */
    @SuppressWarnings("unchecked")
    public FastList(Class<?> clazz) {
        // 默认 32 个元素，ArrayList 中默认数组长度为 10
        this.elementData = (T[]) Array.newInstance(clazz, 32);
        this.clazz = clazz;
    }

    /**
     * Construct a FastList with a specified size.
     *
     * @param clazz    the Class stored in the collection
     * @param capacity the initial size of the FastList
     */
    @SuppressWarnings("unchecked")
    public FastList(Class<?> clazz, int capacity) {
        this.elementData = (T[]) Array.newInstance(clazz, capacity);
        this.clazz = clazz;
    }

    /**
     * 添加元素：
     * 1. 新元素放在数组的尾端
     * 2. 每次扩容时长度为旧数组的两倍
     * <p>
     * 看下 ArrayList 中的 add 方法：
     * 1. 新元素放在数组的尾端
     * 2. 每次扩容时长度为旧数组的 1.5 倍
     * <p>
     * public boolean add(E e) {
     * // 检查数组空间是否充足，若空间不足，执行扩容操作，新数据是旧数组的 1.5 倍
     * ensureCapacityInternal(size + 1);  // Increments modCount!!
     * // 同样是尾插
     * elementData[size++] = e;
     * return true;
     * }
     * <p>
     * Add an element to the tail of the FastList.
     *
     * @param element the element to add
     */
    @Override
    public boolean add(T element) {                          // TODO：VS
        if (size < elementData.length) {
            elementData[size++] = element;
        } else {
            // overflow-conscious code
            final int oldCapacity = elementData.length;
            final int newCapacity = oldCapacity << 1;        // 每次扩容为旧数组的两倍
            @SuppressWarnings("unchecked") final T[] newElementData = (T[]) Array.newInstance(clazz, newCapacity);
            System.arraycopy(elementData, 0, newElementData, 0, oldCapacity);
            newElementData[size++] = element;
            elementData = newElementData;
        }

        return true;
    }

    /**
     * 查询元素
     * 不做范围检查，直接返回数组下标。
     * <p>
     * 在 HikariCP 中，FastList 用于保存 Statement 和链接，程序可以保证 FastList 的元素不会越界，这样可以省去范围检查的耗时。
     * <p>
     * TODO：VS 同样看下 ArrayList 里的 get()
     *   // 先做范围检查，如果数组越界，抛出 IndexOutOfBoundsException
     *   rangeCheck(index);
     * <p>
     * Get the element at the specified index.
     *
     * @param index the index of the element to get
     * @return the element, or ArrayIndexOutOfBounds is thrown if the index is invalid
     */
    @Override
    public T get(int index) {
        return elementData[index];
    }

    /**
     * Remove the last element from the list.  No bound check is performed, so if this
     * method is called on an empty list and ArrayIndexOutOfBounds exception will be
     * thrown.
     *
     * @return the last element of the list
     */
    public T removeLast() {
        T element = elementData[--size];
        elementData[size] = null;
        return element;
    }

    /**
     * This remove method is most efficient when the element being removed
     * is the last element.  Equality is identity based, not equals() based.
     * Only the first matching element is removed.
     *
     * @param element the element to remove
     */
    @Override
    public boolean remove(Object element) {
        for (int index = size - 1; index >= 0; index--) {
            if (element == elementData[index]) {
                final int numMoved = size - index - 1;
                if (numMoved > 0) {
                    System.arraycopy(elementData, index + 1, elementData, index, numMoved);
                }
                elementData[--size] = null;
                return true;
            }
        }

        return false;
    }

    /**
     * Clear the FastList.
     */
    @Override
    public void clear() {
        for (int i = 0; i < size; i++) {
            elementData[i] = null;
        }

        size = 0;
    }

    /**
     * Get the current number of elements in the FastList.
     *
     * @return the number of current elements
     */
    @Override
    public int size() {
        return size;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isEmpty() {
        return size == 0;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public T set(int index, T element) {
        T old = elementData[index];
        elementData[index] = element;
        return old;
    }

    /**
     * 删除元素
     * 而 Statement 通过是后创建出来的先被 Close 掉，这样可以提高查询效率。
     * <p>
     * {@inheritDoc}
     */
    @Override
    public T remove(int index) {
        if (size == 0) {
            return null;
        }

        // TODO：该版本，没有：从后往前遍历

        final T old = elementData[index];

        final int numMoved = size - index - 1;
        if (numMoved > 0) {
            System.arraycopy(elementData, index + 1, elementData, index, numMoved);
        }

        elementData[--size] = null;

        return old;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean contains(Object o) {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Iterator<T> iterator() {
        return new Iterator<T>() {
            private int index;

            @Override
            public boolean hasNext() {
                return index < size;
            }

            @Override
            public T next() {
                if (index < size) {
                    return elementData[index++];
                }

                throw new NoSuchElementException("No more elements in FastList");
            }
        };
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object[] toArray() {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <E> E[] toArray(E[] a) {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean containsAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean addAll(Collection<? extends T> c) {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean addAll(int index, Collection<? extends T> c) {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean removeAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean retainAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void add(int index, T element) {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int indexOf(Object o) {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int lastIndexOf(Object o) {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ListIterator<T> listIterator() {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ListIterator<T> listIterator(int index) {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<T> subList(int fromIndex, int toIndex) {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object clone() {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void forEach(Consumer<? super T> action) {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Spliterator<T> spliterator() {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean removeIf(Predicate<? super T> filter) {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void replaceAll(UnaryOperator<T> operator) {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void sort(Comparator<? super T> c) {
        throw new UnsupportedOperationException();
    }
}
