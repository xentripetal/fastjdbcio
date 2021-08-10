/**
 * This is probably overkill, but I figured it could be useful unless I have completely misunderstand the intention
 * of value providers.
 */
package io.xentripetal.beam.fastjdbcio;

import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.MoreObjects;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;

/**
 * {@link StringBuilderValueProvider} is an implementation of {@link ValueProvider} that mimics part of the StringBuilder
 * API for easily making serializable manipulations of {@link String} based {@link ValueProvider}s
 */
class StringBuilderValueProvider implements ValueProvider<String>, Serializable {

    transient volatile String cachedValue;
    final ValueProvider<String> initialValue;
    List<Op> operations = new ArrayList<Op>();

    StringBuilderValueProvider(ValueProvider<String> initialValue) {
        this.initialValue = initialValue;
    }

    /** Creates a {@link StringBuilderValueProvider} with just an empty string as the default value*/
    public static StringBuilderValueProvider of() {
        return new StringBuilderValueProvider(StaticValueProvider.of(""));
    }

    /** Creates a {@link StringBuilderValueProvider} with the provided string as the initial value
     * @param initialValue Initial value of the string builder.
     * @return StringBuilderValueProvider with the initialValue
     */
    public static StringBuilderValueProvider of(String initialValue) {
        return new StringBuilderValueProvider(StaticValueProvider.of(initialValue));
    }

    /** Creates a {@link StringBuilderValueProvider} with the provided value provider as its initial value
     * @param initialValue Initial value provider for the string builder
     * @return StringBuilderValueProvider with the initialValue
     */
    public static StringBuilderValueProvider of(ValueProvider<String> initialValue) {
        return new StringBuilderValueProvider(initialValue);
    }

    interface Op extends Serializable {
        StringBuilder apply(StringBuilder builder);
        boolean isAccessible();
    }

    class AppendOp implements Op {
        private ValueProvider<String> value;
        public AppendOp(ValueProvider<String> value) {
            this.value = value;
        }
        @Override
        public StringBuilder apply(StringBuilder builder) {
            return builder.append(value.get());
        }

        @Override
        public boolean isAccessible() {
            return value.isAccessible();
        }
    }

    /**
     * Appends the specified string to this character sequence.
     * <p>
     * The characters of the {@code String} argument are appended, in
     * order, increasing the length of this sequence by the length of the
     * argument. If {@code str} is {@code null}, then the four
     * characters {@code "null"} are appended.
     * <p>
     * Let <i>n</i> be the length of this character sequence just prior to
     * execution of the {@code append} method. Then the character at
     * index <i>k</i> in the new character sequence is equal to the character
     * at index <i>k</i> in the old character sequence, if <i>k</i> is less
     * than <i>n</i>; otherwise, it is equal to the character at index
     * <i>k-n</i> in the argument {@code str}.
     *
     * @param   str   a string.
     * @return  a reference to this object.
     */
    public StringBuilderValueProvider append(String str) {
        return append(StaticValueProvider.of(str));
    }


    /**
     * Appends the value providers string at runtime to this character sequence.
     * <p>
     * The characters of the {@code String} argument are appended, in
     * order, increasing the length of this sequence by the length of the
     * argument. If {@code str} is {@code null}, then the four
     * characters {@code "null"} are appended.
     * <p>
     * Let <i>n</i> be the length of this character sequence just prior to
     * execution of the {@code append} method. Then the character at
     * index <i>k</i> in the new character sequence is equal to the character
     * at index <i>k</i> in the old character sequence, if <i>k</i> is less
     * than <i>n</i>; otherwise, it is equal to the character at index
     * <i>k-n</i> in the argument {@code str}.
     *
     * @param   str   a value provider of a string.
     * @return  a reference to this object.
     */
    public StringBuilderValueProvider append(ValueProvider<String> str) {
        operations.add(new AppendOp(str));
        return this;
    }

    class RemoveOp implements Op {
        int start, end;
        public RemoveOp(int start, int end) {
            this.start = start;
            this.end = end;
        }

        @Override
        public StringBuilder apply(StringBuilder builder) {
            return builder.delete(start, end);
        }
        @Override
        public boolean isAccessible() {
            return true;
        }
    }

    /**
     * Removes the characters in a substring of this sequence.
     * The substring begins at the specified {@code start} and extends to
     * the character at index {@code end - 1} or to the end of the
     * sequence if no such character exists. If
     * {@code start} is equal to {@code end}, no changes are made.
     *
     * @param      start  The beginning index, inclusive.
     * @param      end    The ending index, exclusive.
     * @return     This object.
     * @throws     StringIndexOutOfBoundsException  during beam runtime
     *             if {@code start} is negative, greater than the computed
     *             length, or greater than {@code end}.
     */
    public StringBuilderValueProvider remove(int start, int end) {
        operations.add(new RemoveOp(start, end));
        return this;
    }

    class InsertOp implements Op {
        int offset;
        ValueProvider<String> value;
        public InsertOp(int offset, ValueProvider<String> value) {
            this.offset = offset;
            this.value = value;
        }
        @Override
        public StringBuilder apply(StringBuilder builder) {
            return builder.insert(offset, value.get());
        }
        @Override
        public boolean isAccessible() {
            return value.isAccessible();
        }
    }

    /**
     * Inserts the string into this character sequence.
     * <p>
     * The characters of the {@code String} argument are inserted, in
     * order, into this sequence at the indicated offset, moving up any
     * characters originally above that position and increasing the length
     * of this sequence by the length of the argument. If
     * {@code str} is {@code null}, then the four characters
     * {@code "null"} are inserted into this sequence.
     * <p>
     * The character at index <i>k</i> in the new character sequence is
     * equal to:
     * <ul>
     * <li>the character at index <i>k</i> in the old character sequence, if
     * <i>k</i> is less than {@code offset}
     * <li>the character at index <i>k</i>{@code -offset} in the
     * argument {@code str}, if <i>k</i> is not less than
     * {@code offset} but is less than {@code offset+str.length()}
     * <li>the character at index <i>k</i>{@code -str.length()} in the
     * old character sequence, if <i>k</i> is not less than
     * {@code offset+str.length()}
     * </ul><p>
     * The {@code offset} argument must be greater than or equal to
     * {@code 0}, and less than or equal to the computed length
     * of this sequence at runtime.
     *
     * @param      offset   the offset.
     * @param      str      a string.
     * @return     a reference to this object.
     * @throws     StringIndexOutOfBoundsException  at runtime if the offset is invalid.
     */
    public StringBuilderValueProvider insert(int offset, String str) {
        return insert(offset, ValueProvider.StaticValueProvider.of(str));
    }

    /**
     * Inserts the ValueProviders string into this character sequence.
     * <p>
     * The characters of the {@code String} argument are inserted, in
     * order, into this sequence at the indicated offset, moving up any
     * characters originally above that position and increasing the length
     * of this sequence by the length of the argument. If
     * {@code str} is {@code null}, then the four characters
     * {@code "null"} are inserted into this sequence.
     * <p>
     * The character at index <i>k</i> in the new character sequence is
     * equal to:
     * <ul>
     * <li>the character at index <i>k</i> in the old character sequence, if
     * <i>k</i> is less than {@code offset}
     * <li>the character at index <i>k</i>{@code -offset} in the
     * argument {@code str}, if <i>k</i> is not less than
     * {@code offset} but is less than {@code offset+str.length()}
     * <li>the character at index <i>k</i>{@code -str.length()} in the
     * old character sequence, if <i>k</i> is not less than
     * {@code offset+str.length()}
     * </ul><p>
     * The {@code offset} argument must be greater than or equal to
     * {@code 0}, and less than or equal to the computed length
     * of this sequence at runtime.
     *
     * @param      offset   the offset.
     * @param      str      a string.
     * @return     a reference to this object.
     * @throws     StringIndexOutOfBoundsException  at runtime if the offset is invalid.
     */
    public StringBuilderValueProvider insert(int offset, ValueProvider<String> str) {
        operations.add(new InsertOp(offset, str));
        return this;
    }

    String computeValue() {
        StringBuilder builder = new StringBuilder(initialValue.get());
        for (Op op : operations) {
            builder = op.apply(builder);
        }
        return builder.toString();
    }

    @Override
    public String get() {
        if (cachedValue == null) {
            cachedValue = computeValue();
        }
        return cachedValue;
    }
    @Override
    public boolean isAccessible() {
        if (!initialValue.isAccessible()) {
            return false;
        }
        for (Op op : operations) {
            if (!op.isAccessible()) {
                return false;
            }
        }
        return true;
    }

    /** Returns the property name associated with this provider. */
    public String propertyName() {
        throw new RuntimeException(
                "Only a RuntimeValueProvider or a NestedValueProvider can supply"
                        + " a property name.");
    }

    @Override
    public String toString() {
        if (isAccessible()) {
            return String.valueOf(get());
        }
        return super.toString();
    }

    @Override
    public boolean equals(@Nullable Object other) {
        return other instanceof StringBuilderValueProvider
                && Objects.equals(initialValue, ((StringBuilderValueProvider) other).initialValue)
                && Objects.equals(operations, ((StringBuilderValueProvider) other).operations);
    }

    @Override
    public int hashCode() {
        return Objects.hash(initialValue, operations);
    }
}
