//package com.mybit.matching.core.dump;
//
//
//import java.io.IOException;
//
///**
// * An abstraction between an underlying input stream and record iterators, a {@link LogInputStream} only returns
// * the batches at one level. For magic values 0 and 1, this means that it can either handle  iteration
// * at the top level of the log or deep iteration within the payload of a single message, but it does not attempt
// * to handle both. For magic value 2, this is only used for iterating over the top-level record batches (inner
// * records do not follow the {@link RecordBatch} interface.
// *
// * The generic typing allows for implementations which present only a view of the log entries, which enables more
// * efficient iteration when the record data is not actually needed. See for example
// * {@link FileLogInputStream.FileChannelRecordBatch} in which the record is not brought into memory until needed.
// *
// * @param <T> Type parameter of the log entry
// */
//interface LogInputStream<T extends RecordBatch> {
//
//    /**
//     * Get the next record batch from the underlying input stream.
//     *
//     * @return The next record batch or null if there is none
//     * @throws IOException for any IO errors
//     */
//    T nextBatch() throws IOException;
//}
