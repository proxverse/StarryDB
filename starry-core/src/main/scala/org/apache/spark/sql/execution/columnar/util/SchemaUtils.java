package org.apache.spark.sql.execution.columnar.util;

import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.CDataDictionaryProvider;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.util.ArrowUtils;

public final class SchemaUtils {

    // todo release memory
    public static long toArrowSchema(Schema schema) {
        RootAllocator allocator = ArrowUtils.rootAllocator();
        ArrowSchema arrowSchema = ArrowSchema.allocateNew(allocator);
        exportSchema(allocator, schema, arrowSchema);
        return arrowSchema.memoryAddress();
    }

    public static void exportSchema(BufferAllocator allocator, Schema schema, ArrowSchema out) {
        try (CDataDictionaryProvider dictProvider = new CDataDictionaryProvider()) {
            Data.exportSchema(allocator, schema, dictProvider, out);
        }
    }
}
