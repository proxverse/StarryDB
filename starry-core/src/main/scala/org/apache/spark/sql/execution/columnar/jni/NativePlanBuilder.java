package org.apache.spark.sql.execution.columnar.jni;

import org.apache.spark.sql.catalyst.expressions.SortOrder;
import org.apache.spark.sql.execution.columnar.util.PlanUtils;
import org.apache.spark.sql.execution.columnar.extension.plan.VeloxTypeResolver;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;

public class NativePlanBuilder extends NativeClass {
  public NativePlanBuilder() {
    setHandle(nativeCreate());
  }

  private native void nativeJavaScan(String schema);

  private native void nativeFilter(long condition);

  private native void nativeProject(String[] alias, long[] exprs);


  private native void nativeShuffledHashJoin(String joinType,
                                             boolean nullAware,
                                             long[] leftKeys,
                                             long[] rightKeys,
                                             long filter,
                                             String rightPlan,
                                             String output);


  private native void nativeAggregation(String step,
                                        long[] group,
                                        String[] aggNames,
                                        String[] agg,
                                        boolean ignoreNullKey);

  private native void nativeExpand(long[][] projects,
                                   String[] alias);


  private native String nativeBuildAggregate(String functionName,
                                             String[] inputs,
                                             String[] rawInputs,
                                             String step,
                                             String mask,
                                             String[] sortingKeys,
                                             String[] sortOrders,
                                             boolean distinct,
                                             boolean useMergeFunc);

  private native String nativeResolveAggType(String functionName,
                                             long[] argsType,
                                             String step);

  private native String nativeAggregationFunction(String functionName,
                                                  String[] argsType,
                                                  boolean needResolve,
                                                  String returnType);


  private native String nativeBuilder();

  protected native long nativeCreate();

  private native void nativeRelease();

  private native String nativeNodeId();


  public native void nativeTestString(String test);


  private native String nativeWindowFunction(String functionCallJson,
                                             String frameJson,
                                             boolean ignoreNulls);

  native void nativeWindow(String[] partitionKeys,
                           String[] sortingKeys,
                           String[] sortingOrders,
                           String[] windowColumnNames,
                           String[] windowFunctions,
                           boolean inputsSorted);

  native void nativeSort(
      String[] sortingKeys,
      String[] sortingOrders,
      boolean isPartial);

  private native void nativeUnnest(String[] replicateVariables,
                                   String[] unnestVariables,
                                   String[] unnestNames,
                                   String ordinalityName);

  private native void nativeLimit(int offset, int limit);


  @Override
  protected void releaseInternal() {
    nativeRelease();
  }


  // for native call , don't delete

  public NativePlanBuilder scan(StructType schema) {
    nativeJavaScan(schema.catalogString());
    return this;
  }


  public NativePlanBuilder project(String[] alias, long[] exprs) {
    nativeProject(alias, exprs);
    return this;
  }

  public NativePlanBuilder filter(long condition) {
    nativeFilter(condition);
    return this;
  }

  public NativePlanBuilder limit(int offset, int limit) {
    nativeLimit(offset, limit);
    return this;
  }


  public NativePlanBuilder expand(long[][] project, String[] alias) {
    nativeExpand(project, alias);
    return this;
  }

  public NativePlanBuilder join(String joinType, boolean nullAware, long[] leftKeys, long[] rightKeys, long filter, String rightPlan, String output) {
    nativeShuffledHashJoin(joinType, nullAware, leftKeys, rightKeys, filter, rightPlan, output);
    return this;
  }

  public String buildAggregate(String functionName,
                               String[] inputs,
                               String[] rawInputs,
                               String step,
                               String mask,
                               String[] sortingKeys,
                               String[] sortOrders,
                               boolean distinct,
                               boolean useMergeFunc) {
    return nativeBuildAggregate(functionName, inputs, rawInputs, step, mask, sortingKeys, sortOrders, distinct, useMergeFunc);
  }


  public String buildAggregateFunction(String functionName,
                                       String[] argsType,
                                       boolean needResolve,
                                       String returnType) {
    return nativeAggregationFunction(functionName, argsType, needResolve, returnType);
  }

  public DataType resolveAggType(String functionName,
                                 long[] argsType,
                                 String step) {
    return VeloxTypeResolver.parseDataType(nativeResolveAggType(functionName, argsType, step));
  }


  public void aggregate(String step,
                        long[] group,
                        String[] aggNames,
                        String[] agg,
                        boolean ignoreNullKey) {

    nativeAggregation(step, group, aggNames, agg, ignoreNullKey);
  }


  public String windowFunction(String functionCallJson,
                               String frameJson,
                               boolean ignoreNulls) {
    return nativeWindowFunction(functionCallJson, frameJson, ignoreNulls);
  }


  public void window(String[] partitionKeys,
                     String[] sortingKeys,
                     SortOrder[] sortingOrders,
                     String[] windowColumnNames,
                     String[] windowFunctions,
                     boolean inputsSorted) {
    String[] sortingOrders1 = PlanUtils.jsonChildren(sortingOrders);
    nativeWindow(partitionKeys, sortingKeys, sortingOrders1, windowColumnNames, windowFunctions, inputsSorted);
  }

  public void sort(String[] sortingKeys,
                   SortOrder[] sortingOrders,
                   boolean isPartial) {
    String[] sortingOrders1 = PlanUtils.jsonChildren(sortingOrders);
    nativeSort(sortingKeys, sortingOrders1, isPartial);
  }

  public void unnest(String[] replicateVariables,
                     String[] unnestVariables,
                     String[] unnestNames,
                     String ordinalityName) {
    nativeUnnest(replicateVariables, unnestVariables, unnestNames, ordinalityName);
  }


  public String builderAndRelease() {
    String s = nativeBuilder();
    close();
    return s;
  }

  public String nodeId() {
    return nativeNodeId();
  }

  public String builder() {
    return nativeBuilder();
  }


}
