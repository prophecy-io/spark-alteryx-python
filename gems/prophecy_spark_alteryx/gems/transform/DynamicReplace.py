from pyspark.sql import *
from pyspark.sql.functions import *
import dataclasses
from prophecy.cb.server.base.ComponentBuilderBase import *
from prophecy.cb.ui.UISpecUtil import *
from prophecy.cb.ui.uispec import *
from pyspark.sql.types import StructType, ArrayType
from typing import Optional, List
from dataclasses import dataclass, field
from prophecy.cb.server.base import WorkflowContext

class DynamicReplace(ComponentSpec):
    name: str = "DynamicReplace"
    category: str = "Transform"
    gemDescription: str = "Replace a column in a dataframe based on conditions from another dataframe"
    docUrl: str = "https://docs.prophecy.io/low-code-spark/gems/transform/dynamic-replace"

    def optimizeCode(self) -> bool:
        return True

    @dataclass(frozen=True)
    class DynamicReplaceProperties(ComponentProperties):
        fieldNameFieldColumn: str = ""
        booleanExpressionFieldColumn: str = ""
        outputValueFieldColumn: str = ""
        valuesAreExpressions: bool = False

    def dialog(self) -> Dialog:
        return Dialog("DynamicReplace").addElement(
            ColumnsLayout(gap="1rem", height="100%")
            .addColumn(Ports(), "content")
            .addColumn(
                StackLayout(height="100%")
                .addElement(TextBox("Field Name Field").bindProperty("fieldNameFieldColumn"))
                .addElement(TextBox("Boolean Expression Field").bindProperty("booleanExpressionFieldColumn"))
                .addElement(TextBox("Output Value Field").bindProperty("outputValueFieldColumn"))
                .addElement(Checkbox("Values are Expressions").bindProperty("valuesAreExpressions")),
                "5fr"
            )
        )

    def validate(self, context: WorkflowContext, component: Component[DynamicReplaceProperties]) -> List[Diagnostic]:
        diagnostics = []

        if len(component.properties.fieldNameFieldColumn) == 0:
            diagnostics.append(
                Diagnostic(
                    "properties.fieldNameFieldColumn",
                    "Field Name Field cannot be empty.",
                    SeverityLevelEnum.Error,
                )
            )

        if len(component.properties.booleanExpressionFieldColumn) == 0:
            diagnostics.append(
                Diagnostic(
                    "properties.booleanExpressionFieldColumn",
                    "Boolean Expression Field cannot be empty.",
                    SeverityLevelEnum.Error,
                )
            )

        if len(component.properties.outputValueFieldColumn) == 0:
            diagnostics.append(
                Diagnostic(
                    "properties.outputValueFieldColumn",
                    "Output Value Field cannot be empty.",
                    SeverityLevelEnum.Error,
                )
            )

        return diagnostics

    def onChange(self, context: WorkflowContext, oldState: Component[DynamicReplaceProperties], newState: Component[DynamicReplaceProperties]) -> Component[
        DynamicReplaceProperties]:
        return newState
    class DynamicReplaceCode(ComponentCode):
        def __init__(self, newProps):
            self.props: DynamicReplace.DynamicReplaceProperties = newProps

        def apply(self, spark: SparkSession, in0: DataFrame, in1: DataFrame) -> DataFrame:
            from prophecy.utils import ProphecyDataFrame
            in1WithId = in1.withColumn("__rowId", monotonically_increasing_id().cast("integer")).withColumn(self.props.booleanExpressionFieldColumn, call_spark_fcn("convertAlteryxToSparkSql", col(self.props.booleanExpressionFieldColumn)))

            if (self.props.valuesAreExpressions):
                return ProphecyDataFrame(in0, spark).dynamicReplaceExpr(in1WithId._jdf, "__rowId", self.props.fieldNameFieldColumn, self.props.booleanExpressionFieldColumn, self.props.outputValueFieldColumn, spark)
            else:
                return ProphecyDataFrame(in0, spark).dynamicReplace(in1WithId._jdf, "__rowId", self.props.fieldNameFieldColumn, self.props.booleanExpressionFieldColumn, self.props.outputValueFieldColumn, spark)
