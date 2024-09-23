from prophecy.cb.server.base.MetaComponentBuilderBase import *
from prophecy.cb.server.base.ComponentBuilderBase import *
from prophecy.cb.util.config import ConfigurationRecordField
from prophecy.utils import do_union
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import StructField

from prophecy.cb.server.base import WorkflowContext
from prophecy.cb.server.base.datatypes import SInt
from prophecy.cb.ui.uispec import *
from prophecy.config import ConfigBase
from typing import *
from prophecy.cb.util.Utils import generate_element_id
import builtins, dataclasses


class WhileIterator(MetaComponentSpec):
    name: str = "WhileIterator"
    category: str = "CustomSubgraph"
    gemDescription: str = "This subgraph will process the dataframe, looping the results back through the workflow until the input dataframe becomes empty or max iterations are reached."

    def optimizeCode(self) -> bool:
        # Return whether code optimization is enabled for this component
        return True

    @dataclass(frozen=True)
    class WhileIteratorProperties(MetaComponentProperties):
        # properties for the component with default values
        maxIteration: SInt = SInt("5")
        iterationNumberVariableName: str = "iteration_number"
        populateIterationNumber: Optional[bool] = True
        schema: Optional[StructType] = StructType([])
        configVariableNames: Optional[List[str]] = field(default_factory=list)

    def dialog(self) -> Dialog:
        # Define the UI dialog structure for the component
        disabledConfigSelectBox = SchemaColumnsDropdown("Select config variable name to populate iteration number").withMultipleSelection().withDisabled().bindSchema("schema").bindProperty("configVariableNames")
        enabledConfigSelectBox = SchemaColumnsDropdown("Select config variable name to populate iteration number").withMultipleSelection().bindSchema("schema").bindProperty("configVariableNames")
        elementWithIterationNumber = StackLayout(height="100%", gap="2rem").addElement(ExpressionBox("Max Iterations").bindPlaceholder("1000").bindProperty("maxIteration").withFrontEndLanguage()).addElement(Checkbox("Populate iteration number in config variable", "populateIterationNumber")).addElement(enabledConfigSelectBox)
        elementWithoutIterationNumber = StackLayout(height="100%", gap="2rem").addElement(ExpressionBox("Max Iterations").bindPlaceholder("1000").bindProperty("maxIteration").withFrontEndLanguage()).addElement(Checkbox("Populate iteration number in config variable", "populateIterationNumber")).addElement(disabledConfigSelectBox)
        propertiesSection = Condition().ifEqual(PropExpr("component.properties.populateIterationNumber"), BooleanExpr(False)).then(elementWithoutIterationNumber).otherwise(elementWithIterationNumber)
        return (Dialog("WhileIterator", footer=SubgraphDialogFooter())
            .addElement(
                ColumnsLayout(gap="1rem", height="100%")
                .addColumn(Ports(minInputPorts=1, allowOutputRename=True, allowOutputAddOrDelete=True).editableInput(True), "content")
                .addColumn(
                    Tabs()
                    .addTabPane(
                        TabPane("Settings", "Settings")
                        .addElement(propertiesSection)
                    )
                    .addTabPane(
                        TabPane("Configuration", "Configuration")
                        .addElement(
                            SubgraphConfigurationTabs()
                        )
                    ),
                    "3fr"
                )
            )
         )

    def validate(self, context: WorkflowContext, component: MetaComponent[WhileIteratorProperties]) -> List[Diagnostic]:
        # Validate the component's state
        diagnostics = []
        maxIterationDiagMsg = "MaxIteration has to be an integer > 0"
        if component.properties.maxIteration.diagnosticMessages is not None and len(
                component.properties.maxIteration.diagnosticMessages) > 0:
            for message in component.properties.maxIteration.diagnosticMessages:
                diagnostics.append(Diagnostic("properties.maxIteration", message, SeverityLevelEnum.Error))
        else:
            resolved = component.properties.maxIteration.value
            if resolved < 0:
                diagnostics.append(Diagnostic("properties.maxIteration", maxIterationDiagMsg, SeverityLevelEnum.Error))
        if len(component.ports.inputs) > 0:
            if component.ports.inputs[0].isStreaming:
                diagnostics.append(
                    Diagnostic(
                        f"ports",
                        "Cannot iterate on a streaming dataframe. Please connect the first input to a batch Source instead of a streaming Source.",
                        SeverityLevelEnum.Error
                    )
                )
        if (len(component.ports.inputs) != len(component.subgraph_ports.inputs)):
            diagnostics.append(
                Diagnostic(
                    f"ports",
                    f"Output ports of Whileterator's input component should be equal to input ports. Expected: {len(component.ports.inputs)} ports. Found: {len(component.subgraph_ports.inputs)} ports",
                    SeverityLevelEnum.Error
                )
            )
        if (len(component.ports.outputs) != len(component.subgraph_ports.outputs)):
            diagnostics.append(
                Diagnostic(
                    f"ports",
                    f"Count of input and output ports of WhileIterator's Output component should be same. Input port count::{len(component.subgraph_ports.outputs)} Output port count:: {len(component.ports.outputs)}",
                    SeverityLevelEnum.Error
                )
            )
        if component.properties.iterationNumberVariableName is None or len(component.properties.iterationNumberVariableName) == 0:
            diagnostics.append(Diagnostic("properties.iterationNumberVariableName", "Please provide a valid variable name for iteration number", SeverityLevelEnum.Error))
        return diagnostics

    def onChange(self, context: WorkflowContext, oldState: MetaComponent[WhileIteratorProperties], newState: MetaComponent[WhileIteratorProperties]) -> MetaComponent[
    WhileIteratorProperties]:
        # Handle changes in the component's state and return the new state
        populateConfigFlag = True
        if newState.properties.populateIterationNumber == False:
            populateConfigFlag = False
        availableConfigFieldNames = context.config_context.get_field_names()
        lastSelectedVariable = None
        if len(newState.properties.configVariableNames) > 0:
            lastSelectedVariable = newState.properties.configVariableNames[-1]

        newProps = newState.properties
        configAsSchema = [StructField(item, StringType(), True) for item in availableConfigFieldNames]
        return newState.bindProperties(dataclasses.replace(newProps, schema=StructType(configAsSchema), configVariableNames=[lastSelectedVariable], iterationNumberVariableName=lastSelectedVariable, populateIterationNumber=populateConfigFlag))


    class WhileIteratorCode(MetaComponentCode):
        def __init__(self, newProps, config):
            self.props: WhileIterator.WhileIteratorProperties = newProps
            self.config: ConfigBase = config

        def __run__(self, spark: SparkSession, config: ConfigBase, in0: DataFrame, *inDFs: DataFrame) -> (DataFrame, DataFrame, List[DataFrame]):
            pass

        def apply(self, spark: SparkSession, in0: DataFrame, *inDFs: DataFrame) -> (DataFrame, DataFrame, List[DataFrame]):
            # This method contains logic used to generate the spark code from the given inputs.
            max_iteration_limit = self.props.maxIteration
            variableName = self.props.iterationNumberVariableName
            updateConfigVariable = self.props.populateIterationNumber

            def updated_config(config, iteration_number):
                if updateConfigVariable == False:
                    return
                import copy
                newConfig: SubstituteDisabled = copy.deepcopy(config)
                newConfig.update_all(variableName, iteration_number)
                return newConfig

            def is_schema_subset(df1: DataFrame, df2: DataFrame) -> bool:
                normalized_schema1:SubstituteDisabled = StructType([StructField(field.name.lower(), field.dataType, field.nullable) for field in df1.schema])
                normalized_schema2:SubstituteDisabled = StructType([StructField(field.name.lower(), field.dataType, field.nullable) for field in df2.schema])
                return all(field in normalized_schema2 for field in normalized_schema1)


            def recursive_eval(input_df: DataFrame, rest_dfs: List[DataFrame], max_iterations: int) -> List[DataFrame]:
                import copy
                remaining_iterations = max_iterations - 1
                iteration_number = max_iteration_limit - max_iterations + 1

                if max_iterations == 0:
                    empty_rest_dfs1: SubstituteDisabled = [df.limit(0) for df in rest_dfs]
                    output1: SubstituteDisabled = self.__run__(spark, updated_config(self.config, iteration_number), input_df.limit(0), *empty_rest_dfs1)
                    return tuple([input_df] + list(output1[1:]))
                if input_df.rdd.isEmpty():
                    empty_rest_dfs2: SubstituteDisabled = [df.limit(0) for df in rest_dfs]
                    output2: SubstituteDisabled = self.__run__(spark, updated_config(self.config, iteration_number), input_df.limit(0), *empty_rest_dfs2)
                    return tuple([input_df] + list(output2[1:]))
                
                output: SubstituteDisabled = self.__run__(spark, updated_config(self.config, iteration_number), input_df, *rest_dfs)
                recursive_output: SubstituteDisabled = recursive_eval(output[0], rest_dfs, remaining_iterations)

                # Find remaining rows
                remaining_df: SubstituteDisabled = recursive_output[0]

                # Find output for iterative macro run
                output_dataframes: SubstituteDisabled = []

                # Iterate through both lists and union each pair of DataFrames
                for df_a, df_b in zip(output[1:], recursive_output[1:]):
                    union_df: SubstituteDisabled = df_a.unionByName(df_b)
                    output_dataframes.append(union_df)

                return tuple([remaining_df] + output_dataframes)
            ans:SubstituteDisabled = recursive_eval(in0, list(inDFs), self.props.maxIteration)
            assert is_schema_subset(in0, ans[0]), "Schema of in0 must be a subset of out0 for WhileIterator"
            return (ans[0], ans[1], ans[2:])