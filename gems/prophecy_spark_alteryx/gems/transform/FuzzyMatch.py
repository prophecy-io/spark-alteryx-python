import dataclasses

from prophecy.cb.server.base.ComponentBuilderBase import *
from prophecy.cb.ui.uispec import *
from pyspark.sql import *


class MatchField(ABC):
    pass


class FuzzyMatch(ComponentSpec):
    name: str = "FuzzyMatch"
    category: str = "Transform"
    gemDescription: str = "Fuzzy matching operations"

    def optimizeCode(self) -> bool:
        return True

    @dataclass(frozen=True)
    class AddMatchField(MatchField):
        columnName: Optional[str] = None
        matchFunction: str = "custom"

    @dataclass(frozen=True)
    class FuzzyMatchProperties(ComponentProperties):
        # schema: Optional[StructType] = StructType([])
        mode: str = "PURGE"
        recordIdCol: Optional[str] = None
        sourceIdCol: Optional[str] = None
        matchThresholdPercentage: int = 80
        matchFields: List[MatchField] = field(default_factory=list)
        activeTab: str = "configuration"
        includeSimilarityScore: bool = False

    def onButtonClick(self, state: Component[FuzzyMatchProperties]):
        _matchFields = state.properties.matchFields
        _matchFields.append(self.AddMatchField())
        return state.bindProperties(dataclasses.replace(state.properties, matchFields=_matchFields))

    def dialog(self) -> Dialog:
        configurations = (StackLayout(gap=("1rem"), width="70%", height=("100bh"))
        .addElement(TitleElement("Configuration"))
        .addElement(
            SelectBox("Merge/Purge Mode")
            .addOption("Purge mode (All Records Compared)", "PURGE")
            .addOption("Merge (Only Records from a Different Source are Compared)", "MERGE")
            .bindProperty("mode")
        )
        .addElement(
            Condition()
            .ifEqual(
                PropExpr("component.properties.mode"),
                StringExpr("MERGE"),
            )
            .then(
                SchemaColumnsDropdown("Source ID Field")
                .withSearchEnabled()
                # .bindSchema("schema")
                .bindSchema("component.ports.inputs[0].schema")
                .bindProperty("sourceIdCol")
                .showErrorsFor("sourceIdCol")
            )
        )
        .addElement(
            SchemaColumnsDropdown("Record ID Field")
            # .bindSchema("schema")
            .bindSchema("component.ports.inputs[0].schema")
            .bindProperty("recordIdCol")
            .showErrorsFor("recordIdCol")
        )
        .addElement(
            NumberBox("Match Threshold percentage",
                      placeholder="80",
                      minValueVar=0,
                      maxValueVar=100,
                      )
            .bindProperty("matchThresholdPercentage"),
        )
        .addElement(
            Checkbox("Include similarity score column").bindProperty(
                "includeSimilarityScore")
        )
        )
        matchFunction = (SelectBox("Match Function")
                         .addOption("Custom", "custom")
                         .addOption("Exact", "exact")
                         .addOption("Address", "address")
                         .addOption("Name", "name")
                         .addOption("Phone", "phone")
                         .bindProperty("record.AddMatchField.matchFunction")
                         )
        matchFields = StackLayout(gap=("1rem"), height=("100bh")) \
            .addElement(TitleElement("Transformations")) \
            .addElement(
            OrderedList("Match Fields")
            .bindProperty("matchFields")
            .setEmptyContainerText("Add a match field")
            .addElement(
                ColumnsLayout(("1rem"), alignY=("end"))
                .addColumn(
                    ColumnsLayout("1rem")
                    .addColumn(
                        SchemaColumnsDropdown("Field Name")
                        .bindSchema("component.ports.inputs[0].schema")
                        # .bindSchema("schema")
                        .bindProperty("record.AddMatchField.columnName")
                        , "0.6fr")
                    .addColumn(
                        matchFunction,
                        "0.4fr"
                    )
                )
                .addColumn(ListItemDelete("delete"), width="content")
            )
        ) \
            .addElement(SimpleButtonLayout("Add Match Field", self.onButtonClick))

        tabs = Tabs() \
            .bindProperty("activeTab") \
            .addTabPane(
            TabPane("Configuration", "configuration").addElement(configurations)
        ).addTabPane(
            TabPane("Match Fields", "match_fields").addElement(matchFields)
        )
        return Dialog("FuzzyMatch") \
            .addElement(
            ColumnsLayout(gap="1rem", height="100%")
            .addColumn(Ports(), "content")
            .addColumn(VerticalDivider(), width="content")
            .addColumn(
                tabs
            )
        )

    def validate(self, context: WorkflowContext, component: Component[FuzzyMatchProperties]) -> List[Diagnostic]:
        diagnostics = []
        return diagnostics

    def onChange(self, context: WorkflowContext, oldState: Component[FuzzyMatchProperties],
                 newState: Component[FuzzyMatchProperties]) -> Component[FuzzyMatchProperties]:
        return newState

    class FuzzyMatchCode(ComponentCode):
        def __init__(self, newProps):
            self.props: FuzzyMatch.FuzzyMatchProperties = newProps

        def apply(self, spark: SparkSession, in0: DataFrame) -> DataFrame:
            matchFieldsDict = {m.columnName: m.matchFunction for m in self.props.matchFields}
            if self.props.mode == "PURGE":
                from prophecy.utils.transpiler.dataframe_fcns import fuzzyPurgeMode
                out = fuzzyPurgeMode(in0, spark,
                                     recordId=self.props.recordIdCol,
                                     matchFields=matchFieldsDict,
                                     threshold=self.props.matchThresholdPercentage / 100,
                                     includeSimilarityScore=self.props.includeSimilarityScore)
            else:
                from prophecy.utils.transpiler.dataframe_fcns import fuzzyMergeMode
                return fuzzyMergeMode(in0, spark,
                                      recordId=self.props.recordIdCol,
                                      sourceId=self.props.sourceIdCol,
                                      matchFields=matchFieldsDict,
                                      threshold=self.props.matchThresholdPercentage / 100,
                                      includeSimilarityScore=self.props.includeSimilarityScore)
            return out
