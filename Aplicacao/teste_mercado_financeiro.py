from gym_framework.extractors.extractor import CSV_Extractor
from gym_framework.core.dataframe import Dataframe
from gym_framework.handlers.handler import NormalizerHandler, ColumnSelectorHandler, SinkHandler
from gym_framework.handlers.base_handler import HandlerNode
from gym_framework.core.pipeline import PipelineExecutor

output = {}

# Etapas
extractor = CSV_Extractor("mock_score.csv")
normalizer = NormalizerHandler()
selector = ColumnSelectorHandler(["cpf", "score"])
sink = SinkHandler(output_dict=output)

# Encadeamento dos nós
node1 = HandlerNode(name="Extractor", handler=extractor)
node2 = HandlerNode(name="Normalizer", handler=normalizer, dependencies=[node1])
node3 = HandlerNode(name="Selector", handler=selector, dependencies=[node2])
node4 = HandlerNode(name="Sink", handler=sink, dependencies=[node3])

# Pipeline
pipeline = PipelineExecutor(nodes=[node1, node2, node3, node4])
pipeline.run()

print(Dataframe(data = output))