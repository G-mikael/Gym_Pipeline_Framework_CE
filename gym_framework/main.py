from gym_framework.handlers.base_handler import HandlerNode
from gym_framework.handlers.producer import ScoreCSVProducerHandler, ClientsDBProducerHandler, TransactionsDBProducerHandler, NewTransactionsTXTProducerHandler
from gym_framework.handlers.handler import NormalizerHandler, ClassifierHandler, RiskTransactionClassifierHandler, LoaderHandler, SaveToFileHandler, CalculateAverageGainHandler
from gym_framework.core.pipeline import PipelineExecutor
from gym_framework.core.train_riskClassifierModel import treinar_e_salvar_modelo

if __name__ == "__main__":
    # Treinamento antes do pipeline
    print("🔁 Treinando o modelo de risco com dados rotulados...")
    treinar_e_salvar_modelo("gym_framework/tests/mock/transacoes_rotuladas.txt")  

    print("Iniciando pipeline...")

    score_produto_node = HandlerNode("ScoreCSVProducerHandler", ScoreCSVProducerHandler())
    client_produto_node = HandlerNode("ClientsDBProducerHandler", ClientsDBProducerHandler())
    transactions_produto_node = HandlerNode("TransactionsDBProducerHandler", TransactionsDBProducerHandler())
    new_transactions_produto_node = HandlerNode("NewTransactionsTXTProducerHandler", NewTransactionsTXTProducerHandler())


    transformador_node = HandlerNode("NormalizerNode", NormalizerHandler(), dependencies=[client_produto_node])
    loader_node = HandlerNode("LoaderNode", LoaderHandler(), dependencies=[transformador_node])
    classifier_node = HandlerNode("ClassifierHandler", ClassifierHandler(), dependencies=[new_transactions_produto_node])
    risk_classifier_node = HandlerNode("RiskClassifierHandler", RiskTransactionClassifierHandler(), dependencies=[new_transactions_produto_node])
    save_node = HandlerNode("SaveToFileHandler", SaveToFileHandler(), dependencies=[classifier_node])
    calculete_node = HandlerNode("CalculateAverageGainHandler", CalculateAverageGainHandler(), dependencies=[classifier_node])


    pipeline = PipelineExecutor([score_produto_node, client_produto_node, transactions_produto_node, new_transactions_produto_node],
                                [transformador_node, loader_node, classifier_node, risk_classifier_node, save_node, calculete_node])
    pipeline.start()

    print("Pipeline finalizado.")