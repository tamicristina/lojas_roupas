db = db.getSiblingDB("loja_roupas");
db.relatorios.insertOne({
    data: "2025-01-01",
    feedback: "Inicialização OK",
    vendas_total: 0,
    produto_mais_vendido: "Nenhum"
});
